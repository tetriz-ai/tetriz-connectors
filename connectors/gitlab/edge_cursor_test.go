// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestEdge_CursorEntryFromAnOlderShapeWithExtraFields covers a cursor written by a
// version that also stored a page number next to the timestamp. The extra field
// must be ignored and the timestamp kept, not the entry dropped (which would
// re-walk the whole lookback for that project).
func TestEdge_CursorEntryFromAnOlderShapeWithExtraFields(t *testing.T) {
	ctx := context.Background()
	raw := json.RawMessage(`{"ts":"2026-05-21T05:00:00.25+02:00","page":3,"stuck_at":"mr-1!2"}`)
	state, ok := decodeCursorStateEntry(ctx, "100", raw)
	require.True(t, ok)
	want := time.Date(2026, 5, 21, 3, 0, 0, 250_000_000, time.UTC)
	assert.True(t, want.Equal(state.Timestamp), "got %s", state.Timestamp)

	_, ok = decodeCursorStateEntry(ctx, "100", json.RawMessage(`{"ts":null}`))
	assert.False(t, ok, "a null timestamp is no state, never the zero time used as a since")
}

// TestEdge_MRTimestampWithOffsetAndFractionDrivesAMonotonicCursor covers an MR whose
// updated_at carries a non-UTC offset and sub-second precision. The stored high-water
// mark must be the same instant (in UTC), and the next run's updated_after, which is
// rendered at second precision, must not land AFTER that mark: GitLab treats
// updated_after as inclusive, so an earlier floor re-reads the boundary while a later
// one would skip an MR updated in the same second.
func TestEdge_MRTimestampWithOffsetAndFractionDrivesAMonotonicCursor(t *testing.T) {
	zone := time.FixedZone("UTC+2", 2*3600)
	updated := time.Now().In(zone).Add(-time.Hour).Truncate(time.Second).Add(987 * time.Millisecond)

	var updatedAfterSeen []string
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{{ID: 300, PathWithNamespace: "acme/platform/backend/api"}}), nil
			case *connectorpb.GitLabListProjectMergeRequestsRequest:
				updatedAfterSeen = append(updatedAfterSeen, r.GetUpdatedAfter())
				raw := fmt.Sprintf(`{"id":1,"iid":5,"project_id":300,"created_at":%q,"updated_at":%q,"state":"opened","title":"🚀 ünïcode","source_branch":"feature/ü"}`,
					updated.Add(-time.Hour).Format(time.RFC3339Nano), updated.Format(time.RFC3339Nano))
				return &connectorpb.GitLabListProjectMergeRequestsResponse{MergeRequests: [][]byte{[]byte(raw)}}, nil
			case *connectorpb.GitLabListMRDiffsRequest:
				return glDiffsResponse(nil), nil
			case *connectorpb.GitLabListMRCommitsRequest:
				return glMRCommitsResponse(nil), nil
			default:
				return nil, fmt.Errorf("unexpected request %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	cursor, err := (&FetchPRsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)
	state := getRepoCursorState(context.Background(), cursor, "300")
	assert.True(t, updated.Equal(state.Timestamp), "high-water %s must equal the MR's instant %s", state.Timestamp, updated)
	assert.Equal(t, time.UTC, state.Timestamp.Location())

	records := sink.Records()
	require.NotEmpty(t, records)
	assert.Equal(t, "feature/ü", records[0].Metadata["head_branch"])
	assert.Equal(t, "acme/platform/backend/api", records[0].Metadata["repo"], "a nested subgroup path is kept whole")

	_, err = (&FetchPRsOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, cursor))
	require.NoError(t, err)
	require.Len(t, updatedAfterSeen, 2)
	floor, err := time.Parse(time.RFC3339, updatedAfterSeen[1])
	require.NoError(t, err, "updated_after must be RFC3339")
	assert.False(t, floor.After(updated), "updated_after %s must not pass the high-water %s", floor, updated)
	assert.Less(t, updated.Sub(floor), time.Second, "and must not fall a whole second or more behind it")
}

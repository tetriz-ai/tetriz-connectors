// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// When every repository fails, fetch_issues_data fails the run with the first
// repository's error, on a first run and on a later one.
func TestFetchIssuesDataOp_EveryRepositoryFailingFailsTheRun(t *testing.T) {
	authErr := &sdk.SyncError{Kind: sdk.ErrKindAuth, Terminal: true, Cause: assert.AnError}
	gw := &mockVendorGateway{executeFunc: func(context.Context, proto.Message) (proto.Message, error) { return nil, authErr }}
	for _, tc := range []struct {
		name   string
		cursor *sdk.Cursor
	}{
		{"first run", nil},
		{"later run", buildStateCursor(map[string]repoCursorState{"acme/widgets": {Timestamp: time.Now().Add(-time.Hour)}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := testParams(testkit.NewChannelSink(), gw, tc.cursor)
			s3, _ := newRecordingS3(t)
			params.BlobStore = s3
			params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/widgets", "acme/api"}})
			cur, err := (&FetchIssuesDataOp{}).Execute(context.Background(), params)
			var syncErr *sdk.SyncError
			require.ErrorAs(t, err, &syncErr, "fetch_issues_data reported success; cursor=%v", cur)
			assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
			assert.True(t, syncErr.Terminal)
		})
	}
}

// One failing repository of two is skipped: the other repository still syncs
// and the run completes.
func TestFetchIssuesDataOp_OneFailingRepositoryOfTwoIsSkipped(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if strings.Contains(req.(*connectorpb.GitHubGraphQLRequest).GetQuery(), `"api"`) {
			return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, HTTPStatus: 502, Cause: assert.AnError}
		}
		return fetchIssuesDataResponseRaw(777, []ghIssueDescription{{Number: 42, UpdatedAt: now, Body: "body"}}, false, ""), nil
	}}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	s3, _ := newRecordingS3(t)
	params.BlobStore = s3
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/api", "acme/widgets"}})
	cursor, err := (&FetchIssuesDataOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	records := sink.Drain()
	require.Len(t, records, 1)
	assert.Equal(t, "acme/widgets#I-42", records[0].ExternalID)
}

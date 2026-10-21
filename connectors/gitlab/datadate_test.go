// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// The DataDate rule (sdk.RawRecord): a current-state record has no DataDate,
// an event record is dated by its event. DataDate is part of the dedup key, so
// a fetch-time or created-at date on a snapshot forks or misdates its row.

func byExternalID(recs []*sdk.RawRecord) map[string]*sdk.RawRecord {
	out := make(map[string]*sdk.RawRecord, len(recs))
	for _, r := range recs {
		out[r.ExternalID] = r
	}
	return out
}

func TestDataDate_MRSnapshotUndatedEventsDatedByEvent(t *testing.T) {
	created := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return glProjectsResponse([]glProject{{ID: 100, PathWithNamespace: "g/p", Name: "p"}}), nil
		case *connectorpb.GitLabListProjectMergeRequestsRequest:
			return glMRsResponse([]glMergeRequest{{ID: 1, IID: 7, ProjectID: 100, CreatedAt: created, UpdatedAt: merged, MergedAt: &merged, State: "merged"}}), nil
		case *connectorpb.GitLabListMRDiffsRequest:
			return glDiffsResponse(nil), nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchPRsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	recs := byExternalID(sink.Records())
	require.Contains(t, recs, "mr-100!7#snapshot")
	assert.True(t, recs["mr-100!7#snapshot"].DataDate.IsZero(), "#snapshot is current state")
	assert.True(t, recs["mr-100!7#opened"].DataDate.Equal(created))
	assert.True(t, recs["mr-100!7#merged"].DataDate.Equal(merged))
}

func TestDataDate_IssueSnapshotUndated(t *testing.T) {
	created := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if _, ok := req.(*connectorpb.GitLabListIssuesRequest); ok {
			return &connectorpb.GitLabListIssuesResponse{Issues: [][]byte{mustJSON(glIssue{ID: 1, IID: 3, ProjectID: 100, CreatedAt: created, UpdatedAt: created, State: "opened"})}}, nil
		}
		return nil, fmt.Errorf("unexpected request %T", req)
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchIssuesOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)

	recs := byExternalID(sink.Records())
	require.Contains(t, recs, "issue-100!3#snapshot")
	assert.True(t, recs["issue-100!3#snapshot"].DataDate.IsZero(), "#snapshot is current state")
	assert.True(t, recs["issue-100!3#opened"].DataDate.Equal(created))
}

func TestDataDate_ProjectsAndMembersUndated(t *testing.T) {
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return glProjectsResponse([]glProject{{ID: 100, PathWithNamespace: "g/p", Name: "p", CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}}), nil
		case *connectorpb.GitLabListGroupMembersRequest:
			return &connectorpb.GitLabListGroupMembersResponse{Members: [][]byte{[]byte(`{"id":9,"username":"dev","email":"dev@example.com"}`)}}, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	for _, op := range []sdk.APIFetchOperation{&FetchProjectsOp{}, &FetchGroupMembersOp{}} {
		sink := testkit.NewChannelSink()
		_, err := op.Execute(context.Background(), testParams(sink, gw, nil))
		require.NoError(t, err, op.OperationID())
		recs := sink.Records()
		require.NotEmpty(t, recs, op.OperationID())
		for _, r := range recs {
			assert.True(t, r.DataDate.IsZero(), "%s %s is current state, got DataDate %v", op.OperationID(), r.ExternalID, r.DataDate)
		}
	}
}

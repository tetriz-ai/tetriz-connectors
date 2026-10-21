// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestSplitFullName_RejectsEmptySegments: a full name is "workspace/repo_slug" and
// neither half may be empty. "ws/" or "/repo" in settings.repositories is a typo,
// and accepting it builds a request for /repositories/ws/ or /repositories//repo.
func TestSplitFullName_RejectsEmptySegments(t *testing.T) {
	for _, in := range []string{"ws/", "/repo", "/"} {
		t.Run(in, func(t *testing.T) {
			ws, slug, err := splitFullName(in)
			assert.Error(t, err, "splitFullName(%q) = (%q, %q, nil); want an error", in, ws, slug)
		})
	}
}

func activityGateway(activities ...string) *mockVendorGateway {
	return &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			raw := make([][]byte, len(activities))
			for i, a := range activities {
				raw[i] = []byte(a)
			}
			return &connectorpb.BitbucketListPRActivityResponse{Activities: raw}, nil
		},
	}
}

// TestFetchPRActivity_ApprovalsWithoutAccountIDStayDistinct: Bitbucket omits
// account_id for some users (former users, app/bot users) but always carries a
// uuid. Two such users approving the same PR are two approvals; keying the record
// on account_id alone collapses them into one external id, and the downstream
// upsert keeps only one.
func TestFetchPRActivity_ApprovalsWithoutAccountIDStayDistinct(t *testing.T) {
	gw := activityGateway(
		`{"approval":{"date":"2024-05-01T10:00:00.123456+00:00","user":{"uuid":"{11111111-aaaa}","display_name":"Former user"}}}`,
		`{"approval":{"date":"2024-05-01T11:00:00.000000+00:00","user":{"uuid":"{22222222-bbbb}","display_name":"Former user"}}}`,
	)
	sink := testkit.NewChannelSink()
	_, err := fetchPRActivity(context.Background(), gw, sink, "fetch_reviews", "org-test", "ws", "repo", "ws/repo",
		bbPRInfo{ID: 7}, time.Time{}, &pacing.PacingBudget{})
	require.NoError(t, err)
	recs := sink.Records()
	require.Len(t, recs, 2)
	assert.NotEqual(t, recs[0].ExternalID, recs[1].ExternalID,
		"two distinct approvers must not share an external id (got %q)", recs[0].ExternalID)
}

// TestFetchPRActivity_FractionalOffsetApprovalDate: Bitbucket approval dates carry
// microseconds and a numeric offset. The record is dated at that instant (not at
// "now", the unparseable-date fallback) and the returned watermark is that instant.
func TestFetchPRActivity_FractionalOffsetApprovalDate(t *testing.T) {
	gw := activityGateway(
		`{"approval":{"date":"2024-05-01T12:00:00.123456+02:00","user":{"account_id":"acc-1","uuid":"{u1}"}}}`,
	)
	sink := testkit.NewChannelSink()
	latest, err := fetchPRActivity(context.Background(), gw, sink, "fetch_reviews", "org-test", "ws", "repo", "ws/repo",
		bbPRInfo{ID: 7}, time.Time{}, &pacing.PacingBudget{})
	require.NoError(t, err)
	want := time.Date(2024, 5, 1, 10, 0, 0, 123456000, time.UTC)
	assert.True(t, latest.Equal(want), "watermark = %v, want %v", latest, want)
	recs := sink.Records()
	require.Len(t, recs, 1)
	assert.True(t, recs[0].DataDate.Equal(want), "data date = %v, want %v", recs[0].DataDate, want)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(recs[0].Payload, &payload))
	assert.Equal(t, "2024-05-01T10:00:00Z", payload["created_on"])
}

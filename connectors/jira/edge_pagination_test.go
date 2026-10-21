// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// stuckTokenGateway answers every search with the same non-empty page and the
// same nextPageToken — a vendor pager that never advances. After a hard cap it
// errors so a test of a loop with no guard ends instead of hanging.
func stuckTokenGateway(calls *atomic.Int32) *mockVendorGateway {
	issue := jiraIssue{ID: "10001", Key: "PROJ-1", Fields: jiraIssueFields{
		Updated: jiraTime{time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)},
		Created: jiraTime{time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)},
	}}
	return &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if _, ok := req.(*connectorpb.JiraSearchIssuesRequest); !ok {
			return nil, fmt.Errorf("unexpected request %T", req)
		}
		if calls.Add(1) > 20 {
			return nil, fmt.Errorf("test cap: pager never stopped")
		}
		return searchIssuesResponse([]jiraIssue{issue}, "tok-stuck"), nil
	}}
}

// TestEdge_RepeatedNextPageTokenStopsPaging: a search page whose nextPageToken
// equals the token just sent cannot lead anywhere new. The project and board
// listings already guard their stuck pager (empty page); the JQL search loops
// must not re-request the same token forever, burning the shared Jira budget
// and re-emitting the same issue on every call.
func TestEdge_RepeatedNextPageTokenStopsPaging(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	t.Run("fetchIssuesForProject", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		sink := testkit.NewChannelSink()
		_, _ = fetchIssuesForProject(context.Background(), stuckTokenGateway(&calls), sink,
			"fetch_issues", "org-1", "cloud-1", "PROJ", since, "", time.Time{}, nil)
		assert.LessOrEqual(t, calls.Load(), int32(2),
			"the second page echoed the token it was asked with; a third request for the same token cannot make progress")
	})

	t.Run("fetchUpdatedIssueKeys", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		_, _ = fetchUpdatedIssueKeys(context.Background(), stuckTokenGateway(&calls), "cloud-1", "PROJ", since)
		assert.LessOrEqual(t, calls.Load(), int32(2),
			"the second page echoed the token it was asked with; a third request for the same token cannot make progress")
	})
}

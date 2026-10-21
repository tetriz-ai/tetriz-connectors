// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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

// TestFetchIssuesForRepo_EndlessNextChainHitsPageCap: the issue walk stops early
// only when an item falls below `since` or `next` is empty. A vendor whose every
// page is newer than `since` and always claims a next page must fail the repo
// with the page-cap error at maxListPages, not page until the run's deadline.
// The fake errors after a hard stop so the test terminates either way.
func TestFetchIssuesForRepo_EndlessNextChainHitsPageCap(t *testing.T) {
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	const hardStop = 10 * maxListPages
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.BitbucketListIssuesRequest); !ok {
				return nil, fmt.Errorf("unexpected %T", req)
			}
			calls++
			if calls >= hardStop {
				return nil, fmt.Errorf("test hard stop after %d pages", calls)
			}
			issue, _ := json.Marshal(bbIssue{ID: int64(calls), UpdatedOn: since.Add(time.Hour)})
			return &connectorpb.BitbucketListIssuesResponse{
				Issues:      [][]byte{issue},
				NextPageUrl: "https://api.bitbucket.org/2.0/repositories/ws/repo/issues?page=next",
			}, nil
		},
	}
	_, err := fetchIssuesForRepo(context.Background(), gw, testkit.NewChannelSink(), "fetch_issues",
		"org-test", "ws", "repo", "ws/repo", since)
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("exceeded %d pages", maxListPages))
	assert.Equal(t, maxListPages, calls, "the walk must stop exactly at the cap")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

type repoSlugRequest interface{ GetRepoSlug() string }

// One repository that fails with a 502 is that repository's failure in each
// repository loop: the next repository is still read, and the run fails, not
// as a pause, only because every repository failed here.
func TestBitbucketRepoLoopsSkipAFailingRepository(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   sdk.APIFetchOperation
	}{
		{"fetch_commits", &FetchCommitsOp{}},
		{"fetch_issues", &FetchIssuesOp{}},
		{"fetch_reviews", &FetchReviewsOp{}},
		{"fetch_pr_comments", &FetchPRCommentsOp{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := map[string]bool{}
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				if r, ok := req.(repoSlugRequest); ok {
					read[r.GetRepoSlug()] = true
				}
				return nil, vendorapi.ClassifyHTTPErrorWithHeaders(502, []byte(`bad gateway`), nil)
			}}
			params := testParamsMultiRepo(testkit.NewChannelSink(), gw, nil, []string{"myworkspace/a", "myworkspace/b"}, nil)
			params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/a", "myworkspace/b"}, IssueTrackerEnabled: true})
			_, err := tc.op.Execute(context.Background(), params)
			require.Error(t, err, "every repository failed")
			assert.True(t, read["a"] && read["b"], "the repository after the failing one is read: %v", read)
			var se *sdk.SyncError
			if errors.As(err, &se) {
				assert.False(t, se.Kind == sdk.ErrKindAuth && se.Terminal, "a 502 does not pause")
			}
		})
	}
}

// A repository loop whose context ends after the first repository saves that
// repository's watermark through a checkpoint and does not read the next one.
func TestBitbucketRepoLoopKeepsProgressAtTheDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var read []string
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		r, ok := req.(*connectorpb.BitbucketListIssuesRequest)
		if !ok {
			return nil, errors.New("unexpected request")
		}
		read = append(read, r.GetRepoSlug())
		cancel() // the deadline lands once the first repository is read
		return &connectorpb.BitbucketListIssuesResponse{Issues: [][]byte{
			[]byte(`{"id":1,"title":"t","updated_on":"2026-06-01T10:00:00Z","created_on":"2026-06-01T10:00:00Z"}`),
		}}, nil
	}}
	var saved []*sdk.Cursor
	params := testParamsMultiRepo(testkit.NewChannelSink(), gw, nil, []string{"myworkspace/a", "myworkspace/b"}, nil)
	params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/a", "myworkspace/b"}, IssueTrackerEnabled: true})
	params.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = append(saved, c); return nil }
	_, err := (&FetchIssuesOp{}).Execute(ctx, params)
	require.Error(t, err, "the context ended")
	assert.Equal(t, []string{"a"}, read, "the second repository is not read")
	require.NotEmpty(t, saved, "the progress is checkpointed")
	assert.True(t, strings.Contains(string(saved[len(saved)-1].Data), "myworkspace/a"), "the first repository's watermark is saved: %s", saved[len(saved)-1].Data)
}

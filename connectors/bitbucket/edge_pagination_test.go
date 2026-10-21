// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
)

// TestListBitbucketBranches_SelfReferencingNextIsBounded: a vendor whose `next`
// points back at the page just served must not spin forever. The walk stops at
// maxBranchPages with an error rather than looping or returning a duplicated list.
func TestListBitbucketBranches_SelfReferencingNextIsBounded(t *testing.T) {
	self := "https://api.bitbucket.org/2.0/repositories/ws/repo/refs/branches?pagelen=100&page=2"
	gw := &bbBranchGateway{
		branchBodyByURL: func(string) string {
			return `{"values":[{"name":"main","target":{"hash":"abc"}}],"next":"` + self + `"}`
		},
	}
	got, err := listBitbucketBranches(context.Background(), gw, "org-test", "ws", "repo")
	require.Error(t, err, "a next chain that never ends must fail, not loop")
	assert.Contains(t, err.Error(), "exceeded")
	assert.Nil(t, got, "no partial, duplicated branch list is returned")
	assert.Len(t, gw.branchListingURLs(), maxBranchPages)
}

// TestFetchAllRepos_LiveListingIsBoundedWhenNextNeverEmpties: the live workspace
// listing (first sync, no cache) follows NextPageUrl with an incrementing page
// number. A malformed vendor that keeps answering a `next` — the same failure the
// branch, tree and diffstat walks all guard with a page cap — must not keep the
// run paging indefinitely. The fake stops answering after 10k calls so the test
// terminates either way.
func TestFetchAllRepos_LiveListingIsBoundedWhenNextNeverEmpties(t *testing.T) {
	const hardStop = 10_000
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.BitbucketListReposRequest); !ok {
				return nil, fmt.Errorf("unexpected %T", req)
			}
			calls++
			if calls >= hardStop {
				return nil, fmt.Errorf("test hard stop after %d pages", calls)
			}
			// An empty page that still claims a next page.
			return bbReposResponse(nil, true), nil
		},
	}
	_, _ = fetchAllRepos(context.Background(), gw, BitbucketSettings{Workspace: "myworkspace"},
		"fetch_prs", "org-test", &pacing.PacingBudget{})
	assert.Less(t, calls, hardStop,
		"fetchAllRepos must cap its own pagination (like maxBranchPages / maxDiffstatPages); it paged %d times", calls)
}

// TestEnrichCommit_MaxPagesGuardBounds mirrors the PR-diffstat guard for the
// per-commit diffstat walk: an endless `next` stops at the configured
// MaxDiffstatPages, and the aggregates cover exactly the pages walked.
func TestEnrichCommit_MaxPagesGuardBounds(t *testing.T) {
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			calls++
			return bbCommitDiffstatResponse([]bbDiffstatEntry{
				{LinesAdded: 1, LinesRemoved: 1, New: &bbDiffstatFile{Path: fmt.Sprintf("f%d.go", calls)}},
			}, "https://api.bitbucket.org/2.0/repositories/ws/repo/diffstat/sha?page=next"), nil
		},
	}
	cfg := &config.BitbucketPaginationConfig{MaxDiffstatPages: 3}
	got, err := enrichCommit(context.Background(), gw, cfg, "ws", "repo", "sha", json.RawMessage(`{"hash":"sha"}`), nil, "fetch_commits", "org-test")
	require.NoError(t, err)
	assert.Equal(t, 3, calls, "loop must stop at the configured MaxDiffstatPages")
	assert.Equal(t, 3, got.FilesChanged)
	assert.Equal(t, 3, got.Additions)
}

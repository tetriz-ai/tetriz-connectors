// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vcs

import (
	"context"
	"testing"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
)

func indexOne(name string, refs ...string) map[string]*workqueuev1.RepoTrackedRefs {
	return IndexTrackedRefs(&workqueuev1.ResolveTrackedRefsResponse{
		Repos: []*workqueuev1.RepoTrackedRefs{{RepoFullName: name, Refs: refs}},
	})
}

// A ref the resolver names twice (two globs matching one branch) is one walk, not two:
// each walk is refs × pages of provider budget for commits the first walk already read.
func TestEdge_CommitRefsForRepo_DuplicateRefsWalkedOnce(t *testing.T) {
	refs, skip := CommitRefsForRepo(context.Background(), indexOne("acme/w", "main", "release", "main"), true, "acme/w", "main")
	require.False(t, skip)
	assert.Equal(t, []string{"main", "release"}, refs)
}

// Over the cap, duplicates must not take the slots of distinct refs: the stable subset
// should be the default branch plus the first four distinct others.
func TestEdge_CommitRefsForRepo_DuplicatesDoNotCrowdOutRefsOverCap(t *testing.T) {
	byRepo := indexOne("acme/w", "main", "a", "a", "a", "a", "b", "c", "d")
	refs, skip := CommitRefsForRepo(context.Background(), byRepo, true, "acme/w", "main")
	require.False(t, skip)
	assert.Equal(t, []string{"main", "a", "b", "c", "d"}, refs)
}

// Over the cap with the default branch untracked, the default branch is not smuggled in:
// the administrator did not track it, so the walk is the first five sorted refs.
func TestEdge_CommitRefsForRepo_OverCapWithoutDefaultBranch(t *testing.T) {
	resolved := []string{"f", "e", "d", "c", "b", "a"}
	byRepo := indexOne("acme/w", resolved...)
	refs, skip := CommitRefsForRepo(context.Background(), byRepo, true, "acme/w", "main")
	require.False(t, skip)
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, refs)
	assert.Equal(t, []string{"f", "e", "d", "c", "b", "a"}, resolved, "the resolver's slice is not reordered in place")
}

// all_branches wins over a non-empty refs list and walks the default branch.
func TestEdge_CommitRefsForRepo_AllBranchesIgnoresRefs(t *testing.T) {
	byRepo := IndexTrackedRefs(&workqueuev1.ResolveTrackedRefsResponse{
		Repos: []*workqueuev1.RepoTrackedRefs{{RepoFullName: "acme/w", AllBranches: true, Refs: []string{"x"}}},
	})
	refs, skip := CommitRefsForRepo(context.Background(), byRepo, true, "acme/w", "trunk")
	require.False(t, skip)
	assert.Equal(t, []string{"trunk"}, refs)
}

// A resolver that answers (nil, nil) counts as resolved but indexes nothing, so every
// repository degrades to its default branch rather than being skipped.
func TestEdge_ResolveTrackedRefs_NilResponseDegradesToDefaultBranch(t *testing.T) {
	m := sdkmocks.NewMockTrackedRefsResolver(gomock.NewController(t))
	m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).Return(nil, nil)
	resp, ok := ResolveTrackedRefs(context.Background(), m, "org", "github")
	byRepo := IndexTrackedRefs(resp)
	refs, skip := CommitRefsForRepo(context.Background(), byRepo, ok, "acme/w", "main")
	assert.False(t, skip)
	assert.Equal(t, []string{"main"}, refs)
}

// JoinCommitSHAs keeps the provider's order and drops whitespace-only entries.
func TestEdge_JoinCommitSHAs_OrderAndBlanks(t *testing.T) {
	assert.Equal(t, "cccccccccccc,aaaaaaaaaaaa",
		JoinCommitSHAs([]string{"CCCCCCCCCCCC00", "   ", "\t", "aaaaaaaaaaaa11"}))
	assert.Empty(t, JoinCommitSHAs([]string{" ", ""}))
}

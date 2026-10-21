// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchAllRepos_ExplicitListEnrichment pins the repo-ID carry that
// fetch_repo_tree depends on: when the tenant tracks an explicit repo list
// (settings.repositories — the individual/PAT picker path), fetchAllRepos must
// enrich each name with the numeric DatabaseID + default branch from the
// discovered_repos cache, because fetch_repo_tree keys its records on the
// numeric ID that a consumer's repository records use (keying by full name → UpdateTree
// misses the row). A repo absent from the cache degrades to name-only (ID 0),
// preserving the prior behaviour. The explicit-list branch returns before any
// gateway call, so a nil gateway is fine here.
func TestFetchAllRepos_ExplicitListEnrichment(t *testing.T) {
	t.Parallel()

	settings := GitHubSettings{
		Repositories: []string{"octo/alpha", "octo/beta"},
		DiscoveredRepos: []GitHubDiscoveredRepo{
			{FullName: "octo/alpha", DefaultBranch: "main", DatabaseID: 111},
			// octo/beta is intentionally absent from the cache → name-only fallback.
			{FullName: "octo/gamma", DefaultBranch: "trunk", DatabaseID: 222},
		},
	}

	repos, err := fetchAllRepos(context.Background(), nil, settings)
	require.NoError(t, err)
	require.Len(t, repos, 2)

	byName := map[string]repoInfo{}
	for _, r := range repos {
		byName[r.FullName] = r
	}

	// Cache hit: numeric ID + default branch carried through.
	assert.Equal(t, int64(111), byName["octo/alpha"].DatabaseID, "cached repo must carry its numeric ID for tree keying")
	assert.Equal(t, "main", byName["octo/alpha"].DefaultBranch)

	// Cache miss: degrades to name-only (ID 0, empty branch) — no panic, no wrong ID.
	assert.Equal(t, int64(0), byName["octo/beta"].DatabaseID, "uncached repo falls back to ID 0")
	assert.Empty(t, byName["octo/beta"].DefaultBranch)
}

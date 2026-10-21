// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package treewalker

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Two directories with identical content share one tree SHA. Each must be walked under
// its own path; deduplicating by SHA would drop one directory's files.
func TestEdge_Walk_SameSubtreeSHAAtTwoPaths(t *testing.T) {
	api := &fakeAPI{
		root:      []TreeEntry{{Path: "a", Type: "tree", SHA: "SAME"}, {Path: "b", Type: "tree", SHA: "SAME"}},
		rootTrunc: true,
		subtrees: map[string][]TreeEntry{
			"ROOT": {{Path: "a", Type: "tree", SHA: "SAME"}, {Path: "b", Type: "tree", SHA: "SAME"}},
			"SAME": {{Path: ".gitkeep", Type: "blob", SHA: "k"}},
		},
	}
	entries, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)
	paths := pathsOf(entries)
	sort.Strings(paths)
	require.Equal(t, []string{"a", "a/.gitkeep", "b", "b/.gitkeep"}, paths)
}

// A deep directory chain at maxParallel=1 must not deadlock: each level holds a slot of
// its parent's limit while it waits on its own children.
func TestEdge_Walk_DeepChainAtParallelismOne(t *testing.T) {
	const depth = 30
	subtrees := map[string][]TreeEntry{}
	for i := 0; i < depth; i++ {
		sha := fmt.Sprintf("D%d", i)
		if i == 0 {
			sha = "ROOT"
		}
		subtrees[sha] = []TreeEntry{
			{Path: fmt.Sprintf("d%d", i+1), Type: "tree", SHA: fmt.Sprintf("D%d", i+1)},
			{Path: "f.go", Type: "blob", SHA: fmt.Sprintf("f%d", i)},
		}
	}
	subtrees[fmt.Sprintf("D%d", depth)] = nil
	api := &fakeAPI{root: subtrees["ROOT"], rootTrunc: true, subtrees: subtrees}

	done := make(chan struct{})
	var entries []TreeEntry
	var err error
	go func() {
		entries, _, err = Walk(context.Background(), api, "ROOT", 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Walk deadlocked on a deep chain at maxParallel=1")
	}
	require.NoError(t, err)
	require.Len(t, entries, depth*2)
}

// When the recursive call is truncated, its partial entries are discarded and replaced
// by the BFS result, so no path appears twice.
func TestEdge_Walk_TruncatedRootEntriesAreNotDuplicated(t *testing.T) {
	api := &fakeAPI{
		root: []TreeEntry{
			{Path: "src", Type: "tree", SHA: "SRC"},
			{Path: "src/main.go", Type: "blob", SHA: "m"},
		},
		rootTrunc: true,
		subtrees: map[string][]TreeEntry{
			"ROOT": {{Path: "src", Type: "tree", SHA: "SRC"}},
			"SRC":  {{Path: "main.go", Type: "blob", SHA: "m"}, {Path: "util.go", Type: "blob", SHA: "u"}},
		},
	}
	entries, _, err := Walk(context.Background(), api, "ROOT", 2)
	require.NoError(t, err)
	paths := pathsOf(entries)
	sort.Strings(paths)
	require.Equal(t, []string{"src", "src/main.go", "src/util.go"}, paths)
}

// An empty repository tree (no entries, not truncated) is an empty result, not an error.
func TestEdge_Walk_EmptyTree(t *testing.T) {
	entries, trunc, err := Walk(context.Background(), &fakeAPI{root: nil}, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)
	require.Empty(t, entries)
}

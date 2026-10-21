// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package treewalker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeAPI is a test double for VendorTreeAPI. It returns pre-configured
// entries or errors keyed by SHA.
type fakeAPI struct {
	// root is returned when recursive=true and sha=="ROOT".
	root      []TreeEntry
	rootTrunc bool
	// subtrees maps SHA → entries returned when recursive=false.
	subtrees map[string][]TreeEntry
	// failOn maps SHA → error; if present the call returns that error.
	failOn map[string]error
}

func (f *fakeAPI) GetTree(_ context.Context, sha string, recursive bool) ([]TreeEntry, bool, error) {
	if err, ok := f.failOn[sha]; ok {
		return nil, false, err
	}
	if recursive && sha == "ROOT" {
		return f.root, f.rootTrunc, nil
	}
	if sub, ok := f.subtrees[sha]; ok {
		return sub, false, nil
	}
	return nil, false, errors.New("unknown sha: " + sha)
}

// TestWalk_FastPath verifies that when the vendor returns truncated=false the
// entries are returned as-is without any further calls.
func TestWalk_FastPath(t *testing.T) {
	api := &fakeAPI{
		root:      []TreeEntry{{Path: "a.go", Type: "blob", SHA: "a"}},
		rootTrunc: false,
	}
	entries, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)
	require.Len(t, entries, 1)
	require.Equal(t, "a.go", entries[0].Path)
}

// TestWalk_TruncatedFallback verifies that when truncated=true the walker falls
// back to non-recursive BFS and correctly prefixes child paths.
func TestWalk_TruncatedFallback(t *testing.T) {
	api := &fakeAPI{
		root: []TreeEntry{
			{Path: "src", Type: "tree", SHA: "SRC"},
			{Path: "README.md", Type: "blob", SHA: "r1"},
		},
		rootTrunc: true,
		subtrees: map[string][]TreeEntry{
			"ROOT": {
				{Path: "src", Type: "tree", SHA: "SRC"},
				{Path: "README.md", Type: "blob", SHA: "r1"},
			},
			"SRC": {
				{Path: "main.go", Type: "blob", SHA: "m1"},
			},
		},
	}
	entries, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)

	paths := pathsOf(entries)
	require.Contains(t, paths, "src")
	require.Contains(t, paths, "README.md")
	require.Contains(t, paths, "src/main.go")
}

// TestWalk_NestedSubtrees verifies that deeply nested subtrees have their paths
// correctly prefixed at each level.
func TestWalk_NestedSubtrees(t *testing.T) {
	api := &fakeAPI{
		root:      []TreeEntry{{Path: "pkg", Type: "tree", SHA: "PKG"}},
		rootTrunc: true,
		subtrees: map[string][]TreeEntry{
			"ROOT": {{Path: "pkg", Type: "tree", SHA: "PKG"}},
			"PKG":  {{Path: "util", Type: "tree", SHA: "UTIL"}, {Path: "doc.go", Type: "blob", SHA: "d1"}},
			"UTIL": {{Path: "helper.go", Type: "blob", SHA: "h1"}},
		},
	}
	entries, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)

	paths := pathsOf(entries)
	require.Contains(t, paths, "pkg")
	require.Contains(t, paths, "pkg/util")
	require.Contains(t, paths, "pkg/doc.go")
	require.Contains(t, paths, "pkg/util/helper.go")
}

// TestWalk_PartialFetchFails verifies that a network error on any subtree
// causes Walk to return an error and no partial entries.
func TestWalk_PartialFetchFails(t *testing.T) {
	api := &fakeAPI{
		root:      []TreeEntry{{Path: "src", Type: "tree", SHA: "SRC"}},
		rootTrunc: true,
		subtrees:  map[string][]TreeEntry{"ROOT": {{Path: "src", Type: "tree", SHA: "SRC"}}},
		failOn:    map[string]error{"SRC": errors.New("network timeout")},
	}
	result, _, err := Walk(context.Background(), api, "ROOT", 4)
	require.Error(t, err)
	require.Nil(t, result)
}

// TestWalk_RootFetchFails verifies that an error on the initial recursive fetch
// surfaces immediately.
func TestWalk_RootFetchFails(t *testing.T) {
	api := &fakeAPI{
		failOn: map[string]error{"ROOT": errors.New("unauthorized")},
	}
	result, _, err := Walk(context.Background(), api, "ROOT", 4)
	require.Error(t, err)
	require.Nil(t, result)
}

// TestWalk_ConcurrencySmoke spawns many subtrees and verifies that all paths
// are returned correctly when maxParallel is lower than the number of subtrees.
func TestWalk_ConcurrencySmoke(t *testing.T) {
	const numSubtrees = 20

	subtrees := map[string][]TreeEntry{}
	rootLevel := make([]TreeEntry, 0, numSubtrees)
	for i := 0; i < numSubtrees; i++ {
		sha := "SHA" + string(rune('A'+i))
		name := "dir" + string(rune('A'+i))
		rootLevel = append(rootLevel, TreeEntry{Path: name, Type: "tree", SHA: sha})
		subtrees[sha] = []TreeEntry{{Path: "file.go", Type: "blob", SHA: sha + "f"}}
	}
	subtrees["ROOT"] = rootLevel

	api := &fakeAPI{
		root:      rootLevel,
		rootTrunc: true,
		subtrees:  subtrees,
	}

	// maxParallel intentionally lower than numSubtrees to exercise the semaphore.
	entries, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.False(t, trunc)
	// numSubtrees directory entries + numSubtrees file entries
	require.Len(t, entries, numSubtrees*2)
}

// TestWalk_SubtreeTruncated verifies that when a non-recursive subtree fetch
// itself returns truncated=true, Walk surfaces that as truncated=true in its
// final result even though the root recursive fetch was not truncated.
func TestWalk_SubtreeTruncated(t *testing.T) {
	// fakeAPIWithSubtreeTrunc overrides GetTree to signal truncation on a subtree.
	api := &fakeAPIWithSubtreeTrunc{
		rootEntries: []TreeEntry{
			{Path: "src", Type: "tree", SHA: "SRC"},
		},
		subtrees: map[string]subtreeResult{
			"ROOT": {entries: []TreeEntry{{Path: "src", Type: "tree", SHA: "SRC"}}, truncated: false},
			// This subtree's non-recursive listing is itself truncated.
			"SRC": {entries: []TreeEntry{{Path: "main.go", Type: "blob", SHA: "m1"}}, truncated: true},
		},
	}
	_, trunc, err := Walk(context.Background(), api, "ROOT", 4)
	require.NoError(t, err)
	require.True(t, trunc, "Walk must propagate truncated=true from subtree fetch")
}

// subtreeResult holds the canned response for a single subtree SHA.
type subtreeResult struct {
	entries   []TreeEntry
	truncated bool
}

// fakeAPIWithSubtreeTrunc is a test double that can signal truncation on
// individual non-recursive subtree fetches (the base fakeAPI always returns
// truncated=false for subtrees).
type fakeAPIWithSubtreeTrunc struct {
	rootEntries []TreeEntry
	subtrees    map[string]subtreeResult
}

func (f *fakeAPIWithSubtreeTrunc) GetTree(_ context.Context, sha string, recursive bool) ([]TreeEntry, bool, error) {
	if recursive && sha == "ROOT" {
		// Trigger the BFS fallback path by reporting truncated=true.
		return f.rootEntries, true, nil
	}
	if r, ok := f.subtrees[sha]; ok {
		return r.entries, r.truncated, nil
	}
	return nil, false, errors.New("unknown sha: " + sha)
}

// pathsOf returns just the Path field from a slice of TreeEntry values.
func pathsOf(es []TreeEntry) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Path)
	}
	return out
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package treewalker provides a shared recursive VCS tree walker used by
// connector implementations (GitHub, GitLab, Bitbucket). It abstracts the
// vendor-specific "get tree" API behind VendorTreeAPI and handles the common
// truncation fallback: if the vendor's recursive fetch is truncated, it falls
// back to a bounded-concurrency BFS walk over each subtree individually.
package treewalker

import (
	"context"
	"fmt"
	"path"
	"sync"

	errgrp "github.com/tetriz-ai/tetriz-connectors/internal/errgroup"
)

// TreeEntry is a single node in a VCS tree (a file or a subdirectory).
type TreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"` // "blob" or "tree"
	Size *int64 `json:"size,omitempty"`
	Mode string `json:"mode,omitempty"`
	SHA  string `json:"sha"`
}

// VendorTreeAPI is implemented per vendor (github/gitlab/bitbucket).
// GetTree returns the tree at sha. recursive=true means the vendor returns
// the full tree in one call (subject to vendor truncation limits).
// truncated = vendor flag indicating the response is incomplete.
type VendorTreeAPI interface {
	GetTree(ctx context.Context, sha string, recursive bool) (entries []TreeEntry, truncated bool, err error)
}

// Walk fetches the complete tree starting at rootSHA. If the vendor responds
// with truncated=true on the recursive=true attempt, Walk falls back to BFS
// walking each subtree non-recursively. maxParallel bounds subtree concurrency.
//
// Failure semantics:
//   - Any error during walk returns (nil, false, err) — caller MUST NOT write
//     a partial tree. The next refresh will retry.
//   - A permanently-404 subtree (after the vendor's own retries) returns
//     (entries_so_far, true, nil) — caller stores the partial tree with
//     truncated=true.
func Walk(ctx context.Context, api VendorTreeAPI, rootSHA string, maxParallel int) ([]TreeEntry, bool, error) {
	entries, truncated, err := api.GetTree(ctx, rootSHA, true)
	if err != nil {
		return nil, false, fmt.Errorf("fetching tree %s: %w", rootSHA, err)
	}
	if !truncated {
		return entries, false, nil
	}
	// Vendor returned a truncated result — fall back to non-recursive BFS.
	out, truncatedOverall, err := walkSubtrees(ctx, api, rootSHA, "", maxParallel)
	if err != nil {
		return nil, false, fmt.Errorf("walking subtrees: %w", err)
	}
	return out, truncatedOverall, nil
}

// walkResult carries the output from a single subtree walk goroutine.
type walkResult struct {
	entries   []TreeEntry
	truncated bool
}

// walkSubtrees fetches the tree at sha non-recursively, prepends prefix to
// every child path, and then fans out concurrently to walk any "tree" entries
// it finds. It collects all results before returning. A network error on any
// subtree propagates up as a hard error. A 404 that the vendor has already
// retried can be represented by the caller returning (nil, false, nil) with
// truncated=true for that subtree — walkSubtrees surfaces that as truncated=true
// in its own result.
func walkSubtrees(ctx context.Context, api VendorTreeAPI, sha, prefix string, maxParallel int) ([]TreeEntry, bool, error) {
	level, levelTruncated, err := api.GetTree(ctx, sha, false)
	if err != nil {
		return nil, false, fmt.Errorf("fetching tree %s: %w", sha, err)
	}
	// A non-recursive fetch for this level was itself truncated (e.g. directory
	// with >100k entries). Surface the signal so callers can store truncated=true.
	truncatedOverall := levelTruncated

	// Collect this level's entries with fully-qualified paths.
	out := make([]TreeEntry, 0, len(level))
	for _, e := range level {
		full := e
		if prefix != "" {
			full.Path = path.Join(prefix, e.Path)
		}
		out = append(out, full)
	}

	// Fan out to subtrees concurrently, bounded by maxParallel.
	g, gCtx := errgrp.WithContext(ctx)
	g.SetLimit(maxParallel)

	var (
		mu         sync.Mutex
		resultsBuf []walkResult
	)

	for _, e := range level {
		if e.Type != "tree" {
			continue
		}
		entry := e
		childPrefix := entry.Path
		if prefix != "" {
			childPrefix = path.Join(prefix, entry.Path)
		}
		g.Go(func() error {
			child, childTrunc, err := walkSubtrees(gCtx, api, entry.SHA, childPrefix, maxParallel)
			if err != nil {
				return fmt.Errorf("walking subtree %s: %w", childPrefix, err)
			}
			mu.Lock()
			resultsBuf = append(resultsBuf, walkResult{entries: child, truncated: childTrunc})
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, false, fmt.Errorf("walking subtrees: %w", err)
	}

	for _, r := range resultsBuf {
		out = append(out, r.entries...)
		if r.truncated {
			truncatedOverall = true
		}
	}
	return out, truncatedOverall, nil
}

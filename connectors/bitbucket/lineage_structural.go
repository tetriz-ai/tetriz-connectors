// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Bitbucket's file-at-revision fetch for the defect-lineage AST scorer. The scorer needs the whole
// file at a revision to build a syntax tree; the content is returned in memory to parse and discard —
// nothing is persisted.

// structuralFileBodyLimit caps the in-memory size of a fetched file. A file larger than this is not
// worth parsing for AST proximity (and would strain memory under the drain's concurrency), so an
// over-limit file degrades to coverage — its cache key is returned with nil content — rather than
// being parsed from a truncated body. Distinct from maxFileContentBytes, which bounds a different op.
const structuralFileBodyLimit = 8 << 20 // 8 MiB

// FileAtRevision returns the file at path as of revision plus a cache key for its content. Bitbucket's
// raw src endpoint carries no git blob SHA (unlike GitHub/GitLab), so the key falls back to a
// commit+path composite — immutable for a given revision, which is all the verdict cache needs. The
// key is non-empty whenever the file exists, so it also distinguishes the too-large degrade (key, nil
// content) from the path-absent sentinel (empty key, nil content). A 404 means the path does not exist
// at that revision — return an empty key and nil content so the caller degrades to coverage.
func FileAtRevision(ctx context.Context, gw sdk.VendorGateway, orgID, repoFullName, revision, path string) (content []byte, blobSHA string, err error) {
	workspace, repoSlug, ok := splitRepoFullName(repoFullName)
	if !ok {
		return nil, "", fmt.Errorf("bad repo full name %q", repoFullName)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bitbucketFileContentURL(orgID, workspace, repoSlug, revision, path), nil)
	if err != nil {
		return nil, "", fmt.Errorf("building file request %s@%s: %w", path, revision, err)
	}
	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, "", fmt.Errorf("fetching file %s@%s: %w", path, revision, err)
	}
	if resp.Body == nil {
		return nil, "", fmt.Errorf("fetching file %s@%s: nil response body", path, revision)
	}
	defer resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only request
	if resp.StatusCode == http.StatusNotFound {
		return nil, "", nil // path absent at this revision
	}
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, "", fmt.Errorf("fetching file %s@%s: %w", path, revision, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, errBody, resp.Header))
	}
	// Read one byte past the cap so a file at exactly the limit stays distinguishable from one over
	// it: a body that reaches cap+1 is too large to parse safely and must degrade rather than be fed
	// truncated to the AST — LimitReader alone would silently drop the tail with a nil error.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, structuralFileBodyLimit+1))
	if readErr != nil {
		return nil, "", fmt.Errorf("reading file body %s@%s: %w", path, revision, readErr)
	}
	key := structuralCacheKey(revision, path)
	if len(body) > structuralFileBodyLimit {
		return nil, key, nil // exists but too large to parse — caller degrades to coverage
	}
	return body, key, nil
}

// structuralCacheKey is the commit+path surrogate Bitbucket uses in place of a git blob SHA. Unlike a
// blob SHA it is not content-addressed, so identical content at two revisions does not share a key (a
// cache miss, never a false hit) — the correct trade for a provider that exposes no blob identity.
func structuralCacheKey(revision, path string) string {
	return revision + ":" + path
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// GitLab's file-at-revision fetch for the defect-lineage AST scorer. The scorer needs the whole
// file at a revision to build a syntax tree; the content is returned in memory to parse and
// discard — nothing is persisted.

// structuralFileBodyLimit caps the in-memory size of a fetched file. A file larger than this is not
// worth parsing for AST proximity (and would strain memory under the drain's concurrency), so an
// over-limit file degrades to coverage — its blob SHA is returned with nil content — rather than
// being parsed from a silently truncated body.
const structuralFileBodyLimit = 8 << 20 // 8 MiB

// FileAtRevision returns the file at path as of revision plus its git blob SHA (the immutable key
// the verdict cache is keyed on). It reads the raw file endpoint and takes the blob SHA from the
// X-Gitlab-Blob-Id response header. A 404 means the path does not exist at that revision — return
// an empty blob SHA and nil content so the caller degrades to coverage rather than parsing a
// phantom file.
func FileAtRevision(ctx context.Context, gw sdk.VendorGateway, orgID, projectID, revision, path string) (content []byte, blobSHA string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gitlabRawFileURL(orgID, projectID, path, revision), nil)
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
	// it: a body that reaches cap+1 is too large to parse safely, so it must degrade rather than be
	// fed truncated to the AST — LimitReader alone would silently drop the tail with a nil error.
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, structuralFileBodyLimit+1))
	if readErr != nil {
		return nil, "", fmt.Errorf("reading file body %s@%s: %w", path, revision, readErr)
	}
	blobSHA = resp.Header.Get("X-Gitlab-Blob-Id")
	if blobSHA == "" {
		// A 200 with no blob id would hand back an empty SHA that collides with the path-absent
		// sentinel above; fail loud rather than cache a phantom file under an empty key.
		return nil, "", fmt.Errorf("fetching file %s@%s: 200 response with no X-Gitlab-Blob-Id", path, revision)
	}
	if len(body) > structuralFileBodyLimit {
		// Exists but too large to parse. Return the blob SHA with nil content so the caller degrades
		// to coverage instead of parsing a truncated tree.
		return nil, blobSHA, nil
	}
	return body, blobSHA, nil
}

// gitlabRawFileURL builds the raw file-at-ref endpoint. The path is URL-encoded with slashes as
// %2F, the form GitLab's path-parameter encoding requires — the same encoding gitlabBlameURL uses.
func gitlabRawFileURL(orgID, projectID, filePath, ref string) string {
	encodedPath := strings.ReplaceAll(url.PathEscape(filePath), "/", "%2F")
	q := url.Values{}
	q.Set("ref", ref)
	return fmt.Sprintf("%s/projects/%s/repository/files/%s/raw?%s", gitlabRESTBase(orgID), projectID, encodedPath, q.Encode())
}

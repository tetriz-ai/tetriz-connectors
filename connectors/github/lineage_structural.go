// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// GitHub's file-at-revision fetch for the defect-lineage AST scorer. The scorer needs the whole
// file at a revision to build a syntax tree; blame alone cannot. The content is returned in memory
// for the caller to parse and discard — nothing is persisted.

// structuralFileBodyLimit caps the in-memory size of a fetched blob. A blob larger than this is not
// worth parsing for AST proximity (and would strain memory under the drain's concurrency), so an
// over-limit blob degrades to coverage — its blob SHA is returned with nil content — rather than
// being read.
const structuralFileBodyLimit = 8 << 20 // 8 MiB

// encodingBase64 is the encoding the REST git-blob API reports for a blob's content.
const encodingBase64 = "base64"

// ghFileResponse models the blob-at-revision query: the blob's git object id (its content SHA, the
// immutable cache key) plus its text and a truncation flag.
type ghFileResponse struct {
	Repository struct {
		Object struct {
			OID         string `json:"oid"`
			Text        string `json:"text"`
			IsTruncated bool   `json:"isTruncated"`
			ByteSize    int    `json:"byteSize"`
		} `json:"object"`
	} `json:"repository"`
}

// FileAtRevision returns the file at path as of revision, plus its git blob SHA (the immutable key
// the verdict cache is keyed on). It reads the blob over GraphQL and, when GitHub truncates the
// inline text for a large blob, falls back to the REST git-blob API for the full bytes. A missing
// blob (the path does not exist at that revision) returns an empty blob SHA and nil content so the
// caller degrades to coverage rather than parsing a phantom file.
func FileAtRevision(ctx context.Context, gw sdk.VendorGateway, orgID, repoFullName, revision, path string) (content []byte, blobSHA string, err error) {
	owner, name, ok := splitRepoFullName(repoFullName)
	if !ok {
		return nil, "", fmt.Errorf("bad repo full name %q", repoFullName)
	}
	raw, err := doGraphQL(ctx, gw, buildBlobQuery(owner, name, revision, path), false)
	if err != nil {
		return nil, "", fmt.Errorf("blob query %s@%s: %w", path, revision, err)
	}
	var resp ghFileResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, "", fmt.Errorf("decoding blob response %s@%s: %w", path, revision, err)
	}
	obj := resp.Repository.Object
	if obj.OID == "" {
		return nil, "", nil // path absent at this revision
	}
	if obj.ByteSize > structuralFileBodyLimit {
		// Exists but too large to parse. Return the blob SHA (a valid cache key) with nil content
		// so the caller degrades to coverage instead of fetching a blob it will discard.
		return nil, obj.OID, nil
	}
	if !obj.IsTruncated {
		return []byte(obj.Text), obj.OID, nil
	}
	// Large (but within the parse limit) blob: GraphQL truncated the inline text, so read the full
	// bytes by object id.
	full, err := ghBlobBytes(ctx, gw, orgID, owner, name, obj.OID)
	if err != nil {
		return nil, "", fmt.Errorf("blob fallback %s@%s: %w", path, revision, err)
	}
	return full, obj.OID, nil
}

// buildBlobQuery selects a blob's content SHA, text and truncation flag at a revision in one
// request; it hits the gateway's rate limiter and circuit breaker like every other GitHub call.
func buildBlobQuery(owner, name, revision, path string) string {
	return fmt.Sprintf(`query {
  repository(owner: %q, name: %q) {
    object(expression: %q) { ... on Blob { oid text isTruncated byteSize } }
  }
}`, owner, name, revision+":"+path)
}

// ghBlobBytes reads a blob's full content through the REST git-blob API (which serves blobs the
// GraphQL text field truncates), decoding the base64 body.
func ghBlobBytes(ctx context.Context, gw sdk.VendorGateway, orgID, owner, name, oid string) ([]byte, error) {
	// Route through githubRESTBase(orgID) so a simulator-configured org hits its simulator, not the
	// real API, as every other GitHub REST caller does.
	url := fmt.Sprintf("%s/repos/%s/%s/git/blobs/%s", githubRESTBase(orgID), owner, name, oid)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building blob request: %w", err)
	}
	resp, err := gw.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("fetching blob %s: %w", oid, err)
	}
	if resp.Body == nil {
		return nil, fmt.Errorf("fetching blob %s: nil response body", oid)
	}
	defer resp.Body.Close() //nolint:errcheck // read-only request
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("fetching blob %s: %w", oid, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, errBody, resp.Header))
	}
	// The ByteSize gate in FileAtRevision already keeps oversized blobs out of this path, so this
	// bound only guards a response whose declared size lied: base64 inflates the content ~4/3 and
	// the JSON envelope adds a little, so cap at 2x the parse limit and fail loud past it rather
	// than buffering an unbounded body into memory.
	const blobResponseLimit = structuralFileBodyLimit * 2
	raw, err := io.ReadAll(io.LimitReader(resp.Body, blobResponseLimit+1))
	if err != nil {
		return nil, fmt.Errorf("reading blob %s: %w", oid, err)
	}
	if len(raw) > blobResponseLimit {
		return nil, fmt.Errorf("blob %s: response exceeds read limit", oid)
	}
	var body struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("decoding blob %s: %w", oid, err)
	}
	if body.Encoding != encodingBase64 {
		return nil, fmt.Errorf("blob %s: unexpected encoding %q", oid, body.Encoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(stripNewlines(body.Content))
	if err != nil {
		return nil, fmt.Errorf("decoding blob %s base64: %w", oid, err)
	}
	return decoded, nil
}

// stripNewlines removes the line breaks the GitHub blob API inserts into its base64 content.
func stripNewlines(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\n' && s[i] != '\r' {
			out = append(out, s[i])
		}
	}
	return string(out)
}

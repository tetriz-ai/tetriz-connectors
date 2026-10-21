// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// edgeDoJSONGateway records every DoJSON URL and answers it with fn.
type edgeDoJSONGateway struct {
	urls []string
	fn   func(urlStr string, respBody any) error
}

func (g *edgeDoJSONGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	g.urls = append(g.urls, urlStr)
	return g.fn(urlStr, respBody)
}
func (g *edgeDoJSONGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not expected")
}
func (g *edgeDoJSONGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, fmt.Errorf("Execute not expected")
}
func (g *edgeDoJSONGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }
func (g *edgeDoJSONGateway) ForGroup(string) sdk.VendorGateway                      { return g }

// TestEdge_BranchSHALookupEncodesSlashAndUnicode covers default branches named
// like "release/2026" or carrying non-ASCII. GitLab's single-branch endpoint takes
// the branch as ONE path segment, so "/" must travel as %2F (a raw slash routes to
// a different resource) and "#"/"?" must not cut the path short.
func TestEdge_BranchSHALookupEncodesSlashAndUnicode(t *testing.T) {
	branches := []string{"release/2026", "feature/ünïcode-🚀", "fix#12?draft", "a b"}
	for _, branch := range branches {
		t.Run(branch, func(t *testing.T) {
			gw := &edgeDoJSONGateway{fn: func(_ string, respBody any) error {
				return json.Unmarshal([]byte(`{"name":"x","commit":{"id":"abc123"}}`), respBody)
			}}
			sha, err := getGitLabBranchSHA(context.Background(), gw, "org-test", 7, branch)
			require.NoError(t, err)
			assert.Equal(t, "abc123", sha)
			require.Len(t, gw.urls, 1)

			u, err := url.Parse(gw.urls[0])
			require.NoError(t, err)
			assert.Empty(t, u.RawQuery, "no part of the branch may spill into the query")
			assert.Empty(t, u.Fragment, "no part of the branch may spill into a fragment")
			const marker = "/repository/branches/"
			escaped := u.EscapedPath()
			idx := strings.Index(escaped, marker)
			require.GreaterOrEqual(t, idx, 0)
			segment := escaped[idx+len(marker):]
			assert.NotContains(t, segment, "/", "the branch must be a single path segment")
			decoded, err := url.PathUnescape(segment)
			require.NoError(t, err)
			assert.Equal(t, branch, decoded, "the segment must decode back to the exact branch name")
		})
	}
}

// TestEdge_TreeExactMultipleOfPageSizeTerminates covers a tree whose entry count is
// an exact multiple of per_page: page 1 is full, page 2 is empty. The walk must ask
// for page 2, stop on the empty page, and return every entry without error.
func TestEdge_TreeExactMultipleOfPageSizeTerminates(t *testing.T) {
	const perPage = 100
	gw := &edgeDoJSONGateway{fn: func(urlStr string, respBody any) error {
		var entries []glTreeEntry
		if strings.Contains(urlStr, "&page=1&") || strings.HasSuffix(urlStr, "&page=1") {
			for i := range perPage {
				entries = append(entries, glTreeEntry{ID: fmt.Sprintf("s%d", i), Type: "blob", Path: fmt.Sprintf("f%d.go", i)})
			}
		}
		raw, err := json.Marshal(entries)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, respBody)
	}}

	api := &gitlabTreeAPI{gw: gw, orgID: "org-test", projectID: 7}
	got, truncated, err := api.GetTree(context.Background(), "headsha", true)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Len(t, got, perPage)
	assert.Len(t, gw.urls, 2, "a full page asks for one more; the empty page ends the walk")
}

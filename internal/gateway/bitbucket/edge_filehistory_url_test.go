// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// Bitbucket's /filehistory endpoint paginates by its `next` cursor and answers
// 400 "Invalid page" to a `page` query param, so the URL never carries one,
// even when the request names a page. pagelen and the renames opt-out stay.
func TestEdge_FileHistoryURLCarriesNoPageParam(t *testing.T) {
	for _, page := range []int32{0, 1, 3} {
		r := &getFileHistoryVendorRequest{
			req:     &connectorpb.BitbucketGetFileHistoryRequest{Workspace: "ws", RepoSlug: "repo", Commit: "abc", Path: "dir/a b.go", Page: page},
			baseURL: "https://api.bitbucket.org/2.0",
		}
		u, err := url.Parse(r.URL())
		require.NoError(t, err)
		q := u.Query()
		assert.False(t, q.Has("page"), "page=%d leaked into %s", page, r.URL())
		assert.Equal(t, "100", q.Get("pagelen"))
		assert.Equal(t, "false", q.Get("renames"))
		assert.Equal(t, "/2.0/repositories/ws/repo/filehistory/abc/dir/a%20b.go", u.EscapedPath())
	}
}

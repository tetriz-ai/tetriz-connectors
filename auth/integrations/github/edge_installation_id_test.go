// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pathRecorder answers every request 404 and records the raw path it was
// sent to, so no request leaves the process.
type pathRecorder struct {
	mu    sync.Mutex
	paths []string
}

func (p *pathRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	p.mu.Lock()
	p.paths = append(p.paths, req.Method+" "+req.URL.EscapedPath()+"?"+req.URL.RawQuery)
	p.mu.Unlock()
	return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}, Request: req}, nil
}

// The installation id reaches GetInstallation and DeleteInstallation from a
// connect request (and from stored credentials) and is signed for with the
// App's own JWT. A value that is not a plain number must not steer that
// App-authenticated call to another path: "1/suspended" would turn the
// uninstall DELETE into GitHub's unsuspend endpoint, and "../../app/hook/config"
// walks out of /app/installations.
func TestEdge_InstallationIDCannotRewriteTheAppPath(t *testing.T) {
	_, pemStr := generateTestPEM(t)
	client, err := NewGitHubAppClient(&GitHubAppConfig{ClientID: "cid", PrivateKeyPEM: pemStr})
	require.NoError(t, err)
	concrete := client.(*githubAppClient)
	rec := &pathRecorder{}
	concrete.httpClient = &http.Client{Transport: rec}

	for _, id := range []string{"1/suspended", "1/../../app/hook/config", "1?per_page=100", "1/access_tokens"} {
		_, _ = concrete.GetInstallation(context.Background(), id)
		_ = concrete.DeleteInstallation(context.Background(), id)
	}
	for _, p := range rec.paths {
		pathOnly := strings.SplitN(strings.SplitN(p, " ", 2)[1], "?", 2)
		rest := strings.TrimPrefix(pathOnly[0], "/app/installations/")
		assert.False(t, strings.Contains(rest, "/") || pathOnly[1] != "",
			"App-JWT request sent to %q: the installation id escaped its path segment", p)
	}
}

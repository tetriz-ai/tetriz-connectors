// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every GitHub App call identifies itself as "tetriz-connectors".
func TestAppCallsSendTheUserAgent(t *testing.T) {
	_, pemStr := generateTestPEM(t)
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("User-Agent"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":1,"account":{"login":"acme","id":2,"type":"Organization"},"avatar_url":"https://example.com/a.png"}`))
	}))
	defer server.Close()

	client, err := NewGitHubAppClient(&GitHubAppConfig{ClientID: "test-client-id", PrivateKeyPEM: pemStr})
	require.NoError(t, err)
	c := client.(*githubAppClient)
	c.httpClient = server.Client()
	c.httpClient.Transport = &rewriteTransport{base: server.Client().Transport, baseURL: server.URL}

	ctx := context.Background()
	_, _ = c.GetInstallation(ctx, "1")
	_ = c.DeleteInstallation(ctx, "1")
	_, _ = c.GetOrgAvatarURL(ctx, "acme")
	require.Len(t, seen, 3)
	for _, ua := range seen {
		assert.Equal(t, "tetriz-connectors", ua)
	}
}

// The installation-token calls (ListRepos, BranchExists) set the same value;
// no other User-Agent literal exists in the client.
func TestClientHasOneUserAgent(t *testing.T) {
	src, err := os.ReadFile("client.go")
	require.NoError(t, err)
	for _, m := range regexp.MustCompile(`Header\.Set\("User-Agent", ([^)]*)\)`).FindAllSubmatch(src, -1) {
		assert.Equal(t, "userAgent", string(m[1]))
	}
}

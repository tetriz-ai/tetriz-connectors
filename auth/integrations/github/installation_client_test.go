// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// installationServer answers the installation-token exchange and hands every
// other request to api. The client reaches it through WithGitHubTransport,
// which carries both the token exchange and the installation calls.
func installationServer(t *testing.T, api http.HandlerFunc) GitHubAppClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/app/installations/77/access_tokens" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "inst-token", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
			return
		}
		assert.Equal(t, "token inst-token", r.Header.Get("Authorization"))
		api(w, r)
	}))
	t.Cleanup(srv.Close)
	_, pemStr := generateTestPEM(t)
	c, err := NewGitHubAppClient(&GitHubAppConfig{AppID: 1, ClientID: "cid", PrivateKeyPEM: pemStr},
		WithGitHubTransport(&rewriteTransport{base: srv.Client().Transport, baseURL: srv.URL}))
	require.NoError(t, err)
	return c
}

func errorAPI(status int, contentType, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// ListRepos lists every non-archived repository through the installation, and
// an error keeps only the vendor's message fields, never the body text.
func TestListRepos(t *testing.T) {
	ctx := context.Background()
	c := installationServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/orgs/acme/repos", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"full_name": "acme/api", "default_branch": "main", "private": true},
			{"full_name": "acme/old", "default_branch": "main", "archived": true},
		})
	})
	repos, err := c.ListRepos(ctx, "77", "acme")
	require.NoError(t, err)
	assert.Equal(t, []RepoInfo{{FullName: "acme/api", DefaultBranch: "main", Private: true}}, repos)

	_, err = installationServer(t, errorAPI(http.StatusForbidden, "application/json", jsonErrorBody)).ListRepos(ctx, "77", "acme")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `message="Bad credentials"`)
	assert.NotContains(t, err.Error(), "SECRET-TOKEN-xyz")

	_, err = installationServer(t, errorAPI(http.StatusBadGateway, "text/html", htmlErrorBody)).ListRepos(ctx, "77", "acme")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "text/html body")
	assert.NotContains(t, err.Error(), "SECRET-HTML-abc")
}

// BranchExists answers true for a branch, false for a 404, and an error for
// anything else that keeps only the vendor's message fields.
func TestBranchExists(t *testing.T) {
	ctx := context.Background()
	c := installationServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/branches/main") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"main"}`))
			return
		}
		http.NotFound(w, r)
	})
	ok, err := c.BranchExists(ctx, "77", "acme", "api", "main")
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = c.BranchExists(ctx, "77", "acme", "api", "gone")
	require.NoError(t, err)
	assert.False(t, ok)

	_, err = installationServer(t, errorAPI(http.StatusForbidden, "application/json", jsonErrorBody)).BranchExists(ctx, "77", "acme", "api", "main")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `message="Bad credentials"`)
	assert.NotContains(t, err.Error(), "SECRET-TOKEN-xyz")

	_, err = installationServer(t, errorAPI(http.StatusBadGateway, "text/html", htmlErrorBody)).BranchExists(ctx, "77", "acme", "api", "main")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "text/html body")
	assert.NotContains(t, err.Error(), "SECRET-HTML-abc")
}

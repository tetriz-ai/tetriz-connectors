// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// The program syncs the token's repositories through the built-in GitHub
// gateway with no Redis and no database: here against a fake GitHub API.
func TestSyncReposEmbedsGitHubWithoutRedis(t *testing.T) {
	var authed atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Authorization"), "bearer github_pat_example") {
			authed.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4999")
		w.Header().Set("X-RateLimit-Reset", "4102444800")
		w.Header().Set("X-RateLimit-Resource", "core")
		if r.URL.Path != "/user/repos" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 101, "name": "api", "full_name": "octo/api", "private": true, "owner": map[string]any{"login": "octo"}, "default_branch": "main"},
			{"id": 102, "name": "web", "full_name": "octo/web", "owner": map[string]any{"login": "octo"}, "default_branch": "main"},
		})
	}))
	defer api.Close()

	cfg := gateway.DefaultConfig()
	for i, ep := range cfg.GitHub {
		ep.BaseURL = strings.Replace(ep.BaseURL, "https://api.github.com", api.URL, 1)
		cfg.GitHub[i] = ep
	}
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	repos, err := SyncRepos(context.Background(), "github_pat_example")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"octo/api", "octo/web"}, repos)
	assert.Positive(t, authed.Load(), "the requests carry the token")
}

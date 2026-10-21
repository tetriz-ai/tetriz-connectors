// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// TestEdge_SingleBranchWithSlashInName covers GET /repository/branches/{name} for a
// branch like "release/2026". GitLab takes the name URL-encoded as one segment
// (release%2F2026), and the connector's repo-tree walk sends it that way
// (url.PathEscape). The simulator must resolve it rather than 404.
func TestEdge_SingleBranchWithSlashInName(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{
			"id":9100101,
			"default_branch":"release/2026",
			"branches":[{"name":"release/2026","default":true,"commit":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}]
		}`),
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/branches/release%2F2026")
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var b struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(body, &b))
	assert.Equal(t, "release/2026", b.Name)
}

// TestEdge_FaultRulesApplyToProjectScopedRoutes covers the fault-injection groups on
// the project-scoped routes the connectors page through after listing projects (a
// project id encodes its group as id/100). Each must answer the rule's status with
// GitLab's {"message":…} body, and the 429 must carry Retry-After so a client backs off.
func TestEdge_FaultRulesApplyToProjectScopedRoutes(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	routes := []string{
		"/merge_requests?page=2",
		"/merge_requests/1/commits",
		"/repository/commits?page=3",
		"/repository/branches",
		"/repository/branches/main",
		"/repository/tree?recursive=true",
	}
	groups := []struct {
		projectID string
		status    int
	}{
		{"9190101", http.StatusForbidden},
		{"9190201", http.StatusTooManyRequests},
		{"9190301", http.StatusInternalServerError},
		{"9190401", http.StatusNotFound},
	}
	for _, g := range groups {
		for _, route := range routes {
			t.Run(g.projectID+route, func(t *testing.T) {
				resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/"+g.projectID+route)
				assert.Equal(t, g.status, resp.StatusCode)
				assertGitLabError(t, body)
				if g.status == http.StatusTooManyRequests {
					assert.Equal(t, "60", resp.Header.Get("Retry-After"))
					assert.Equal(t, "0", resp.Header.Get("RateLimit-Remaining"))
				} else {
					assert.Empty(t, resp.Header.Get("Retry-After"))
				}
			})
		}
	}
}

// TestEdge_ProjectIDWithZeroOrOutOfRangeRepoIndex covers project ids that do not
// decode to a catalog project: repo_index 0 and an id whose group is unknown.
func TestEdge_ProjectIDWithZeroOrOutOfRangeRepoIndex(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()
	for _, id := range []string{"9100100", "100", "-9100101", "99999999999999999999"} {
		resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/"+id+"/merge_requests")
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, id)
		assertGitLabError(t, body)
	}
}

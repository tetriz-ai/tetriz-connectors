// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// --- mock DAO ---

type mockDAO struct {
	// rows returned by ListPaged and ListAll (filtered by entityType in ListAll).
	rows []dao.Row
	// prByNumber is keyed by repo+number and returned by GetByPRNumber.
	prByNumber map[string]*dao.Row
	// repos returned by ListDistinctRepos.
	repos []string
}

func (m *mockDAO) ListPaged(_ context.Context, _, _, entityType, _ string, _ *time.Time, limit int) ([]dao.Row, error) {
	var out []dao.Row
	for _, r := range m.rows {
		if r.EntityType == entityType {
			out = append(out, r)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *mockDAO) ListAll(_ context.Context, _, _, entityType string) ([]dao.Row, error) {
	var out []dao.Row
	for _, r := range m.rows {
		if r.EntityType == entityType {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *mockDAO) ListDistinctRepos(_ context.Context, _, _ string) ([]string, error) {
	return m.repos, nil
}

func (m *mockDAO) GetByPRNumber(_ context.Context, _, _, repo string, number int) (*dao.Row, error) {
	key := fmt.Sprintf("%s/%d", repo, number)
	r := m.prByNumber[key]
	return r, nil
}

// --- helpers ---

func newTestServer(d dao.IDAO) *httptest.Server {
	svc := New(d)
	r := mux.NewRouter()
	svc.RegisterRoutes(r)
	return httptest.NewServer(r)
}

func prRow(payload string) dao.Row {
	return dao.Row{
		OrgName:    "test-org",
		EntityType: "pr",
		Payload:    []byte(payload),
		CursorVal:  time.Now().UTC(),
	}
}

func simplePayload() string {
	return `{"number":1,"title":"demo","additions":10,"updatedAt":"2026-06-01T10:00:00Z","repo":"widgets"}`
}

func repoPayload(name string) dao.Row {
	return dao.Row{
		EntityType: "repo",
		Payload:    []byte(fmt.Sprintf(`{"id":1,"name":%q,"full_name":"org/%s"}`, name, name)),
	}
}

// --- tests ---

func TestGraphQL_RulePermanentlyDenied(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	body := `{"query":"{ repository(owner: \"sim-perm-denied-org\", name: \"widgets\") { pullRequests(first:10) { nodes { additions } } } }"}`
	resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestGraphQL_PRs_ThreeRows(t *testing.T) {
	rows := []dao.Row{
		prRow(simplePayload()),
		prRow(simplePayload()),
		prRow(simplePayload()),
	}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	body := `{"query":"{ repository(owner: \"acme\", name: \"widgets\") { pullRequests(first:100) { nodes { additions } pageInfo { hasNextPage endCursor } } } }"}`
	resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Repository struct {
				PullRequests struct {
					Nodes    []json.RawMessage `json:"nodes"`
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"pullRequests"`
			} `json:"repository"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	prs := out.Data.Repository.PullRequests
	if len(prs.Nodes) != 3 {
		t.Errorf("nodes length = %d, want 3", len(prs.Nodes))
	}
	if prs.PageInfo.HasNextPage {
		t.Error("hasNextPage should be false for 3 rows with pageSize 100")
	}
}

// TestGraphQL_RepositoryDatabaseID_MatchesRepoEntity: the GraphQL repo response
// echoes the SAME databaseId the repo entity carries (the value handleRepos
// serves and the connector emits as the repository id), not a constant. A
// constant databaseId would make every pull request's platform_repo_id collide,
// so a consumer could never join a pull request to its repository.
func TestGraphQL_RepositoryDatabaseID_MatchesRepoEntity(t *testing.T) {
	const repoDBID = 6249873
	tests := []struct {
		name     string
		rows     []dao.Row
		wantDBID int64
	}{
		{
			name: "databaseId matches the seeded repo entity's id",
			rows: []dao.Row{
				{OrgName: "acme", EntityType: "repo", Payload: []byte(`{"id":6249873,"name":"widgets","full_name":"acme/widgets"}`)},
				prRow(simplePayload()),
			},
			wantDBID: repoDBID,
		},
		{
			// Multiple repos in one org: the response must echo the id of the
			// repo the query names (widgets), not just the first repo row — this
			// is the per-repo discrimination the whole fix exists to provide
			// (a name-agnostic regression would collide ids across repos).
			name: "picks the id of the queried repo among several",
			rows: []dao.Row{
				{OrgName: "acme", EntityType: "repo", Payload: []byte(`{"id":42,"name":"frontend","full_name":"acme/frontend"}`)},
				{OrgName: "acme", EntityType: "repo", Payload: []byte(`{"id":6249873,"name":"widgets","full_name":"acme/widgets"}`)},
				prRow(simplePayload()),
			},
			wantDBID: repoDBID,
		},
		{
			// Repo entities present but none named `widgets` → the loop-no-match
			// branch (distinct from the empty-rows path) → 0.
			name: "repos present but none match the query name yields 0",
			rows: []dao.Row{
				{OrgName: "acme", EntityType: "repo", Payload: []byte(`{"id":42,"name":"frontend","full_name":"acme/frontend"}`)},
				prRow(simplePayload()),
			},
			wantDBID: 0,
		},
		{
			// No repo entity → 0 (connector then omits platform_repo_id),
			// never the old colliding constant 1.
			name:     "no repo entity yields 0, not a constant",
			rows:     []dao.Row{prRow(simplePayload())},
			wantDBID: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(&mockDAO{rows: tt.rows})
			defer srv.Close()

			body := `{"query":"{ repository(owner: \"acme\", name: \"widgets\") { databaseId pullRequests(first:100) { nodes { additions } pageInfo { hasNextPage } } } }"}`
			resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			var out struct {
				Data struct {
					Repository struct {
						DatabaseID int64 `json:"databaseId"`
					} `json:"repository"`
				} `json:"data"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if out.Data.Repository.DatabaseID != tt.wantDBID {
				t.Errorf("repository.databaseId = %d, want %d", out.Data.Repository.DatabaseID, tt.wantDBID)
			}
		})
	}
}

func TestGraphQL_PRs_HasNextPage(t *testing.T) {
	// 101 rows, pageSize 100 → hasNextPage true
	rows := make([]dao.Row, 101)
	for i := range rows {
		rows[i] = prRow(simplePayload())
	}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	body := `{"query":"{ repository(owner: \"acme\", name: \"widgets\") { pullRequests(first:100) { nodes { additions } pageInfo { hasNextPage endCursor } } } }"}`
	resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out struct {
		Data struct {
			Repository struct {
				PullRequests struct {
					Nodes    []json.RawMessage `json:"nodes"`
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"pullRequests"`
			} `json:"repository"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	prs := out.Data.Repository.PullRequests
	if len(prs.Nodes) != 100 {
		t.Errorf("nodes length = %d, want 100", len(prs.Nodes))
	}
	if !prs.PageInfo.HasNextPage {
		t.Error("hasNextPage should be true for 101 rows with pageSize 100")
	}
	if prs.PageInfo.EndCursor == "" {
		t.Error("endCursor should be non-empty when hasNextPage is true")
	}
}

func TestGraphQL_SimEmpty_ReturnsValidEmptyResponse(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	body := `{"query":"{ repository(owner: \"sim-empty-corp\", name: \"widgets\") { pullRequests(first:100) { nodes { additions } pageInfo { hasNextPage } } } }"}`
	resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Repository struct {
				PullRequests struct {
					Nodes    []json.RawMessage `json:"nodes"`
					PageInfo struct {
						HasNextPage bool `json:"hasNextPage"`
					} `json:"pageInfo"`
				} `json:"pullRequests"`
			} `json:"repository"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	prs := out.Data.Repository.PullRequests
	if len(prs.Nodes) != 0 {
		t.Errorf("nodes length = %d, want 0", len(prs.Nodes))
	}
	if prs.PageInfo.HasNextPage {
		t.Error("hasNextPage should be false for empty response")
	}
}

func TestRepos_TwoRows(t *testing.T) {
	rows := []dao.Row{repoPayload("widgets"), repoPayload("frontend")}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/github/orgs/acme/repos")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 2 {
		t.Errorf("repos length = %d, want 2", len(out))
	}
}

func TestReviews_ReturnsReviewNodes(t *testing.T) {
	payload := `{"number":7,"reviews":{"nodes":[{"databaseId":101,"state":"APPROVED","submittedAt":"2026-06-01T10:00:00Z","author":{"login":"bob","databaseId":42}}]}}`
	prByNum := map[string]*dao.Row{
		"widgets/7": {EntityType: "pr", Payload: []byte(payload)},
	}
	srv := newTestServer(&mockDAO{prByNumber: prByNum})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/github/repos/acme/widgets/pulls/7/reviews")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("reviews length = %d, want 1", len(out))
	}
}

func TestReviews_NotFound_ReturnsEmpty(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/github/repos/acme/widgets/pulls/999/reviews")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("[]")) {
		t.Errorf("expected empty array, got: %s", body)
	}
}

func TestPRDiff_ReturnsDiffText(t *testing.T) {
	payload := `{"number":5,"diff":"diff --git a/main.go b/main.go\n--- a/main.go\n+++ b/main.go","commits":{"nodes":[{"oid":"abc123"}]}}`
	prByNum := map[string]*dao.Row{
		"widgets/5": {EntityType: "pr", Payload: []byte(payload)},
	}
	srv := newTestServer(&mockDAO{prByNumber: prByNum})
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/github/repos/acme/widgets/pulls/5", nil)
	req.Header.Set("Accept", "application/vnd.github.diff")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "diff --git") {
		t.Errorf("expected diff text, got: %s", body)
	}
}

func TestBranchSHA_ReturnsCommitSHA(t *testing.T) {
	rows := []dao.Row{
		{EntityType: "pr", Payload: []byte(`{"commits":{"nodes":[{"oid":"deadbeef1234567890"}]}}`), CursorVal: time.Now()},
	}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/github/repos/acme/widgets/branches/main")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Name   string `json:"name"`
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Name != "main" {
		t.Errorf("name = %q, want \"main\"", out.Name)
	}
	if out.Commit.SHA == "" {
		t.Error("commit SHA should be non-empty")
	}
}

func TestGitTree_ReturnsEntries(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/github/repos/acme/widgets/git/trees/abc123?recursive=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		SHA       string           `json:"sha"`
		Truncated bool             `json:"truncated"`
		Tree      []map[string]any `json:"tree"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.SHA != "abc123" {
		t.Errorf("sha = %q, want \"abc123\"", out.SHA)
	}
	if out.Truncated {
		t.Error("truncated should be false")
	}
	if len(out.Tree) == 0 {
		t.Error("tree should have at least one entry")
	}
}

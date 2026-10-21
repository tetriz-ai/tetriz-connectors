// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// mockDAO backs the httptest server, implementing the gitlab store interface.
// ListPageWindow mirrors the real query's entity_type + repo filter and
// offset/limit window; ListPageWindowByProjectID filters payload project_id;
// GetByProjectIDAndIID resolves per-MR nested routes.
type mockDAO struct {
	rows    []dao.Row
	prByKey map[string]*dao.Row // keyed "projectID/iid"
	err     error
}

func (m *mockDAO) ListPageWindow(_ context.Context, _, org, entityType, repo string, limit, offset int) ([]dao.Row, error) {
	if m.err != nil {
		return nil, m.err
	}
	var filtered []dao.Row
	for _, r := range m.rows {
		if r.EntityType != entityType {
			continue
		}
		if r.OrgName != "" && r.OrgName != org {
			continue
		}
		if repo != "" && payloadRepo(r.Payload) != repo {
			continue
		}
		filtered = append(filtered, r)
	}
	return windowRows(filtered, limit, offset), nil
}

func (m *mockDAO) ListPageWindowByProjectID(_ context.Context, _, org, entityType string, projectID int64, limit, offset int) ([]dao.Row, error) {
	if m.err != nil {
		return nil, m.err
	}
	var filtered []dao.Row
	for _, r := range m.rows {
		if r.EntityType != entityType {
			continue
		}
		if r.OrgName != "" && r.OrgName != org {
			continue
		}
		if payloadProjectID(r.Payload) != projectID {
			continue
		}
		filtered = append(filtered, r)
	}
	return windowRows(filtered, limit, offset), nil
}

func (m *mockDAO) GetByProjectIDAndIID(_ context.Context, _, _ string, projectID int64, iid int) (*dao.Row, error) {
	return m.prByKey[fmt.Sprintf("%d/%d", projectID, iid)], nil
}

func windowRows(filtered []dao.Row, limit, offset int) []dao.Row {
	if offset < 0 || offset >= len(filtered) {
		return nil
	}
	if limit <= 0 {
		return filtered[offset:]
	}
	end := min(offset+limit, len(filtered))
	return filtered[offset:end]
}

func payloadRepo(payload []byte) string {
	var p struct {
		Repo string `json:"repo"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.Repo
}

func payloadProjectID(payload []byte) int64 {
	var p struct {
		ProjectID int64 `json:"project_id"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.ProjectID
}

func newTestServer(d store) *httptest.Server {
	svc := New(d)
	r := mux.NewRouter()
	svc.RegisterRoutes(r)
	return httptest.NewServer(r)
}

func row(entityType, payload string) dao.Row {
	return dao.Row{EntityType: entityType, Payload: []byte(payload), CursorVal: time.Now().UTC()}
}

func orgRow(org, entityType, payload string) dao.Row {
	r := row(entityType, payload)
	r.OrgName = org
	return r
}

func getJSON(t *testing.T, url string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func decodeArray(t *testing.T, body []byte) []json.RawMessage {
	t.Helper()
	var arr []json.RawMessage
	if err := json.Unmarshal(body, &arr); err != nil {
		t.Fatalf("decode array: %v (%s)", err, body)
	}
	return arr
}

func TestGroupProjects_ReturnsProjectID9100101(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"id":9100101,"path":"ledger","path_with_namespace":"demo-gl-regulated/ledger","default_branch":"main"}`),
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91001/projects")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 1)
	var proj struct {
		ID int64 `json:"id"`
	}
	require.NoError(t, json.Unmarshal(arr[0], &proj))
	assert.Equal(t, int64(9100101), proj.ID)
}

func TestUnknownGroup_404(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/99999/projects")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assertGitLabError(t, body)
}

func TestRule_OrgPermDenied(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91901/projects")
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assertGitLabError(t, body)
}

func TestRule_RateLimitCarriesAllHeaders(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	before := time.Now().Unix()
	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91902/projects")
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assertGitLabError(t, body)
	assert.Equal(t, "60", resp.Header.Get("Retry-After"))
	assert.Equal(t, "0", resp.Header.Get("RateLimit-Remaining"))
	assert.Equal(t, "10", resp.Header.Get("RateLimit-Limit"))
	reset, err := strconv.ParseInt(resp.Header.Get("RateLimit-Reset"), 10, 64)
	require.NoError(t, err, "RateLimit-Reset must be a unix timestamp")
	assert.GreaterOrEqual(t, reset, before+60)
	assert.LessOrEqual(t, reset, before+70)
}

func TestPagination_FullPageSetsXNextPage_ShortPageOmits(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"id":9100101,"path":"ledger"}`),
		row("repo", `{"id":9100102,"path":"compliance"}`),
		row("repo", `{"id":9100103,"path":"reporting"}`),
	}})
	defer srv.Close()

	full, body1 := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91001/projects?page=1&per_page=2")
	require.Equal(t, http.StatusOK, full.StatusCode)
	assert.Equal(t, "2", full.Header.Get("X-Next-Page"))
	assert.Len(t, decodeArray(t, body1), 2)

	short, body2 := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91001/projects?page=2&per_page=2")
	require.Equal(t, http.StatusOK, short.StatusCode)
	assert.Empty(t, short.Header.Get("X-Next-Page"))
	assert.Len(t, decodeArray(t, body2), 1)
}

func TestSimEmpty_ListRouteReturnsBareArray(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"id":9190501,"path":"hidden"}`),
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91905/projects")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("X-Next-Page"))
	assert.Equal(t, "[]", string(bytes.TrimSpace(body)))
}

func TestSimEmpty_SingleProjectIs404(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9190501")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assertGitLabError(t, body)
}

func TestMRCommits_ByIIDFromNestedArray(t *testing.T) {
	pr := row("pr", `{"iid":1,"project_id":9100101,"commits":[
		{"id":"a1b2c3d4e5f678901234567890abcdef12345678","authored_date":"2026-06-17T08:00:00Z","message":"Add settlement flow"}
	]}`)
	srv := newTestServer(&mockDAO{prByKey: map[string]*dao.Row{"9100101/1": &pr}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests/1/commits")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 1)
	var c struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(arr[0], &c))
	assert.Equal(t, "a1b2c3d4e5f678901234567890abcdef12345678", c.ID)
	assert.Equal(t, "Add settlement flow", c.Message)

	missing, _ := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests/99/commits")
	assert.Equal(t, http.StatusNotFound, missing.StatusCode)
}

func TestGroupIssues_GitLabIssuesIncludeAuthor_EmptyWhenNoRows(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		orgRow("demo-gl-scaleup", "issue", `{"id":80001,"iid":7,"project_id":9100201,"title":"Fix rounding","author":{"id":91002001,"username":"alice"}}`),
	}})
	defer srv.Close()

	// gitlab-issues org (91002 / demo-gl-scaleup) returns issues with author.
	gl, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91002/issues")
	require.Equal(t, http.StatusOK, gl.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 1)
	var issue struct {
		Author struct {
			Username string `json:"username"`
		} `json:"author"`
	}
	require.NoError(t, json.Unmarshal(arr[0], &issue))
	assert.Equal(t, "alice", issue.Author.Username)

	// An org with no issue rows (91001 has none in this mock) returns a bare [].
	empty, emptyBody := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91001/issues")
	require.Equal(t, http.StatusOK, empty.StatusCode)
	assert.Equal(t, "[]", string(bytes.TrimSpace(emptyBody)))
}

func TestMRApprovals_ReturnsObjectNotArray(t *testing.T) {
	pr := row("pr", `{"iid":1,"project_id":9100101,"approvals":{"approved":true,"approved_by":[{"user":{"id":91001002,"username":"bob"}}]}}`)
	srv := newTestServer(&mockDAO{prByKey: map[string]*dao.Row{"9100101/1": &pr}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests/1/approvals")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var obj struct {
		Approved   bool `json:"approved"`
		ApprovedBy []struct {
			User struct {
				Username string `json:"username"`
			} `json:"user"`
		} `json:"approved_by"`
	}
	require.NoError(t, json.Unmarshal(body, &obj))
	assert.True(t, obj.Approved)
	require.Len(t, obj.ApprovedBy, 1)
	assert.Equal(t, "bob", obj.ApprovedBy[0].User.Username)
	// Must be an object, not a JSON array.
	assert.Equal(t, byte('{'), bytes.TrimSpace(body)[0])
}

func TestCommitDetail_ReturnsStats(t *testing.T) {
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("commit", `{"id":"`+sha+`","project_id":9100101,"stats":{"additions":10,"deletions":3,"total":13}}`),
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/commits/"+sha+"?stats=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		ID    string `json:"id"`
		Stats struct {
			Additions int `json:"additions"`
			Deletions int `json:"deletions"`
		} `json:"stats"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, sha, got.ID)
	assert.Equal(t, 10, got.Stats.Additions)
	assert.Equal(t, 3, got.Stats.Deletions)
	assert.Equal(t, byte('{'), bytes.TrimSpace(body)[0])

	missing, _ := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/repository/commits/deadbeef")
	assert.Equal(t, http.StatusNotFound, missing.StatusCode)
}

func TestMRList_UpdatedAfterAndSortAsc(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("pr", `{"iid":1,"project_id":9100101,"updated_at":"2026-01-01T00:00:00Z"}`),
		row("pr", `{"iid":2,"project_id":9100101,"updated_at":"2026-01-10T00:00:00Z"}`),
		row("pr", `{"iid":3,"project_id":9100101,"updated_at":"2026-01-20T00:00:00Z"}`),
	}})
	defer srv.Close()

	resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/projects/9100101/merge_requests?updated_after=2026-01-10T00:00:00Z&sort=asc&order_by=updated_at")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	arr := decodeArray(t, body)
	require.Len(t, arr, 2)
	var first, second struct {
		IID int `json:"iid"`
	}
	require.NoError(t, json.Unmarshal(arr[0], &first))
	require.NoError(t, json.Unmarshal(arr[1], &second))
	assert.Equal(t, 2, first.IID)
	assert.Equal(t, 3, second.IID)
}

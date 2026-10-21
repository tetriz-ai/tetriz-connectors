// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// mockStore backs the httptest server. ListIssuesAscending reproduces the real
// query's project + since filters, the (cursor_val, id) keyset resume, and the
// ascending order + limit, so pagination behaviour is exercised without a DB.
type mockStore struct {
	issues   []dao.IssueRow
	projects []dao.Row
	sprints  []dao.Row
	byKey    map[string]*dao.Row
}

func (m *mockStore) ListIssuesAscending(_ context.Context, _, _, project string, since, afterCursor *time.Time, afterID string, limit int) ([]dao.IssueRow, error) {
	var out []dao.IssueRow
	for _, r := range m.issues {
		if project != "" && issuePayloadProjectKey(r.Payload) != project {
			continue
		}
		if since != nil && r.CursorVal.Before(*since) {
			continue
		}
		if afterCursor != nil && afterID != "" && !keysetAfter(r.CursorVal, r.ID, *afterCursor, afterID) {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CursorVal.Equal(out[j].CursorVal) {
			return out[i].ID < out[j].ID
		}
		return out[i].CursorVal.Before(out[j].CursorVal)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *mockStore) ListAll(_ context.Context, _, _, entityType string) ([]dao.Row, error) {
	switch entityType {
	case "project":
		return m.projects, nil
	case "sprint":
		return m.sprints, nil
	default:
		return nil, nil
	}
}

func (m *mockStore) GetByIssueKey(_ context.Context, _, _, key string) (*dao.Row, error) {
	return m.byKey[key], nil
}

func keysetAfter(cv time.Time, id string, acv time.Time, aid string) bool {
	if cv.After(acv) {
		return true
	}
	if cv.Equal(acv) {
		return id > aid
	}
	return false
}

func issuePayloadProjectKey(payload []byte) string {
	var p struct {
		Fields struct {
			Project struct {
				Key string `json:"key"`
			} `json:"project"`
		} `json:"fields"`
	}
	_ = json.Unmarshal(payload, &p) //nolint:errcheck // test helper; malformed payload → empty key
	return p.Fields.Project.Key
}

func newTestServer(d store) *httptest.Server {
	svc := New(d)
	r := mux.NewRouter()
	svc.RegisterRoutes(r)
	return httptest.NewServer(r)
}

// issueRow builds a PLAT issue row with the given key, id, and cursor timestamp.
func issueRow(key, id string, cursor time.Time) dao.IssueRow {
	payload := fmt.Sprintf(`{"key":%q,"fields":{"project":{"key":"PLAT"},"updated":%q}}`, key, cursor.Format("2006-01-02T15:04:05.000-0700"))
	return dao.IssueRow{Row: dao.Row{EntityType: "issue", Payload: []byte(payload), CursorVal: cursor}, ID: id}
}

// searchResp mirrors the connector's searchIssuesVendorResponse unmarshal — the
// contract the simulator must satisfy.
type searchResp struct {
	Issues        []json.RawMessage `json:"issues"`
	MaxResults    int               `json:"maxResults"`
	NextPageToken string            `json:"nextPageToken"`
}

func postSearch(t *testing.T, srvURL, cloudID, jql, token string, maxResults int) (*http.Response, searchResp) {
	t.Helper()
	body := map[string]any{"jql": jql, "maxResults": maxResults}
	if token != "" {
		body["nextPageToken"] = token
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(srvURL+"/jira/ex/jira/"+cloudID+"/rest/api/3/search/jql", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	var sr searchResp
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
			t.Fatalf("decode search response: %v", err)
		}
	}
	return resp, sr
}

const searchJQL = `project = PLAT AND updated >= "2026-06-01 10:00" ORDER BY updated ASC`

func TestSearch_PermDenied(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()
	resp, _ := postSearch(t, srv.URL, "acme-sim-perm-denied", searchJQL, "", 100)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	var env struct {
		ErrorMessages []string       `json:"errorMessages"`
		Errors        map[string]any `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if len(env.ErrorMessages) == 0 || env.Errors == nil {
		t.Errorf("want Jira error envelope, got %+v", env)
	}
}

func TestSearch_ThreeRows(t *testing.T) {
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	srv := newTestServer(&mockStore{issues: []dao.IssueRow{
		issueRow("PLAT-1", "a", base),
		issueRow("PLAT-2", "b", base.Add(time.Hour)),
		issueRow("PLAT-3", "c", base.Add(2*time.Hour)),
	}})
	defer srv.Close()
	resp, sr := postSearch(t, srv.URL, "demo-gh-enterprise", searchJQL, "", 100)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(sr.Issues) != 3 {
		t.Errorf("issues = %d, want 3", len(sr.Issues))
	}
	if sr.NextPageToken != "" {
		t.Errorf("short page must not carry a nextPageToken, got %q", sr.NextPageToken)
	}
}

func TestSearch_FullPageThenResume_NoOverlap(t *testing.T) {
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	rows := []dao.IssueRow{
		issueRow("PLAT-1", "a", base),
		issueRow("PLAT-2", "b", base.Add(time.Hour)),
		issueRow("PLAT-3", "c", base.Add(2*time.Hour)),
		issueRow("PLAT-4", "d", base.Add(3*time.Hour)),
	}
	srv := newTestServer(&mockStore{issues: rows})
	defer srv.Close()

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		resp, sr := postSearch(t, srv.URL, "demo-gh-enterprise", searchJQL, token, 2)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		for _, raw := range sr.Issues {
			key := issuePayloadProjectKeyFull(raw)
			if seen[key] {
				t.Fatalf("overlap: issue %s returned on more than one page", key)
			}
			seen[key] = true
		}
		pages++
		if sr.NextPageToken == "" {
			break
		}
		token = sr.NextPageToken
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 4 {
		t.Errorf("distinct issues across pages = %d, want 4", len(seen))
	}
}

func issuePayloadProjectKeyFull(raw json.RawMessage) string {
	var p struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(raw, &p) //nolint:errcheck // test helper
	return p.Key
}

func TestSearch_EmptyRule(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()
	resp, sr := postSearch(t, srv.URL, "demo-sim-empty", searchJQL, "", 100)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (sim-empty is a valid empty success)", resp.StatusCode)
	}
	if len(sr.Issues) != 0 || sr.NextPageToken != "" {
		t.Errorf("sim-empty search = %+v, want empty issues, no token", sr)
	}
}

type projectSearchResp struct {
	Values []json.RawMessage `json:"values"`
	Total  int               `json:"total"`
	IsLast bool              `json:"isLast"`
}

func getProjectSearch(t *testing.T, srvURL, cloudID, query string) (*http.Response, projectSearchResp) {
	t.Helper()
	resp, err := http.Get(srvURL + "/jira/ex/jira/" + cloudID + "/rest/api/3/project/search" + query)
	if err != nil {
		t.Fatal(err)
	}
	var pr projectSearchResp
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
			t.Fatalf("decode project response: %v", err)
		}
	}
	return resp, pr
}

func TestProjectSearch_TwoRows(t *testing.T) {
	srv := newTestServer(&mockStore{projects: []dao.Row{
		{EntityType: "project", Payload: []byte(`{"key":"PLAT","name":"Platform"}`)},
		{EntityType: "project", Payload: []byte(`{"key":"ENG","name":"Engineering"}`)},
	}})
	defer srv.Close()
	resp, pr := getProjectSearch(t, srv.URL, "demo-gh-enterprise", "?startAt=0&maxResults=50")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(pr.Values) != 2 || pr.Total != 2 || !pr.IsLast {
		t.Errorf("project search = %+v, want 2 values, total 2, isLast true", pr)
	}
}

func TestProjectSearch_EmptyRule(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()
	resp, pr := getProjectSearch(t, srv.URL, "org-sim-empty", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(pr.Values) != 0 || pr.Total != 0 || !pr.IsLast {
		t.Errorf("sim-empty project search = %+v, want empty values, total 0, isLast true", pr)
	}
}

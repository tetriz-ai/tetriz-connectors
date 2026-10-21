// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// --- mock DAO ---

// mockDAO backs the httptest server, implementing the bitbucket store interface.
// ListPageWindow mirrors the real query's entity_type + repo filter and offset/limit
// window so pagination behaviour is exercised; GetByPRID resolves the per-PR routes.
type mockDAO struct {
	rows         []dao.Row           // filtered by entity_type (+ payload->>'repo') in ListPageWindow
	prByID       map[string]*dao.Row // keyed "repo/id"
	commitByHash map[string]*dao.Row // keyed "repo/hash"
}

func (m *mockDAO) ListPageWindow(_ context.Context, _, _, entityType, repo string, limit, offset int) ([]dao.Row, error) {
	var filtered []dao.Row
	for _, r := range m.rows {
		if r.EntityType != entityType {
			continue
		}
		if repo != "" && payloadRepo(r.Payload) != repo {
			continue
		}
		filtered = append(filtered, r)
	}
	if offset < 0 || offset >= len(filtered) {
		return nil, nil
	}
	end := min(offset+limit, len(filtered))
	return filtered[offset:end], nil
}

func (m *mockDAO) GetByPRID(_ context.Context, _, _, repo string, id int) (*dao.Row, error) {
	return m.prByID[fmt.Sprintf("%s/%d", repo, id)], nil
}

func (m *mockDAO) GetByCommitHash(_ context.Context, _, _, repo, hash string) (*dao.Row, error) {
	return m.commitByHash[fmt.Sprintf("%s/%s", repo, hash)], nil
}

func payloadRepo(payload []byte) string {
	var p struct {
		Repo string `json:"repo"`
	}
	_ = json.Unmarshal(payload, &p)
	return p.Repo
}

// --- helpers ---

func newTestServer(d store) *httptest.Server {
	svc := New(d)
	r := mux.NewRouter()
	svc.RegisterRoutes(r)
	return httptest.NewServer(r)
}

func row(entityType, payload string) dao.Row {
	return dao.Row{EntityType: entityType, Payload: []byte(payload), CursorVal: time.Now().UTC()}
}

type listEnvelope struct {
	Pagelen int               `json:"pagelen"`
	Values  []json.RawMessage `json:"values"`
	Page    int               `json:"page"`
	Size    int               `json:"size"`
	Next    string            `json:"next"`
}

func getEnvelope(t *testing.T, url string) (*http.Response, listEnvelope) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	var env listEnvelope
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
	}
	_ = resp.Body.Close()
	return resp, env
}

// --- tests ---

func TestRule_WorkspacePermDenied(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/sim-perm-denied-org")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	assertBitbucketErrorEnvelope(t, body)
}

func TestRule_RateLimitCarriesRetryAfter(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/sim-rate-limit-org")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q, want \"60\"", got)
	}
}

func TestRepos_ListNoNext(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"repo":"ledger-core","slug":"ledger-core"}`),
		row("repo", `{"repo":"ledger","slug":"ledger"}`),
	}})
	defer srv.Close()

	resp, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if env.Size != 2 || len(env.Values) != 2 {
		t.Errorf("size = %d, values = %d, want 2/2", env.Size, len(env.Values))
	}
	if env.Next != "" {
		t.Errorf("next should be absent for a partial page, got %q", env.Next)
	}
}

func TestPRList_PaginationOffsetWindowAndNext(t *testing.T) {
	rows := []dao.Row{
		row("pr", `{"repo":"widgets","id":1}`),
		row("pr", `{"repo":"widgets","id":2}`),
		row("pr", `{"repo":"widgets","id":3}`),
		row("pr", `{"repo":"other","id":9}`), // different repo — must be filtered out
	}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	// page 1, pagelen 2 → full page → next present
	_, p1 := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests?page=1&pagelen=2")
	if p1.Size != 2 || p1.Next == "" {
		t.Errorf("page1: size = %d next = %q, want size 2 with next", p1.Size, p1.Next)
	}
	// page 2, pagelen 2 → the offset window returns the last row → no next
	_, p2 := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests?page=2&pagelen=2")
	if p2.Size != 1 || p2.Next != "" {
		t.Errorf("page2: size = %d next = %q, want size 1 no next", p2.Size, p2.Next)
	}
	var got struct {
		ID int `json:"id"`
	}
	_ = json.Unmarshal(p2.Values[0], &got)
	if got.ID != 3 {
		t.Errorf("page2 row id = %d, want 3 (the offset window)", got.ID)
	}
}

func TestSingleRepo_FoundUnknownAndEmpty(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"repo":"ledger-core","mainbranch":{"name":"develop"}}`),
	}})
	defer srv.Close()

	// found → the stored repo object (not a list envelope)
	resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/ledger-core")
	if err != nil {
		t.Fatal(err)
	}
	var repoObj struct {
		Repo       string `json:"repo"`
		Mainbranch struct {
			Name string `json:"name"`
		} `json:"mainbranch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&repoObj); err != nil {
		t.Fatalf("decode: %v", err)
	}
	_ = resp.Body.Close()
	if repoObj.Mainbranch.Name != "develop" {
		t.Errorf("mainbranch = %q, want develop", repoObj.Mainbranch.Name)
	}

	// unknown → 404
	resp2, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/nope")
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("unknown repo status = %d, want 404", resp2.StatusCode)
	}
	_ = resp2.Body.Close()

	// sim-empty → empty object, not an error, not a list envelope
	resp3, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/sim-empty-corp/ledger-core")
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("sim-empty status = %d, want 200", resp3.StatusCode)
	}
	body, _ := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	if string(bytes.TrimSpace(body)) != "{}" {
		t.Errorf("sim-empty single-repo body = %s, want {}", body)
	}
}

func TestSimEmpty_ListRouteReturnsEmptyEnvelope(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	resp, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/sim-empty-corp/widgets/pullrequests")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if env.Size != 0 || len(env.Values) != 0 || env.Next != "" {
		t.Errorf("sim-empty envelope = %+v, want empty values, size 0, no next", env)
	}
}

func TestPRDiffstat_ByIDAndUnknown(t *testing.T) {
	pr := row("pr", `{"repo":"widgets","id":42,"diffstat":[{"status":"modified","lines_added":80}]}`)
	srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/42": &pr}})
	defer srv.Close()

	resp, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/diffstat")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if env.Size != 1 {
		t.Errorf("diffstat size = %d, want 1", env.Size)
	}

	// unknown PR id → 404
	resp2, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/pullrequests/999/diffstat")
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("unknown-id status = %d, want 404", resp2.StatusCode)
	}
	_ = resp2.Body.Close()
}

func TestPRCommits_SortByDateAscending(t *testing.T) {
	// Stored newest-first; sort=date must return oldest-first.
	pr := row("pr", `{"repo":"widgets","id":42,"commits":[
		{"hash":"newest","date":"2026-06-20T10:00:00Z"},
		{"hash":"oldest","date":"2026-06-01T10:00:00Z"},
		{"hash":"mid","date":"2026-06-10T10:00:00Z"}
	]}`)
	srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/42": &pr}})
	defer srv.Close()

	// Without sort → stored order (newest first).
	_, stored := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/commits")
	if h := commitHash(stored.Values[0]); h != "newest" {
		t.Errorf("no-sort first commit = %q, want newest (stored order)", h)
	}

	// sort=date → ascending; first is the oldest.
	_, sorted := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/commits?sort=date")
	wantOrder := []string{"oldest", "mid", "newest"}
	for i, want := range wantOrder {
		if h := commitHash(sorted.Values[i]); h != want {
			t.Errorf("sort=date [%d] = %q, want %q", i, h, want)
		}
	}

	// The first-commit enrichment path: sort=date&pagelen=1 → exactly the oldest.
	_, first := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/commits?sort=date&pagelen=1")
	if first.Size != 1 || commitHash(first.Values[0]) != "oldest" {
		t.Errorf("enrichment page: size = %d first = %q, want size 1 / oldest", first.Size, commitHash(first.Values[0]))
	}
}

func TestPRCommits_SortByDate_UndatedSortsLast(t *testing.T) {
	// A commit with a missing/garbage date must sort LAST under sort=date, so the
	// enrichment reads a real oldest commit — never an undated one — for first_commit_at.
	pr := row("pr", `{"repo":"widgets","id":7,"commits":[
		{"hash":"dated-new","date":"2026-06-20T10:00:00Z"},
		{"hash":"no-date"},
		{"hash":"garbage-date","date":"not-a-timestamp"},
		{"hash":"dated-old","date":"2026-06-01T10:00:00Z"}
	]}`)
	srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/7": &pr}})
	defer srv.Close()

	_, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/7/commits?sort=date")
	if env.Size != 4 {
		t.Fatalf("size = %d, want 4", env.Size)
	}
	if h := commitHash(env.Values[0]); h != "dated-old" {
		t.Errorf("first commit = %q, want dated-old (oldest real date)", h)
	}
	// The two undated entries sort last (after both dated ones), preserving order.
	last := []string{commitHash(env.Values[2]), commitHash(env.Values[3])}
	for _, h := range last {
		if h != "no-date" && h != "garbage-date" {
			t.Errorf("expected undated entries last, got %q in the tail", h)
		}
	}
}

func commitHash(raw json.RawMessage) string {
	var c struct {
		Hash string `json:"hash"`
	}
	_ = json.Unmarshal(raw, &c)
	return c.Hash
}

// --- Commit, pull request activity, member and group handler tests ---

func TestCommitsList_RepoScopedPaginated(t *testing.T) {
	rows := []dao.Row{
		row("commit", `{"repo":"widgets","hash":"c1"}`),
		row("commit", `{"repo":"widgets","hash":"c2"}`),
		row("commit", `{"repo":"widgets","hash":"c3"}`),
		row("commit", `{"repo":"other","hash":"x9"}`), // different repo — filtered out
	}
	srv := newTestServer(&mockDAO{rows: rows})
	defer srv.Close()

	_, all := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/commits")
	if all.Size != 3 || all.Next != "" {
		t.Errorf("commits list: size = %d next = %q, want 3 / no next", all.Size, all.Next)
	}
	_, p1 := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/commits?page=1&pagelen=2")
	if p1.Size != 2 || p1.Next == "" {
		t.Errorf("commits page1: size = %d next = %q, want 2 with next", p1.Size, p1.Next)
	}
}

func TestCommitDiffstat_ByShaAndUnknown(t *testing.T) {
	commit := row("commit", `{"repo":"widgets","hash":"abc123","diffstat":[{"status":"modified","lines_added":9}]}`)
	srv := newTestServer(&mockDAO{commitByHash: map[string]*dao.Row{"widgets/abc123": &commit}})
	defer srv.Close()

	resp, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/diffstat/abc123")
	if resp.StatusCode != http.StatusOK || env.Size != 1 {
		t.Errorf("commit diffstat: status = %d size = %d, want 200 / 1", resp.StatusCode, env.Size)
	}
	resp2, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/diffstat/deadbeef")
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("unknown sha status = %d, want 404", resp2.StatusCode)
	}
	_ = resp2.Body.Close()
}

func TestPRActivityAndComments_ExtractedByID(t *testing.T) {
	pr := row("pr", `{"repo":"widgets","id":42,
		"activity":[{"approval":{"date":"2026-06-20T10:30:00Z"}},{"comment":{"id":7001}}],
		"comments":[{"id":7001,"created_on":"2026-06-19T15:00:00Z"}]}`)
	srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/42": &pr}})
	defer srv.Close()

	_, act := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/activity")
	if act.Size != 2 {
		t.Errorf("activity size = %d, want 2", act.Size)
	}
	_, com := getEnvelope(t, srv.URL+"/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/comments")
	if com.Size != 1 {
		t.Errorf("comments size = %d, want 1", com.Size)
	}
	// Unknown PR id → 404 (shared per-PR path).
	resp, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/pullrequests/999/activity")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown-id activity status = %d, want 404", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestMembers_ListAndSimEmpty(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("member", `{"user":{"account_id":"alice-acct","display_name":"Alice"}}`),
		row("member", `{"user":{"account_id":"bob-acct","display_name":"Bob"}}`),
	}})
	defer srv.Close()

	_, env := getEnvelope(t, srv.URL+"/bitbucket/2.0/workspaces/acme/members")
	if env.Size != 2 {
		t.Errorf("members size = %d, want 2", env.Size)
	}
	_, empty := getEnvelope(t, srv.URL+"/bitbucket/2.0/workspaces/sim-empty-corp/members")
	if empty.Size != 0 || len(empty.Values) != 0 {
		t.Errorf("sim-empty members = %+v, want empty envelope", empty)
	}
}

func TestGroups_AlwaysBareEmptyArray(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()

	// Normal + sim-empty both return a bare [] (not a list envelope).
	for _, ws := range []string{"acme", "sim-empty-corp"} {
		resp, err := http.Get(srv.URL + "/bitbucket/1.0/groups/" + ws + "/")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("groups[%s] status = %d, want 200", ws, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(bytes.TrimSpace(body)) != "[]" {
			t.Errorf("groups[%s] body = %s, want []", ws, body)
		}
	}
	// An error keyword still injects the Bitbucket error envelope.
	resp, _ := http.Get(srv.URL + "/bitbucket/1.0/groups/sim-perm-denied-org/")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("groups error status = %d, want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assertBitbucketErrorEnvelope(t, body)
}

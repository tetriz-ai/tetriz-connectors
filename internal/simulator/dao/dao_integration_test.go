// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// testPool is the pgx pool over a leased simulator test database, initialised
// once in TestMain. The sim DAO is pgx-based (not gorm), so we take the leased
// DB's DSN and open our own pool against it.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("simulator dao", err)
		os.Exit(0)
	}

	ctx := context.Background()
	_, leasedDSN, cleanup, err := testdb.Setup(ctx, testdb.Simulator)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulator dao test schema load failed: %v\n", err)
		os.Exit(1)
	}
	dsn := leasedDSN
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pgx pool: %v\n", err)
		os.Exit(1)
	}

	testPool = pool
	code := testguard.Run(m)
	pool.Close()
	cleanup()
	os.Exit(code)
}

func insertIssue(t *testing.T, org, key, projectKey string, cursor time.Time) {
	t.Helper()
	payload := fmt.Sprintf(`{"key":%q,"fields":{"project":{"key":%q},"updated":%q}}`,
		key, projectKey, cursor.Format("2006-01-02T15:04:05.000-0700"))
	_, err := testPool.Exec(context.Background(),
		`INSERT INTO sim_records (vendor, org_name, entity_type, payload, cursor_val, entity_id)
		 VALUES ('jira', $1, 'issue', $2::jsonb, $3, $4)`,
		org, payload, cursor, key)
	if err != nil {
		t.Fatalf("insert issue %s: %v", key, err)
	}
}

func issueKeyOf(t *testing.T, payload []byte) string {
	t.Helper()
	var p struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("decode issue key: %v", err)
	}
	return p.Key
}

// TestListIssuesAscending_KeysetWalk drives the real ascending-keyset query end to
// end — the guardrail for this query's one novel risk, which the
// handler tests (against a mock) cannot exercise. It asserts the project filter,
// the inclusive `since` lower bound, ascending order, and no-overlap/no-gap keyset
// resume across `(cursor_val, id)` — including an equal-cursor_val tie resolved by
// the id tiebreak, and the `@afterID::uuid` cast against the UUID primary key.
func TestListIssuesAscending_KeysetWalk(t *testing.T) {
	ctx := context.Background()
	const org = "demo-gh-enterprise-keysetwalk" // unique org so parallel/other rows can't bleed in
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)

	// Four PLAT issues, the last two sharing a cursor_val (tie → id tiebreak).
	insertIssue(t, org, "PLAT-1", "PLAT", base)
	insertIssue(t, org, "PLAT-2", "PLAT", base.Add(1*time.Hour))
	insertIssue(t, org, "PLAT-3", "PLAT", base.Add(2*time.Hour))
	insertIssue(t, org, "PLAT-4", "PLAT", base.Add(2*time.Hour))
	// Excluded by the project filter, and by the since bound, respectively.
	insertIssue(t, org, "ENG-1", "ENG", base.Add(30*time.Minute))
	insertIssue(t, org, "PLAT-0", "PLAT", base.Add(-48*time.Hour))

	d := dao.New(testPool)
	since := base.Add(-1 * time.Hour) // includes base..base+2h, excludes PLAT-0

	seen := map[string]bool{}
	var lastCursor *time.Time
	var lastID string
	var prevCursor time.Time
	pages := 0
	for {
		rows, err := d.ListIssuesAscending(ctx, "jira", org, "PLAT", &since, lastCursor, lastID, 2)
		if err != nil {
			t.Fatalf("ListIssuesAscending page %d: %v", pages, err)
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			key := issueKeyOf(t, r.Payload)
			if seen[key] {
				t.Fatalf("overlap: %s returned on more than one page", key)
			}
			seen[key] = true
			if !r.CursorVal.Before(prevCursor) {
				prevCursor = r.CursorVal
			} else {
				t.Errorf("out-of-order: %s cursor %v precedes previous %v", key, r.CursorVal, prevCursor)
			}
		}
		last := rows[len(rows)-1]
		lastCursor = &last.CursorVal
		lastID = last.ID
		pages++
		if len(rows) < 2 {
			break
		}
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	want := []string{"PLAT-1", "PLAT-2", "PLAT-3", "PLAT-4"}
	if len(seen) != len(want) {
		t.Fatalf("saw %d distinct issues %v, want %d %v", len(seen), keys(seen), len(want), want)
	}
	for _, k := range want {
		if !seen[k] {
			t.Errorf("missing issue %s", k)
		}
	}
	if seen["ENG-1"] {
		t.Error("ENG-1 leaked past the project filter")
	}
	if seen["PLAT-0"] {
		t.Error("PLAT-0 leaked past the since lower bound")
	}
}

func TestGetByIssueKey(t *testing.T) {
	ctx := context.Background()
	const org = "demo-gh-enterprise-getbykey"
	insertIssue(t, org, "PLAT-42", "PLAT", time.Date(2026, 6, 20, 11, 0, 0, 0, time.UTC))

	d := dao.New(testPool)
	got, err := d.GetByIssueKey(ctx, "jira", org, "PLAT-42")
	if err != nil {
		t.Fatalf("GetByIssueKey: %v", err)
	}
	if got == nil || issueKeyOf(t, got.Payload) != "PLAT-42" {
		t.Fatalf("GetByIssueKey = %v, want the PLAT-42 row", got)
	}

	missing, err := d.GetByIssueKey(ctx, "jira", org, "NOPE-1")
	if err != nil {
		t.Fatalf("GetByIssueKey(missing): %v", err)
	}
	if missing != nil {
		t.Errorf("GetByIssueKey(missing) = %v, want nil (optional-lookup contract)", missing)
	}
}

func insertGitLabPR(t *testing.T, org string, projectID int64, iid int, cursor time.Time) {
	t.Helper()
	entityID := fmt.Sprintf("mr-%d!%d", projectID, iid)
	payload := fmt.Sprintf(`{"project_id":%d,"iid":%d}`, projectID, iid)
	_, err := testPool.Exec(context.Background(),
		`INSERT INTO sim_records (vendor, org_name, entity_type, payload, cursor_val, entity_id)
		 VALUES ('gitlab', $1, 'pr', $2::jsonb, $3, $4)`,
		org, payload, cursor, entityID)
	if err != nil {
		t.Fatalf("insert gitlab pr %s: %v", entityID, err)
	}
}

func gitlabPRKeys(t *testing.T, payload []byte) (projectID int64, iid int) {
	t.Helper()
	var p struct {
		ProjectID int64 `json:"project_id"`
		IID       int   `json:"iid"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("decode gitlab pr: %v", err)
	}
	return p.ProjectID, p.IID
}

// TestListPageWindowByProjectID_FiltersProjectID is a SQL guardrail:
// ListPageWindow filters payload->>'repo', which GitLab
// payloads do not carry. These rows have project_id and no repo key — a reuse
// of ListPageWindow would return every MR in the org, not one project's.
func TestListPageWindowByProjectID_FiltersProjectID(t *testing.T) {
	ctx := context.Background()
	const org = "demo-gl-dao-project-window"
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	const projectA int64 = 9100101
	const projectB int64 = 9100102

	insertGitLabPR(t, org, projectA, 1, base)
	insertGitLabPR(t, org, projectA, 2, base.Add(time.Hour))
	insertGitLabPR(t, org, projectB, 1, base.Add(2*time.Hour))

	d := dao.New(testPool)
	rows, err := d.ListPageWindowByProjectID(ctx, "gitlab", org, "pr", projectA, 10, 0)
	if err != nil {
		t.Fatalf("ListPageWindowByProjectID: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows for project A, want 2 (B must not leak)", len(rows))
	}
	seen := map[int]bool{}
	for _, r := range rows {
		_, iid := gitlabPRKeys(t, r.Payload)
		seen[iid] = true
	}
	if !seen[1] || !seen[2] {
		t.Errorf("project A iids = %v, want {1,2}", keysInt(seen))
	}

	page1, err := d.ListPageWindowByProjectID(ctx, "gitlab", org, "pr", projectA, 1, 0)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	page2, err := d.ListPageWindowByProjectID(ctx, "gitlab", org, "pr", projectA, 1, 1)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page1) != 1 || len(page2) != 1 {
		t.Fatalf("window pages = %d/%d, want 1/1", len(page1), len(page2))
	}
	p1, i1 := gitlabPRKeys(t, page1[0].Payload)
	p2, i2 := gitlabPRKeys(t, page2[0].Payload)
	if p1 == p2 && i1 == i2 {
		t.Error("offset window returned the same row twice")
	}

	all, err := d.ListPageWindowByProjectID(ctx, "gitlab", org, "pr", projectA, 0, 0)
	if err != nil {
		t.Fatalf("unbounded ListPageWindowByProjectID: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("limit=0 returned %d rows, want 2 (full project set)", len(all))
	}
}

func TestGetByProjectIDAndIID(t *testing.T) {
	ctx := context.Background()
	const org = "demo-gl-dao-get-iid"
	const projectID int64 = 9100201
	insertGitLabPR(t, org, projectID, 7, time.Date(2026, 6, 20, 11, 0, 0, 0, time.UTC))
	insertGitLabPR(t, org, projectID+1, 7, time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC))

	d := dao.New(testPool)
	got, err := d.GetByProjectIDAndIID(ctx, "gitlab", org, projectID, 7)
	if err != nil {
		t.Fatalf("GetByProjectIDAndIID: %v", err)
	}
	if got == nil {
		t.Fatal("GetByProjectIDAndIID = nil, want iid 7")
	}
	gotProject, gotIID := gitlabPRKeys(t, got.Payload)
	if gotProject != projectID || gotIID != 7 {
		t.Fatalf("GetByProjectIDAndIID = project %d iid %d, want project %d iid 7", gotProject, gotIID, projectID)
	}

	missing, err := d.GetByProjectIDAndIID(ctx, "gitlab", org, projectID, 99)
	if err != nil {
		t.Fatalf("GetByProjectIDAndIID(missing iid): %v", err)
	}
	if missing != nil {
		t.Errorf("GetByProjectIDAndIID(missing) = %v, want nil (optional-lookup contract)", missing)
	}
}

func keysInt(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ListOrgs lists each organization holding a repository once, sorted, and
// skips one that holds only other entities.
func TestListOrgs(t *testing.T) {
	ctx := context.Background()
	const vendor = "dao-list-orgs"
	for _, r := range []struct{ org, entity, id string }{
		{"org-b", "repo", "r1"}, {"org-a", "repo", "r2"}, {"org-a", "repo", "r3"}, {"org-c", "pr", "p1"},
	} {
		if _, err := testPool.Exec(ctx,
			`INSERT INTO sim_records (vendor, org_name, entity_type, payload, cursor_val, entity_id)
			 VALUES ($1, $2, $3, '{}'::jsonb, now(), $4)`, vendor, r.org, r.entity, r.id); err != nil {
			t.Fatalf("insert %v: %v", r, err)
		}
	}
	got, err := dao.New(testPool).ListOrgs(ctx, vendor)
	if err != nil {
		t.Fatalf("ListOrgs: %v", err)
	}
	if fmt.Sprint(got) != "[org-a org-b]" {
		t.Fatalf("ListOrgs = %v, want [org-a org-b]", got)
	}
}

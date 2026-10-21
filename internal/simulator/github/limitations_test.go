// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

// Tests of the vendor behaviour the simulator reproduces for the merge-method
// query, branch listings, commit history since and paging, and an unknown
// organization.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// gapDAO holds one organization, acme, with one repository, widgets, and its
// pull requests, and knows which organizations exist.
type gapDAO struct {
	mockDAO
	prs []dao.Row
}

func (d *gapDAO) ListAll(_ context.Context, _, org, entityType string) ([]dao.Row, error) {
	if org != "acme" || entityType != "repo" {
		return nil, nil
	}
	return []dao.Row{{OrgName: "acme", EntityType: "repo", Payload: []byte(`{"id":4242,"name":"widgets","full_name":"acme/widgets","default_branch":"main"}`)}}, nil
}

func (d *gapDAO) ListPaged(_ context.Context, _, org, entityType, _ string, _ *time.Time, _ int) ([]dao.Row, error) {
	if org != "acme" || entityType != "pr" {
		return nil, nil
	}
	return d.prs, nil
}

func (d *gapDAO) OrgExists(_ context.Context, _, org string) (bool, error) { return org == "acme", nil }

// gapPR builds a pull request row whose commits are committed at the given
// hours of 2026-01-01.
func gapPR(number int, state, head string, hours ...int) dao.Row {
	var commits []string
	for _, h := range hours {
		commits = append(commits, fmt.Sprintf(`{"oid":"%040d","committedDate":%q}`, number*100+h,
			time.Date(2026, 1, 1, h, 0, 0, 0, time.UTC).Format(time.RFC3339)))
	}
	return dao.Row{OrgName: "acme", EntityType: "pr", Payload: fmt.Appendf(nil,
		`{"number":%d,"state":%q,"headRefName":%q,"repo":"widgets","commits":{"nodes":[%s]}}`,
		number, state, head, strings.Join(commits, ","))}
}

func newGapDAO() *gapDAO {
	return &gapDAO{prs: []dao.Row{
		gapPR(3, "OPEN", "feature/c", 9, 10),
		gapPR(2, "MERGED", "feature/b", 5, 6),
		gapPR(1, "OPEN", "feature/a", 1, 2),
	}}
}

func postGraphQL(t *testing.T, base, query string, out any) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"/github/graphql", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode
}

// The simulator answers the GitHub connector's merge-method query, so
// simulated repositories carry the three toggles. An unknown repository gets
// a null repository and a NOT_FOUND error, as on GitHub.
func TestMergeMethodQueryIsAnswered(t *testing.T) {
	srv := newTestServer(newGapDAO())
	defer srv.Close()
	query := func(repo string) string {
		return fmt.Sprintf(`{ repository(owner: "acme", name: %q) { squashMergeAllowed mergeCommitAllowed rebaseMergeAllowed } }`, repo)
	}
	var got struct {
		Data struct {
			Repository *struct {
				Squash *bool `json:"squashMergeAllowed"`
				Merge  *bool `json:"mergeCommitAllowed"`
				Rebase *bool `json:"rebaseMergeAllowed"`
			} `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Type string `json:"type"`
		} `json:"errors"`
	}
	if code := postGraphQL(t, srv.URL, query("widgets"), &got); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	r := got.Data.Repository
	if r == nil || r.Squash == nil || r.Merge == nil || r.Rebase == nil || !*r.Squash || !*r.Merge {
		t.Fatalf("repository = %+v, want all three toggles, squash and merge commit allowed", r)
	}
	if rebaseAllowed("acme", "widgets") != *r.Rebase {
		t.Fatalf("rebaseMergeAllowed is not the stable per-repository value")
	}

	got.Data.Repository, got.Errors = nil, nil
	if code := postGraphQL(t, srv.URL, query("nope"), &got); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got.Data.Repository != nil || len(got.Errors) != 1 || got.Errors[0].Type != "NOT_FOUND" {
		t.Fatalf("unknown repository = %+v, want a null repository and one NOT_FOUND error", got)
	}
}

type branch struct {
	Name   string `json:"name"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
	Protected bool `json:"protected"`
}

func getBranches(t *testing.T, base, path string) (int, []branch, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out []branch
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, out, resp.Header.Get("Link")
}

// The simulator serves branch listings: the default branch, protected, and
// each open pull request's head branch, sorted by name and paged with
// per_page, page and a Link header. An unknown repository is a 404.
func TestBranchListingIsServed(t *testing.T) {
	srv := newTestServer(newGapDAO())
	defer srv.Close()

	code, all, link := getBranches(t, srv.URL, "/github/repos/acme/widgets/branches?per_page=100")
	if code != http.StatusOK || link != "" {
		t.Fatalf("status %d, link %q; want 200 and no Link on a single page", code, link)
	}
	var names []string
	for _, b := range all {
		names = append(names, b.Name)
		if b.Commit.SHA == "" || b.Protected != (b.Name == "main") {
			t.Fatalf("branch %+v: want a head SHA, and only main protected", b)
		}
	}
	if got := strings.Join(names, ","); got != "feature/a,feature/c,main" {
		t.Fatalf("branches %s, want feature/a,feature/c,main (the merged pull request's branch is gone)", got)
	}

	code, page1, link := getBranches(t, srv.URL, "/github/repos/acme/widgets/branches?per_page=2")
	if code != http.StatusOK || len(page1) != 2 || !strings.Contains(link, `rel="next"`) {
		t.Fatalf("page 1: status %d, %d branches, link %q; want 2 and a next link", code, len(page1), link)
	}
	_, page2, link := getBranches(t, srv.URL, "/github/repos/acme/widgets/branches?per_page=2&page=2")
	if len(page2) != 1 || page2[0].Name != "main" || strings.Contains(link, `rel="next"`) {
		t.Fatalf("page 2: %+v, link %q; want main and no next link", page2, link)
	}

	if code, _, _ := getBranches(t, srv.URL, "/github/repos/acme/nope/branches"); code != http.StatusNotFound {
		t.Fatalf("unknown repository: status %d, want 404", code)
	}
}

type historyResp struct {
	Data struct {
		Repository struct {
			DefaultBranchRef struct {
				Target struct {
					History struct {
						Nodes []struct {
							OID string `json:"oid"`
						} `json:"nodes"`
						PageInfo pageInfo `json:"pageInfo"`
					} `json:"history"`
				} `json:"target"`
			} `json:"defaultBranchRef"`
		} `json:"repository"`
	} `json:"data"`
}

func history(t *testing.T, base, since, after string, first int) historyResp {
	t.Helper()
	afterArg := "null"
	if after != "" {
		afterArg = fmt.Sprintf("%q", after)
	}
	q := fmt.Sprintf(`{ repository(owner: "acme", name: "widgets") { databaseId defaultBranchRef { target { ... on Commit {
  history(first: %d, after: %s, since: %q) { nodes { oid committedDate } pageInfo { hasNextPage endCursor } } } } } } }`,
		first, afterArg, since)
	var out historyResp
	if code := postGraphQL(t, base, q, &out); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	return out
}

func oids(h historyResp) []string {
	var out []string
	for _, n := range h.Data.Repository.DefaultBranchRef.Target.History.Nodes {
		out = append(out, strings.TrimLeft(n.OID, "0"))
	}
	return out
}

// The simulator's commit history honours since (inclusive, as on GitHub)
// and pages by first and after, newest first, so an incremental run reads
// only commits at or after its cursor.
func TestCommitHistoryHonoursSinceAndPages(t *testing.T) {
	srv := newTestServer(newGapDAO())
	defer srv.Close()

	all := history(t, srv.URL, "2026-01-01T00:00:00Z", "", 100)
	if got := strings.Join(oids(all), ","); got != "310,309,206,205,102,101" {
		t.Fatalf("history %s, want every commit newest first", got)
	}
	since := history(t, srv.URL, "2026-01-01T06:00:00Z", "", 100)
	if got := strings.Join(oids(since), ","); got != "310,309,206" {
		t.Fatalf("since 06:00: %s, want the commits at or after it", got)
	}

	var walked []string
	after := ""
	for page := 0; page < 10; page++ {
		h := history(t, srv.URL, "2026-01-01T00:00:00Z", after, 4)
		walked = append(walked, oids(h)...)
		pi := h.Data.Repository.DefaultBranchRef.Target.History.PageInfo
		if !pi.HasNextPage {
			break
		}
		after = pi.EndCursor
	}
	if got := strings.Join(walked, ","); got != "310,309,206,205,102,101" {
		t.Fatalf("paged walk %s, want every commit once", got)
	}
}

// An unknown GitHub organization is a 404, as on GitHub, not an empty one.
func TestUnknownOrganizationIsNotFound(t *testing.T) {
	srv := newTestServer(newGapDAO())
	defer srv.Close()
	for org, want := range map[string]int{"acme": http.StatusOK, "no-such-org": http.StatusNotFound} {
		resp, err := http.Get(srv.URL + "/github/orgs/" + org + "/repos")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: status %d, want %d", org, resp.StatusCode, want)
		}
	}
}

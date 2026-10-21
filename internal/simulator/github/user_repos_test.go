// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// orgDAO holds repositories per organization and lists the organizations.
type orgDAO struct {
	mockDAO
	repos map[string][]dao.Row
}

func (d *orgDAO) ListAll(_ context.Context, _, org, entityType string) ([]dao.Row, error) {
	if entityType != "repo" {
		return nil, nil
	}
	return d.repos[org], nil
}

func (d *orgDAO) ListOrgs(context.Context, string) ([]string, error) {
	var out []string
	for o := range d.repos {
		out = append(out, o)
	}
	sort.Strings(out)
	return out, nil
}

func repoRow(org, name string, private bool) dao.Row {
	return dao.Row{OrgName: org, EntityType: "repo", Payload: fmt.Appendf(nil,
		`{"id":%d,"name":%q,"full_name":%q,"private":%t}`, len(org)*100+len(name), name, org+"/"+name, private)}
}

func newOrgDAO() *orgDAO {
	return &orgDAO{repos: map[string][]dao.Row{
		"org-b": {repoRow("org-b", "zeta", false), repoRow("org-b", "alpha", true)},
		"org-a": {repoRow("org-a", "one", false), repoRow("org-a", "two", false), repoRow("org-a", "three", false)},
	}}
}

type ghRepo struct {
	FullName   string `json:"full_name"`
	Visibility string `json:"visibility"`
	Owner      struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"owner"`
}

func getUserRepos(t *testing.T, base, query string, header map[string]string) (int, []ghRepo, http.Header) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/github/user/repos"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	var repos []ghRepo
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&repos); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
	return resp.StatusCode, repos, resp.Header
}

func names(repos []ghRepo) string {
	out := make([]string, len(repos))
	for i, r := range repos {
		out[i] = r.FullName
	}
	return strings.Join(out, ",")
}

// Without an organization header, /user/repos lists every simulated
// organization's repositories, sorted by full_name, with owner and
// visibility filled in.
func TestUserRepos_ListsEveryOrganization(t *testing.T) {
	srv := newTestServer(newOrgDAO())
	defer srv.Close()
	code, repos, _ := getUserRepos(t, srv.URL, "", nil)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if got, want := names(repos), "org-a/one,org-a/three,org-a/two,org-b/alpha,org-b/zeta"; got != want {
		t.Fatalf("repos %s, want %s", got, want)
	}
	if r := repos[3]; r.Owner.Login != "org-b" || r.Owner.Type != "Organization" || r.Visibility != "private" {
		t.Fatalf("org-b/alpha = %+v, want owner org-b (Organization), visibility private", r)
	}
}

func TestUserRepos_Filters(t *testing.T) {
	srv := newTestServer(newOrgDAO())
	defer srv.Close()
	for _, c := range []struct {
		query, org string
		code       int
		want       string
	}{
		{"?visibility=public", "", 200, "org-a/one,org-a/three,org-a/two,org-b/zeta"},
		{"?visibility=private", "", 200, "org-b/alpha"},
		{"?type=private", "", 200, "org-b/alpha"},
		{"?affiliation=owner,collaborator", "", 200, ""},
		{"?affiliation=owner,organization_member", "", 200, "org-a/one,org-a/three,org-a/two,org-b/alpha,org-b/zeta"},
		{"?type=owner", "", 200, ""},
		{"?type=all&visibility=all", "", 422, ""},
		{"?visibility=secret", "", 422, ""},
		{"", "org-b", 200, "org-b/alpha,org-b/zeta"},
		{"", "org-b, org-a", 200, "org-a/one,org-a/three,org-a/two,org-b/alpha,org-b/zeta"},
		{"", "no-such-org", 200, ""},
	} {
		t.Run(c.query+"|"+c.org, func(t *testing.T) {
			var h map[string]string
			if c.org != "" {
				h = map[string]string{OrgHeader: c.org}
			}
			code, repos, _ := getUserRepos(t, srv.URL, c.query, h)
			if code != c.code {
				t.Fatalf("status %d, want %d", code, c.code)
			}
			if got := names(repos); got != c.want {
				t.Fatalf("repos %q, want %q", got, c.want)
			}
		})
	}
}

// Pages follow per_page and page, with GitHub's Link header.
func TestUserRepos_Paginates(t *testing.T) {
	srv := newTestServer(newOrgDAO())
	defer srv.Close()
	var all []string
	for page, wantLink := range []string{`rel="next"`, `rel="prev"`, `rel="first"`} {
		_, repos, h := getUserRepos(t, srv.URL, fmt.Sprintf("?per_page=2&page=%d", page+1), nil)
		link := h.Get("Link")
		if !strings.Contains(link, wantLink) {
			t.Fatalf("page %d Link %q lacks %s", page+1, link, wantLink)
		}
		if page < 2 && !strings.Contains(link, `page=3&per_page=2>; rel="last"`) {
			t.Fatalf("page %d Link %q lacks the last page", page+1, link)
		}
		if page == 2 && strings.Contains(link, `rel="next"`) {
			t.Fatalf("last page Link %q has a next page", link)
		}
		all = append(all, names(repos))
	}
	if got := strings.Join(all, "|"); got != "org-a/one,org-a/three|org-a/two,org-b/alpha|org-b/zeta" {
		t.Fatalf("pages %s", got)
	}
	_, repos, h := getUserRepos(t, srv.URL, "?per_page=2&page=9", nil)
	if len(repos) != 0 || strings.Contains(h.Get("Link"), `rel="next"`) {
		t.Fatalf("past the end: %v, Link %q", repos, h.Get("Link"))
	}
}

// An organization named by a fault rule answers with the rule's status.
func TestUserRepos_FaultRule(t *testing.T) {
	srv := newTestServer(newOrgDAO())
	defer srv.Close()
	code, _, h := getUserRepos(t, srv.URL, "", map[string]string{OrgHeader: "acme-sim-rate-limit"})
	if code != http.StatusTooManyRequests || h.Get("Retry-After") != "60" {
		t.Fatalf("status %d, Retry-After %q", code, h.Get("Retry-After"))
	}
}

func TestUser(t *testing.T) {
	srv := newTestServer(newOrgDAO())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/github/user") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	var u struct {
		Login string `json:"login"`
		Type  string `json:"type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil || u.Login != SimulatedUserLogin || u.Type != "User" {
		t.Fatalf("GET /user = %+v (%v)", u, err)
	}
}

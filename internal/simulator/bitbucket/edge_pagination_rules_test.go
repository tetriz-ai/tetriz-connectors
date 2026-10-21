// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// TestPRDiffstat_PagelenAboveCapPaginatesWithoutLoss: the connector asks for
// diffstat at pagelen=500 while the simulator clamps to 100. Following `next`
// until it is absent must return every entry exactly once, including when the
// total is an exact multiple of the effective page size (the last full page has
// a `next` that leads to an empty page with none).
func TestPRDiffstat_PagelenAboveCapPaginatesWithoutLoss(t *testing.T) {
	for _, total := range []int{150, 200} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			entries := make([]string, total)
			for i := range entries {
				entries[i] = fmt.Sprintf(`{"new":{"path":"f%03d.go"},"lines_added":1}`, i)
			}
			pr := row("pr", `{"repo":"widgets","id":42,"diffstat":[`+strings.Join(entries, ",")+`]}`)
			srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/42": &pr}})
			defer srv.Close()

			next := srv.URL + "/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/diffstat?pagelen=500"
			seen := map[string]bool{}
			for pages := 0; next != ""; pages++ {
				if pages > 10 {
					t.Fatal("next chain did not terminate")
				}
				resp, env := getEnvelope(t, next)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d", resp.StatusCode)
				}
				if env.Pagelen > maxPagelen {
					t.Errorf("envelope pagelen = %d, want the effective (clamped) %d", env.Pagelen, maxPagelen)
				}
				for _, v := range env.Values {
					var e struct {
						New struct{ Path string } `json:"new"`
					}
					if err := json.Unmarshal(v, &e); err != nil {
						t.Fatal(err)
					}
					if seen[e.New.Path] {
						t.Errorf("entry %s served twice", e.New.Path)
					}
					seen[e.New.Path] = true
				}
				if env.Next != "" {
					u, err := url.Parse(env.Next)
					if err != nil || u.Host == "" {
						t.Fatalf("next %q must be an absolute URL", env.Next)
					}
					if got := u.Query().Get("pagelen"); got != "500" {
						t.Errorf("next dropped the requested pagelen: %q", env.Next)
					}
				}
				next = env.Next
			}
			if len(seen) != total {
				t.Errorf("collected %d entries, want %d", len(seen), total)
			}
		})
	}
}

// bbRoutes lists one concrete URL per registered route, with {ws} for the workspace.
var bbRoutes = []string{
	"/bitbucket/2.0/repositories/{ws}",
	"/bitbucket/2.0/repositories/{ws}/widgets",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests/42/diffstat",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests/42/commits",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests/42/activity",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests/42/comments",
	"/bitbucket/2.0/repositories/{ws}/widgets/pullrequests/42/diff",
	"/bitbucket/2.0/repositories/{ws}/widgets/commits",
	"/bitbucket/2.0/repositories/{ws}/widgets/diffstat/abc123",
	"/bitbucket/2.0/repositories/{ws}/widgets/refs/branches",
	"/bitbucket/2.0/repositories/{ws}/widgets/refs/branches/main",
	"/bitbucket/2.0/repositories/{ws}/widgets/src/abc123/",
	"/bitbucket/2.0/workspaces/{ws}/members",
	"/bitbucket/1.0/groups/{ws}/",
}

// TestRules_EveryRouteInjectsTheSameFailure: a workspace naming a fault keyword
// must fail the same way on every route — including the per-PR and direct-URL
// routes a connector reaches mid-run — so a fault test is not silently answered
// 200 by one route. 429 carries Retry-After; both carry the Bitbucket envelope.
func TestRules_EveryRouteInjectsTheSameFailure(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()
	cases := []struct {
		ws         string
		status     int
		retryAfter string
	}{
		{"acme-sim-rate-limit", http.StatusTooManyRequests, "60"},
		{"acme-sim-server-error", http.StatusInternalServerError, ""},
		{"acme-sim-not-found", http.StatusNotFound, ""},
	}
	for _, c := range cases {
		for _, route := range bbRoutes {
			path := strings.ReplaceAll(route, "{ws}", c.ws)
			t.Run(c.ws+path, func(t *testing.T) {
				resp, err := http.Get(srv.URL + path)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != c.status {
					t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, c.status, body)
				}
				if got := resp.Header.Get("Retry-After"); got != c.retryAfter {
					t.Errorf("Retry-After = %q, want %q", got, c.retryAfter)
				}
				assertBitbucketErrorEnvelope(t, body)
			})
		}
	}
}

// TestBranchSHA_SlashAndUnicodeBranchNames: the connector resolves a branch tip at
// /refs/branches/{url.PathEscape(branch)}, so "release/1.0" arrives as
// "release%2F1.0". Bitbucket answers it; the simulator must too, and must hand
// back the decoded name, or fetch_repo_tree fails on every slash-named branch.
func TestBranchSHA_SlashAndUnicodeBranchNames(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()
	for _, branch := range []string{"release/1.0", "feature/PROJ-12-émoji-🚀", "fé"} {
		t.Run(branch, func(t *testing.T) {
			resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/refs/branches/" + url.PathEscape(branch))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			var got struct {
				Name   string `json:"name"`
				Target struct {
					Hash string `json:"hash"`
				} `json:"target"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got.Name != branch {
				t.Errorf("name = %q, want %q", got.Name, branch)
			}
			if want := deriveBranchHash("acme", "widgets", branch); got.Target.Hash != want {
				t.Errorf("hash = %q, want %q (must agree with the listing route)", got.Target.Hash, want)
			}
		})
	}
}

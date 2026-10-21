// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// --- Diff, branch and source-walk handler tests ---

func TestPRDiff_TextPlainByIDUnknownAndSimEmpty(t *testing.T) {
	const diff = "diff --git a/svc.go b/svc.go\n@@ -1 +1 @@\n-old\n+new\n"
	pr := row("pr", `{"repo":"widgets","id":42,"diff":`+strconv.Quote(diff)+`}`)
	srv := newTestServer(&mockDAO{prByID: map[string]*dao.Row{"widgets/42": &pr}})
	defer srv.Close()

	// Found → raw diff as text/plain, byte-for-byte.
	resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/pullrequests/42/diff")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("content-type = %q, want text/plain", ct)
	}
	if string(body) != diff {
		t.Errorf("diff body = %q, want %q", body, diff)
	}

	// Unknown PR id → 404 (connector maps this to NOT_AVAILABLE).
	resp2, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/pullrequests/999/diff")
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("unknown-id status = %d, want 404", resp2.StatusCode)
	}
	_ = resp2.Body.Close()

	// sim-empty → 200 with an empty text body (never a 404).
	resp3, _ := http.Get(srv.URL + "/bitbucket/2.0/repositories/sim-empty-corp/widgets/pullrequests/42/diff")
	body3, _ := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK || len(body3) != 0 {
		t.Errorf("sim-empty diff: status = %d body = %q, want 200 / empty", resp3.StatusCode, body3)
	}
}

type branchEntry struct {
	Name   string `json:"name"`
	Target struct {
		Hash string `json:"hash"`
	} `json:"target"`
}

func TestBranches_MultiPageFollowedViaNextOnSimHost(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()
	simHost := mustHost(t, srv.URL)

	// Mirror the connector: start at the base and follow `next` verbatim until empty.
	// pagelen=1 forces multiple pages so the next-follow path is exercised.
	next := srv.URL + "/bitbucket/2.0/repositories/acme/widgets/refs/branches?pagelen=1"
	var names []string
	hashByName := map[string]string{}
	for i := 0; next != "" && i < 10; i++ {
		resp, env := getEnvelope(t, next)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("branches page status = %d, want 200", resp.StatusCode)
		}
		for _, v := range env.Values {
			var b branchEntry
			if err := json.Unmarshal(v, &b); err != nil {
				t.Fatalf("decode branch: %v", err)
			}
			names = append(names, b.Name)
			hashByName[b.Name] = b.Target.Hash
			if len(b.Target.Hash) != 40 {
				t.Errorf("branch %q hash = %q, want 40 hex chars", b.Name, b.Target.Hash)
			}
		}
		if env.Next != "" && mustHost(t, env.Next) != simHost {
			t.Errorf("next host = %q, want simulator host %q (the connector refuses a next link on another host)", mustHost(t, env.Next), simHost)
		}
		next = env.Next
	}
	if !sortedEqual(names, []string{"develop", "main"}) {
		t.Errorf("branches collected across pages = %v, want main+develop", names)
	}

	// handleBranchSHA agrees with the listing's hash for the same branch.
	resp, err := http.Get(srv.URL + "/bitbucket/2.0/repositories/acme/widgets/refs/branches/main")
	if err != nil {
		t.Fatal(err)
	}
	var single branchEntry
	json.NewDecoder(resp.Body).Decode(&single) //nolint:errcheck // asserted below
	_ = resp.Body.Close()
	if single.Name != "main" || single.Target.Hash != hashByName["main"] {
		t.Errorf("branch-SHA route = {%q,%q}, want {main,%q} consistent with the listing", single.Name, single.Target.Hash, hashByName["main"])
	}
}

type srcEntry struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func TestSrcWalk_Terminates(t *testing.T) {
	srv := newTestServer(&mockDAO{})
	defer srv.Close()
	const commit = "abc123def"

	// Mirror the connector's BFS: fetch a directory, follow `next` for pages, and
	// recurse into commit_directory entries. A cap guards against a non-terminating
	// tree — the assertion is that the walk finishes under it.
	queue := []string{""} // repo-relative dir paths; "" = root
	var files []string
	reqs := 0
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		next := srv.URL + "/bitbucket/2.0/repositories/acme/widgets/src/" + commit + "/" + dir
		for next != "" {
			reqs++
			if reqs > 50 {
				t.Fatalf("src walk did not terminate within 50 requests (dir=%q)", dir)
			}
			resp, env := getEnvelope(t, next)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("src status = %d, want 200", resp.StatusCode)
			}
			for _, v := range env.Values {
				var e srcEntry
				if err := json.Unmarshal(v, &e); err != nil {
					t.Fatalf("decode src entry: %v", err)
				}
				if e.Type == "commit_directory" {
					queue = append(queue, e.Path)
				} else {
					files = append(files, e.Path)
				}
			}
			next = env.Next
		}
	}
	if !sortedEqual(files, []string{"README.md", "go.mod", "src/main.go", "src/service.go"}) {
		t.Errorf("walked files = %v, want the synthetic tree's leaves", files)
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return u.Host
}

func sortedEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	g := append([]string(nil), got...)
	sort.Strings(g)
	sort.Strings(want)
	for i := range g {
		if g[i] != want[i] {
			return false
		}
	}
	return true
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// siteStore is a mockStore that knows which sites exist.
type siteStore struct {
	mockStore
	sites map[string]bool
}

func (s *siteStore) OrgExists(_ context.Context, _, org string) (bool, error) {
	return s.sites[org], nil
}

func commentIssue(key string) *dao.Row {
	return &dao.Row{EntityType: "issue", Payload: []byte(`{"key":"` + key + `","fields":{"created":"2026-01-01T09:00:00.000+0000",` +
		`"reporter":{"accountId":"rep"},"assignee":{"accountId":"asg"}}}`)}
}

type commentList struct {
	Comments []struct {
		ID      string `json:"id"`
		Created string `json:"created"`
		Author  struct {
			AccountID string `json:"accountId"`
		} `json:"author"`
		Body struct {
			Type string `json:"type"`
		} `json:"body"`
	} `json:"comments"`
	Total   int `json:"total"`
	StartAt int `json:"startAt"`
}

func getComments(t *testing.T, base, cloud, key, query string) (int, commentList) {
	t.Helper()
	resp, err := http.Get(base + "/jira/ex/jira/" + cloud + "/rest/api/3/issue/" + key + "/comment" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out commentList
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, out
}

// The simulator serves issue comments: zero to two per issue, derived from
// the issue, ordered by creation, each with an id, an author and an ADF body,
// windowed by startAt and maxResults. An unknown issue is a 404.
func TestIssueCommentsAreServed(t *testing.T) {
	byKey := map[string]*dao.Row{}
	keys := []string{"PLAT-1", "PLAT-2", "PLAT-3", "PLAT-4", "PLAT-5", "PLAT-6"}
	for _, k := range keys {
		byKey[k] = commentIssue(k)
	}
	srv := newTestServer(&mockStore{byKey: byKey})
	defer srv.Close()

	ids := map[string]bool{}
	withTwo := ""
	for _, k := range keys {
		code, got := getComments(t, srv.URL, "acme", k, "")
		if code != http.StatusOK || got.Total != len(got.Comments) || got.Total > 2 {
			t.Fatalf("%s: status %d, %+v; want 200 and at most two comments", k, code, got)
		}
		for i, c := range got.Comments {
			if c.ID == "" || ids[c.ID] || c.Body.Type != "doc" || c.Author.AccountID == "" {
				t.Fatalf("%s comment %d = %+v: want a unique id, an author and an ADF body", k, i, c)
			}
			ids[c.ID] = true
			if i > 0 && c.Created <= got.Comments[i-1].Created {
				t.Fatalf("%s: comments not ordered by creation", k)
			}
		}
		if got.Total == 2 {
			withTwo = k
		}
	}
	if len(ids) == 0 || withTwo == "" {
		t.Fatalf("want some issues with comments and one with two; got %d comments", len(ids))
	}
	code, page := getComments(t, srv.URL, "acme", withTwo, "?startAt=1&maxResults=1")
	if code != http.StatusOK || page.Total != 2 || page.StartAt != 1 || len(page.Comments) != 1 {
		t.Fatalf("window: status %d, %+v; want the second of two", code, page)
	}

	if code, _ := getComments(t, srv.URL, "acme", "PLAT-999", ""); code != http.StatusNotFound {
		t.Fatalf("unknown issue: status %d, want 404", code)
	}
}

// A cloud id the store holds nothing for is a 404, as on Atlassian, not an
// empty site. A fault-rule cloud id still gets its rule's answer.
func TestUnknownSiteIsNotFound(t *testing.T) {
	srv := newTestServer(&siteStore{sites: map[string]bool{"acme": true}})
	defer srv.Close()
	for cloud, want := range map[string]int{"acme": http.StatusOK, "no-such-site": http.StatusNotFound, "sim-empty-corp": http.StatusOK} {
		resp, err := http.Get(srv.URL + "/jira/ex/jira/" + cloud + "/rest/api/3/project/search")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: status %d, want %d", cloud, resp.StatusCode, want)
		}
	}
}

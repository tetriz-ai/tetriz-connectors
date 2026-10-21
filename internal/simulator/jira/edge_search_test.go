// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// TestEdge_SearchExactlyFullLastPageTerminates: when the remaining rows exactly
// fill a page the simulator still hands back a token; the next request must be an
// empty page with no token, so the connector's loop ends without losing or
// repeating a row.
func TestEdge_SearchExactlyFullLastPageTerminates(t *testing.T) {
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	rows := []dao.IssueRow{
		issueRow("PLAT-1", "00000000-0000-0000-0000-000000000001", base),
		issueRow("PLAT-2", "00000000-0000-0000-0000-000000000002", base),
		issueRow("PLAT-3", "00000000-0000-0000-0000-000000000003", base),
		issueRow("PLAT-4", "00000000-0000-0000-0000-000000000004", base.Add(time.Minute)),
	}
	srv := newTestServer(&mockStore{issues: rows})
	defer srv.Close()

	seen := map[string]int{}
	token, pages := "", 0
	for {
		resp, sr := postSearch(t, srv.URL, "demo-org", searchJQL, token, 2)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		for _, raw := range sr.Issues {
			seen[issuePayloadProjectKeyFull(raw)]++
		}
		pages++
		if sr.NextPageToken == "" {
			if len(sr.Issues) != 0 {
				t.Errorf("final page carried %d issues, want the empty tail page", len(sr.Issues))
			}
			break
		}
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		token = sr.NextPageToken
	}
	if pages != 3 {
		t.Errorf("pages = %d, want 3 (two full pages and an empty tail)", pages)
	}
	for _, k := range []string{"PLAT-1", "PLAT-2", "PLAT-3", "PLAT-4"} {
		if seen[k] != 1 {
			t.Errorf("%s returned %d times, want once (same-timestamp rows must page by id)", k, seen[k])
		}
	}
}

// TestEdge_SearchProjectKeyIsCaseInsensitive: Jira resolves `project = "plat"`
// to project PLAT — JQL project keys are matched case-insensitively. A settings
// project_keys entry typed in lowercase must return the same issues from the
// simulator as from the vendor.
func TestEdge_SearchProjectKeyIsCaseInsensitive(t *testing.T) {
	base := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	srv := newTestServer(&mockStore{issues: []dao.IssueRow{issueRow("PLAT-1", "a", base)}})
	defer srv.Close()

	resp, sr := postSearch(t, srv.URL, "demo-org", `project = "plat" ORDER BY updated ASC`, "", 10)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(sr.Issues) != 1 {
		t.Errorf("lowercase project key returned %d issues, want 1 (Jira matches project keys case-insensitively)", len(sr.Issues))
	}
}

// TestEdge_SearchRuleResponses: the POST search route is the one route
// TestRuleResponsesOverHTTP does not drive. Its 429 must carry Retry-After and
// each failure must be Jira's error envelope, since fetch_issues pages through
// this route and the gateway classifies retry off status and header.
func TestEdge_SearchRuleResponses(t *testing.T) {
	srv := newTestServer(&mockStore{})
	defer srv.Close()

	tests := []struct {
		cloudID    string
		wantStatus int
		wantRetry  string
	}{
		{"demo-sim-rate-limit", http.StatusTooManyRequests, "60"},
		{"demo-sim-server-error", http.StatusInternalServerError, ""},
		{"demo-sim-not-found", http.StatusNotFound, ""},
		{"demo-sim-perm-denied", http.StatusForbidden, ""},
	}
	for _, tt := range tests {
		t.Run(tt.cloudID, func(t *testing.T) {
			b, _ := json.Marshal(map[string]any{"jql": searchJQL, "maxResults": 2, "nextPageToken": encodeToken(time.Now(), "x")})
			resp, err := http.Post(srv.URL+"/jira/ex/jira/"+tt.cloudID+"/rest/api/3/search/jql", "application/json", bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if got := resp.Header.Get("Retry-After"); got != tt.wantRetry {
				t.Errorf("Retry-After = %q, want %q", got, tt.wantRetry)
			}
			var env jiraErrEnvelope
			if err := json.Unmarshal(body, &env); err != nil || len(env.ErrorMessages) != 1 || env.Errors == nil {
				t.Errorf("body is not a Jira error envelope: %s (%v)", body, err)
			}
		})
	}
}

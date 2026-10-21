// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// The rule table is unit-tested on checkRule's return value, which leaves the write
// path — the status line, the Retry-After header, and the envelope actually sent —
// unproven. VendorGateway classifies retry behaviour off the status code, so a rule
// that computed the right code and wrote the wrong one would mis-drive the
// connector's retry decision with nothing to catch it.

type jiraErrEnvelope struct {
	ErrorMessages []string       `json:"errorMessages"`
	Errors        map[string]any `json:"errors"`
}

// ruleRoutes is every route that resolves an org from its cloudId path segment, so a
// rule applies to all of them. accessible-resources is absent by design: it carries
// no cloudId.
func ruleRoutes(cloudID string) map[string]string {
	base := "/jira/ex/jira/" + cloudID + "/rest"
	return map[string]string{
		"project/search": base + "/api/3/project/search",
		"worklog":        base + "/api/3/issue/PLAT-1/worklog",
		"board":          base + "/agile/1.0/board?type=scrum",
		"board sprints":  base + "/agile/1.0/board/9000/sprint",
	}
}

func TestRuleResponsesOverHTTP(t *testing.T) {
	tests := []struct {
		name          string
		cloudID       string
		wantStatus    int
		wantMessage   string
		wantRetryAfte string
	}{
		{
			name:          "rate limit carries Retry-After so the gateway backs off",
			cloudID:       "demo-sim-rate-limit",
			wantStatus:    http.StatusTooManyRequests,
			wantMessage:   "Rate limit exceeded.",
			wantRetryAfte: "60",
		},
		{
			name:        "permission denied",
			cloudID:     "demo-sim-perm-denied",
			wantStatus:  http.StatusForbidden,
			wantMessage: "You do not have permission to view this resource.",
		},
		{
			name:        "server error",
			cloudID:     "demo-sim-server-error",
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "Internal server error.",
		},
		{
			name:        "not found",
			cloudID:     "demo-sim-not-found",
			wantStatus:  http.StatusNotFound,
			wantMessage: "Issue does not exist or you do not have permission to see it.",
		},
	}

	srv := newTestServer(&mockStore{})
	defer srv.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for routeName, path := range ruleRoutes(tt.cloudID) {
				resp, err := http.Get(srv.URL + path)
				if err != nil {
					t.Fatalf("%s: %v", routeName, err)
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode != tt.wantStatus {
					t.Errorf("%s: status = %d, want %d", routeName, resp.StatusCode, tt.wantStatus)
				}
				if got := resp.Header.Get("Retry-After"); got != tt.wantRetryAfte {
					t.Errorf("%s: Retry-After = %q, want %q", routeName, got, tt.wantRetryAfte)
				}
				var env jiraErrEnvelope
				if err := json.Unmarshal(body, &env); err != nil {
					t.Fatalf("%s: body is not a Jira error envelope: %v (%s)", routeName, err, body)
				}
				if len(env.ErrorMessages) != 1 || env.ErrorMessages[0] != tt.wantMessage {
					t.Errorf("%s: errorMessages = %v, want [%q]", routeName, env.ErrorMessages, tt.wantMessage)
				}
				if env.Errors == nil {
					t.Errorf("%s: envelope must carry an errors object, not null", routeName)
				}
			}
		})
	}
}

// TestSimEmptyPerRoute covers the two routes the sim-empty coverage missed. Their
// bodies are route-specific, and an empty result must read as a valid success — a
// connector paging loop treats a non-200 as a sync failure, not as "no more rows".
func TestSimEmptyPerRoute(t *testing.T) {
	srv := newTestServer(&mockStore{byKey: map[string]*dao.Row{}})
	defer srv.Close()
	base := "/jira/ex/jira/demo-sim-empty/rest"

	t.Run("worklog", func(t *testing.T) {
		var wr worklogResp
		resp := getJSON(t, srv.URL+base+"/api/3/issue/PLAT-1/worklog", &wr)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if len(wr.Worklogs) != 0 || wr.Total != 0 || wr.StartAt != 0 {
			t.Errorf("got %+v, want an empty windowed worklog body", wr)
		}
		if wr.MaxResults != defaultWorklogMaxResults {
			t.Errorf("maxResults = %d, want %d — the connector's loop reads it", wr.MaxResults, defaultWorklogMaxResults)
		}
	})

	t.Run("board sprints", func(t *testing.T) {
		var vr valuesResp
		resp := getJSON(t, srv.URL+base+"/agile/1.0/board/9000/sprint", &vr)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if len(vr.Values) != 0 || vr.Total != 0 {
			t.Errorf("got %+v, want an empty values body", vr)
		}
		if !vr.IsLast {
			t.Error("isLast must be true, or the connector pages forever")
		}
	})
}

// TestSearchResponseCarriesDescription pins the ADF projection. Nothing in this
// package mentions description — it reaches the connector only because the search
// handler returns the stored payload whole. fetch_issues_data uploads
// fields.description to S3, so were that passthrough ever narrowed to a field
// projection, it would ship empty blobs with every other test still green.
func TestSearchResponseCarriesDescription(t *testing.T) {
	row := issueRow("PLAT-1", "a", time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC))
	row.Payload = []byte(`{"key":"PLAT-1","fields":{"project":{"key":"PLAT"},"updated":"2026-06-01T10:00:00.000+0000",` +
		`"description":{"type":"doc","version":1,"content":[{"type":"paragraph","content":[{"type":"text","text":"hello"}]}]}}}`)

	srv := newTestServer(&mockStore{issues: []dao.IssueRow{row}})
	defer srv.Close()

	_, sr := postSearch(t, srv.URL, "demo-gh-enterprise", `project = "PLAT" ORDER BY updated ASC`, "", 50)
	if len(sr.Issues) != 1 {
		t.Fatalf("issues = %d, want 1", len(sr.Issues))
	}
	var issue struct {
		Fields struct {
			Description *struct {
				Type    string            `json:"type"`
				Content []json.RawMessage `json:"content"`
			} `json:"description"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(sr.Issues[0], &issue); err != nil {
		t.Fatal(err)
	}
	if issue.Fields.Description == nil {
		t.Fatal("search response dropped fields.description; fetch_issues_data would upload empty blobs")
	}
	if issue.Fields.Description.Type != "doc" || len(issue.Fields.Description.Content) == 0 {
		t.Errorf("description = %+v, want a non-empty ADF doc", issue.Fields.Description)
	}
}

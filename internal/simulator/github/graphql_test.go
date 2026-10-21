// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

func TestExtractOwner(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "owner field",
			body: `{ repository(owner: "acme", name: "widgets") { pullRequests { nodes { additions } } } }`,
			want: "acme",
		},
		{
			name: "login field (org-level query)",
			body: `{ organization(login: "myorg") { membersWithRole { nodes { login } } } }`,
			want: "myorg",
		},
		{
			name: "no owner",
			body: `{ viewer { login } }`,
			want: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := extractOwner(tt.body); got != tt.want {
				t.Errorf("extractOwner = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractRepoName(t *testing.T) {
	tests := []struct {
		body string
		want string
	}{
		{`repository(owner: "acme", name: "platform")`, "platform"},
		{`organization(login: "acme")`, ""},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.body[:20], func(t *testing.T) {
			if got := extractRepoName(tt.body); got != tt.want {
				t.Errorf("extractRepoName = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractPageSize(t *testing.T) {
	tests := []struct {
		body string
		want int
	}{
		{`pullRequests(first: 50)`, 50},
		{`pullRequests(first: 100)`, 100},
		{`pullRequests(first: 0)`, 100}, // zero treated as default
		{`no first argument here`, 100}, // default
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.body, func(t *testing.T) {
			if got := extractPageSize(tt.body); got != tt.want {
				t.Errorf("extractPageSize = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestExtractCursor(t *testing.T) {
	ts := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	encoded := base64.StdEncoding.EncodeToString([]byte(ts.Format(time.RFC3339Nano)))

	tests := []struct {
		body    string
		wantNil bool
		wantT   time.Time
	}{
		{`pullRequests(first: 50, after: "` + encoded + `")`, false, ts},
		{`pullRequests(first: 50)`, true, time.Time{}},
		{`pullRequests(first: 50, after: "notbase64!!!")`, true, time.Time{}},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("case%d", i), func(t *testing.T) {
			got := extractCursor(tt.body)
			if tt.wantNil {
				if got != nil {
					t.Errorf("expected nil cursor, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil cursor")
			}
			if !got.Equal(tt.wantT) {
				t.Errorf("cursor = %v, want %v", got, tt.wantT)
			}
		})
	}
}

func TestEncodeCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 5, 15, 8, 30, 0, 500, time.UTC)
	encoded := encodeCursor(ts)
	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !decoded.Equal(ts) {
		t.Errorf("round-trip: got %v, want %v", decoded, ts)
	}
}

func TestExtractClosingIssues(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		wantLogin  string
		wantNumber int
	}{
		{
			name: "PR author present",
			payload: `{"updatedAt":"2026-05-01T00:00:00Z","author":{"login":"alice","databaseId":42},
				"closingIssuesReferences":{"nodes":[{"number":7,"title":"fix it","state":"CLOSED"}]}}`,
			wantLogin:  "alice",
			wantNumber: 7,
		},
		{
			name: "PR author missing falls back to a non-empty login",
			payload: `{"updatedAt":"2026-05-01T00:00:00Z",
				"closingIssuesReferences":{"nodes":[{"number":8,"title":"fix it too","state":"OPEN"}]}}`,
			wantLogin:  "ghost",
			wantNumber: 8,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows := []dao.Row{{Payload: []byte(tt.payload)}}
			nodes := extractClosingIssues(rows, "acme", "widgets")
			if len(nodes) != 1 {
				t.Fatalf("expected 1 node, got %d", len(nodes))
			}
			var got struct {
				Number int `json:"number"`
				Author struct {
					Login string `json:"login"`
				} `json:"author"`
			}
			if err := json.Unmarshal(nodes[0], &got); err != nil {
				t.Fatalf("unmarshal node: %v", err)
			}
			if got.Number != tt.wantNumber {
				t.Errorf("number = %d, want %d", got.Number, tt.wantNumber)
			}
			if got.Author.Login != tt.wantLogin {
				t.Errorf("author.login = %q, want %q", got.Author.Login, tt.wantLogin)
			}
			if got.Author.Login == "" {
				t.Error("author.login must never be empty — ParseIssue hard-fails on it")
			}
		})
	}
}

// TestHandleCommits_RespondsUnderQueriedFieldName asserts handleCommits
// responds under the top-level field name the query actually asked for
// (`ref` or `defaultBranchRef`), not a fixed default. The connector treats a
// missing/nil field as a clean end-of-history page rather than an error, so a
// mismatch here is undetectable except by checking the field name directly —
// exactly what this test does. resolveCommitBranch always resolves a
// non-empty branch, so every simulator org's fetch_commits sends the
// `ref(qualifiedName: ...)` shape, never the plain `defaultBranchRef` one.
func TestHandleCommits_RespondsUnderQueriedFieldName(t *testing.T) {
	prPayload := `{"number":1,"repo":"widgets","updatedAt":"2026-06-01T10:00:00Z",` +
		`"commits":{"nodes":[{"oid":"abc123","message":"chore: commit 1",` +
		`"authoredDate":"2026-06-01T09:55:00Z","committedDate":"2026-06-01T10:00:00Z",` +
		`"additions":10,"deletions":5,"changedFilesIfAvailable":2,` +
		`"author":{"name":"Alice","email":"alice@example.com","user":{"login":"alice","databaseId":42}},` +
		`"parents":{"totalCount":1}}]}}`
	rows := []dao.Row{{OrgName: "acme", EntityType: "pr", Payload: []byte(prPayload), CursorVal: time.Now().UTC()}}

	tests := []struct {
		name        string
		query       string
		wantField   string
		wrongFields []string
	}{
		{
			name:        "plain defaultBranchRef query (no tracked-branch override)",
			query:       `{ repository(owner: "acme", name: "widgets") { databaseId defaultBranchRef { target { ... on Commit { history(first: 100, after: null, since: "2026-01-01T00:00:00Z") { nodes { oid additions deletions } pageInfo { hasNextPage endCursor } } } } } } }`,
			wantField:   "defaultBranchRef",
			wrongFields: []string{"ref"},
		},
		{
			name:        "branch-pinned ref(qualifiedName) query — what every simulator org actually sends, since resolveCommitBranch always resolves a branch",
			query:       `{ repository(owner: "acme", name: "widgets") { databaseId ref(qualifiedName: "refs/heads/main") { target { ... on Commit { history(first: 100, after: null, since: "2026-01-01T00:00:00Z") { nodes { oid additions deletions } pageInfo { hasNextPage endCursor } } } } } } }`,
			wantField:   "ref",
			wrongFields: []string{"defaultBranchRef"},
		},
		{
			// Neither literal is present — a hypothetical future query-shape
			// drift. Must fail safe (a valid, decodable response under some
			// field) rather than panic; the defaulting-with-a-warn-log
			// behavior itself is exercised by reading the code, not asserted
			// here since it only affects log output, not the response shape.
			name:        "unrecognized shape falls back to defaultBranchRef rather than panicking",
			query:       `{ repository(owner: "acme", name: "widgets") { databaseId someFutureField { target { ... on Commit { history(first: 100, after: null, since: "2026-01-01T00:00:00Z") { nodes { oid additions deletions } pageInfo { hasNextPage endCursor } } } } } } }`,
			wantField:   "defaultBranchRef",
			wrongFields: []string{"ref"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(&mockDAO{rows: rows})
			defer srv.Close()

			bodyJSON, err := json.Marshal(map[string]string{"query": tt.query})
			if err != nil {
				t.Fatalf("marshal request body: %v", err)
			}
			resp, err := http.Post(srv.URL+"/github/graphql", "application/json", strings.NewReader(string(bodyJSON)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}

			var out map[string]json.RawMessage
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			var data map[string]json.RawMessage
			if err := json.Unmarshal(out["data"], &data); err != nil {
				t.Fatalf("decode data: %v", err)
			}
			var repository map[string]json.RawMessage
			if err := json.Unmarshal(data["repository"], &repository); err != nil {
				t.Fatalf("decode repository: %v", err)
			}

			raw, ok := repository[tt.wantField]
			if !ok {
				t.Fatalf("repository.%s missing from response; got keys %v", tt.wantField, keysOf(repository))
			}
			for _, wrong := range tt.wrongFields {
				if _, present := repository[wrong]; present {
					t.Errorf("repository.%s should not be present when the query asked for %s", wrong, tt.wantField)
				}
			}

			var target struct {
				Target struct {
					History struct {
						Nodes []json.RawMessage `json:"nodes"`
					} `json:"history"`
				} `json:"target"`
			}
			if err := json.Unmarshal(raw, &target); err != nil {
				t.Fatalf("decode %s: %v", tt.wantField, err)
			}
			if len(target.Target.History.Nodes) != 1 {
				t.Errorf("history.nodes length = %d, want 1 (the seeded commit)", len(target.Target.History.Nodes))
			}
		})
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

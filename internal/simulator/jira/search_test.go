// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"testing"
	"time"
)

func TestParseJQL(t *testing.T) {
	tests := []struct {
		name        string
		jql         string
		wantProject string
		wantSince   string // RFC3339, or "" for nil
	}{
		{
			name:        "project and updated",
			jql:         `project = PLAT AND updated >= "2026-06-01 10:00" ORDER BY updated ASC`,
			wantProject: "PLAT",
			wantSince:   "2026-06-01T10:00:00Z",
		},
		{
			name:        "quoted project key",
			jql:         `project = "ENG" AND updated >= "2026-01-02 03:04"`,
			wantProject: "ENG",
			wantSince:   "2026-01-02T03:04:00Z",
		},
		{
			// The connector's first-sync template verbatim (op_issues.go:168):
			// a zero cursor omits the `updated >=` clause entirely, so the whole
			// stored history for the project must come back.
			name:        "connector first-sync form: quoted key, no updated clause",
			jql:         `project = "PLAT" ORDER BY updated ASC`,
			wantProject: "PLAT",
			wantSince:   "",
		},
		{
			// The connector's incremental template verbatim (op_issues.go:170).
			name:        "connector incremental form: quoted key and quoted timestamp",
			jql:         `project = "PLAT" AND updated >= "2026-06-01 10:00" ORDER BY updated ASC`,
			wantProject: "PLAT",
			wantSince:   "2026-06-01T10:00:00Z",
		},
		{
			name:        "all-projects, no project clause",
			jql:         `updated >= "2026-06-01 10:00" ORDER BY updated ASC`,
			wantProject: "",
			wantSince:   "2026-06-01T10:00:00Z",
		},
		{
			name:        "malformed timestamp degrades to no lower bound",
			jql:         `project = PLAT AND updated >= "not-a-date"`,
			wantProject: "PLAT",
			wantSince:   "",
		},
		{
			name:        "empty jql yields no filter",
			jql:         "",
			wantProject: "",
			wantSince:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			project, since := parseJQL(context.Background(), tt.jql)
			if project != tt.wantProject {
				t.Errorf("project = %q, want %q", project, tt.wantProject)
			}
			switch {
			case tt.wantSince == "" && since != nil:
				t.Errorf("since = %v, want nil", since)
			case tt.wantSince != "" && since == nil:
				t.Errorf("since = nil, want %s", tt.wantSince)
			case tt.wantSince != "":
				if got := since.UTC().Format(time.RFC3339); got != tt.wantSince {
					t.Errorf("since = %s, want %s", got, tt.wantSince)
				}
			}
		})
	}
}

func TestPageToken_RoundTrip(t *testing.T) {
	cv := time.Date(2026, 6, 20, 11, 0, 0, 0, time.UTC)
	tok := encodeToken(cv, "abc-123")
	got, err := decodeToken(tok)
	if err != nil {
		t.Fatalf("decodeToken failed on a token we just encoded: %v", err)
	}
	if got == nil || !got.CursorVal.Equal(cv) || got.ID != "abc-123" {
		t.Errorf("round-trip = %+v, want (%v, abc-123)", got, cv)
	}
}

func TestDecodeToken_EmptyIsFirstPage(t *testing.T) {
	// An empty token is "start from the first page", not an error.
	got, err := decodeToken("")
	if err != nil || got != nil {
		t.Errorf("decodeToken(\"\") = (%v, %v), want (nil, nil)", got, err)
	}
}

func TestDecodeToken_Malformed(t *testing.T) {
	for _, s := range []string{"!!!not-base64!!!", "aGVsbG8="} { // non-base64, valid-base64-non-JSON
		if _, err := decodeToken(s); err == nil {
			t.Errorf("decodeToken(%q) = %v, want a decode error", s, err)
		}
	}
}

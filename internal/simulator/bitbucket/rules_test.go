// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"testing"
)

func TestCheckRule(t *testing.T) {
	tests := []struct {
		name        string
		workspace   string
		wantStatus  int
		wantMatched bool
		wantHeader  string
	}{
		{name: "no match", workspace: "demo-bb-enterprise", wantStatus: 0, wantMatched: false},
		{name: "perm denied", workspace: "sim-perm-denied", wantStatus: 403, wantMatched: true},
		{name: "rate limit carries Retry-After", workspace: "sim-rate-limit", wantStatus: 429, wantMatched: true, wantHeader: "Retry-After"},
		{name: "server error", workspace: "sim-server-error", wantStatus: 500, wantMatched: true},
		{name: "not found", workspace: "sim-not-found", wantStatus: 404, wantMatched: true},
		{name: "sim-empty is the sentinel", workspace: "sim-empty", wantStatus: simEmpty, wantMatched: true},
		{name: "case insensitive", workspace: "SIM-PERM-DENIED", wantStatus: 403, wantMatched: true},
		{name: "substring of a longer workspace", workspace: "acme-sim-rate-limit-corp", wantStatus: 429, wantMatched: true, wantHeader: "Retry-After"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body, header, _, matched := checkRule(tt.workspace)
			if matched != tt.wantMatched {
				t.Fatalf("matched = %v, want %v", matched, tt.wantMatched)
			}
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if tt.wantHeader != "" && header != tt.wantHeader {
				t.Errorf("extraHeader = %q, want %q", header, tt.wantHeader)
			}
			// Error rules carry the Bitbucket error envelope; sim-empty/no-match do not.
			if matched && status > 0 {
				assertBitbucketErrorEnvelope(t, body)
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	assertBitbucketErrorEnvelope(t, errorBody("boom"))
}

// assertBitbucketErrorEnvelope fails unless b is {"type":"error","error":{"message":…}}
// — the shape real Bitbucket returns for every error, distinct from GitHub's {"message":…}.
func assertBitbucketErrorEnvelope(t *testing.T, b []byte) {
	t.Helper()
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("error body is not valid JSON: %v (%s)", err, b)
	}
	if env.Type != "error" {
		t.Errorf("type = %q, want \"error\" (body: %s)", env.Type, b)
	}
	if env.Error.Message == "" {
		t.Errorf("error.message empty (body: %s)", b)
	}
}

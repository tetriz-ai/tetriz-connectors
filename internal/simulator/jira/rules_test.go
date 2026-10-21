// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestCheckRule(t *testing.T) {
	tests := []struct {
		name        string
		cloudID     string
		wantStatus  int
		wantMatched bool
	}{
		{"perm denied substring", "acme-sim-perm-denied", http.StatusForbidden, true},
		{"rate limit substring", "sim-rate-limit-corp", http.StatusTooManyRequests, true},
		{"server error", "org-sim-server-error", http.StatusInternalServerError, true},
		{"not found", "sim-not-found", http.StatusNotFound, true},
		{"empty sentinel", "demo-sim-empty", simEmpty, true},
		{"case insensitive", "ACME-SIM-PERM-DENIED", http.StatusForbidden, true},
		{"no match clean org", "demo-gh-enterprise", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, _, _, matched := checkRule(tt.cloudID)
			if matched != tt.wantMatched {
				t.Fatalf("matched = %v, want %v", matched, tt.wantMatched)
			}
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
		})
	}
}

func TestCheckRule_RateLimitHeader(t *testing.T) {
	_, _, hdr, val, matched := checkRule("sim-rate-limit")
	if !matched || hdr != "Retry-After" || val != "60" {
		t.Fatalf("rate-limit rule: matched=%v hdr=%q val=%q, want Retry-After/60", matched, hdr, val)
	}
}

func TestErrorBody_JiraEnvelope(t *testing.T) {
	var env struct {
		ErrorMessages []string       `json:"errorMessages"`
		Errors        map[string]any `json:"errors"`
	}
	if err := json.Unmarshal(errorBody("nope"), &env); err != nil {
		t.Fatalf("errorBody is not valid JSON: %v", err)
	}
	if len(env.ErrorMessages) != 1 || env.ErrorMessages[0] != "nope" {
		t.Errorf("errorMessages = %v, want [nope]", env.ErrorMessages)
	}
	if env.Errors == nil {
		t.Error("errors object must be present (Jira envelope always carries it)")
	}
}

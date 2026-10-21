// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"testing"
)

func TestCheckRule(t *testing.T) {
	tests := []struct {
		orgName     string
		wantStatus  int
		wantMatched bool
	}{
		{"acme-sim-perm-denied", 403, true},
		{"sim-perm-denied-corp", 403, true},
		{"SIM-RATE-LIMIT-corp", 429, true},
		{"corp-SIM-SERVER-ERROR", 500, true},
		{"org-SIM-NOT-FOUND", 404, true},
		{"org-sim-empty", simEmpty, true},
		{"normal-acme", 0, false},
		{"demo-org-growth", 0, false},
		// Keyword must be a substring match, not a full match.
		{"sim-perm", 0, false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.orgName, func(t *testing.T) {
			gotCode, _, _, _, gotMatched := checkRule(tt.orgName)
			if gotMatched != tt.wantMatched {
				t.Errorf("checkRule(%q) matched=%v, want %v", tt.orgName, gotMatched, tt.wantMatched)
			}
			if gotMatched && gotCode != tt.wantStatus {
				t.Errorf("checkRule(%q) status=%d, want %d", tt.orgName, gotCode, tt.wantStatus)
			}
		})
	}
}

func TestCheckRule_RateLimitHeader(t *testing.T) {
	_, _, hdr, val, matched := checkRule("corp-sim-rate-limit-test")
	if !matched {
		t.Fatal("expected sim-rate-limit rule to match")
	}
	if hdr != "Retry-After" || val != "60" {
		t.Errorf("expected Retry-After: 60, got %s: %s", hdr, val)
	}
}

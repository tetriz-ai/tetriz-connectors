// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckRule(t *testing.T) {
	type args struct {
		orgName string
	}
	tests := []struct {
		name        string
		args        args
		wantStatus  int
		wantMatched bool
	}{
		{name: "no match on a catalog org", args: args{orgName: "demo-gl-regulated"}, wantStatus: 0, wantMatched: false},
		{name: "perm denied", args: args{orgName: "sim-perm-denied"}, wantStatus: 403, wantMatched: true},
		{name: "rate limit", args: args{orgName: "sim-rate-limit"}, wantStatus: 429, wantMatched: true},
		{name: "server error", args: args{orgName: "sim-server-error"}, wantStatus: 500, wantMatched: true},
		{name: "not found", args: args{orgName: "sim-not-found"}, wantStatus: 404, wantMatched: true},
		{name: "sim-empty is the sentinel", args: args{orgName: "sim-empty"}, wantStatus: simEmpty, wantMatched: true},
		{name: "case insensitive", args: args{orgName: "SIM-PERM-DENIED"}, wantStatus: 403, wantMatched: true},
		{name: "substring of a longer catalog name", args: args{orgName: "demo-gl-sim-rate-limit"}, wantStatus: 429, wantMatched: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body, matched := checkRule(tt.args.orgName)
			if matched != tt.wantMatched {
				t.Fatalf("matched = %v, want %v", matched, tt.wantMatched)
			}
			if status != tt.wantStatus {
				t.Errorf("status = %d, want %d", status, tt.wantStatus)
			}
			if matched && status > 0 {
				assertGitLabError(t, body)
			}
		})
	}
}

func TestErrorBodyShape(t *testing.T) {
	assertGitLabError(t, errorBody("boom"))
}

// assertGitLabError fails unless b is {"message":…} — the shape real GitLab
// returns, distinct from Bitbucket's {"type":"error","error":{"message":…}}.
func assertGitLabError(t *testing.T, b []byte) {
	t.Helper()
	var env struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("error body is not valid JSON: %v (%s)", err, b)
	}
	assert.NotEmpty(t, env.Message)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http"
	"strings"
)

type rule struct {
	keyword     string
	statusCode  int
	message     string
	extraHeader string
	extraValue  string
}

// simEmpty is a sentinel status signalling "return a valid empty response". Unlike
// the error rules, its body shape is route-specific (a list envelope, a bare object,
// or a v1.0 group array), so each handler writes its own empty body on a match.
const simEmpty = 0

// rules share their failure-injection keywords with the base_simulator design — the
// same workspace name produces the same failure class on every vendor simulator — but
// the bodies use Bitbucket's error envelope, not GitHub's {"message":…}.
var rules = []rule{
	{keyword: "sim-perm-denied", statusCode: http.StatusForbidden, message: "Access denied. You must have write or admin access."},
	{keyword: "sim-rate-limit", statusCode: http.StatusTooManyRequests, message: "Rate limit for this resource has been exceeded", extraHeader: "Retry-After", extraValue: "60"},
	{keyword: "sim-server-error", statusCode: http.StatusInternalServerError, message: "Something went wrong"},
	{keyword: "sim-not-found", statusCode: http.StatusNotFound, message: "Repository not found"},
	{keyword: "sim-empty", statusCode: simEmpty, message: ""},
}

// checkRule matches a workspace name against the failure-injection keywords
// (first match wins). Returns (statusCode, body, extraHeader, extraValue, matched);
// statusCode==simEmpty signals "return a valid empty response" (route-specific shape,
// so body is nil and the handler writes it). matched==false means no rule hit.
func checkRule(workspace string) (int, []byte, string, string, bool) {
	lower := strings.ToLower(workspace)
	for _, r := range rules {
		if strings.Contains(lower, r.keyword) {
			if r.statusCode == simEmpty {
				return simEmpty, nil, "", "", true
			}
			return r.statusCode, errorBody(r.message), r.extraHeader, r.extraValue, true
		}
	}
	return 0, nil, "", "", false
}

// errorBody renders the Bitbucket error envelope {"type":"error","error":{"message":…}}.
func errorBody(msg string) []byte {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		"type":  "error",
		"error": map[string]string{"message": msg},
	})
	return b
}

func writeRule(w http.ResponseWriter, statusCode int, body []byte, extraHeader, extraValue string) {
	if extraHeader != "" {
		w.Header().Set(extraHeader, extraValue)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(body) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

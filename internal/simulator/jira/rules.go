// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

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
// the error rules, its body shape is route-specific (a JQL search envelope, a
// values-list envelope, or a worklog envelope), so each handler writes its own
// empty body on a match.
const simEmpty = 0

// rules share their failure-injection keywords with the base_simulator design — the
// same cloud ID produces the same failure class on every vendor simulator — but the
// bodies use Jira's error envelope {"errorMessages":[…],"errors":{}}, which real
// Jira returns for every error regardless of route.
var rules = []rule{
	{keyword: "sim-perm-denied", statusCode: http.StatusForbidden, message: "You do not have permission to view this resource."},
	{keyword: "sim-rate-limit", statusCode: http.StatusTooManyRequests, message: "Rate limit exceeded.", extraHeader: "Retry-After", extraValue: "60"},
	{keyword: "sim-server-error", statusCode: http.StatusInternalServerError, message: "Internal server error."},
	{keyword: "sim-not-found", statusCode: http.StatusNotFound, message: "Issue does not exist or you do not have permission to see it."},
	{keyword: "sim-empty", statusCode: simEmpty, message: ""},
}

// checkRule matches a cloud ID against the failure-injection keywords (first match
// wins). Returns (statusCode, body, extraHeader, extraValue, matched); statusCode==
// simEmpty signals "return a valid empty response" (route-specific shape, so body is
// nil and the handler writes it). matched==false means no rule hit.
func checkRule(cloudID string) (int, []byte, string, string, bool) {
	lower := strings.ToLower(cloudID)
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

// errorBody renders Jira's error envelope {"errorMessages":[…],"errors":{}}.
func errorBody(msg string) []byte {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		"errorMessages": []string{msg},
		"errors":        map[string]any{},
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(errorBody(msg)) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

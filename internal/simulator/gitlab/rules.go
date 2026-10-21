// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type rule struct {
	keyword    string
	statusCode int
	message    string
}

// simEmpty is a sentinel status signalling "return a valid empty response".
// Unlike the error rules, its body shape is route-specific (a bare [] on lists,
// a 404 on single-project GET), so each handler writes its own empty body.
const simEmpty = 0

// rules share their failure-injection keywords with the other vendor simulators
// — the same catalog-name substring produces the same failure class — but the
// bodies use GitLab's {"message":…} envelope, not Bitbucket's type/error object.
var rules = []rule{
	{keyword: "sim-perm-denied", statusCode: http.StatusForbidden, message: "Access denied. You must have write or admin access."},
	{keyword: "sim-rate-limit", statusCode: http.StatusTooManyRequests, message: "Rate limit for this resource has been exceeded"},
	{keyword: "sim-server-error", statusCode: http.StatusInternalServerError, message: "Something went wrong"},
	{keyword: "sim-not-found", statusCode: http.StatusNotFound, message: "404 Not Found"},
	{keyword: "sim-empty", statusCode: simEmpty, message: ""},
}

// checkRule matches a catalog org name against the failure-injection keywords
// (first match wins, case-insensitive substring). Returns (statusCode, body, matched);
// statusCode==simEmpty signals "return a valid empty response" (route-specific
// shape, so body is nil and the handler writes it). matched==false means no rule hit.
func checkRule(orgName string) (int, []byte, bool) {
	lower := strings.ToLower(orgName)
	for _, r := range rules {
		if strings.Contains(lower, r.keyword) {
			if r.statusCode == simEmpty {
				return simEmpty, nil, true
			}
			return r.statusCode, errorBody(r.message), true
		}
	}
	return 0, nil, false
}

// errorBody renders GitLab's error JSON {"message":…}.
func errorBody(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"message": msg}) //nolint:errcheck // literal map of JSON-safe types, cannot fail
	return b
}

// writeRule writes a GitLab-shaped error. A 429 also sets Retry-After plus the
// RateLimit-* trio: ClassifyError reads Retry-After, and ParseBudget reads
// Remaining/Limit/Reset — omitting either side loses backoff or the budget snapshot.
func writeRule(w http.ResponseWriter, statusCode int, body []byte) {
	if statusCode == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "60")
		w.Header().Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10))
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("RateLimit-Limit", "10")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(body) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

// applyRules writes a GitLab error when org matches a failure-injection keyword.
// Returns (simEmpty, stop). stop means the handler must return; simEmpty means
// the handler writes its own empty success shape.
func applyRules(w http.ResponseWriter, org string) (simEmptyMatch, stop bool) {
	status, body, matched := checkRule(org)
	if !matched {
		return false, false
	}
	if status > 0 {
		writeRule(w, status, body)
		return false, true
	}
	return true, false
}

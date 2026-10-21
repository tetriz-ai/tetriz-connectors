// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

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

// simEmpty is a sentinel status that signals "return a valid empty response".
const simEmpty = 0

var rules = []rule{
	{keyword: "sim-perm-denied", statusCode: 403, message: "Resource not accessible by integration"},
	{keyword: "sim-rate-limit", statusCode: 429, message: "API rate limit exceeded", extraHeader: "Retry-After", extraValue: "60"},
	{keyword: "sim-server-error", statusCode: 500, message: "Server Error"},
	{keyword: "sim-not-found", statusCode: 404, message: "Not Found"},
	{keyword: "sim-empty", statusCode: simEmpty, message: ""},
}

// checkRule returns (statusCode, body, extraHeader, extraValue, matched).
// statusCode==0 means "return valid empty response". matched==false means no rule hit.
func checkRule(orgName string) (int, []byte, string, string, bool) {
	lower := strings.ToLower(orgName)
	for _, r := range rules {
		if strings.Contains(lower, r.keyword) {
			if r.statusCode == simEmpty {
				return simEmpty, nil, "", "", true
			}
			body, _ := json.Marshal(map[string]string{"message": r.message}) //nolint:errcheck // literal map of JSON-safe types, cannot fail
			return r.statusCode, body, r.extraHeader, r.extraValue, true
		}
	}
	return 0, nil, "", "", false
}

func writeRule(w http.ResponseWriter, statusCode int, body []byte, extraHeader, extraValue string) {
	if extraHeader != "" {
		w.Header().Set(extraHeader, extraValue)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(body) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

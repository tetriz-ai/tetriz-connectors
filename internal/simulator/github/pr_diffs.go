// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handlePRDiff serves GET /repos/{owner}/{repo}/pulls/{number}.
// When the request carries Accept: application/vnd.github.diff it returns the
// raw unified diff stored in the PR payload; otherwise it returns JSON metadata
// (head/base SHA) consumed by the diff-metadata phase of fetch_pr_diffs.
func (s *Service) handlePRDiff(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]
	repo := vars["repo"]
	number, _ := strconv.Atoi(vars["number"]) //nolint:errcheck // malformed number falls through to a not-found lookup below

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}

	wantDiff := strings.Contains(r.Header.Get("Accept"), "vnd.github.diff")

	if matched {
		if wantDiff {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("")) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
		} else {
			writeJSON(w, map[string]any{"head": map[string]any{fieldSHA: ""}, "base": map[string]any{fieldSHA: ""}})
		}
		return
	}

	row, err := s.dao.GetByPRNumber(r.Context(), s.vendor, org, repo, number)
	if err != nil {
		logger.Error(r.Context(), "simulator: handlePRDiff", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	if wantDiff {
		w.Header().Set("Content-Type", "text/plain")
		if row != nil {
			w.Write([]byte(extractDiffText(row.Payload))) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
		}
		return
	}

	// JSON metadata — connector needs head.sha and base.sha.
	var headSHA, baseSHA string
	if row != nil {
		headSHA, baseSHA = extractPRSHAs(row.Payload)
	}
	writeJSON(w, map[string]any{
		"head": map[string]any{fieldSHA: headSHA},
		"base": map[string]any{fieldSHA: baseSHA},
	})
}

// handlePRFiles serves GET /repos/{owner}/{repo}/pulls/{number}/files.
// The connector uses this as the paginated fallback when the unified diff
// endpoint fails or is too large. Demo-seed does not populate per-file diffs,
// so the simulator always returns an empty array.
func (s *Service) handlePRFiles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	_ = matched
	writeJSON(w, []any{})
}

// extractDiffText returns the raw diff string from a PR row payload, or empty
// string if the field is absent. The demo-seed populates this field; absence
// returns an empty diff, so the route still answers for a row seeded without one.
func extractDiffText(payload []byte) string {
	var p struct {
		Diff string `json:"diff"`
	}
	if err := json.Unmarshal(payload, &p); err != nil { // absent/malformed diff field is a graceful no-op, see doc comment above
		return ""
	}
	return p.Diff
}

// extractPRSHAs derives head/base SHAs from a PR row payload.
// head SHA comes from the first commit's oid; base SHA from the top-level
// baseSha field populated by demo-seed.
func extractPRSHAs(payload []byte) (head, base string) {
	var p struct {
		BaseSha string `json:"baseSha"`
		Commits struct {
			Nodes []struct {
				OID string `json:"oid"`
			} `json:"nodes"`
		} `json:"commits"`
	}
	if err := json.Unmarshal(payload, &p); err != nil { // absent/malformed SHA fields are a graceful no-op, see doc comment above
		return "", ""
	}
	if len(p.Commits.Nodes) > 0 {
		head = p.Commits.Nodes[0].OID
	}
	return head, p.BaseSha
}

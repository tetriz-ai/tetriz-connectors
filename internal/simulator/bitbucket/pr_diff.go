// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handlePRDiff serves GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/diff.
// Bitbucket's diff endpoint always returns the raw unified diff as text/plain (no
// Accept negotiation, unlike GitHub's dual JSON/diff route), so this handler writes
// the PR payload's stored `diff` string verbatim. Unknown PR id → 404, which the
// connector maps to its NOT_AVAILABLE sentinel.
func (s *Service) handlePRDiff(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo := vars["workspace"], vars["repo"]
	id, _ := strconv.Atoi(vars["id"]) //nolint:errcheck // malformed id falls through to a not-found lookup

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty: a success body, which for the diff route is empty text
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("")) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
		return
	}

	row, err := s.dao.GetByPRID(r.Context(), s.vendor, workspace, repo, id)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handlePRDiff", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "Pull request not found")
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(extractPRDiff(row.Payload))) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
}

// extractPRDiff returns the raw diff string from a PR row payload, or empty string
// if the field is absent — a graceful no-op so the route works before demo-seed
// backfills the field.
func extractPRDiff(payload []byte) string {
	var p struct {
		Diff string `json:"diff"`
	}
	if err := json.Unmarshal(payload, &p); err != nil { // absent/malformed diff field is a graceful no-op, see doc comment
		return ""
	}
	return p.Diff
}

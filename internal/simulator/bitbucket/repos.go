// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleRepos serves GET /2.0/repositories/{workspace} — the paginated repo list
// that drives repo discovery (and the ACTIVE/DELETED reconciliation of
// DiscoveredRepos) when the connector config has no explicit repository list.
func (s *Service) handleRepos(w http.ResponseWriter, r *http.Request) {
	workspace := mux.Vars(r)["workspace"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty
		emptyListEnvelope(w)
		return
	}

	page, pagelen := parsePagination(r)
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, workspace, "repo", "", pagelen, (page-1)*pagelen)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handleRepos", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeEnvelope(w, r, payloads(rows), page, pagelen)
}

// handleSingleRepo serves GET /2.0/repositories/{workspace}/{repo} — the single
// repo object the connector reads for default-branch resolution. Unknown repo → 404
// (Bitbucket returns a not-found error, not an empty object).
func (s *Service) handleSingleRepo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo := vars["workspace"], vars["repo"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty → empty object (single-repo shape, not a list envelope)
		writeJSON(w, map[string]any{})
		return
	}

	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, workspace, "repo", repo, 1, 0)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handleSingleRepo", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if len(rows) == 0 {
		writeError(w, http.StatusNotFound, "Repository not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(rows[0].Payload) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

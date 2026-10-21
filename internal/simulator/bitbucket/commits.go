// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleCommits serves GET /2.0/repositories/{workspace}/{repo}/commits — the
// repo-level commit stream on the default branch, newest-first and page-windowed.
// This stream includes direct-push commits with no PR (the demo-bb-solo archetype),
// so it is served from dedicated `commit` rows, not from PR-nested commits.
func (s *Service) handleCommits(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo := vars["workspace"], vars["repo"]

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
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, workspace, "commit", repo, pagelen, (page-1)*pagelen)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handleCommits", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeEnvelope(w, r, payloads(rows), page, pagelen)
}

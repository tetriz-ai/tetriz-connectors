// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleCommitDiffstat serves GET /2.0/repositories/{workspace}/{repo}/diffstat/{sha}
// — the per-commit diffstat, extracted from the commit row (keyed on payload->>'hash')
// and windowed into the standard list envelope. Unknown sha → 404.
func (s *Service) handleCommitDiffstat(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo, sha := vars["workspace"], vars["repo"], vars["sha"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty
		emptyListEnvelope(w)
		return
	}

	row, err := s.dao.GetByCommitHash(r.Context(), s.vendor, workspace, repo, sha)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handleCommitDiffstat", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "Commit not found")
		return
	}

	values := extractArray(r.Context(), row.Payload, "diffstat", "handleCommitDiffstat")
	page, pagelen := parsePagination(r)
	writeEnvelope(w, r, windowSlice(values, page, pagelen), page, pagelen)
}

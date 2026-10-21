// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handlePRList serves GET /2.0/repositories/{workspace}/{repo}/pullrequests — the
// paginated PR enumeration. state/sort/fields/include query params are accepted and
// ignored: the simulator always returns the full stored shape newest-first, which
// matches what the connector (all states, descending) would receive anyway.
func (s *Service) handlePRList(w http.ResponseWriter, r *http.Request) {
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
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, workspace, "pr", repo, pagelen, (page-1)*pagelen)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handlePRList", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeEnvelope(w, r, payloads(rows), page, pagelen)
}

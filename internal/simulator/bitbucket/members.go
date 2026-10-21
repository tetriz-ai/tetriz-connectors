// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleMembers serves GET /2.0/workspaces/{workspace}/members — the workspace member
// list (entity_type "member", workspace-scoped, not repo-scoped), page-windowed.
func (s *Service) handleMembers(w http.ResponseWriter, r *http.Request) {
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
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, workspace, "member", "", pagelen, (page-1)*pagelen)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket handleMembers", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeEnvelope(w, r, payloads(rows), page, pagelen)
}

// handleGroups serves GET /1.0/groups/{workspace}/ — the v1.0 groups endpoint
// the connector derives from the /2.0 base by string replacement. The demo data models
// no groups, so it always returns a bare `[]` (the connector unmarshals `[]bbGroup`) —
// distinct from the 2.0 list envelope. An error keyword still injects the Bitbucket
// error envelope; every other case (including sim-empty) is the same empty array.
func (s *Service) handleGroups(w http.ResponseWriter, r *http.Request) {
	workspace := mux.Vars(r)["workspace"]

	if statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace); matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	writeJSON(w, []json.RawMessage{})
}

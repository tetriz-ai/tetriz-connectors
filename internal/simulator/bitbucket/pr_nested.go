// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handlePRNestedArray serves a per-PR route that returns a nested array field of the
// PR row (keyed on payload->>'id'), windowed into the list envelope. It is the shared
// body behind the diffstat, activity, and comments routes — Bitbucket stores those
// arrays in their final REST shape on the PR payload, so no field conversion is
// needed (unlike the GitHub sibling, which rebuilds REST shapes from GraphQL nodes).
// Unknown PR id → 404. label names the caller for log lines and the extract warning.
func (s *Service) handlePRNestedArray(w http.ResponseWriter, r *http.Request, field, label string) {
	vars := mux.Vars(r)
	workspace, repo := vars["workspace"], vars["repo"]
	id, _ := strconv.Atoi(vars["id"]) //nolint:errcheck // malformed id falls through to a not-found lookup

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty
		emptyListEnvelope(w)
		return
	}

	row, err := s.dao.GetByPRID(r.Context(), s.vendor, workspace, repo, id)
	if err != nil {
		logger.Error(r.Context(), "simulator: bitbucket "+label, zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "Pull request not found")
		return
	}

	values := extractArray(r.Context(), row.Payload, field, label)
	page, pagelen := parsePagination(r)
	writeEnvelope(w, r, windowSlice(values, page, pagelen), page, pagelen)
}

// handlePRActivity serves GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/activity.
func (s *Service) handlePRActivity(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "activity", "handlePRActivity")
}

// handlePRComments serves GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/comments.
func (s *Service) handlePRComments(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "comments", "handlePRComments")
}

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

// handlePRCommits serves GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/commits.
// The commits live as a nested array on the PR row (keyed on payload->>'id'). When
// sort=date is requested the commits are returned oldest-first so fetch_prs's
// first-commit enrichment (sort=date, reads the earliest) resolves first_commit_at;
// every other caller gets the stored order. Unknown PR id → 404.
func (s *Service) handlePRCommits(w http.ResponseWriter, r *http.Request) {
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
		logger.Error(r.Context(), "simulator: bitbucket handlePRCommits", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "Pull request not found")
		return
	}

	values := extractArray(r.Context(), row.Payload, "commits", "handlePRCommits")
	if r.URL.Query().Get("sort") == "date" {
		values = sortCommitsByDateAsc(values)
	}
	page, pagelen := parsePagination(r)
	writeEnvelope(w, r, windowSlice(values, page, pagelen), page, pagelen)
}

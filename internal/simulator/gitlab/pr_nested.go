// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handlePRNestedArray serves a per-MR route that returns a nested array field of
// the PR row (keyed on project_id + iid), windowed into a bare JSON array.
// Unknown iid → 404.
func (s *Service) handlePRNestedArray(w http.ResponseWriter, r *http.Request, field, label string) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	iid, err := parsePositiveIID(r)
	if err != nil {
		logger.Debug(r.Context(), "simulator: gitlab invalid merge request iid", zap.Error(err))
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	row, err := s.dao.GetByProjectIDAndIID(r.Context(), s.vendor, org, projectID, iid)
	if err != nil {
		writeDBError(r.Context(), w, label, err)
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	values := extractArray(r.Context(), row.Payload, field, label)
	page, perPage := parsePagination(r)
	writeList(w, windowSlice(values, page, perPage), page, perPage)
}

// handleMRCommits serves GET /projects/{id}/merge_requests/{iid}/commits.
func (s *Service) handleMRCommits(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "commits", "handleMRCommits")
}

// handleMRNotes serves GET /projects/{id}/merge_requests/{iid}/notes.
func (s *Service) handleMRNotes(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "notes", "handleMRNotes")
}

// handleMRDiffs serves GET /projects/{id}/merge_requests/{iid}/diffs.
// A PR with no diffs[] returns [] — the connector treats that as an empty
// change set, not a missing MR.
func (s *Service) handleMRDiffs(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "diffs", "handleMRDiffs")
}

// handleMRApprovals serves GET /projects/{id}/merge_requests/{iid}/approvals.
// The stored field is an object {"approved","approved_by"}, not an array — the
// connector unmarshals glApprovalsResponse from this body.
func (s *Service) handleMRApprovals(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		writeJSON(w, json.RawMessage(defaultApprovals))
		return
	}

	iid, err := parsePositiveIID(r)
	if err != nil {
		logger.Debug(r.Context(), "simulator: gitlab invalid merge request iid", zap.Error(err))
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	row, err := s.dao.GetByProjectIDAndIID(r.Context(), s.vendor, org, projectID, iid)
	if err != nil {
		writeDBError(r.Context(), w, "handleMRApprovals", err)
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(extractObject(r.Context(), row.Payload, "approvals", "handleMRApprovals")) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

func parsePositiveIID(r *http.Request) (int, error) {
	iid, err := strconv.Atoi(mux.Vars(r)["iid"])
	if err != nil {
		return 0, fmt.Errorf("parse iid: %w", err)
	}
	if iid <= 0 {
		return 0, fmt.Errorf("iid %d is not positive", iid)
	}
	return iid, nil
}

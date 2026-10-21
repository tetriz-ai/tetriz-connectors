// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const defaultWorklogMaxResults = 50

// handleWorklog serves GET /issue/{issueIdOrKey}/worklog for fetch_worklogs. The
// worklogs live nested on the issue row (payload.fields.worklogs) — the same
// everything-on-the-parent-row pattern GitHub uses for reviews/comments — so this
// reads the issue by key and windows its worklogs by startAt/maxResults.
func (s *Service) handleWorklog(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, worklogEnvelope(nil, 0, 0, defaultWorklogMaxResults))
		return
	}

	key := mux.Vars(r)["issueIdOrKey"]
	startAt := parseNonNegQuery(r, fieldStartAt, 0)
	maxResults := parseNonNegQuery(r, fieldMaxResults, defaultWorklogMaxResults)

	row, err := s.dao.GetByIssueKey(r.Context(), s.vendor, cloudID, key)
	if err != nil {
		logger.Error(r.Context(), "jira simulator: worklog lookup failed",
			zap.String("cloudId", cloudID), zap.String("issue", key), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "worklog lookup failed")
		return
	}
	// An unknown issue key is a valid empty response, not a 404.
	if row == nil {
		writeJSON(w, worklogEnvelope(nil, 0, startAt, maxResults))
		return
	}

	all := extractWorklogs(row.Payload)
	window := window(all, startAt, maxResults)
	writeJSON(w, worklogEnvelope(window, len(all), startAt, maxResults))
}

// extractWorklogs pulls the nested fields.worklogs array from an issue payload.
func extractWorklogs(payload []byte) []json.RawMessage {
	var p struct {
		Fields struct {
			Worklogs []json.RawMessage `json:"worklogs"`
		} `json:"fields"`
	}
	_ = json.Unmarshal(payload, &p) //nolint:errcheck // this package seeded the payload; a missing worklogs → nil → empty
	return p.Fields.Worklogs
}

// worklogEnvelope builds Jira's startAt-paginated worklog response envelope; the
// connector stops when startAt+len(worklogs) >= total (op_worklogs paging loop).
func worklogEnvelope(worklogs []json.RawMessage, total, startAt, maxResults int) map[string]any {
	if worklogs == nil {
		worklogs = []json.RawMessage{}
	}
	return map[string]any{
		"worklogs":      worklogs,
		fieldTotal:      total,
		fieldStartAt:    startAt,
		fieldMaxResults: maxResults,
	}
}

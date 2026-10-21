// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const defaultProjectMaxResults = 50

// handleProjectSearch serves GET /project/search for fetch_projects and the
// internal project-discovery call every issue sync makes first. It returns a
// startAt/maxResults window over the org's stored `project` rows with the
// offset-pagination envelope {values, total, isLast} the connector expects.
func (s *Service) handleProjectSearch(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, map[string]any{fieldValues: []json.RawMessage{}, fieldTotal: 0, fieldIsLast: true})
		return
	}

	startAt := parseNonNegQuery(r, fieldStartAt, 0)
	maxResults := parseNonNegQuery(r, fieldMaxResults, defaultProjectMaxResults)
	if maxResults <= 0 {
		maxResults = defaultProjectMaxResults
	}

	all, err := s.dao.ListAll(r.Context(), s.vendor, cloudID, "project")
	if err != nil {
		logger.Error(r.Context(), "jira simulator: project search failed", zap.String("cloudId", cloudID), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "project search failed")
		return
	}
	total := len(all)
	window := window(all, startAt, maxResults)
	values := make([]json.RawMessage, 0, len(window))
	for _, row := range window {
		values = append(values, json.RawMessage(row.Payload))
	}
	writeJSON(w, map[string]any{
		fieldValues: values,
		fieldTotal:  total,
		// isLast is true once this window reaches the end, so the connector's
		// startAt += len loop terminates.
		fieldIsLast: startAt+len(window) >= total,
	})
}

// parseNonNegQuery reads a positive integer query param, falling back to def when
// the param is absent, unparseable, or non-positive. startAt=0 collapses to def
// (which is 0 for startAt), so the absent/explicit-zero cases coincide.
func parseNonNegQuery(r *http.Request, name string, def int) int {
	// Atoi returns 0 on an absent/garbage param, so the >0 guard keeps the default;
	// the parse error carries no more than that and is intentionally dropped.
	if v, _ := strconv.Atoi(r.URL.Query().Get(name)); v > 0 { //nolint:errcheck // absent/garbage → 0 → default kept
		return v
	}
	return def
}

// window returns the [startAt, startAt+maxResults) slice, or an empty slice past
// the end — Jira's startAt/maxResults paging over a list already held in memory,
// which is how every route except the keyset-paged issue search reports a page.
func window[T any](items []T, startAt, maxResults int) []T {
	if startAt < 0 || startAt >= len(items) {
		return nil
	}
	return items[startAt:min(startAt+maxResults, len(items))]
}

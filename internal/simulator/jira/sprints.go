// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const defaultAgileMaxResults = 50

// deriveBoardID maps a Jira project's numeric id to its synthetic scrum board id
// (projectID - 1000, e.g. project 10001 → board 9001). Boards are not a
// stored entity — the board-listing handler derives one board per project row, and
// a sprint row's originBoardId is stamped with the same formula, so board listing
// and per-board sprint lookup agree. The demoseed builder carries an independent
// copy of this formula (the two packages don't import each other); the seed contract
// test pins them equal. A non-numeric id yields a non-positive board id, which
// boardFromProject rejects.
func deriveBoardID(projectID string) int64 {
	n, _ := strconv.ParseInt(projectID, 10, 64) //nolint:errcheck // non-numeric id → 0 → a non-positive board id the caller rejects
	return n - 1000
}

// handleBoardList serves GET /rest/agile/1.0/board for fetch_sprints. Boards are
// synthesized one-per-project (all scrum), so a non-scrum type filter returns none.
func (s *Service) handleBoardList(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, emptyValuesEnvelope())
		return
	}
	if t := r.URL.Query().Get("type"); t != "" && t != "scrum" {
		writeJSON(w, emptyValuesEnvelope())
		return
	}

	startAt := parseNonNegQuery(r, fieldStartAt, 0)
	maxResults := parseNonNegQuery(r, fieldMaxResults, defaultAgileMaxResults)

	projects, err := s.dao.ListAll(r.Context(), s.vendor, cloudID, "project")
	if err != nil {
		logger.Error(r.Context(), "jira simulator: board listing failed", zap.String("cloudId", cloudID), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "board listing failed")
		return
	}
	boards := make([]json.RawMessage, 0, len(projects))
	for _, p := range projects {
		if b, ok := boardFromProject(r.Context(), p.Payload); ok {
			boards = append(boards, b)
		}
	}
	window := window(boards, startAt, maxResults)
	writeJSON(w, valuesEnvelope(window, len(boards), startAt, maxResults))
}

// handleBoardSprints serves GET /rest/agile/1.0/board/{boardId}/sprint — the stored
// sprint rows whose originBoardId matches the board.
func (s *Service) handleBoardSprints(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, emptyValuesEnvelope())
		return
	}

	boardID := mux.Vars(r)["boardId"]
	startAt := parseNonNegQuery(r, fieldStartAt, 0)
	maxResults := parseNonNegQuery(r, fieldMaxResults, defaultAgileMaxResults)

	rows, err := s.dao.ListAll(r.Context(), s.vendor, cloudID, "sprint")
	if err != nil {
		logger.Error(r.Context(), "jira simulator: sprint listing failed", zap.String("cloudId", cloudID), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "sprint listing failed")
		return
	}
	matched := make([]json.RawMessage, 0, len(rows))
	for _, sp := range rows {
		if sprintOriginBoardID(r.Context(), sp.Payload) == boardID {
			matched = append(matched, json.RawMessage(sp.Payload))
		}
	}
	window := window(matched, startAt, maxResults)
	writeJSON(w, valuesEnvelope(window, len(matched), startAt, maxResults))
}

// boardFromProject synthesizes a scrum board payload from a stored project row. The
// location.projectKey/projectId let the connector resolve the sprint's owning
// project without a separate board sync.
func boardFromProject(ctx context.Context, projectPayload []byte) (json.RawMessage, bool) {
	var p struct {
		ID   string `json:"id"`
		Key  string `json:"key"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(projectPayload, &p); err != nil {
		logger.WarnWithCtx(ctx, "jira simulator: skipping malformed project payload in board derivation", zap.Error(err))
		return nil, false
	}
	if p.Key == "" {
		return nil, false
	}
	boardID := deriveBoardID(p.ID)
	// Refuse to synthesize a board from a project whose id doesn't yield a positive
	// board id: a 0 id would otherwise match every originBoardId-less sprint row.
	if boardID <= 0 {
		return nil, false
	}
	projID, _ := strconv.ParseInt(p.ID, 10, 64) //nolint:errcheck // seeded numeric id; already validated positive via deriveBoardID
	board := map[string]any{
		"id":   boardID,
		"name": p.Key + " board",
		"type": "scrum",
		"location": map[string]any{
			"projectId":  projID,
			"projectKey": p.Key,
		},
	}
	b, _ := json.Marshal(board) //nolint:errcheck // literal map of JSON-safe values
	return b, true
}

// sprintOriginBoardID reads a sprint payload's originBoardId as a string, to match
// the {boardId} path segment.
func sprintOriginBoardID(ctx context.Context, payload []byte) string {
	var p struct {
		OriginBoardID int64 `json:"originBoardId"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		logger.WarnWithCtx(ctx, "jira simulator: skipping malformed sprint payload in board filter", zap.Error(err))
		return ""
	}
	return fmt.Sprint(p.OriginBoardID)
}

// valuesEnvelope builds the Jira Agile offset-paginated list envelope
// {values,startAt,maxResults,isLast,total}; isLast terminates the connector's
// startAt+=len paging loop.
func valuesEnvelope(values []json.RawMessage, total, startAt, maxResults int) map[string]any {
	if values == nil {
		values = []json.RawMessage{}
	}
	return map[string]any{
		fieldValues:     values,
		fieldStartAt:    startAt,
		fieldMaxResults: maxResults,
		fieldIsLast:     startAt+len(values) >= total,
		fieldTotal:      total,
	}
}

// emptyValuesEnvelope is the route's sim-empty body.
func emptyValuesEnvelope() map[string]any {
	return map[string]any{fieldValues: []json.RawMessage{}, fieldTotal: 0, fieldIsLast: true}
}

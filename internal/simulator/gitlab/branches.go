// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleBranches serves GET /projects/{id}/repository/branches. Prefers a
// stored `branches` array on the repo payload; otherwise synthesizes one
// default branch from `default_branch` (or "main") so a repo without that
// array still has a HEAD the tree walk can pin.
func (s *Service) handleBranches(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	values, err := s.branchesForProject(r.Context(), org, projectID)
	if err != nil {
		writeDBError(r.Context(), w, "handleBranches", err)
		return
	}
	page, perPage := parsePagination(r)
	writeList(w, windowSlice(values, page, perPage), page, perPage)
}

// handleBranch serves GET /projects/{id}/repository/branches/{name}. The
// connector reads commit.id as the HEAD SHA to seed the repo-tree walk.
func (s *Service) handleBranch(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	name := mux.Vars(r)["name"]
	values, err := s.branchesForProject(r.Context(), org, projectID)
	if err != nil {
		writeDBError(r.Context(), w, "handleBranch", err)
		return
	}
	for _, raw := range values {
		var b struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &b); err != nil {
			logger.Error(r.Context(), "simulator: gitlab skip malformed branch object", zap.Error(err))
			continue
		}
		if b.Name == name {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(raw) //nolint:gosec // best-effort write; client disconnects aren't actionable
			return
		}
	}
	writeError(w, http.StatusNotFound, "404 Not Found")
}

func (s *Service) branchesForProject(ctx context.Context, org string, projectID int64) ([]json.RawMessage, error) {
	row, err := s.findRepo(ctx, org, projectID)
	if err != nil {
		return nil, fmt.Errorf("branches for project %d: %w", projectID, err)
	}
	if row == nil {
		return []json.RawMessage{syntheticBranch("main", projectID, true)}, nil
	}
	if stored := extractArray(ctx, row.Payload, "branches", "branchesForProject"); len(stored) > 0 {
		return stored, nil
	}
	var p struct {
		DefaultBranch string `json:"default_branch"`
	}
	_ = json.Unmarshal(row.Payload, &p) //nolint:errcheck // missing field → empty → "main"
	name := p.DefaultBranch
	if name == "" {
		name = "main"
	}
	return []json.RawMessage{syntheticBranch(name, projectID, true)}, nil
}

// syntheticBranch is the slim GitLab branch object the connector unmarshals:
// name plus commit.id. The SHA is derived from project_id so it is stable
// without a DB hit and agrees between the listing and single-branch routes.
func syntheticBranch(name string, projectID int64, isDefault bool) json.RawMessage {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck,gosec // marshaling a fixed map shape never fails
		"name":    name,
		"default": isDefault,
		"commit":  map[string]any{"id": deriveCommitSHA(projectID)},
	})
	return b
}

// deriveCommitSHA returns a deterministic 40-hex commit SHA from project_id.
func deriveCommitSHA(projectID int64) string {
	return fmt.Sprintf("%040x", uint64(projectID)) //nolint:gosec // project_id is a synthetic positive catalog id
}

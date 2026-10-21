// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/http"
)

// handleGroupProjects serves GET /groups/{id}/projects — the paginated project
// list that drives repo discovery. entity_type is `repo` (GitLab's REST name is
// "project"; the seed discriminator stays `repo` to share the catalog type).
func (s *Service) handleGroupProjects(w http.ResponseWriter, r *http.Request) {
	org, emptyMatch, ok := s.orgFromGroupPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	page, perPage := parsePagination(r)
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, org, "repo", "", perPage, (page-1)*perPage)
	if err != nil {
		writeDBError(r.Context(), w, "handleGroupProjects", err)
		return
	}
	writeList(w, payloads(rows), page, perPage)
}

// handleSingleProject serves GET /projects/{id}. Unknown or sim-empty project
// is 404: a missing project is not an empty list.
func (s *Service) handleSingleProject(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	row, err := s.findRepo(r.Context(), org, projectID)
	if err != nil {
		writeDBError(r.Context(), w, "handleSingleProject", err)
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(row.Payload) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

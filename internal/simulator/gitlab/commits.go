// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// handleRepoCommits serves GET /projects/{id}/repository/commits — the
// project-level commit stream (entity_type `commit`). First-class commit rows
// exist so FlavorDirectPush orgs with zero MRs still have a commit list.
func (s *Service) handleRepoCommits(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	page, perPage := parsePagination(r)
	rows, err := s.dao.ListPageWindowByProjectID(r.Context(), s.vendor, org, "commit", projectID, perPage, (page-1)*perPage)
	if err != nil {
		writeDBError(r.Context(), w, "handleRepoCommits", err)
		return
	}
	writeList(w, payloads(rows), page, perPage)
}

// handleCommitDetail serves GET /projects/{id}/repository/commits/{sha}?stats=true.
// fetch_commits hits the detail URL; the collection payload has no stats, so a
// 404 here would emit zero added/deleted lines for every commit.
func (s *Service) handleCommitDetail(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	sha := mux.Vars(r)["sha"]
	for offset := 0; ; offset += maxPerPage {
		rows, err := s.dao.ListPageWindowByProjectID(r.Context(), s.vendor, org, "commit", projectID, maxPerPage, offset)
		if err != nil {
			writeDBError(r.Context(), w, "handleCommitDetail", err)
			return
		}
		for _, row := range rows {
			if payloadSHA(row.Payload) == sha {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(row.Payload) //nolint:gosec // best-effort write; client disconnects aren't actionable
				return
			}
		}
		if len(rows) < maxPerPage {
			break
		}
	}
	writeError(w, http.StatusNotFound, "404 Not Found")
}

func payloadSHA(payload []byte) string {
	var p struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(payload, &p) //nolint:errcheck // missing id → empty, treated as no match
	return p.ID
}

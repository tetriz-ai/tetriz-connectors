// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/http"
)

// handleGroupMembers serves GET /groups/{id}/members/all — the group member
// list (entity_type `member`, group-scoped, not project-scoped), page-windowed.
func (s *Service) handleGroupMembers(w http.ResponseWriter, r *http.Request) {
	org, emptyMatch, ok := s.orgFromGroupPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	page, perPage := parsePagination(r)
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, org, "member", "", perPage, (page-1)*perPage)
	if err != nil {
		writeDBError(r.Context(), w, "handleGroupMembers", err)
		return
	}
	writeList(w, payloads(rows), page, perPage)
}

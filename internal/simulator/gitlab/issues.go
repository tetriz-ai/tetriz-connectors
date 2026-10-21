// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/http"
)

// handleGroupIssues serves GET /groups/{id}/issues. gitlab-issues orgs store
// author on every issue; ParseIssue skips a record that has neither author.id
// nor author.username. An org with no issue rows returns a bare [] — there is
// no special catalog branch.
func (s *Service) handleGroupIssues(w http.ResponseWriter, r *http.Request) {
	org, emptyMatch, ok := s.orgFromGroupPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	page, perPage := parsePagination(r)
	rows, err := s.dao.ListPageWindow(r.Context(), s.vendor, org, "issue", "", perPage, (page-1)*perPage)
	if err != nil {
		writeDBError(r.Context(), w, "handleGroupIssues", err)
		return
	}
	writeList(w, payloads(rows), page, perPage)
}

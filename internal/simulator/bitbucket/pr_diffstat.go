// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import "net/http"

// handlePRDiffstat serves GET /2.0/repositories/{workspace}/{repo}/pullrequests/{id}/diffstat
// — the PR's file-level diffstat, a nested array on the PR row.
func (s *Service) handlePRDiffstat(w http.ResponseWriter, r *http.Request) {
	s.handlePRNestedArray(w, r, "diffstat", "handlePRDiffstat")
}

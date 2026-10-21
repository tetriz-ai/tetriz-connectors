// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import "net/http"

// handleAccessibleResources serves the setup-time cloud-site discovery call
// (GET /oauth/token/accessible-resources). The sync path never hits it — the demo
// config carries a static cloud_id — so this returns a single synthetic site,
// enough to keep a manual ForGroup probe from 404ing. It is not on any operation's
// critical path.
func (s *Service) handleAccessibleResources(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []map[string]any{{
		"id":     "demo-simulator-site",
		"name":   "Demo",
		"url":    "https://example.atlassian.net",
		"scopes": []string{},
	}})
}

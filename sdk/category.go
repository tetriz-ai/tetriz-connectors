// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

// ConnectorCategory classifies connectors by their data domain.
type ConnectorCategory string

const (
	CategoryVCS           ConnectorCategory = "vcs"
	CategoryMessaging     ConnectorCategory = "messaging"
	CategoryIssueTracking ConnectorCategory = "issue_tracking"
	CategoryWorkspace     ConnectorCategory = "workspace"
	CategoryIdentity      ConnectorCategory = "identity"
	CategoryAnalytics     ConnectorCategory = "analytics"
)

// HasCategory checks whether a connector belongs to the given category.
// Returns false if c is nil.
func HasCategory(c Connector, cat ConnectorCategory) bool {
	if c == nil {
		return false
	}
	for _, cc := range c.Categories() {
		if cc == cat {
			return true
		}
	}
	return false
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
)

func integration(id, name, desc string, status connintv1.IntegrationStatus, logo string) *connintv1.Integration {
	return &connintv1.Integration{Id: id, Name: name, Description: desc, Status: status, LogoUrl: logo}
}

func category(id, name string, integrations ...*connintv1.Integration) *connintv1.IntegrationCategory {
	return &connintv1.IntegrationCategory{Id: id, Name: name, Integrations: integrations}
}

// Catalog returns the static connector catalog: one entry per connector this
// module ships, plus github-issues, a setting of the GitHub connector. A host
// serves it through GetIntegrations. Each LogoUrl is host-relative,
// /icons/integrations/<name>.svg: the host serves the icons, and this module
// ships none.
func Catalog() *connintv1.GetIntegrationsResponse { return staticIntegrationsResponse() }

func staticIntegrationsResponse() *connintv1.GetIntegrationsResponse {
	return &connintv1.GetIntegrationsResponse{
		Categories: []*connintv1.IntegrationCategory{
			category("ai-agents", "AI Agents & Usage",
				integration("cursor", "Cursor", "Sync Cursor usage events", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/cursor.svg"),
				integration(string(connectorIDClaudeCode), "Claude Code", "Sync Claude Code users, skills, cost, usage and organization members", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/anthropic.svg"),
				integration(string(connectorIDCodex), "Codex", "Sync Codex usage", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/codex.svg"),
				integration("kiro", "Kiro", "Sync Kiro user activity reports from your own S3 bucket", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/kiro.svg"),
			),
			category("version-control", "Version Control",
				integration(connectorIDGitHub, "GitHub", "Sync GitHub repositories, pull requests, commits, reviews, comments, organization members and Copilot usage", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/github.svg"),
				integration(connectorIDGitLab, "GitLab", "Sync GitLab projects, merge requests, commits, issues, reviews, comments and members", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/gitlab.svg"),
				integration(connectorIDBitbucket, "Bitbucket", "Sync Bitbucket repositories, pull requests, commits, reviews, comments and workspace members", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/bitbucket.svg"),
			),
			category("project-management", "Project Management",
				integration(connectorIDJira, "Jira", "Sync Jira projects, issues, worklogs and sprints", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/jira.svg"),
				integration("github-issues", "GitHub Issues", "Sync GitHub issues", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/github.svg"),
				integration("linear", "Linear", "Sync Linear teams, users, projects, labels, sprints and issues", connintv1.IntegrationStatus_INTEGRATION_STATUS_NOT_CONNECTED, "/icons/integrations/linear.svg"),
			),
		},
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorID sdk.ConnectorID = "jira"

// JiraConnector implements sdk.Connector for the Jira Cloud integration.
//
// cursorOverlap is resolved once at construction from
// config.JiraConfig.CursorOverlapWindow (falling back to
// defaultCursorOverlapWindow when nil/zero) and threaded into the three ops
// that build incremental JQL filters: fetch_issues, fetch_comments,
// fetch_worklogs.
type JiraConnector struct {
	cursorOverlap time.Duration
}

// NewConnector creates a new JiraConnector. jiraCfg may be nil — the
// connector falls back to defaultCursorOverlapWindow (5 minutes).
func NewConnector(jiraCfg *config.JiraConfig) *JiraConnector {
	return &JiraConnector{cursorOverlap: resolveCursorOverlap(jiraCfg)}
}

func (c *JiraConnector) ID() sdk.ConnectorID { return connectorID }
func (c *JiraConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryIssueTracking}
}
func (c *JiraConnector) DisplayName() string { return "Jira" }

func (c *JiraConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:              "fetch_projects",
			DisplayName:     "Fetch Projects",
			Description:     "Fetches projects from a Jira Cloud site via REST API",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchProjectsOp{} },
		},
		{
			ID:              "fetch_issues",
			DisplayName:     "Fetch Issues",
			Description:     "Fetches issues from Jira Cloud projects via JQL search with incremental cursor",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchIssuesOp{cursorOverlap: c.cursorOverlap} },
		},
		{
			ID:              "fetch_issues_data",
			DisplayName:     "Fetch Issue Descriptions",
			Description:     "Fetches issue description (ADF) via JQL, writes it to the blob store, and emits a description_ref record",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchIssuesDataOp{} },
		},
		{
			ID:          "fetch_comments",
			DisplayName: "Fetch Comments",
			Description: "Fetches comments from recently updated Jira issues",
			// fetch_comments has no default schedule; run it with sync.
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchCommentsOp{cursorOverlap: c.cursorOverlap} },
		},
		{
			ID:              "fetch_worklogs",
			DisplayName:     "Fetch Worklogs",
			Description:     "Fetches worklogs from recently updated Jira issues",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchWorklogsOp{cursorOverlap: c.cursorOverlap} },
		},
		{
			ID:              "fetch_sprints",
			DisplayName:     "Fetch Sprints",
			Description:     "Fetches sprints from Jira Agile scrum boards via the Agile REST API",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchSprintsOp{} },
		},
	}
}

func (c *JiraConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := parseConfig(cfg)
	if err != nil {
		return fmt.Errorf("jira: %w", err)
	}

	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("jira: %w", err)
	}

	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("jira: %w", err)
	}

	return nil
}

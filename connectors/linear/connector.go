// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorID sdk.ConnectorID = "linear"

// LinearConnector implements sdk.Connector for the Linear integration.
type LinearConnector struct{}

// NewConnector returns a new LinearConnector.
func NewConnector() *LinearConnector { return &LinearConnector{} }

func (c *LinearConnector) ID() sdk.ConnectorID { return connectorID }
func (c *LinearConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryIssueTracking}
}
func (c *LinearConnector) DisplayName() string { return "Linear" }

func (c *LinearConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:              "fetch_teams",
			DisplayName:     "Fetch Teams",
			Description:     "Fetches all Linear teams in the connected workspace",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchTeamsOp{} },
		},
		{
			ID:              "fetch_users",
			DisplayName:     "Fetch Users",
			Description:     "Fetches all Linear workspace members",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchUsersOp{} },
		},
		{
			ID:              "fetch_projects",
			DisplayName:     "Fetch Projects",
			Description:     "Fetches Linear projects (cross-team initiatives) in the workspace",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchProjectsOp{} },
		},
		{
			ID:              "fetch_labels",
			DisplayName:     "Fetch Labels",
			Description:     "Fetches Linear issue labels (workspace and team scoped)",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchLabelsOp{} },
		},
		{
			// Registered as "fetch_sprints", the operation id other connectors'
			// sprint records use, so a consumer handles cycles as sprints.
			ID:              "fetch_sprints",
			DisplayName:     "Fetch Cycles",
			Description:     "Fetches Linear cycles (sprint equivalents) per team",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchCyclesOp{} },
		},
		{
			ID:              "fetch_issues",
			DisplayName:     "Fetch Issues",
			Description:     "Fetches Linear issues per team with incremental cursor",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchIssuesOp{} },
		},
	}
}

func (c *LinearConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := ParseConfig(cfg)
	if err != nil {
		return fmt.Errorf("linear: %w", err)
	}
	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("linear: %w", err)
	}
	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("linear: %w", err)
	}
	return nil
}

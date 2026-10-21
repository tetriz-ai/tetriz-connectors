// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorID is declared here rather than in internal/vendorids: that package's const
// block is scoped to VCS connector IDs, and every non-VCS connector (slack, jira,
// linear) keeps its own.
const connectorID sdk.ConnectorID = "cursor"

// CursorConnector implements sdk.Connector for the Cursor Admin API.
type CursorConnector struct{}

// NewConnector returns a new CursorConnector.
func NewConnector() *CursorConnector { return &CursorConnector{} }

func (c *CursorConnector) ID() sdk.ConnectorID { return connectorID }

// Categories classifies Cursor as analytics — it contributes billed usage and
// cost, not source control. This is load-bearing beyond routing: the guard that
// prevents a second source-control connector keys on CategoryVCS, so classifying
// Cursor correctly is what keeps that guard from firing on a connector it was
// never meant to cover.
func (c *CursorConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryAnalytics}
}

func (c *CursorConnector) DisplayName() string { return "Cursor" }

// Operations declares the one scheduled pull: the billed-events read.
//
// The credential probe is not here. It is issued by the connect flow, so validating a
// key never waits on a schedule.
func (c *CursorConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:          opFetchUsageEvents,
			DisplayName: "Fetch Usage Events",
			Description: "Fetches billed Cursor model calls with their token counts and cost",
			// Once a day, after the previous day has closed. The window never includes
			// the current day, so running more often would re-read the same span.
			DefaultSchedule: "17 3 * * *",
			IngestionType:   sdk.IngestionPull,
			// Sized for the first run rather than the steady state: a ninety-day first
			// sync is two orders of magnitude larger than a daily one, and the scheduler
			// default would cancel it partway. Only the scheduled path honours this — a
			// manual trigger runs under a fixed ceiling this cannot raise, so a first
			// sync is a scheduled run by design.
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchUsageEventsOp{} },
		},
	}
}

func (c *CursorConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := ParseConfig(cfg)
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	return nil
}

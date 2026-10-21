// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorID must equal connectorsource.ConnectorIDClaudeCode: the gate that withholds this
// connector's schedules is keyed by connector id and treats an unknown id as "not
// gated", so a drift would silently un-gate the connector. A test in connectorsource pins
// the two together.
const connectorID sdk.ConnectorID = "claude-code"

// Operation ids. One per endpoint, because a failure in one leaves the others running and
// the shapes fail differently — an activity walk stops on a not-yet-aggregated day while a
// report walk stops on an expired cursor.
const (
	opFetchAnalyticsUsers      sdk.OperationID = "fetch_analytics_users"
	opFetchAnalyticsSkills     sdk.OperationID = "fetch_analytics_skills"
	opFetchAnalyticsUserCost   sdk.OperationID = "fetch_analytics_user_cost"
	opFetchAnalyticsUserUsage  sdk.OperationID = "fetch_analytics_user_usage"
	opFetchOrganizationMembers sdk.OperationID = "fetch_organization_members"
)

// Connector implements sdk.Connector for the Claude Enterprise Analytics API.
type Connector struct{}

// NewConnector returns a new Connector.
func NewConnector() *Connector { return &Connector{} }

func (c *Connector) ID() sdk.ConnectorID { return connectorID }

// Categories classifies this as analytics — it contributes usage and cost, not source
// control. Load-bearing beyond routing: the one-source-control-connector guard keys on
// CategoryVCS, so a misclassification here would make it reject a legitimate integration.
func (c *Connector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryAnalytics}
}

func (c *Connector) DisplayName() string { return "Claude Code" }

// Operations declares the five scheduled pulls.
//
// The credential probe is not among them: it is issued by the connect flow, so validating
// a key never waits on a schedule.
//
// The schedules are staggered rather than stacked, because all five share one
// organisation-level request pool — see constants.GroupAnthropicAnalytics for why. Five
// operations starting together would pace each other for nothing.
//
// A REPORT operation runs first, and the order is load-bearing rather than cosmetic. The
// report endpoints are the only ones that say which Anthropic organisation answered, so they
// are the only ones that can detect a key now pointing at a different one — and that detection
// pauses the whole connector. Running an unguarded operation first would emit another
// organisation's rows before anything noticed.
func (c *Connector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:          opFetchAnalyticsUsers,
			DisplayName: "Fetch Per-User Activity",
			Description: "Fetches per-user daily Claude activity, including Claude Code commits, pull requests and sessions",
			// After the cost report, which is the guarded operation — see Operations.
			DefaultSchedule: "25 11 * * *",
			IngestionType:   sdk.IngestionPull,
			// Sized for the first run rather than the steady state: a ninety-day first
			// sync is two orders of magnitude larger than a daily one, and the scheduler
			// default would cancel it partway. Only the scheduled path honours this, so a
			// first sync is a scheduled run by design.
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchAnalyticsUsersOp{} },
		},
		{
			ID:               opFetchAnalyticsSkills,
			DisplayName:      "Fetch Per-User Skill Usage",
			Description:      "Fetches per-user skill usage per day, grouped so each row is attributable to one user",
			DefaultSchedule:  "40 11 * * *",
			IngestionType:    sdk.IngestionPull,
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchAnalyticsSkillsOp{} },
		},
		{
			ID:          opFetchAnalyticsUserCost,
			DisplayName: "Fetch Per-User Cost",
			Description: "Fetches per-user daily cost by product and model, as decimal amounts stored verbatim",
			// First, after the vendor's 10:00 UTC aggregation for the previous day with
			// margin: it is a guarded operation, so it runs before the unguarded ones.
			DefaultSchedule:  "10 11 * * *",
			IngestionType:    sdk.IngestionPull,
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchAnalyticsUserCostOp{} },
		},
		{
			ID:               opFetchAnalyticsUserUsage,
			DisplayName:      "Fetch Per-User Token Usage",
			Description:      "Fetches per-user daily token usage by product and model",
			DefaultSchedule:  "55 11 * * *",
			IngestionType:    sdk.IngestionPull,
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchAnalyticsUserUsageOp{} },
		},
		{
			ID:          opFetchOrganizationMembers,
			DisplayName: "Fetch Organisation Members",
			Description: "Fetches the organisation's member roster, which is the denominator an activity count needs",
			// A whole-account roll-up: the denominator means nothing for a single-person
			// tenant, and reading it anyway spends the request budget the whole family shares.
			RequiresMultiUser: true,
			// Weekly: a roster changes on the order of weeks where activity changes daily.
			DefaultSchedule:  "0 12 * * 1",
			IngestionType:    sdk.IngestionPull,
			ExecutionTimeout: 10 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchOrganizationMembersOp{} },
		},
	}
}

// ValidateConfig refuses a configuration the operations cannot run with, so the failure
// lands at configuration time rather than inside a scheduled run.
func (c *Connector) ValidateConfig(_ context.Context, cfg *sdk.ConnectorConfig) error {
	if _, err := ParseCredentials(cfg); err != nil {
		return fmt.Errorf("claude-code: %w", err)
	}
	if _, err := ParseSettings(cfg); err != nil {
		return fmt.Errorf("claude-code: %w", err)
	}
	return nil
}

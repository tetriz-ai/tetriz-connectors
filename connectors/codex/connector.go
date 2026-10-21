// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorID must equal connectorsource.ConnectorIDCodex: the gate that withholds this
// connector's schedules is keyed by connector id and treats an unknown id as "not
// gated", so a drift would silently un-gate the connector. A test in connectorsource pins
// the two together.
const connectorID sdk.ConnectorID = "codex"

// OpFetchUsage is the one operation. It is exported because the operation id is half of
// where a stored record lives, so tooling that reads a position or a record has to name the
// same value the connector writes rather than restate the string.
//
// The id is part of the record uniqueness key, so it is fixed at first write: renaming it
// later does not migrate rows, it orphans every one of them.
const OpFetchUsage sdk.OperationID = "fetch_codex_usage"

// Connector implements sdk.Connector for the Codex Enterprise Analytics API.
type Connector struct{}

// NewConnector returns a new Connector.
func NewConnector() *Connector { return &Connector{} }

func (c *Connector) ID() sdk.ConnectorID { return connectorID }

// Categories classifies this as analytics — it contributes usage, not source control.
// Load-bearing beyond routing: the one-source-control-connector guard keys on CategoryVCS,
// so a misclassification here would make it reject a legitimate integration.
func (c *Connector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryAnalytics}
}

func (c *Connector) DisplayName() string { return "Codex" }

// Operations declares the single scheduled pull.
//
// One, not the sibling's five, because the vendor exposes one endpoint under one scope.
// The credential probe is not among them: it is issued by the connect flow, so validating a
// key never waits on a schedule.
func (c *Connector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:          OpFetchUsage,
			DisplayName: "Fetch Codex Usage",
			Description: "Fetches per-developer daily Codex usage for the connected workspace",
			// Early UTC, so a day is requested well after it closed. OpenAI publishes no
			// aggregation-lag statement, which is also why the walk never asks for today.
			DefaultSchedule: "20 4 * * *",
			IngestionType:   sdk.IngestionPull,
			// Sized for the first run rather than the steady state: a ninety-day first sync
			// is two orders of magnitude larger than a daily one, and the scheduler default
			// would cancel it partway. Only the scheduled path honours this, so a first sync
			// is a scheduled run by design.
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchUsageOp{} },
		},
	}
}

// ValidateConfig refuses a configuration the operation cannot run with, so the failure
// lands at configuration time rather than inside a scheduled run.
//
// The settings half is not optional here as it is for both siblings: their credential
// implies the tenant, whereas a Codex request cannot be formed at all without the stored
// workspace.
func (c *Connector) ValidateConfig(_ context.Context, cfg *sdk.ConnectorConfig) error {
	if _, err := ParseCredentials(cfg); err != nil {
		return fmt.Errorf("codex: %w", err)
	}
	if _, err := ParseSettings(cfg); err != nil {
		return fmt.Errorf("codex: %w", err)
	}
	return nil
}

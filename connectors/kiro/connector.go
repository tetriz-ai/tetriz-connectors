// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// connectorID is declared here rather than in internal/vendorids: that package's const block is
// scoped to VCS connector IDs, and every non-VCS connector keeps its own — the same
// choice the cursor and claude-code connectors document.
const connectorID sdk.ConnectorID = "kiro"

// opFetchUserActivityReport collects the vendor's daily per-client report.
const opFetchUserActivityReport sdk.OperationID = "fetch_user_activity_report"

// Connector implements sdk.Connector for Kiro's S3 report surface.
//
// It is the first connector in this repository that reads object storage rather than an
// HTTP API, which is why its gateway carries a second, narrower interface alongside the
// framework's — see the gateway package.
type Connector struct{}

// NewConnector returns a new Kiro connector.
func NewConnector() *Connector { return &Connector{} }

// ID returns the connector's identifier.
func (c *Connector) ID() sdk.ConnectorID { return connectorID }

// Categories classifies Kiro as analytics: it reports usage and cost, not source
// control. The classification is load-bearing beyond routing — the
// guard preventing a second source-control connector keys on the VCS category, so
// classifying this correctly is what keeps that guard from firing on a connector it was
// never meant to cover.
func (c *Connector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryAnalytics}
}

// DisplayName returns the vendor's name as an administrator knows it.
func (c *Connector) DisplayName() string { return "Kiro" }

// Operations declares the one scheduled pull.
//
// The connect-time validation is not here. It is issued by the connect flow, so checking
// a bucket never waits on a schedule.
func (c *Connector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:          opFetchUserActivityReport,
			DisplayName: "Fetch User Activity Report",
			Description: "Copies Kiro's daily per-client activity reports out of the customer's bucket",
			// The vendor's own documentation puts publication at 02:00 co-ordinated
			// universal time — taken from that rather than observed, so the margin here is
			// deliberately wide: an hour and forty minutes is enough that a publication
			// running late, or the hour being wrong, costs a day rather than a report. And
			// once a day because a day is the vendor's publication unit; there is nothing
			// finer to react to.
			DefaultSchedule: "40 3 * * *",
			IngestionType:   sdk.IngestionPull,
			// Sized for a first run rather than a steady-state day, which is two orders of
			// magnitude smaller. A first run over a long history performs one listing per
			// day plus one fetch per file, serially; under the scheduler's default deadline
			// it would be cancelled partway. The operation checkpoints per completed day,
			// so a cancellation still banks its progress — but the deadline is what keeps
			// a first run from needing many attempts to finish.
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchUserActivityReportOp{} },
		},
	}
}

// ValidateConfig checks a stored configuration can address a bucket.
func (c *Connector) ValidateConfig(_ context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := ParseConfig(cfg)
	if err != nil {
		return fmt.Errorf("kiro: %w", err)
	}
	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("kiro: %w", err)
	}
	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("kiro: %w", err)
	}
	return nil
}

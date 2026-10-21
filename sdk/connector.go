// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

//go:generate go tool mockgen -destination=../internal/mocks/sdk/mock_sdk.go -package=mocks github.com/tetriz-ai/tetriz-connectors/sdk Connector,DataSink,FetchCache,WorkQueue,TrackedRefsResolver,BlobStore,BudgetObserver,TenantDirectory

import (
	"context"
	"encoding/json"
	"time"
)

// ConnectorID is a unique string identifier, for example "github", "jira", "claude-code".
type ConnectorID string

// Connector represents an integration with an external system.
// It declares a fixed set of operations and a single auth mechanism.
//
// Capabilities are derived from the operations list — each OperationDescriptor
// declares its IngestionType. To check if a connector supports webhooks,
// iterate Operations() and look for IngestionWebhook.
type Connector interface {
	// ID returns the unique identifier for this connector type.
	ID() ConnectorID

	// Categories returns the data domains this connector belongs to.
	// Most connectors belong to a single domain, but some (e.g., GitHub) span multiple.
	// Implementations must return a new slice on each call.
	Categories() []ConnectorCategory

	// DisplayName returns a human-readable name (e.g., "GitHub", "Claude Code").
	DisplayName() string

	// Operations returns the complete list of operations this connector supports.
	// This is a static declaration — it does not change at runtime.
	Operations() []OperationDescriptor

	// ValidateConfig checks whether the provided connector configuration is valid
	// (e.g., required credentials present, API key format correct).
	ValidateConfig(ctx context.Context, cfg *ConnectorConfig) error
}

// ConnectorConfig holds the per-organization configuration for a connector instance.
// Stored in the database and passed to operations at execution time.
type ConnectorConfig struct {
	ConnectorID    ConnectorID
	OrganizationID string

	// Credentials holds auth-related data (OAuth tokens, API keys, etc.)
	// The Postgres store encrypts it with the key passed to
	// NewConnectorConfigRepo, which is TETRIZ_CREDENTIALS_KEY under serve.
	// Each connector defines its own schema.
	Credentials json.RawMessage

	// Settings holds connector-specific settings
	// (e.g., which GitHub org, which Slack workspace, which Google domain).
	Settings json.RawMessage

	// OperationOverrides allows per-org override of operation defaults.
	// Key: OperationID, Value: operation-specific config (schedule override, enabled flag, etc.)
	OperationOverrides map[OperationID]json.RawMessage

	// Enabled controls whether this connector instance is active.
	Enabled bool

	// HealthState gates scheduled syncs independently of Enabled — see HealthState.
	HealthState HealthState

	// CredentialsUpdatedAt is when this connector's credentials were last
	// (re)written — advanced only by a genuine credential write (an OAuth/PAT
	// reconnect or explicit disconnect), never by an unrelated settings/cache
	// write on the same row. Used as a freshness fence: a caller that loaded
	// this config before a concurrent credential write must not use it to make
	// a health-state decision that would override the newer write (see
	// MarkAuthRevoked).
	CredentialsUpdatedAt time.Time
}

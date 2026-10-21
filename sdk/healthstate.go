// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

// HealthState represents whether a connector can currently make vendor API calls.
type HealthState string

const (
	// HealthStateHealthy is the default state — the connector's credentials are
	// assumed valid and syncs run normally.
	HealthStateHealthy HealthState = "healthy"

	// HealthStateAuthRevoked means a sync run failed with a Terminal auth-kind
	// error (the credential is confirmed dead — a refresh was attempted and
	// failed). The orchestrator gates future scheduled runs for this connector
	// until a fresh credential write resets the state back to healthy.
	HealthStateAuthRevoked HealthState = "auth_revoked"
)

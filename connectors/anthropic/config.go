// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package anthropic is the Claude Code connector (id claude-code): organization members,
// Claude Code activity and skill usage, and per-user cost and token usage reports.
package anthropic

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// DefaultLookbackDays is the first-sync window used when no per-org override is set.
//
// Ninety days, matching the sibling analytics connector so the two stay comparable when
// someone reads a first sync's volume. It is clamped to Anthropic's floor of 2026-01-01 —
// a deeper request is answered with empty pages rather than an error, so the clamp makes
// the request honest rather than preventing a failure.
//
// The bound that does bite is not this one: the report pair caps a single request at a
// thirty-one day range, so a ninety-day first sync is a sequence of chunked ranges and an
// implementation that sends the whole window as one request receives a 400 before
// ingesting anything.
const DefaultLookbackDays = 90

// Credentials is stored encrypted in connector_configs.credentials.
//
// One field, because the Enterprise Analytics API authenticates with a single opaque key
// presented in x-api-key. There is no refresh token and no expiry, and scopes are fixed
// when the key is minted — so rotation and re-scoping are both "paste a new key".
type Credentials struct {
	APIKey string `json:"api_key"`
}

// Settings is stored in connector_configs.settings.
type Settings struct {
	// AnthropicOrganizationID is the organisation the stored key resolved to when it was
	// saved. It is the machine-comparable identity: a later reconnect can see that a key
	// now points at a different organisation, which no member list can establish because
	// a roster legitimately changes.
	AnthropicOrganizationID string `json:"anthropic_organization_id,omitempty"`
	// LookbackDays caps how far back a first sync reads. Zero means DefaultLookbackDays.
	// This is the connector-level window; the report pair's re-read window is an
	// operation-level default and does not live here.
	LookbackDays int `json:"lookback_days,omitempty"`
}

// GetLookbackDays returns the effective first-sync lookback, falling back to
// DefaultLookbackDays when no per-org override is set.
func (s *Settings) GetLookbackDays() int {
	if s == nil || s.LookbackDays <= 0 {
		return DefaultLookbackDays
	}
	return s.LookbackDays
}

// ParseCredentials reads the stored credential blob.
func ParseCredentials(cfg *sdk.ConnectorConfig) (*Credentials, error) {
	if cfg == nil || len(cfg.Credentials) == 0 {
		return nil, fmt.Errorf("anthropic: no credentials stored")
	}
	var creds Credentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("anthropic: unmarshalling credentials: %w", err)
	}
	if creds.APIKey == "" {
		return nil, fmt.Errorf("anthropic: stored credentials carry no api key")
	}
	return &creds, nil
}

// ParseSettings reads the stored settings blob. Absent settings are not an error: every
// field has a default, and a configuration saved before a field existed has none of it.
func ParseSettings(cfg *sdk.ConnectorConfig) (*Settings, error) {
	if cfg == nil || len(cfg.Settings) == 0 {
		return &Settings{}, nil
	}
	var s Settings
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return nil, fmt.Errorf("anthropic: unmarshalling settings: %w", err)
	}
	return &s, nil
}

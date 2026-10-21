// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// DefaultLookbackDays is the first-sync window used when no per-org override is
// set. Ninety rather than the year the source-control and communication
// connectors use: the vendor states no retention for its usage endpoint, and a
// live probe found the event count plateauing at about thirty days. Requesting
// past that costs only empty pages, whereas a tighter default would risk
// truncating an organisation whose retention runs longer — and a first sync
// happens once.
const DefaultLookbackDays = 90

// DefaultTrailingOverlapDays is how far back a steady-state run re-reads when no per-org
// override is set.
//
// A re-read is nearly free at the vendor and it is the only way a corrected charge is
// ever picked up: Cursor re-prices a billed call after the fact, and the stored record is
// verbatim as of the last read. Three days is a guess at a number the vendor has not
// published — it states no bound on how long a call can still be re-priced — so a
// correction landing on day four is never collected. That is a known limitation.
// Widening this does not solve it, since the natural bound is the billing cycle.
//
// What it costs is warehouse writes, not vendor calls: a day's events are rewritten once
// per day for as long as they stay in the window, so this number IS the write multiplier.
// That is why it is adjustable per organisation — narrowing it is the response to
// capture-pipeline volume, and it must not require a deploy.
const DefaultTrailingOverlapDays = 3

// CursorCredentials is stored encrypted in connector_configs.credentials.
//
// One field, because Cursor authenticates with a single opaque team key presented
// as HTTP Basic with the key as username and an empty password. There is no
// refresh token and no expiry: rotation means pasting a new key.
type CursorCredentials struct {
	APIKey string `json:"api_key"`
}

// CursorSettings is stored in connector_configs.settings.
type CursorSettings struct {
	// LookbackDays caps how far back a first sync reads. Zero means use
	// DefaultLookbackDays. Per-org rather than per-operation because that is where
	// the source-control and communication connectors keep their own first-sync
	// lookback, and an operation-level default cannot be overridden for one
	// organisation without reaching for a different mechanism.
	LookbackDays int `json:"lookback_days,omitempty"`
	// TrailingOverlapDays is how far back each steady-state run re-reads. Zero means
	// use DefaultTrailingOverlapDays. It is the warehouse write multiplier, so it is the
	// dial an operator turns down when capture volume is the problem.
	TrailingOverlapDays int `json:"trailing_overlap_days,omitempty"`
}

// GetLookbackDays returns the effective first-sync lookback window, falling back
// to DefaultLookbackDays when no per-org override is set.
func (s CursorSettings) GetLookbackDays() int {
	if s.LookbackDays > 0 {
		return s.LookbackDays
	}
	return DefaultLookbackDays
}

// GetTrailingOverlapDays returns the effective steady-state overlap, falling back to
// DefaultTrailingOverlapDays when no per-org override is set.
func (s CursorSettings) GetTrailingOverlapDays() int {
	if s.TrailingOverlapDays > 0 {
		return s.TrailingOverlapDays
	}
	return DefaultTrailingOverlapDays
}

// ParseConfig unmarshals the connector config into typed credentials and settings.
func ParseConfig(cfg *sdk.ConnectorConfig) (CursorCredentials, CursorSettings, error) {
	var creds CursorCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, CursorSettings{}, fmt.Errorf("parse credentials: %w", err)
	}
	var settings CursorSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}
	return creds, settings, nil
}

func validateCredentials(creds CursorCredentials) error {
	if creds.APIKey == "" {
		return fmt.Errorf("credentials must include api_key")
	}
	return nil
}

func validateSettings(settings CursorSettings) error {
	// A negative lookback would silently invert the first-sync window rather than
	// failing, so it is rejected here. Zero is legal and means the default.
	if settings.LookbackDays < 0 {
		return fmt.Errorf("settings.lookback_days must not be negative, got %d", settings.LookbackDays)
	}
	// Rejected for the same reason as the lookback, and note what does NOT rely on it:
	// the getter floors a non-positive value to the default, so the run path is safe
	// whether or not this validator is reached. Zero means the default.
	if settings.TrailingOverlapDays < 0 {
		return fmt.Errorf("settings.trailing_overlap_days must not be negative, got %d", settings.TrailingOverlapDays)
	}
	return nil
}

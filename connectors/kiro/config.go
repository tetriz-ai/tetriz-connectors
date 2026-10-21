// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// DefaultTrailingOverlapDays is how far back a steady-state run re-reads when no per-org
// override is set.
//
// The vendor may republish a corrected day, and a re-read is the only way that correction
// is ever collected. Three days is a guess at a bound the vendor does not document, so a
// correction landing on day four is never picked up — recorded as a limitation rather
// than solved here, since the honest bound is the billing cycle.
//
// What it costs is repeated object reads, not vendor quota: a day is re-fetched once per
// run for as long as it stays in the window, so this number is the read multiplier. That
// is why it is adjustable per organisation.
const DefaultTrailingOverlapDays = 3

// DefaultFirstRunWindowDays caps how far back a first run reads when no per-org override
// is set.
//
// It is a cap, not a target: the run reads no further back than the bucket actually goes,
// because history is bounded by whichever came later — the day reporting was enabled, or
// the horizon the customer's own lifecycle rules leave intact. Both are facts about their
// bucket rather than choices of ours.
//
// Widening this after a first run has completed collects nothing, because the cursor is a
// high-water mark and the older days sit behind it. Deepening history afterwards is the
// collection script's execute mode.
const DefaultFirstRunWindowDays = 400

// Credentials is stored in connector_configs.credentials, which is encrypted at rest.
//
// Nothing about reaching the bucket is secret in the way this connector reads it: the
// customer grants the runner's principal, so there is no key or token to hold. The fields are
// the shape of the assumed-role grant a stricter security review asks for, and this
// connector does not assume a role — so a configuration carrying them is refused rather
// than served with the wrong principal. See validateCredentials.
//
// The bucket path is deliberately NOT here — see Settings.
type Credentials struct {
	RoleARN    string `json:"role_arn,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
}

// Settings is stored in connector_configs.settings, which is stored as written.
//
// The bucket, prefix and region live here rather than in Credentials, and the placement is
// load-bearing rather than filing. Credentials are encrypted into a versioned ciphertext
// envelope, so nothing inside them is legible to a query — and the rule that one bucket
// belongs to one organisation is an index over the bucket value. Expressed over the
// encrypted field it would match nothing, silently, in every environment with a key.
//
// Worse, the repository treats an absent key as no encryption, so that rule exercised
// against a keyless setup would pass while production accepted the same bucket twice.
// Keeping the value here is what makes the constraint real rather than apparently real.
type Settings struct {
	// Bucket is the customer's reports bucket. Not a secret: the administrator typed it.
	Bucket string `json:"bucket"`
	// Prefix is the path within it. The vendor cannot write to a bucket root, so a prefix
	// always exists.
	Prefix string `json:"prefix"`
	// Region is discovered during validation rather than asked for, and it is part of the
	// key path, so listing a day needs it. It cannot be confirmed by reading a key back:
	// a listing prefix already contains the region, so every key it returns carries the
	// one we supplied. The bucket's own reported location is what settles it.
	Region string `json:"region"`
	// AccountID is the AWS account the reports are written under. It appears in the key
	// path, so listing a day needs it.
	AccountID string `json:"account_id"`

	// TrailingOverlapDays is the read multiplier — the dial an operator turns down when
	// repeated reads are the problem. Zero means DefaultTrailingOverlapDays.
	TrailingOverlapDays int `json:"trailing_overlap_days,omitempty"`
	// FirstRunWindowDays caps a first run's reach. Zero means DefaultFirstRunWindowDays.
	//
	// The Cursor connector holds the same concept — its own comment calls it the
	// first-sync window — under the key `lookback_days`, so one idea has two spellings in
	// this column. The clearer name is kept here rather than renaming a connector with
	// stored values, and it is flagged so a third connector picks deliberately instead of
	// picking whichever it read first. Nothing writes either key today.
	FirstRunWindowDays int `json:"first_run_window_days,omitempty"`
}

// GetTrailingOverlapDays returns the effective trailing re-read window.
func (s Settings) GetTrailingOverlapDays() int {
	if s.TrailingOverlapDays > 0 {
		return s.TrailingOverlapDays
	}
	return DefaultTrailingOverlapDays
}

// GetFirstRunWindowDays returns the effective first-run cap.
func (s Settings) GetFirstRunWindowDays() int {
	if s.FirstRunWindowDays > 0 {
		return s.FirstRunWindowDays
	}
	return DefaultFirstRunWindowDays
}

// ParseConfig reads the stored credentials and settings for an organisation.
func ParseConfig(cfg *sdk.ConnectorConfig) (Credentials, Settings, error) {
	var creds Credentials
	var settings Settings
	if len(cfg.Credentials) > 0 {
		if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
			return creds, settings, fmt.Errorf("parsing credentials: %w", err)
		}
	}
	if len(cfg.Settings) > 0 {
		if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
			return creds, settings, fmt.Errorf("parsing settings: %w", err)
		}
	}
	return creds, settings, nil
}

// validateSettings refuses a configuration that cannot address a bucket.
//
// Each check is a case the connect flow should already have prevented; this is the
// second line, and it fails as a configuration fault rather than a transient one, the
// class for a connector no retry can fix.
func validateSettings(s Settings) error {
	switch {
	case strings.TrimSpace(s.Bucket) == "":
		return fmt.Errorf("bucket is required")
	case strings.Trim(s.Prefix, "/ ") == "":
		// The vendor refuses to write to a bucket root, so an empty prefix means the
		// configuration describes a layout the vendor never produces.
		return fmt.Errorf("prefix is required: the vendor does not write reports to a bucket root")
	case strings.TrimSpace(s.Region) == "":
		return fmt.Errorf("region is required and is discovered at connect time, not entered")
	case strings.TrimSpace(s.AccountID) == "":
		return fmt.Errorf("account id is required: it appears in the vendor's key path")
	}
	return nil
}

// validateCredentials refuses a configuration naming a role, because the gateway reads the
// bucket with the ambient principal and assumes nothing.
//
// Refusing is the point. A stored role that no code reads is the failure that looks like a
// success: an operator who set one would believe reads were scoped by it, while every read
// went out under whatever principal the process happens to hold — which is a wider grant
// than the one they asked for, and invisible. Failing here says so instead. It fails as a
// configuration fault, the class for a connector no retry fixes.
func validateCredentials(c Credentials) error {
	if strings.TrimSpace(c.RoleARN) != "" || strings.TrimSpace(c.ExternalID) != "" {
		return fmt.Errorf("a role arn or external id is configured, but this connector reads the bucket " +
			"with the ambient principal and assumes no role: the grant would not be the one configured")
	}
	return nil
}

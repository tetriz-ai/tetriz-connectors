// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package codex is the Codex connector: per-developer daily workspace usage.
package codex

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// VendorLookbackCapDays is how far back the analytics endpoint will answer at all. A
// deeper request comes back empty rather than refused, so this is a domain fact about
// OpenAI and not a value anyone here may raise.
const VendorLookbackCapDays = 90

// DefaultLookbackDays is the first-sync window used when no per-org override is set.
// Equal to the cap today because there is no reason to backfill less than the vendor
// will answer, but it is a separate constant because it is a separate decision: this one
// is ours to change, the cap above is not.
const DefaultLookbackDays = 90

// RecordKeyDelimiter separates the parts of a Codex record key. It is defined once, in
// the package that owns the record shape, so that every reader and writer of a key
// checks for the same character — a delimiter restated at a use site drifts from the one
// keys are actually built with, and the mismatch surfaces as unkeyable rows rather than
// as a compile error.
const RecordKeyDelimiter = "|"

// Credentials is stored encrypted in connector_configs.credentials.
//
// One field, because the analytics endpoint authenticates with a single opaque key
// presented as an Authorization bearer token. There is no refresh token and no expiry,
// and the one scope it must carry cannot be widened after the key is minted — so both
// rotation and re-scoping are "paste a new key".
type Credentials struct {
	APIKey string `json:"api_key"`
}

// Settings is stored in connector_configs.settings.
type Settings struct {
	// WorkspaceID is the ChatGPT Enterprise workspace the stored key reads. It is a value
	// an administrator supplied rather than one the vendor attested, so its presence is
	// never evidence that the stored key resolves to it. Every request carries it as a path
	// parameter, so a configuration without one cannot form a request at all.
	WorkspaceID string `json:"workspace_id,omitempty"`
	// LookbackDays caps how far back a first sync reads. Zero means DefaultLookbackDays.
	LookbackDays int `json:"lookback_days,omitempty"`
}

// GetLookbackDays returns the effective first-sync lookback, falling back to
// DefaultLookbackDays when no per-org override is set. A value beyond the vendor's cap is
// clamped rather than refused: the request would otherwise ask for a window that can only
// come back empty, which reads as a broken sync rather than as an unanswerable question.
func (s *Settings) GetLookbackDays() int {
	if s == nil || s.LookbackDays <= 0 {
		return DefaultLookbackDays
	}
	if s.LookbackDays > VendorLookbackCapDays {
		return VendorLookbackCapDays
	}
	return s.LookbackDays
}

// ValidWorkspaceID reports whether id is a workspace identifier the endpoint can address.
//
// A UUID, because that is what the vendor's path parameter is: the endpoint parses the segment
// as one and answers a malformed value with 400 before it authenticates, so a wrong shape is
// refused at the field an administrator typed it into rather than as an opaque vendor error.
//
// It also settles the identifier's second life, which is the one that bites late: it is a
// segment of the pipe-delimited record key, and a workspace carrying that delimiter saves an
// enabled connector and then fails every scheduled run as unkeyable — connected, never
// syncing. A UUID cannot contain the delimiter, so that holds by construction rather than by
// an allow-list that has to be complete.
//
// Checked here and not only at the proto boundary because this package guards the paths the
// proto does not reach — a row written by a script or a repair, read back at sync time,
// passes no interceptor.
func ValidWorkspaceID(id string) bool {
	return workspaceIDPattern.MatchString(id)
}

// workspaceIDPattern is the bare 36-character UUID and nothing else — the same expression the
// proto declares and the browser applies, character for character, so the three cannot disagree
// about what they accept.
//
// Stricter than the endpoint, which also takes the brace-wrapped and urn:uuid: spellings
// (confirmed against it directly). One canonical form is required anyway, because this value is
// a record-key segment: admitting three spellings of one workspace would key the same
// workspace's records three ways.
//
// Anchored, so it needs no separate length bound — the alternation is exactly 36 characters.
var workspaceIDPattern = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ParseCredentials reads the stored credential blob.
func ParseCredentials(cfg *sdk.ConnectorConfig) (*Credentials, error) {
	if cfg == nil || len(cfg.Credentials) == 0 {
		return nil, fmt.Errorf("codex: no credentials stored")
	}
	var creds Credentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("codex: unmarshalling credentials: %w", err)
	}
	if creds.APIKey == "" {
		return nil, fmt.Errorf("codex: stored credentials carry no api key")
	}
	return &creds, nil
}

// ParseSettings reads the stored settings blob.
//
// A missing workspace is an error here, unlike every optional field beside it: it is a
// path parameter, so a configuration without one cannot form a single request. Returning
// an empty Settings would push that failure to the vendor call, where a malformed path
// comes back as a 404 and reads as a wrong workspace rather than as a missing one.
func ParseSettings(cfg *sdk.ConnectorConfig) (*Settings, error) {
	if cfg == nil || len(cfg.Settings) == 0 {
		return nil, fmt.Errorf("codex: no settings stored, so no workspace to read")
	}
	var s Settings
	if err := json.Unmarshal(cfg.Settings, &s); err != nil {
		return nil, fmt.Errorf("codex: unmarshalling settings: %w", err)
	}
	if !ValidWorkspaceID(s.WorkspaceID) {
		return nil, fmt.Errorf("codex: stored settings carry no usable workspace id")
	}
	return &s, nil
}

// containsDelimiter reports whether s carries the record key delimiter. Named rather than
// inlined so the key builder and the workspace guard ask the same question of the same
// constant.
func containsDelimiter(s string) bool { return strings.Contains(s, RecordKeyDelimiter) }

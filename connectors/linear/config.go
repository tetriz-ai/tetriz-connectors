// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// OAuthStateTTL is the lifetime of a CSRF state token stored in Redis during the
// OAuth authorize → callback round-trip.
const OAuthStateTTL = 10 * time.Minute

// LinearCredentials is stored encrypted in connector_configs.credentials.
type LinearCredentials struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

// LinearSettings is stored in connector_configs.settings.
type LinearSettings struct {
	// WorkspaceID is the Linear organization UUID returned by the organization query.
	WorkspaceID string `json:"workspace_id"`
	// OrgSlug is the Linear urlKey (e.g. "acme"), used for display and logging.
	OrgSlug string `json:"org_slug"`
	// ConnectedByName is the display name of the user who authorized the OAuth flow.
	ConnectedByName string `json:"connected_by_name"`
}

// OAuthStateKey returns the Redis key for a CSRF state token.
func OAuthStateKey(state string) string { return "linear:oauth:state:" + state }

// AccessTokenKey returns the Redis key for an org's cached Linear access token.
// The user-actor token is valid ~24h; sync caches it here and refreshes via the
// stored refresh token on expiry/401.
//
// The v2 key namespace keeps entries under the unversioned
// "linear:access_token:<orgID>" key from being read: those hold a token shared
// across organizations, and reading one would defeat tenant isolation.
func AccessTokenKey(orgID string) string { return "linear:access_token:v2:" + orgID }

// RefreshLockKey returns the Redis key for the distributed token-refresh lock, so
// concurrent operations for one org do not race the refresh-token exchange (which
// rotates the refresh token, invalidating a concurrent refresh).
func RefreshLockKey(orgID string) string { return "linear:refresh_lock:" + orgID }

// ParseConfig unmarshals the connector config into typed credentials and settings.
func ParseConfig(cfg *sdk.ConnectorConfig) (LinearCredentials, LinearSettings, error) {
	var creds LinearCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, LinearSettings{}, fmt.Errorf("parse credentials: %w", err)
	}
	var settings LinearSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}
	return creds, settings, nil
}

func validateCredentials(creds LinearCredentials) error {
	if creds.AccessToken == "" {
		return fmt.Errorf("credentials must include access_token")
	}
	return nil
}

func validateSettings(settings LinearSettings) error {
	if settings.WorkspaceID == "" {
		return fmt.Errorf("settings.workspace_id is required")
	}
	return nil
}

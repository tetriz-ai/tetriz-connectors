// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Per-org config types stored as JSON in the database (ConnectorConfig.Credentials / .Settings).
// These are NOT the service-level config — that lives in gateway/config.go (gateway/defaults.yaml or TETRIZ_GATEWAY_CONFIG).
//
// Structs vs Protos decision:
//   - These stay as Go structs because they never cross a network boundary.

// JiraCredentials maps to ConnectorConfig.Credentials (encrypted at rest).
type JiraCredentials struct {
	// OAuth 2.0 token auth (primary method for Jira Cloud via Atlassian OAuth).
	// Access token is cached in Redis and refreshed transparently by the VendorGateway.
	// Only the refresh token is persisted in the database.
	OAuthRefreshToken string `json:"oauth_refresh_token,omitempty"`

	// Webhook secret for verifying inbound webhooks (if configured).
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// JiraSettings maps to ConnectorConfig.Settings.
type JiraSettings struct {
	// CloudID is the Atlassian cloud site identifier (from /oauth/token/accessible-resources).
	CloudID string `json:"cloud_id"`

	// CloudName is the display name of the Jira cloud site.
	CloudName string `json:"cloud_name,omitempty"`

	// SiteURL is the URL of the Jira cloud site (e.g., "https://myorg.atlassian.net").
	SiteURL string `json:"site_url,omitempty"`

	// ProjectKeys optionally restricts sync to specific projects.
	// If empty, all projects are synced.
	ProjectKeys []string `json:"project_keys,omitempty"`

	// ConnectedByName is the display name of the admin who authorized the connection.
	ConnectedByName string `json:"connected_by_name,omitempty"`
}

// parseConfig extracts JiraCredentials and JiraSettings from a ConnectorConfig.
func parseConfig(cfg *sdk.ConnectorConfig) (JiraCredentials, JiraSettings, error) {
	var creds JiraCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, JiraSettings{}, fmt.Errorf("parse credentials: %w", err)
	}

	var settings JiraSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}

	return creds, settings, nil
}

// validateCredentials checks that OAuth credentials are present.
func validateCredentials(creds JiraCredentials) error {
	if creds.OAuthRefreshToken == "" {
		return fmt.Errorf("credentials must include OAuth refresh token")
	}
	return nil
}

// validateSettings checks required settings fields.
func validateSettings(settings JiraSettings) error {
	if settings.CloudID == "" {
		return fmt.Errorf("settings.cloud_id is required")
	}
	return nil
}

// Redis key helpers — centralized here so all flows (setup, sync, disconnect)
// reference the same key patterns.

// AccessTokenKey returns the Redis key for a cached Jira access token.
func AccessTokenKey(orgID string) string {
	return "jira_access_token:" + orgID
}

// RefreshLockKey returns the Redis key for the distributed refresh lock.
func RefreshLockKey(orgID string) string {
	return "jira_refresh_lock:" + orgID
}

// OAuthStateKey returns the Redis key for a CSRF state token.
func OAuthStateKey(state string) string {
	return "oauth_state:jira:" + state
}

// SiteSelectionKey returns the Redis key for a pending multi-site selection session.
func SiteSelectionKey(selectionKey string) string {
	return "jira_site_selection:" + selectionKey
}

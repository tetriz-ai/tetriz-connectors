// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package config holds the settings of the connector install flows and the
// VCS connectors: OAuth application ids and secrets, pagination, checkpoint
// budgets. The host fills a Config; the secret fields are read from the
// environment or a secret store by the host, never from a checked-in file.
package config

import (
	"time"
)

// Config is the connector service configuration.
type Config struct {
	// GitHub App secrets populated from a secret store by the host.
	GitHubAppPrivateKeyPEM string
	GitHubAppWebhookSecret string
	// GitHubAppClientSecret is the App OAuth client secret supplied by the host.
	GitHubAppClientSecret string

	// Bitbucket OAuth client secret populated from a secret store by the host.
	BitbucketOAuthClientSecret string

	// Jira OAuth client secret populated from a secret store by the host.
	JiraOAuthClientSecret string

	// Slack app client secret of the slack_comms connector,
	// which holds a bot token for posting. The host supplies it through the
	// connector configuration.
	SlackCommsAppClientSecret string

	// GitLab OAuth client secret populated from a secret store by the host.
	GitLabOAuthClientSecret string

	// Linear OAuth client secret populated from a secret store by the host.
	LinearOAuthClientSecret string

	// ConnectorCredentialsEncryptionKey is the base64-encoded 32-byte AES-256 key that encrypts
	// connector_configs.credentials at rest.
	// The runner reads it from TETRIZ_CREDENTIALS_KEY.
	ConnectorCredentialsEncryptionKey string

	// DisableCredentialsEncryption is an escape hatch for exceptional local/dev workflows.
	// When false or unset, ConnectorCredentialsEncryptionKey must be configured and
	// connector credentials are encrypted at rest. When true, credentials are stored unencrypted.
	DisableCredentialsEncryption bool

	// EnabledConnectors controls which connectors are available for connection.
	// Connectors absent from the map or set to false are treated as "coming soon"
	// in the catalog and their connect/disconnect RPCs are rejected.
	EnabledConnectors map[string]bool

	GitHubApp      *GitHubAppStaticConfig
	BitbucketOAuth *BitbucketOAuthConfig
	JiraOAuth      *JiraOAuthConfig
	SlackCommsApp  *SlackAppStaticConfig
	GitLabOAuth    *GitLabOAuthConfig
	LinearOAuth    *LinearOAuthConfig

	// GitHubPagination holds per-operation page sizing for the GitHub connector.
	GitHubPagination *GitHubPaginationConfig

	// BitbucketPagination holds per-operation page sizing for the Bitbucket
	// connector. Both /pullrequests/{id}/diffstat and /repositories/.../diffstat/{sha}
	// share a single pagelen + max-page cap; per-op overrides live under
	// `Operations["fetch_pr_diffstat"]` and `Operations["fetch_commit_diffstat"]`
	// so a per-customer rate-limit squeeze can dial each down independently.
	BitbucketPagination *BitbucketPaginationConfig

	// CheckpointBudget controls deadline-aware checkpointing for connector operations.
	CheckpointBudget *CheckpointBudgetConfig

	// JiraConfig holds Jira-connector-specific tunables (currently cursor
	// overlap window). Optional: nil falls back to in-code defaults so a
	// missing config doesn't bring the connector down.
	JiraConfig *JiraConfig
}

// GitHubAppStaticConfig holds the non-secret GitHub App parameters loaded from YAML.
type GitHubAppStaticConfig struct {
	AppID    int64
	Slug     string
	ClientID string
}

// BitbucketOAuthConfig holds the non-secret Bitbucket OAuth 2.0 consumer
// parameters loaded from YAML.
//
// We register a single OAuth consumer for the host in Bitbucket
// (Bitbucket Settings → OAuth consumers). Bitbucket issues a ClientID +
// ClientSecret pair that identifies the host when users authorize access
// to their workspaces. The same consumer is reused across all onboarded
// organisations — each org gets its own OAuth grant via the authorization
// code flow.
//
// ClientSecret is loaded separately via Config.BitbucketOAuthClientSecret, which the host populates from a secret store.
type BitbucketOAuthConfig struct {
	ClientID    string
	CallbackURL string
}

// JiraOAuthConfig holds the non-secret Jira OAuth 2.0 parameters loaded from YAML.
//
// Jira Cloud and Bitbucket Cloud share the same Atlassian OAuth 2.0 platform.
// We register a single OAuth app in the Atlassian Developer Console. The
// ClientID + ClientSecret pair identifies the host when users authorize access
// to their Jira sites. Each org gets its own OAuth grant.
//
// Jira uses rotating refresh tokens — each refresh returns a NEW refresh token
// that must be persisted. The VendorGateway handles this with a distributed
// Redis lock to avoid concurrent refresh races.
//
// ClientSecret is loaded separately via Config.JiraOAuthClientSecret, which the host populates from a secret store.
type JiraOAuthConfig struct {
	ClientID    string
	CallbackURL string
}

// SlackAppStaticConfig holds the non-secret Slack App parameters loaded from YAML.
type SlackAppStaticConfig struct {
	ClientID    string
	RedirectURI string
	Scopes      string // comma-separated scopes for the OAuth authorize URL
	S3Bucket    string
	// APIBaseURL overrides the Slack Web API base for OAuth/exchange calls
	// (default https://slack.com/api). Set it to a mock so the connect flow
	// runs headlessly; leave it empty against the real vendor.
	APIBaseURL string
}

// GitLabOAuthConfig holds the non-secret GitLab OAuth 2.0 application
// parameters loaded from YAML.
//
// We register a single OAuth application for the host in GitLab
// (gitlab.com → Applications). GitLab issues a ClientID + ClientSecret
// pair. The same application is reused across all onboarded organisations.
//
// ClientSecret is loaded separately via Config.GitLabOAuthClientSecret, which the host populates from a secret store.
type GitLabOAuthConfig struct {
	ClientID    string
	CallbackURL string
	WebhookURL  string
}

// LinearOAuthConfig holds the non-secret Linear OAuth 2.0 application
// parameters loaded from YAML.
//
// We register a single OAuth application for the host in the Linear developer
// settings. Linear issues a ClientID + ClientSecret pair. The same application
// is reused across all onboarded organisations.
//
// Linear access tokens are long-lived and do not expire or require rotation.
//
// ClientSecret is loaded separately via Config.LinearOAuthClientSecret, which the host populates from a secret store.
type LinearOAuthConfig struct {
	ClientID    string
	CallbackURL string
}

// OperationPaginationConfig controls adaptive page sizing for a single operation.
// InlinePerIssue and MaxTailPages are only consumed by fetch_issues — left zero
// (the helper defaults) for every other op.
type OperationPaginationConfig struct {
	PageSize       int // starting page size for queries
	MinPageSize    int // stop retrying below this
	BackoffDivisor int // divide page size by this on 5xx (e.g. 2 = halve)
	// InlinePerIssue (fetch_issues only) is the number of comment nodes
	// returned per issue in the initial buildIssuesQuery response. A
	// per-page bound: issues with more comments are paginated by
	// fetchRemainingComments.
	InlinePerIssue int
	// MaxTailPages (fetch_issues only) caps fetchRemainingComments at this
	// many tail pages per issue. Bounds blast radius if GitHub returns a
	// stuck endCursor and caps per-issue cost so a mega-thread can't
	// dominate a sync run.
	MaxTailPages int
}

// GitHubPaginationConfig holds per-operation page sizing for the GitHub connector.
type GitHubPaginationConfig struct {
	Operations map[string]*OperationPaginationConfig
}

// BitbucketPaginationConfig holds per-operation page sizing for the
// Bitbucket connector. Currently consumed by the diffstat enrichment paths
// (PR diffstat and per-commit diffstat) only — list endpoints
// (`/pullrequests`, `/commits`) use fixed pagelens because their cursor
// logic depends on them.
//
// `MaxDiffstatPages` caps both diffstat loops so a pathological vendor
// `next` URL can't run the sync indefinitely inside the cron envelope.
// Zero values fall back to package-level defaults (`diffstatPagelen=500`,
// `maxDiffstatPages=200` in `connector/bitbucket/operations.go`).
type BitbucketPaginationConfig struct {
	Operations       map[string]*OperationPaginationConfig
	MaxDiffstatPages int
}

// CheckpointBudgetConfig controls deadline-aware checkpointing.
type CheckpointBudgetConfig struct {
	Reserve time.Duration // stop processing new items when remaining time < Reserve
}

// JiraConfig holds Jira-connector-specific tunables loaded from YAML.
//
// CursorOverlapWindow is the look-back subtracted from cursor.Timestamp when
// fetch_issues / fetch_comments / fetch_worklogs build their incremental JQL
// `updated >= ?` filter. Defense-in-depth against boundary misses: JQL's
// minute-precision floor, clock skew between Atlassian's API and the
// connector node, intra-second writes that the high-water mark elides, and
// the unmodeled secondary-sort behaviour of Jira's pagination snapshot.
// An idempotent upsert in the consumer absorbs the re-emission.
//
// The connector always subtracts a further fixed 14 hours on top of this window,
// because Jira reads a JQL date in the authorizing user's profile time zone and the
// date cannot carry a zone; this window covers only the remaining boundary risks.
//
// Set to 0 (or omit the field) to fall back to the in-code default
// (5 minutes). The cost of a larger window is bounded re-fetch volume per
// run; the cost of a too-small window is the small probability of missing
// edge-case updates.
type JiraConfig struct {
	CursorOverlapWindow time.Duration
}

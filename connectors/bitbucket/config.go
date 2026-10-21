// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	bitbucketpb "github.com/tetriz-ai/tetriz-connectors/gen/bitbucket/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Per-org config types stored as JSON in the database (ConnectorConfig.Credentials / .Settings).
// These are NOT the service-level config — that lives in gateway/config.go (gateway/defaults.yaml or TETRIZ_GATEWAY_CONFIG).
//
// Structs vs Protos decision:
//   - These stay as Go structs because they never cross a network boundary.

// BitbucketCredentials maps to ConnectorConfig.Credentials (encrypted at rest).
type BitbucketCredentials struct {
	// OAuth 2.0 tokens, the only way the runner authenticates to Bitbucket.
	// The connect flow in auth/integrations writes them.
	OAuthAccessToken  string `json:"oauth_access_token,omitempty"`
	OAuthRefreshToken string `json:"oauth_refresh_token,omitempty"`
	TokenExpiresAt    string `json:"token_expires_at,omitempty"`

	// Webhook secret for verifying inbound webhooks (if configured).
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// BitbucketSettings maps to ConnectorConfig.Settings.
type BitbucketSettings struct {
	Workspace                  string   `json:"workspace"`
	WorkspaceUUID              string   `json:"workspace_uuid"`
	WorkspaceName              string   `json:"workspace_name"`
	AuthorizingUserUUID        string   `json:"authorizing_user_uuid,omitempty"`
	AuthorizingUserDisplayName string   `json:"authorizing_user_display_name,omitempty"`
	Repositories               []string `json:"repositories,omitempty"`
	IssueTrackerEnabled        bool     `json:"issue_tracker_enabled,omitempty"`

	// TrackedBranch overrides which branch is used for commit fetching.
	// When set, commits are fetched from this branch instead of the repository's default branch.
	// When empty, falls back to the repository's default branch.
	TrackedBranch string `json:"tracked_branch,omitempty"`

	// RepoBranchOverrides is a map of repo full name → branch name.
	// Takes precedence over TrackedBranch for the specified repos.
	RepoBranchOverrides map[string]string `json:"repo_branch_overrides,omitempty"`

	// TrackAllBranches / RepoAllBranches opt repos into counting every branch
	// instead of one tracked branch. TrackAllBranches
	// is the org-level fallback; RepoAllBranches lists repos that track all
	// branches and wins over RepoBranchOverrides + the org-level settings.
	TrackAllBranches bool     `json:"track_all_branches,omitempty"`
	RepoAllBranches  []string `json:"repo_all_branches,omitempty"`

	// DiscoveredRepos caches the list of repositories discovered from the Bitbucket workspace.
	// Populated by the repo discovery flow; branch overrides are resolved from it
	// without re-fetching the repo list on every sync.
	DiscoveredRepos []BitbucketDiscoveredRepo `json:"discovered_repos,omitempty"`

	// BotAccountIDs is an org-level allow-list of Bitbucket account_id values to be
	// treated as bot reviewers when computing distinct-pull-request reviewer load.
	// Bitbucket lacks a clean vendor-side bot signal (see detectIsBot), so this
	// list is the override path. Empty list is the valid default.
	// Consumed by the downstream consumer, not the connector itself.
	BotAccountIDs []string `json:"bot_account_ids,omitempty"`

	// DisableCommitDiffstat is the per-org opt-out for the commit-diffstat
	// enrichment in fetch_commits. Default zero (false) = enrich every new
	// commit. Flip to true for orgs where the extra `/diffstat/{sha}` calls
	// would push the connector past its vendor rate-limit budget.
	DisableCommitDiffstat bool `json:"disable_commit_diffstat,omitempty"`

	// VendorRateLimits records the most-recently-observed X-RateLimit-Limit
	// per gateway group (e.g. "bitbucket-rest"). The gateway factory
	// reads this map and scales WithRateLimits per-group instead of fanning
	// a single cap across groups with different vendor budgets.
	// Empty/missing entries fall back to the yml floor.
	//
	// Type alias for pacing.VendorRateLimitObservation — the wire shape
	// is shared across connectors so the scheduler factory can parse any
	// connector's settings without per-vendor coupling.
	VendorRateLimits map[string]pacing.VendorRateLimitObservation `json:"vendor_rate_limits,omitempty"`
}

// PRDiffstatCacheEntry is the cached enrichPR result for a final-state PR.
//
// FilesStr is the JSON-encoded []string of file paths; FilesStatStr is the
// JSON-encoded []bbPRFileLineStat with per-file additions/deletions; both
// shapes match what enrichPR returns. Additions/Deletions are the aggregate
// line counts that enrichPR injects into the PR payload itself
// (see enrichPR) — a downstream consumer reads
// `additions`/`deletions` straight off the payload. Without caching
// the aggregates, a cache-hit emission would carry zero LOC and silently
// break cycle-time / AI-attribution / LOC-trend metrics for terminal PRs.
type PRDiffstatCacheEntry struct {
	FilesStr     string `json:"files"`
	FilesStatStr string `json:"files_line_stats"`
	Additions    int    `json:"additions"`
	Deletions    int    `json:"deletions"`
}

// CommitDiffstatCacheEntry is the cached aggregate result of a
// `/diffstat/{sha}` call — metadata only, NOT the unified-diff body. Actual
// commit diffs (when needed for content analysis) flow through the separate
// `fetch_pr_diffs` op which uploads to S3; this cache only holds the three
// integer aggregates that feed the consumer's commit metrics.
//
// Per-file breakdowns are intentionally not retained — the commit metric
// surface is aggregate-only. A future per-file schema would belong in a
// sibling type to keep the row footprint bounded.
type CommitDiffstatCacheEntry struct {
	Additions    int `json:"additions"`
	Deletions    int `json:"deletions"`
	FilesChanged int `json:"files_changed"`
}

// resolveCommitBranch returns the branch that commit + repo-tree fetches walk.
// These always follow the repo's default branch, decoupled from the tracked-
// branch / all-branches setting (which governs only the PR read-filter). Commits
// and the tree therefore reflect the canonical mainline regardless of which
// branch(es) an org elects to count PRs against.
//
// Falls back to "main" only as a true last resort — when defaultBranch is empty
// because every upstream source failed: the DiscoveredRepos cache missed AND the
// per-repo BitbucketGetRepo resolution (resolveDefaultBranch) errored. This guard
// is load-bearing for Bitbucket specifically: the commits endpoint with an empty
// `include` is `git log --all` (every branch) — NOT the default branch — so
// passing "" would silently ingest all branches and violate the default-branch-
// only contract. A concrete single branch ("main") keeps it single-branch; if a
// repo's real default is "master" this last-resort miss under-fetches rather than
// over-fetching all branches (the safe failure direction). Both the
// DiscoveredRepos path and the GetRepo resolution supply the real default, so
// this fallback fires only on a GetRepo network/parse failure.
func (s BitbucketSettings) resolveCommitBranch(defaultBranch string) string {
	if defaultBranch == "" {
		return "main"
	}
	return defaultBranch
}

// isFinalPRState reports whether a Bitbucket PR state is terminal — no new
// commits can be added. The diffstat for such PRs is therefore stable and
// cacheable. Bitbucket uses these state strings verbatim in its API; see
// https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/.
func isFinalPRState(state string) bool {
	switch state {
	case "MERGED", "DECLINED", "SUPERSEDED":
		return true
	default:
		return false
	}
}

// BitbucketDiscoveredRepo represents a repository discovered from the Bitbucket workspace.
// Cached in settings to avoid redundant API calls during subsequent sync operations.
//
// Per-repo lifecycle fields (Status / LastSeenAt / LastErrorAt /
// LastHTTPStatus) let `fetch_prs` survive per-repo failures: one deleted
// repo does not fail the entire sync run.
// Zero-value Status (UNSPECIFIED) means active and is elided from JSON via
// omitempty — keeping the wire format compact for the everyday case.
//
// LastErrorAt + LastHTTPStatus are diagnostic stamps recorded on EVERY
// per-repo failure (auth / rate-limit / 5xx / 404), independent of whether
// the failure transitioned Status. Operators can read them via
// `connector_configs.settings->'discovered_repos'` to see "what HTTP code
// last broke this repo and when". A value of 0 for LastHTTPStatus means
// "no HTTP status observed" — used when fetch_repos noticed the repo was
// absent from the workspace listing (no per-repo request was made).
type BitbucketDiscoveredRepo struct {
	FullName       string                          `json:"full_name"`
	DefaultBranch  string                          `json:"default_branch,omitempty"`
	Private        bool                            `json:"private"`
	Status         bitbucketpb.BitbucketRepoStatus `json:"status,omitempty"`
	LastSeenAt     string                          `json:"last_seen_at,omitempty"`     // RFC3339 UTC — set by fetch_repos when the repo is observed live
	LastErrorAt    string                          `json:"last_error_at,omitempty"`    // RFC3339 UTC — set on any per-repo failure (auth / rate-limit / 5xx / 404)
	LastHTTPStatus int                             `json:"last_http_status,omitempty"` // HTTP status of the last per-repo failure; 0 means "absent from listing, no per-repo request made"
}

// IsActive reports whether the repo should be included in pull operations.
// Only DELETED is excluded. UNSPECIFIED, the value of an entry with no status,
// is treated as active, and the next `fetch_repos` reconciliation writes an
// explicit ACTIVE.
func (r BitbucketDiscoveredRepo) IsActive() bool {
	return r.Status != bitbucketpb.BitbucketRepoStatus_BITBUCKET_REPO_STATUS_DELETED
}

// parseConfig extracts BitbucketCredentials and BitbucketSettings from a ConnectorConfig.
func parseConfig(cfg *sdk.ConnectorConfig) (BitbucketCredentials, BitbucketSettings, error) {
	var creds BitbucketCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, BitbucketSettings{}, fmt.Errorf("parse credentials: %w", err)
	}

	var settings BitbucketSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}

	return creds, settings, nil
}

// errAppPassword refuses a config that carries app-password credentials: the
// runner builds only an OAuth client for Bitbucket, so such a config would
// pass and then fail every sync.
var errAppPassword = errors.New("app passwords are not supported; the runner authenticates Bitbucket through OAuth (the connect flow in auth/integrations writes the tokens)")

// validateCredentials checks that the credentials carry an OAuth token, the
// only method the runner uses. raw is the stored credentials JSON, read for
// app-password keys the struct no longer has.
func validateCredentials(raw json.RawMessage, creds BitbucketCredentials) error {
	var keys map[string]json.RawMessage
	if json.Unmarshal(raw, &keys) == nil {
		if _, ok := keys["app_password"]; ok {
			return errAppPassword
		}
		if _, ok := keys["username"]; ok {
			return errAppPassword
		}
	}
	if creds.OAuthAccessToken == "" && creds.OAuthRefreshToken == "" {
		return errors.New("credentials must include an OAuth token")
	}
	return nil
}

// validateSettings checks required settings fields.
func validateSettings(settings BitbucketSettings) error {
	if settings.Workspace == "" {
		return fmt.Errorf("settings.workspace is required")
	}
	return nil
}

// Redis key helpers — centralized here so all flows (setup, sync, disconnect)
// reference the same key patterns.

// AccessTokenKey returns the Redis key for a cached Bitbucket access token.
func AccessTokenKey(orgID string) string {
	return "bitbucket_access_token:" + orgID
}

// RefreshLockKey returns the Redis key for the distributed refresh lock.
func RefreshLockKey(orgID string) string {
	return "bitbucket_refresh_lock:" + orgID
}

// OAuthStateKey returns the Redis key for a CSRF state token.
func OAuthStateKey(state string) string {
	return "oauth_state:bitbucket:" + state
}

// WorkspaceSelectionKey returns the Redis key for temporary workspace
// selection state (tokens + workspace list) during the two-phase setup flow.
func WorkspaceSelectionKey(selectionKey string) string {
	return "bb:workspace-selection:" + selectionKey
}

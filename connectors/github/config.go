// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Per-org config types stored as JSON in the database (ConnectorConfig.Credentials / .Settings).
// These are NOT the service-level config — that lives in gateway/config.go (gateway/defaults.yaml or TETRIZ_GATEWAY_CONFIG).
//
// Structs vs Protos decision:
//   - These stay as Go structs because they never cross a network boundary.
//     They are serialized as JSON into the database and deserialized within this service only.
//   - If we later add a gRPC API to create/update connector configs, the request/response
//     types should be protobuf — but these internal types stay as Go structs.

// GitHubCredentials maps to ConnectorConfig.Credentials (encrypted at rest).
// A connector authenticates as exactly one of two mutually-exclusive abilities:
//   - a GitHub App installation (InstallationID set) — the org model; AppID and
//     the private key are static runner config and only the
//     per-org installation_id varies and is stored here.
//   - a fine-grained personal access token (PersonalAccessToken set) — the
//     single-user (individual) model; the token is the whole credential and is
//     persisted in this encrypted blob (no static App identity is involved).
//
// validateCredentials enforces the XOR. The gateway factory reads which one is
// set to pick the transport (ghinstallation for App, bearer token for PAT).
type GitHubCredentials struct {
	InstallationID string `json:"installation_id,omitempty"`

	// PersonalAccessToken is a GitHub fine-grained PAT used for a single-user
	// account that has no App installation. Mutually exclusive with
	// InstallationID. Stored verbatim in the encrypted credentials blob.
	PersonalAccessToken string `json:"personal_access_token,omitempty"`

	// Webhook secret for HMAC-SHA256 signature verification of inbound events.
	// Set during GitHub App setup and rotated via the GitHub App settings page.
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// IsPAT reports whether these credentials authenticate via a fine-grained
// personal access token rather than a GitHub App installation.
func (c GitHubCredentials) IsPAT() bool { return c.PersonalAccessToken != "" }

// GitHubSettings maps to ConnectorConfig.Settings.
// Base URLs and rate limits are configured in gateway/defaults.yaml, not here.
type GitHubSettings struct {
	Organization string `json:"organization"`
	// InstallationID copies the GitHub App installation a connect flow stored in
	// the credentials. The credentials are encrypted, so this copy is what a
	// unique index can see: one installation belongs to one organization.
	InstallationID string   `json:"installation_id,omitempty"`
	Repositories   []string `json:"repositories,omitempty"`
	IncludePrivate bool     `json:"include_private"`

	// TrackedBranch overrides which branch is used for commit fetching.
	// When set, fetch_commits uses ref(qualifiedName:) instead of defaultBranchRef.
	// When empty, falls back to the repository's default branch.
	TrackedBranch string `json:"tracked_branch,omitempty"`

	// RepoBranchOverrides is a map of repo full name → branch name.
	// Takes precedence over TrackedBranch for the specified repos.
	// Example: {"acme/api": "master", "acme/tickets": "main"}
	RepoBranchOverrides map[string]string `json:"repo_branch_overrides,omitempty"`

	// TrackAllBranches / RepoAllBranches opt repos into counting every branch
	// instead of one tracked branch. TrackAllBranches
	// is the org-level fallback; RepoAllBranches lists repos that track all
	// branches and wins over RepoBranchOverrides + the org-level settings.
	TrackAllBranches bool     `json:"track_all_branches,omitempty"`
	RepoAllBranches  []string `json:"repo_all_branches,omitempty"`

	// IdentityVerification controls the hint_source used by fetch_org_members.
	// "saml" → uses SAML SCIM identities API (high-confidence, tag: "saml_scim").
	// ""     → uses standard org members GraphQL (medium-confidence, tag: "org_members_api").
	IdentityVerification string `json:"identity_verification,omitempty"`

	// DiscoveredRepos caches the list of repositories discovered during fetch_repos.
	// Populated by the repo discovery flow; branch overrides are resolved from it
	// without re-fetching the repo list on every sync.
	DiscoveredRepos []GitHubDiscoveredRepo `json:"discovered_repos,omitempty"`

	// GitHubIssuesEnabled controls whether the GitHub Issues integration is active
	// for this org. Toggled independently of the GitHub VCS connector since issues
	// piggyback on the same GitHub App installation.
	GitHubIssuesEnabled bool `json:"github_issues_enabled,omitempty"`

	// LookbackDays caps how far back first-sync fetch operations scan for
	// history. Applies to all fetch ops (PRs, commits, issues, reviews,
	// pr_comments, copilot) when they have no prior cursor. Incremental
	// runs use the saved cursor, so this only affects initial backfill size.
	// 0 = use DefaultLookbackDays (365). Per-env throttling: write a row
	// into connector_configs for the local dev org.
	LookbackDays int `json:"lookback_days,omitempty"`

	// BotAccountIDs is an org-level allow-list of GitHub user database IDs
	// that should be treated as bots regardless of vendor signal. The
	// consumer reads this list (via context plumbing) to filter PR-key
	// sets used for reviewer-load metrics. Stable across login renames;
	// the caller that writes it maps login -> ID. Empty list is the
	// default and disables the override path.
	BotAccountIDs []string `json:"bot_account_ids,omitempty"`

	// VendorRateLimits records the most-recently-observed X-RateLimit-Limit
	// per gateway group (e.g. "github-graphql", "github-rest"). The
	// gateway factory reads this map and scales WithRateLimits per-group
	// instead of fanning a single cap across REST + GraphQL + search, which
	// would shrink GraphQL throughput. Empty/missing entries
	// fall back to the yml floor for that group.
	//
	// GitHub Cloud splits caps per resource bucket (core / graphql / search)
	// — Premium and enterprise tiers bump each independently — so this
	// per-group map matches the vendor's accounting.
	//
	// Type alias for pacing.VendorRateLimitObservation — the wire shape
	// is shared across connectors so the scheduler factory can parse any
	// connector's settings without per-vendor coupling.
	VendorRateLimits map[string]pacing.VendorRateLimitObservation `json:"vendor_rate_limits,omitempty"`
}

// DefaultLookbackDays is the fallback first-sync window when LookbackDays
// is unset on GitHubSettings.
const DefaultLookbackDays = 365

// copilotFinalizationLagDays is the trailing window fetch_copilot_usage
// re-pulls every run. The Copilot metrics API returns a rolling window whose
// most-recent day is in-progress; its counts settle once the day closes.
// Re-pulling this many days behind the watermark lets a day's finalized counts
// overwrite the partial values first captured (re-emit is idempotent via the
// stable externalID). Without it, the <= watermark skip locks each day in at
// its partial value forever.
//
// GitHub publishes no finalization SLA for Copilot metrics, so 2 is a
// conservative default rather than a measured bound. It is cheap to over-shoot:
// once a day settles its payload stops changing, so the re-pull upserts a
// byte-identical record and the downstream conflict clause skips reprocessing
// (it resets processing status only when the payload actually differs).
const copilotFinalizationLagDays = 2

// GetLookbackDays returns the effective first-sync lookback window,
// falling back to DefaultLookbackDays (365) when no per-org override is set.
// Dev orgs throttle via a fixture SQL that seeds a small value on the
// connector_configs row.
func (s GitHubSettings) GetLookbackDays() int {
	if s.LookbackDays > 0 {
		return s.LookbackDays
	}
	return DefaultLookbackDays
}

// GitHubDiscoveredRepo represents a repository discovered from the GitHub organization.
// Cached in settings to avoid redundant API calls during subsequent sync operations.
type GitHubDiscoveredRepo struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
	// DatabaseID is GitHub's stable numeric repo ID. It is the platform_repo_id
	// a consumer's repository records are keyed on, so an explicit (settings.repositories) sync
	// must carry it forward from this cache or fetch_repo_tree would key tree
	// records by full name and UpdateTree would miss the row. Omitted (0) when
	// unknown, as for an explicit repository the cache does not hold; callers
	// fall back then.
	DatabaseID int64 `json:"database_id,omitempty"`
}

// HintSource returns the identity hint_source value for fetch_org_members.
func (s GitHubSettings) HintSource() string {
	if s.IdentityVerification == "saml" {
		return HintSourceSAMLSCIM
	}
	return HintSourceOrgMembersAPI
}

// branchPolicy is the resolved per-repo branch decision. AllBranches=true means
// count every branch (Branch is ignored); otherwise Branch is the single branch
// to track (empty → the caller's auto-detected default).
type branchPolicy struct {
	AllBranches bool
	Branch      string
}

// resolveRepoBranchPolicy resolves the effective branch policy for a repo.
// Precedence (most specific wins): RepoAllBranches → all; RepoBranchOverrides →
// that branch; TrackAllBranches → all; TrackedBranch → that branch; else the
// auto-detected default. Mirrors the connector read RPC's resolution so the
// query predicate and the fetch/write path agree.
func (s GitHubSettings) resolveRepoBranchPolicy(repoFullName, autoDetected string) branchPolicy {
	for _, r := range s.RepoAllBranches {
		if r == repoFullName {
			return branchPolicy{AllBranches: true}
		}
	}
	if branch, ok := s.RepoBranchOverrides[repoFullName]; ok && branch != "" {
		return branchPolicy{Branch: branch}
	}
	if s.TrackAllBranches {
		return branchPolicy{AllBranches: true}
	}
	if s.TrackedBranch != "" {
		return branchPolicy{Branch: s.TrackedBranch}
	}
	return branchPolicy{Branch: autoDetected}
}

// resolveRepoBranch returns the single branch to track for a repo. A repo in
// all-branches mode resolves to autoDetected here; callers that need the full
// policy (e.g. branch enumeration) call resolveRepoBranchPolicy directly.
func (s GitHubSettings) resolveRepoBranch(repoFullName string, autoDetected string) string {
	p := s.resolveRepoBranchPolicy(repoFullName, autoDetected)
	if p.AllBranches {
		return autoDetected
	}
	return p.Branch
}

// resolveCommitBranch returns the branch that commit + repo-tree fetches walk.
// These always follow the repo's default branch, decoupled from the tracked-
// branch / all-branches setting (which governs only the PR read-filter via
// fetch_repos). Commits and the tree therefore reflect the canonical mainline
// regardless of which branch(es) an org elects to count PRs against.
func (s GitHubSettings) resolveCommitBranch(defaultBranch string) string {
	return defaultBranch
}

// Identity hint source constants for GitHub identity enrichment.
const (
	HintSourceSAMLSCIM      = "saml_scim"
	HintSourceOrgMembersAPI = "org_members_api"
)

// OAuthStateKey returns the Redis key for a GitHub App install CSRF state token.
func OAuthStateKey(state string) string { return "oauth_state:github:" + state }

// parseConfig extracts GitHubCredentials and GitHubSettings from a ConnectorConfig.
func parseConfig(cfg *sdk.ConnectorConfig) (GitHubCredentials, GitHubSettings, error) {
	var creds GitHubCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, GitHubSettings{}, fmt.Errorf("parse credentials: %w", err)
	}

	var settings GitHubSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}

	return creds, settings, nil
}

// validateCredentials enforces that exactly one ability is configured: either a
// GitHub App installation_id (org model) or a fine-grained personal_access_token
// (single-user model). Neither and both are rejected — the gateway factory and
// fetch_repos branch on which one is set, so an ambiguous credential would
// silently pick the wrong transport/endpoint.
func validateCredentials(creds GitHubCredentials) error {
	hasApp := creds.InstallationID != ""
	hasPAT := creds.PersonalAccessToken != ""
	switch {
	case hasApp && hasPAT:
		return fmt.Errorf("credentials must set exactly one of installation_id (GitHub App) or personal_access_token (PAT), not both")
	case !hasApp && !hasPAT:
		return fmt.Errorf("credentials must include installation_id (GitHub App installation) or personal_access_token (fine-grained PAT)")
	}
	return nil
}

// validateSettings checks required settings fields. organization is required
// only for an App credential: the org-listing/copilot/org-member paths key off
// it. A PAT (single-user) connector has no org — its listing source is
// /user/repos, which never reads settings.Organization — so the field is
// optional there.
func validateSettings(creds GitHubCredentials, settings GitHubSettings) error {
	if !creds.IsPAT() && settings.Organization == "" {
		return fmt.Errorf("settings.organization is required")
	}
	return nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// GitLabCredentials maps to ConnectorConfig.Credentials (encrypted at rest).
type GitLabCredentials struct {
	OAuthRefreshToken string `json:"oauth_refresh_token,omitempty"`
	WebhookSecret     string `json:"webhook_secret,omitempty"`
}

// GitLabSettings maps to ConnectorConfig.Settings.
type GitLabSettings struct {
	GroupID                    int64  `json:"group_id"`
	GroupPath                  string `json:"group_path"`
	GroupFullPath              string `json:"group_full_path"`
	GroupName                  string `json:"group_name"`
	WebhookHookID              int64  `json:"webhook_hook_id,omitempty"`
	AuthorizingUserID          int64  `json:"authorizing_user_id,omitempty"`
	AuthorizingUserUsername    string `json:"authorizing_user_username,omitempty"`
	AuthorizingUserDisplayName string `json:"authorizing_user_display_name,omitempty"`

	// TrackedBranch overrides which branch is used for commit fetching.
	// When set, commits are fetched from this branch instead of the project's default branch.
	// When empty, falls back to the repository's default branch.
	TrackedBranch string `json:"tracked_branch,omitempty"`

	// RepoBranchOverrides is a map of project path_with_namespace → branch name.
	// Takes precedence over TrackedBranch for the specified projects.
	RepoBranchOverrides map[string]string `json:"repo_branch_overrides,omitempty"`

	// TrackAllBranches / RepoAllBranches opt projects into counting every branch
	// instead of one tracked branch. TrackAllBranches
	// is the org-level fallback; RepoAllBranches lists projects that track all
	// branches and wins over RepoBranchOverrides + the org-level settings.
	TrackAllBranches bool     `json:"track_all_branches,omitempty"`
	RepoAllBranches  []string `json:"repo_all_branches,omitempty"`

	// DiscoveredRepos caches the list of projects discovered from the GitLab group.
	// Populated by the repo discovery flow; branch overrides are resolved from it
	// without re-fetching the project list on every sync.
	DiscoveredRepos []GitLabDiscoveredRepo `json:"discovered_repos,omitempty"`

	// BotAccountIDs is an org-level reviewer override list — GitLab user IDs
	// (as strings, for cross-connector consistency) that the org has classified
	// as bots regardless of the vendor's own `bot` flag. Consumed by the
	// consumer to exclude these reviewers from distinct-pull-request review counts.
	// Empty list is the valid default.
	BotAccountIDs []string `json:"bot_account_ids,omitempty"`

	// FetchNativeIssueLinks opts this org into the per-MR closes_issues fetch
	// that feeds native GitLab PR→issue linkage; false (the default) skips
	// the call and leaves native linkage unset (fetch_prs preserves prior
	// linkage rather than clobbering it). It is opt-in because the fetch is a
	// distinct extra per-MR GitLab REST call — enable it only for an org whose
	// fetch_prs rate headroom is confirmed to absorb it; an org near the budget
	// ceiling needs the asynq escalation instead. See the rate-budget note at
	// FetchPRsOp's per-MR loop (operations.go).
	FetchNativeIssueLinks bool `json:"fetch_native_issue_links,omitempty"`
}

// GitLabDiscoveredRepo represents a project discovered from the GitLab group.
// Cached in settings to avoid redundant API calls during subsequent sync operations.
type GitLabDiscoveredRepo struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
	ProjectID     int64  `json:"project_id"`
}

// resolveCommitBranch returns the branch that commit + repo-tree fetches walk.
// These always follow the project's default branch, decoupled from the tracked-
// branch / all-branches setting (which governs only the PR read-filter). Commits
// and the tree therefore reflect the canonical mainline regardless of which
// branch(es) an org elects to count PRs against.
func (s GitLabSettings) resolveCommitBranch(defaultBranch string) string {
	return defaultBranch
}

// HintSource returns the identity hint_source value for fetch_org_members.
func (s GitLabSettings) HintSource() string {
	return HintSourceGroupMembersAPI
}

// Identity hint source constant for GitLab identity enrichment.
const HintSourceGroupMembersAPI = "group_members_api"

func parseConfig(cfg *sdk.ConnectorConfig) (GitLabCredentials, GitLabSettings, error) {
	var creds GitLabCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return creds, GitLabSettings{}, fmt.Errorf("parse credentials: %w", err)
	}
	var settings GitLabSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return creds, settings, fmt.Errorf("parse settings: %w", err)
	}
	return creds, settings, nil
}

func validateCredentials(creds GitLabCredentials) error {
	if creds.OAuthRefreshToken == "" {
		return fmt.Errorf("credentials must include oauth_refresh_token")
	}
	return nil
}

func validateSettings(settings GitLabSettings) error {
	if settings.GroupID == 0 {
		return fmt.Errorf("settings.group_id is required")
	}
	return nil
}

// Redis key helpers — centralized here so all flows reference the same key patterns.

func AccessTokenKey(orgID string) string  { return "gitlab_access_token:" + orgID }
func RefreshLockKey(orgID string) string  { return "gitlab_refresh_lock:" + orgID }
func OAuthStateKey(state string) string   { return "oauth_state:gitlab:" + state }
func GroupSelectionKey(key string) string { return "gl:group-selection:" + key }

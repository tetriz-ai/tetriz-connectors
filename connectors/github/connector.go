// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorID sdk.ConnectorID = vendorids.GitHub

// GitHubConnector implements sdk.Connector for the GitHub integration.
type GitHubConnector struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

// NewConnector creates a new GitHubConnector with the given pagination and checkpoint config.
func NewConnector(paginationCfg *config.GitHubPaginationConfig, checkpointReserve time.Duration) *GitHubConnector {
	return &GitHubConnector{
		paginationCfg:     paginationCfg,
		checkpointReserve: checkpointReserve,
	}
}

// CheckpointReserve is how long before its deadline a run stops taking new
// items so it can checkpoint. Zero disables the early stop.
func (c *GitHubConnector) CheckpointReserve() time.Duration { return c.checkpointReserve }

// PaginationConfig is the per-operation page sizing the connector was built
// with; nil means every operation uses its defaults.
func (c *GitHubConnector) PaginationConfig() *config.GitHubPaginationConfig { return c.paginationCfg }

func (c *GitHubConnector) ID() sdk.ConnectorID { return connectorID }
func (c *GitHubConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryVCS, sdk.CategoryIssueTracking}
}
func (c *GitHubConnector) DisplayName() string { return "GitHub" }

func (c *GitHubConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:                 opFetchPRs,
			DisplayName:        "Fetch Pull Requests",
			Description:        "Fetches pull requests from GitHub repositories via GraphQL API",
			DefaultSchedule:    "*/15 * * * *",
			IngestionType:      sdk.IngestionPull,
			RequiresTenantType: true,
			Factory: func() sdk.Operation {
				return &FetchPRsOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:              opFetchCommits,
			DisplayName:     "Fetch Commits",
			Description:     "Fetches commits from GitHub repositories via GraphQL API",
			DefaultSchedule: "0 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchCommitsOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:              opFetchIssues,
			DisplayName:     "Fetch Issues",
			Description:     "Fetches issues from GitHub repositories via GraphQL API",
			DefaultSchedule: "0 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchIssuesOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:              opFetchReviews,
			DisplayName:     "Fetch Reviews",
			Description:     "Fetches pull request reviews: finds recently-updated PRs, then fetches reviews per PR via REST",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchReviewsOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:              opFetchPRComments,
			DisplayName:     "Fetch PR Review Comments",
			Description:     "Fetches per-user pull request review comments for comment attribution metrics",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchPRCommentsOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:                "fetch_copilot_usage",
			DisplayName:       "Fetch Copilot Usage",
			Description:       "Fetches org-level and per-user Copilot usage metrics via REST API",
			DefaultSchedule:   "0 */6 * * *",
			IngestionType:     sdk.IngestionPull,
			RequiresMultiUser: true,
			Factory:           func() sdk.Operation { return &FetchCopilotUsageOp{} },
		},
		{
			ID:              "webhook_events",
			DisplayName:     "Webhook Events",
			Description:     "Processes inbound GitHub webhook payloads a host passes in",
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionWebhook,
			Factory:         func() sdk.Operation { return &WebhookEventsOp{} },
		},
		{
			ID:                "fetch_org_members",
			DisplayName:       "Fetch Org Members",
			Description:       "Fetches org member identities for identity enrichment. Uses SAML externalIdentities when identity_verification=saml, otherwise standard org members GraphQL",
			DefaultSchedule:   "0 */6 * * *", // Every 6 hours
			IngestionType:     sdk.IngestionPull,
			RequiresMultiUser: true,
			Factory:           func() sdk.Operation { return &FetchOrgMembersOp{} },
		},
		{
			ID:              opFetchPRDiffs,
			DisplayName:     "Fetch PR Diffs",
			Description:     "Fetches unified diffs for merged PRs that the host's work queue lists, writes each to the blob store, and reports back to the queue",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    fetchPRDiffsRequiresHost,
			Factory:         func() sdk.Operation { return &FetchPRDiffsOp{} },
		},
		{
			ID:              opDrainDefectLineage,
			DisplayName:     "Drain Defect Lineage",
			Description:     "Defect lineage: claims bug fixes from the host, blames the lines each fix changed on GitHub to find the commits that introduced the bug (SZZ-style), and submits those candidates to the host",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    drainDefectLineageRequiresHost,
			Factory:         func() sdk.Operation { return &DrainDefectLineageOp{} },
		},
		{
			ID:              opDrainDefectScoring,
			DisplayName:     "Drain Defect Scoring",
			Description:     "Defect lineage: claims candidate bug-introducing commits from the host, scores how close each one's change sits to the fix in the syntax tree, and submits the score to the host",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    drainDefectScoringRequiresHost,
			Factory:         func() sdk.Operation { return &DrainDefectScoringOp{} },
		},
		{
			ID:              opFetchPRsData,
			DisplayName:     "Fetch PR Descriptions",
			Description:     "Fetches PR description via GraphQL, writes it to the blob store, and emits a description_ref record",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchPRsDataOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:              opFetchIssuesData,
			DisplayName:     "Fetch Issue Descriptions",
			Description:     "Fetches issue description via GraphQL, writes it to the blob store, and emits a description_ref record",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory: func() sdk.Operation {
				return &FetchIssuesDataOp{paginationCfg: c.paginationCfg, checkpointReserve: c.checkpointReserve}
			},
		},
		{
			ID:          opFetchProjects,
			DisplayName: "Fetch Projects",
			Description: "Fetches the organization's GitHub Projects (ProjectV2) metadata via GraphQL and emits one record per project",
			// Not auto-scheduled; run it with sync.
			DefaultSchedule:   "",
			IngestionType:     sdk.IngestionPull,
			RequiresMultiUser: true,
			Factory:           func() sdk.Operation { return &FetchProjectsOp{paginationCfg: c.paginationCfg} },
		},
		{
			ID:          opFetchRepos,
			DisplayName: "Fetch Repositories",
			Description: "Lists all accessible repositories and emits one normalized RepositoryRecord per repo",
			// Branches the listing endpoint on credential ability × tenant_type;
			// the orchestrator fails the run loud on an unresolved intent rather
			// than defaulting to the org endpoint.
			RequiresTenantType: true,
			// Hourly (not 6-hourly): until a newly permitted repo is discovered, a
			// consumer receives its pull requests before its repository record; a
			// tighter discovery cadence bounds that window. Cost scales with repo count
			// (paginated REST list + one GraphQL merge-toggles call per repo), on par
			// with fetch_commits / fetch_issues which already run hourly.
			DefaultSchedule: "0 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchReposOp{} },
		},
		{
			ID:              opFetchRepoTree,
			DisplayName:     "Fetch Repository Tree",
			Description:     "Resolves HEAD SHA per repo, walks the git tree, writes a JSON blob to the blob store, and emits a RepoTreeRecord",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchRepoTreeOp{} },
		},
		{
			ID:              opFetchBranches,
			DisplayName:     "Fetch Branches",
			Description:     "Lists every branch of each accessible repo and emits one record per branch",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchBranchesOp{} },
		},
		{
			ID:          opFetchFileContent,
			DisplayName: "Fetch File Content",
			Description: "Pulls the AI coding-agent configuration files (skills, rules, hooks, CLAUDE.md, AGENTS.md and the like) that the host's work queue lists, at their pinned commit, writes each body to the blob store, and emits one record per configuration item",
			// Daily, at 03:00 — deliberately OFF the fetch_repo_tree cron's tick
			// (0 */6, i.e. 00:00/06:00/12:00/18:00). Content pulls read the tree the tree op
			// persisted; firing them on the same minute risks the content work-list reading a
			// pre-refresh tree (stale-converged), skipping that day's changes until the next
			// run. 03:00 lets the 00:00 tree fetch fully settle first. Content changes
			// slowly and the whole-repo tree_sha skip makes an unchanged repo cost zero vendor
			// calls, so a daily tick on a converged org is near-free while cutting a large
			// org's cold-start backfill. Runtime-overridable per org via connector_schedules / OperationOverrides.
			DefaultSchedule: "0 3 * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    fetchFileContentRequiresHost,
			Factory:         func() sdk.Operation { return &FetchFileContentOp{} },
		},
		{
			ID:          opFetchConfig,
			DisplayName: "Fetch Repository Config",
			Description: "Captures per-repo governance config (default branch + effective branch rulesets: required reviews, allowed merge methods) as the per-repo config record. Latest-only.",
			// Unscheduled: run it with `sync`.
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchConfigOp{} },
		},
	}
}

func (c *GitHubConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := parseConfig(cfg)
	if err != nil {
		return fmt.Errorf("github: %w", err)
	}

	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("github: %w", err)
	}

	if err := validateSettings(creds, settings); err != nil {
		return fmt.Errorf("github: %w", err)
	}

	return nil
}

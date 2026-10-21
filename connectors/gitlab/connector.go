// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorID sdk.ConnectorID = vendorids.GitLab

// GitLabConnector implements sdk.Connector for the GitLab integration.
type GitLabConnector struct{}

// NewConnector creates a new GitLabConnector.
func NewConnector() *GitLabConnector { return &GitLabConnector{} }

func (c *GitLabConnector) ID() sdk.ConnectorID { return connectorID }
func (c *GitLabConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryVCS}
}
func (c *GitLabConnector) DisplayName() string { return "GitLab" }

func (c *GitLabConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:              "fetch_projects",
			DisplayName:     "Fetch Projects",
			Description:     "Fetches projects from a GitLab group via REST API",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchProjectsOp{} },
		},
		{
			ID:              "fetch_prs",
			DisplayName:     "Fetch Merge Requests",
			Description:     "Fetches merge requests from GitLab group via REST API",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchPRsOp{} },
		},
		{
			ID:              "fetch_commits",
			DisplayName:     "Fetch Commits",
			Description:     "Fetches commits from GitLab projects via REST API",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchCommitsOp{} },
		},
		{
			ID:              "fetch_issues",
			DisplayName:     "Fetch Issues",
			Description:     "Fetches issues from GitLab group via REST API",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchIssuesOp{} },
		},
		{
			ID:              "fetch_reviews",
			DisplayName:     "Fetch Reviews",
			Description:     "Fetches notes (comments/reviews) from GitLab projects via REST API",
			DefaultSchedule: "*/30 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchReviewsOp{} },
		},
		{
			ID:              "fetch_pr_comments",
			DisplayName:     "Fetch MR Comments",
			Description:     "Fetches per-user MR discussion comments for comment attribution metrics",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchPRCommentsOp{} },
		},
		{
			ID:              "webhook_events",
			DisplayName:     "Webhook Events",
			Description:     "Entry point for all inbound GitLab webhooks — routes each event type to its dedicated operation",
			DefaultSchedule: "", // Empty — push-driven via webhooks, not scheduled polling
			IngestionType:   sdk.IngestionWebhook,
			Factory:         func() sdk.Operation { return &WebhookEventsOp{} },
		},
		{
			ID:                "fetch_org_members",
			DisplayName:       "Fetch Group Members",
			Description:       "Fetches group member identities for identity enrichment. Uses GitLab /groups/{id}/members/all API",
			DefaultSchedule:   "0 */6 * * *",
			IngestionType:     sdk.IngestionPull,
			RequiresMultiUser: true,
			Factory:           func() sdk.Operation { return &FetchGroupMembersOp{} },
		},
		{
			ID:              opFetchMRDiffs,
			DisplayName:     "Fetch MR Diffs",
			Description:     "Fetches diffs for merged MRs that the host's work queue lists, writes each to the blob store, and reports back to the queue",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    fetchMRDiffsRequiresHost,
			Factory:         func() sdk.Operation { return &FetchMRDiffsOp{} },
		},
		{
			ID:              opDrainDefectLineage,
			DisplayName:     "Drain Defect Lineage",
			Description:     "Defect lineage: claims bug fixes from the host, blames the lines each fix changed on GitLab to find the commits that introduced the bug (SZZ-style), and submits those candidates to the host",
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
			DisplayName:     "Fetch MR Descriptions",
			Description:     "Fetches MR description via REST, writes it to the blob store, and emits a description_ref record",
			DefaultSchedule: "*/15 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchPRsDataOp{} },
		},
		{
			ID:              opFetchRepos,
			DisplayName:     "Fetch Repositories",
			Description:     "Lists all group projects and emits one normalized RepositoryRecord per project",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchReposOp{} },
		},
		{
			ID:              opFetchRepoTree,
			DisplayName:     "Fetch Repository Trees",
			Description:     "Fetches the full git tree for each project whose HEAD SHA has changed and writes it to the blob store",
			DefaultSchedule: "0 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchRepoTreeOp{} },
		},
		{
			ID:              opFetchBranches,
			DisplayName:     "Fetch Branches",
			Description:     "Lists every branch of each group project and emits one record per branch",
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
			Description: "Captures per-project governance config (merge_method, squash_option, default_branch) + per-group defaults, as the per-repo config record. Latest-only.",
			// Intentionally unscheduled (default-OFF): opt-in via manual trigger / later schedule flip.
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchConfigOp{} },
		},
	}
}

func (c *GitLabConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := parseConfig(cfg)
	if err != nil {
		return fmt.Errorf("gitlab: %w", err)
	}
	if err := validateCredentials(creds); err != nil {
		return fmt.Errorf("gitlab: %w", err)
	}
	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("gitlab: %w", err)
	}
	return nil
}

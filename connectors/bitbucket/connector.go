// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorID sdk.ConnectorID = vendorids.Bitbucket

// bbPRPullGroup is the stagger group the five PR-pull ops share. They all draw on one
// per-org bitbucket-rest token bucket, so no two may fire on the same minute (the bucket
// bounds a window's count without spacing requests inside it), yet pinning them to fixed
// global minutes makes the whole fleet spike together. The group primitive resolves both:
// members hold a fixed spacing from each other while the group shifts as a unit per org.
// Each member's StaggerOffsetMinutes (below) is a minute within the shared 15-minute
// period, distinct modulo 15 so the spacing holds for every org's phase, and set equal to
// the phase of that op's DefaultSchedule so the verbatim fallback stays collision-free —
// see cronstagger.GroupEntryFor.
const bbPRPullGroup = "bitbucket-pr-pull"

// BitbucketConnector implements sdk.Connector for the Bitbucket integration.
type BitbucketConnector struct {
	paginationCfg *config.BitbucketPaginationConfig
}

// NewConnector creates a new BitbucketConnector. paginationCfg may be nil —
// the connector falls back to package-level defaults (diffstatPagelen=500,
// maxDiffstatPages=200) for both PR and commit diffstat enrichment loops.
func NewConnector(paginationCfg *config.BitbucketPaginationConfig) *BitbucketConnector {
	return &BitbucketConnector{paginationCfg: paginationCfg}
}

func (c *BitbucketConnector) ID() sdk.ConnectorID { return connectorID }
func (c *BitbucketConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryVCS}
}
func (c *BitbucketConnector) DisplayName() string { return "Bitbucket" }

func (c *BitbucketConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{
		{
			ID:          "fetch_repos",
			DisplayName: "Fetch Repositories",
			Description: "Fetches repositories from a Bitbucket workspace via REST API",
			// Earliest of the six-hourly band deliberately: it maintains the
			// settings.discovered_repos list that fetch_repo_tree (twice daily)
			// and fetch_branches iterate, so discovery precedes them. See
			// fetch_workspace_members for the band's spacing rule.
			DefaultSchedule: "7 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchReposOp{} },
		},
		{
			ID:          "fetch_prs",
			DisplayName: "Fetch Pull Requests",
			Description: "Fetches pull requests from Bitbucket repositories via REST API",
			// Base member of the shared PR-pull stagger group (offset 0). Its `*/15`
			// default is only the verbatim fallback; the group primitive emits the
			// per-org phase. fetch_prs MUST be in the group — staggering it alone lets it
			// roam onto a pinned sibling's minute for ~1-in-4 orgs.
			DefaultSchedule:      "*/15 * * * *",
			StaggerGroup:         bbPRPullGroup,
			StaggerOffsetMinutes: 0,
			IngestionType:        sdk.IngestionPull,
			Factory:              func() sdk.Operation { return &FetchPRsOp{paginationCfg: c.paginationCfg} },
		},
		{
			ID:          "fetch_commits",
			DisplayName: "Fetch Commits",
			Description: "Fetches commits from Bitbucket repositories via REST API",
			// :08 keeps the hourly commit sweep clear of the fixed six-hourly band
			// (:07/:22/:37/:52). It also shares the per-org bitbucket-rest bucket with
			// the PR-pull group, whose members roam per-org, so one may coincide with
			// :08 for some orgs — the same accepted single-op collision documented on
			// fetch_workspace_members.
			DefaultSchedule: "8 * * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchCommitsOp{paginationCfg: c.paginationCfg} },
		},
		// fetch_issues is not registered: issue tracking for Bitbucket
		// repositories usually runs through Jira. FetchIssuesOp stays in
		// operations.go so it can be registered when needed.
		{
			ID:          "fetch_reviews",
			DisplayName: "Fetch Reviews",
			Description: "Fetches PR approvals and comments from Bitbucket repositories via PR activity API",
			// PR-pull group member at offset 5: five minutes past the group's base
			// phase, so it never enqueues on the same minute as another PR-pull op on
			// the shared per-org bitbucket-rest bucket (0.5556 RPS, burst 1). Combined
			// with the per-PR /activity watermark cache gating repeat calls, baseline
			// call volume stays far below the 1000 req/hr free-tier cap. The `5-59/15`
			// default is the collision-free verbatim fallback; the group emits the
			// per-org phase.
			DefaultSchedule:      "5-59/15 * * * *",
			StaggerGroup:         bbPRPullGroup,
			StaggerOffsetMinutes: 5,
			IngestionType:        sdk.IngestionPull,
			Factory:              func() sdk.Operation { return &FetchReviewsOp{} },
		},
		{
			ID:          "fetch_pr_comments",
			DisplayName: "Fetch PR Comments",
			Description: "Fetches per-user PR comments for comment attribution metrics",
			// PR-pull group member at offset 10 — see the comment on fetch_reviews for
			// the shared-bucket spacing rule. The `10-59/15` default is the
			// collision-free verbatim fallback; the group emits the per-org phase.
			DefaultSchedule:      "10-59/15 * * * *",
			StaggerGroup:         bbPRPullGroup,
			StaggerOffsetMinutes: 10,
			IngestionType:        sdk.IngestionPull,
			Factory:              func() sdk.Operation { return &FetchPRCommentsOp{} },
		},
		{
			ID:          "fetch_workspace_members",
			DisplayName: "Fetch Workspace Members",
			Description: "Fetches all members of a Bitbucket workspace via REST API",
			// The six-hourly ops share one per-org bitbucket-rest bucket (the limiter
			// bounds a window's COUNT without spacing requests inside it, so ops landing
			// together spend the window at once), so they sit on distinct minutes: :07
			// (fetch_repos), :22 (here) and :52 (fetch_branches), with fetch_repo_tree on
			// :37 but only twice daily. The PR-pull group members roam per-org (see
			// bbPRPullGroup), so one may share a minute with one of these a few times a
			// day for some orgs — accepted, since those collisions are single-op and rare
			// next to the sustained five-op burst the group stagger removes.
			// TestFixedScheduleOpsDoNotCollide enforces the six-hourly spacing.
			DefaultSchedule:   "22 */6 * * *",
			IngestionType:     sdk.IngestionPull,
			RequiresMultiUser: true,
			Factory:           func() sdk.Operation { return &FetchWorkspaceMembersOp{} },
		},
		{
			ID:              "webhook_events",
			DisplayName:     "Webhook Events",
			Description:     "Processes inbound Bitbucket webhook payloads a host passes in",
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionWebhook,
			Factory:         func() sdk.Operation { return &WebhookEventsOp{} },
		},
		{
			ID:          opFetchPRDiffs,
			DisplayName: "Fetch PR Diffs",
			Description: "Fetches unified diffs for merged PRs that the host's work queue lists, writes each to the blob store, and reports back to the queue",
			// PR-pull group member at offset 12 — see the comment on fetch_reviews for
			// the shared-bucket spacing rule. The `12-59/15` default is the
			// collision-free verbatim fallback; the group emits the per-org phase.
			DefaultSchedule:      "12-59/15 * * * *",
			StaggerGroup:         bbPRPullGroup,
			StaggerOffsetMinutes: 12,
			IngestionType:        sdk.IngestionPull,
			RequiresHost:         fetchPRDiffsRequiresHost,
			Factory:              func() sdk.Operation { return &FetchPRDiffsOp{} },
		},
		{
			ID:          opFetchPRsData,
			DisplayName: "Fetch PR Descriptions",
			Description: "Fetches PR description via REST, writes it to the blob store, and emits a description_ref record",
			// PR-pull group member at offset 3 — see the comment on fetch_reviews for
			// the shared-bucket spacing rule. The `3-59/15` default is the
			// collision-free verbatim fallback; the group emits the per-org phase.
			DefaultSchedule:      "3-59/15 * * * *",
			StaggerGroup:         bbPRPullGroup,
			StaggerOffsetMinutes: 3,
			IngestionType:        sdk.IngestionPull,
			Factory:              func() sdk.Operation { return &FetchPRsDataOp{paginationCfg: c.paginationCfg} },
		},
		{
			ID:          opFetchRepoTree,
			DisplayName: "Fetch Repository Trees",
			Description: "Fetches the full git tree for each repository and writes it as a JSON blob to the blob store",
			// Twice daily (see fetch_workspace_members for the minute).
			// Bitbucket has no recursive tree endpoint, so this walk
			// costs one API call per directory per changed repo — by far the
			// most expensive op here. Its consumers derive monthly figures from
			// it, so a 12-hourly refresh
			// still leaves a wide freshness margin at roughly half the volume.
			DefaultSchedule: "37 3,15 * * *",
			IngestionType:   sdk.IngestionPull,
			// The walk paces itself against the per-org per-minute Bitbucket
			// budget, so its runtime is set by that budget rather than by how
			// fast it can work. The scheduler default is sized for ops issuing a
			// handful of calls and would cancel this one mid-repo.
			ExecutionTimeout: 30 * time.Minute,
			Factory:          func() sdk.Operation { return &FetchRepoTreeOp{} },
		},
		{
			ID:          opFetchBranches,
			DisplayName: "Fetch Branches",
			Description: "Lists every branch of each workspace repo and emits one record per branch",
			// Last of the six-hourly ops — see fetch_workspace_members.
			DefaultSchedule: "52 */6 * * *",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchBranchesOp{} },
		},
		{
			ID:          opFetchFileContent,
			DisplayName: "Fetch File Content",
			Description: "Pulls the AI coding-agent configuration files (skills, rules, hooks, CLAUDE.md, AGENTS.md and the like) that the host's work queue lists, at their pinned commit, writes each body to the blob store, and emits one record per configuration item",
			// Daily, 05:02 — AFTER that morning's fetch_repo_tree tick
			// (03:37), not before it. Content pulls read the tree the tree op
			// persisted, so running first would work off the previous evening's
			// tree and silently omit anything committed overnight until the next
			// run. The gap also leaves the paced tree walk room to finish. A daily
			// tick on a converged org is near-free (whole-repo tree_sha skip) while
			// cutting a large org's cold-start backfill.
			// Runtime-overridable per org via connector_schedules / OperationOverrides.
			DefaultSchedule: "2 5 * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    fetchFileContentRequiresHost,
			Factory:         func() sdk.Operation { return &FetchFileContentOp{} },
		},
		{
			ID:          opFetchConfig,
			DisplayName: "Fetch Repository Config",
			Description: "Captures per-repo governance config (default branch + allowed/default merge strategies) as the per-repo config record. Latest-only.",
			// Intentionally unscheduled (default-OFF): opt-in via manual trigger / later schedule flip.
			DefaultSchedule: "",
			IngestionType:   sdk.IngestionPull,
			Factory:         func() sdk.Operation { return &FetchConfigOp{} },
		},
		{
			ID:          opDrainDefectLineage,
			DisplayName: "Drain Defect Lineage",
			Description: "Defect lineage: claims bug fixes from the host, finds the commits that introduced each bug (SZZ-style) from file history and per-commit diffs, as Bitbucket has no blame endpoint, and submits those candidates to the host",
			// Every 30 minutes at :23/:53 — a phase clear of the PR-pull /15 family and the
			// minute-offset hourly ops, per the collision-free staggering rule above.
			DefaultSchedule: "23,53 * * * *",
			IngestionType:   sdk.IngestionPull,
			// The reconstruction is bounded by Bitbucket's rate budget, not its own speed, so it
			// paces itself and needs far longer than the scheduler default — too short a deadline
			// cancels it mid-run. The per-fix timeout and the tick-wide pacing budget bound the work.
			ExecutionTimeout: drainExecutionTimeout,
			RequiresHost:     drainDefectLineageRequiresHost,
			Factory:          func() sdk.Operation { return &DrainDefectLineageOp{} },
		},
		{
			ID:              opDrainDefectScoring,
			DisplayName:     "Drain Defect Scoring",
			Description:     "Defect lineage: claims candidate bug-introducing commits from the host, scores how close each one's change sits to the fix in the syntax tree, and submits the score to the host",
			DefaultSchedule: "26,56 * * * *",
			IngestionType:   sdk.IngestionPull,
			RequiresHost:    drainDefectScoringRequiresHost,
			Factory:         func() sdk.Operation { return &DrainDefectScoringOp{} },
		},
	}
}

func (c *BitbucketConnector) ValidateConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	creds, settings, err := parseConfig(cfg)
	if err != nil {
		return fmt.Errorf("bitbucket: %w", err)
	}

	if err := validateCredentials(cfg.Credentials, creds); err != nil {
		return fmt.Errorf("bitbucket: %w", err)
	}

	if err := validateSettings(settings); err != nil {
		return fmt.Errorf("bitbucket: %w", err)
	}

	return nil
}

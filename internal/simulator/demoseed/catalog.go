// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

// Workflow flavor constants: the branching workflow a seeded repository follows.
const (
	FlavorMonorepo     = "monorepo"
	FlavorTrunkBased   = "trunk_based"
	FlavorGitHubFlow   = "github_flow"
	FlavorGitFlow      = "git_flow"
	FlavorReleaseTrain = "release_train"
	FlavorForkAndPull  = "fork_and_pull"
	FlavorGitLabFlow   = "gitlab_flow"
	FlavorDirectPush   = "direct_push"
)

// Vendor names, used both in the catalog below and in seed.go's per-vendor
// dispatch — a shared constant keeps the two in sync.
const (
	vendorGitHub    = "github"
	vendorGitLab    = "gitlab"
	vendorBitbucket = "bitbucket"
)

// PM tool names.
const (
	pmToolJira         = "jira"
	pmToolGitHubIssues = "github-issues"
	pmToolGitLabIssues = "gitlab-issues"
)

// Entity type discriminators, shared between OrgCatalog.Entities (below) and
// seedOrg's per-type dispatch switch (seed.go) — a shared constant keeps the
// two in sync instead of relying on matching string literals.
const (
	entityTypeMember         = "member"
	entityTypeRepo           = "repo"
	entityTypeProject        = "project"
	entityTypeCopilotMetrics = "copilot_metrics"
	entityTypeCopilotUser    = "copilot_user"
	// entityTypeCommit is the repo-level commit stream (fetch_commits) — a
	// first-class entity because it includes direct pushes with no PR, which
	// GitHub's PR-nested commits cannot represent. Bitbucket and GitLab both
	// seed this type.
	entityTypeCommit = "commit"

	// entityTypeIssue is the sim_records entity_type of a Jira issue row (with
	// vendor='jira'); the simulator jira DAO/handlers query it as "issue".
	// Jira project rows reuse entityTypeProject ("project") — distinct from
	// GitHub's projectsV2 rows by the vendor column, per the (vendor, entity_type)
	// discriminator.
	entityTypeIssue = "issue"

	// entityTypeSprint is the sim_records entity_type of a Jira sprint row
	// (vendor='jira'); the per-board sprint handler queries it as "sprint".
	entityTypeSprint = "sprint"

	// entityTypeJiraProject / entityTypeJiraIssue / entityTypeJiraSprint are seedOrg
	// DISPATCH keys — the EntitySpec.Type appended for a jira-PM org so the switch
	// routes to the Jira seeders. They are NOT the row entity_type: the rows carry
	// "project"/"issue"/"sprint" (above), which is what the handlers read.
	entityTypeJiraProject = "jira_project"
	entityTypeJiraIssue   = "jira_issue"
	entityTypeJiraSprint  = "jira_sprint"
)

const (
	branchMain    = "main"
	prefixFeature = "feature/"
)

// FlavorProfile defines per-repo PR generation characteristics.
// Values give each workflow flavor a plausible shape for seeding.
type FlavorProfile struct {
	DefaultBranch string
	FeaturePrefix string // branch name prefix for feature branches
	MergeRatio    float64
	PRsPerWeek    int // new PRs added per weekly run
	Reviewers     int
	BaseAdds      int // baseline additions per PR
	BaseDels      int // baseline deletions per PR
}

// Profiles maps each flavor to its generation characteristics.
var Profiles = map[string]FlavorProfile{
	FlavorMonorepo:     {DefaultBranch: branchMain, FeaturePrefix: "platform/", MergeRatio: 0.85, PRsPerWeek: 50, Reviewers: 3, BaseAdds: 300, BaseDels: 80},
	FlavorTrunkBased:   {DefaultBranch: branchMain, FeaturePrefix: "feat/", MergeRatio: 0.92, PRsPerWeek: 40, Reviewers: 1, BaseAdds: 80, BaseDels: 20},
	FlavorGitHubFlow:   {DefaultBranch: branchMain, FeaturePrefix: prefixFeature, MergeRatio: 0.80, PRsPerWeek: 25, Reviewers: 2, BaseAdds: 120, BaseDels: 35},
	FlavorGitFlow:      {DefaultBranch: "develop", FeaturePrefix: prefixFeature, MergeRatio: 0.70, PRsPerWeek: 15, Reviewers: 2, BaseAdds: 200, BaseDels: 60},
	FlavorReleaseTrain: {DefaultBranch: branchMain, FeaturePrefix: "release/", MergeRatio: 0.75, PRsPerWeek: 10, Reviewers: 3, BaseAdds: 150, BaseDels: 45},
	FlavorForkAndPull:  {DefaultBranch: branchMain, FeaturePrefix: prefixFeature, MergeRatio: 0.70, PRsPerWeek: 20, Reviewers: 2, BaseAdds: 90, BaseDels: 25},
	FlavorGitLabFlow:   {DefaultBranch: branchMain, FeaturePrefix: prefixFeature, MergeRatio: 0.78, PRsPerWeek: 20, Reviewers: 2, BaseAdds: 110, BaseDels: 30},
	FlavorDirectPush:   {DefaultBranch: branchMain, FeaturePrefix: "", MergeRatio: 0.0, PRsPerWeek: 0, Reviewers: 0, BaseAdds: 60, BaseDels: 15},
}

// RepoSpec defines one repository within an org and its workflow flavor.
type RepoSpec struct {
	Name   string
	Flavor string
}

// EntitySpec defines seeding behavior for one entity type within an org.
// For PR entities PerRunCount is derived from each repo's FlavorProfile.PRsPerWeek;
// for other timeseries entities (copilot) it is the number of rows per run.
// Static entities (members, repos, projects) ignore PerRunCount — the catalog
// fields (MemberNames, Repos, ProjectTitles) determine their count.
type EntitySpec struct {
	Type         string
	PerRunCount  int
	IsTimeSeries bool
}

// OrgCatalog defines a demo org and the shape of data to generate for it.
// OrgName is the simulated vendor org name used as the key in sim_records;
// it must match connector_configs.settings.organization for the corresponding
// host organization.
type OrgCatalog struct {
	OrgName       string
	Vendor        string // "github", "gitlab", "bitbucket"
	PMTool        string // "github-issues", "gitlab-issues", "jira", or "" for none
	Scenario      string
	Repos         []RepoSpec
	MemberNames   []string
	ProjectTitles []string
	Entities      []EntitySpec
}

// demoMemberRoster is the fixed, ordered pool of synthetic member logins.
// Every org in DefaultCatalog uses a prefix of this roster, sized to that
// org's headcount — a single source of truth instead of re-listing names.
var demoMemberRoster = []string{"alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"}

// DefaultCatalog is the 9-org matrix (3 vendors × 3 archetypes).
var DefaultCatalog = []OrgCatalog{
	// ── GitHub orgs ──────────────────────────────────────────────────────────
	{
		OrgName:  "demo-gh-enterprise",
		Vendor:   vendorGitHub,
		PMTool:   pmToolJira,
		Scenario: "Large multi-repo enterprise org — Jira-linked PRs, high review activity",
		Repos: []RepoSpec{
			{Name: "platform-monorepo", Flavor: FlavorMonorepo},
			{Name: "payments", Flavor: FlavorGitFlow},
			{Name: "infra", Flavor: FlavorReleaseTrain},
			{Name: "docs", Flavor: FlavorGitHubFlow},
		},
		MemberNames:   demoMemberRoster[:8],
		ProjectTitles: []string{"Q3 Roadmap", "Bug Bash", "Tech Debt Reduction"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeProject, IsTimeSeries: false},
			{Type: entityTypeCopilotMetrics, PerRunCount: 7, IsTimeSeries: true},
			{Type: entityTypeCopilotUser, IsTimeSeries: true},
		},
	},
	{
		OrgName:  "demo-gh-startup",
		Vendor:   vendorGitHub,
		PMTool:   pmToolGitHubIssues,
		Scenario: "Fast-moving startup — trunk-based development, high merge rate",
		Repos: []RepoSpec{
			{Name: "web-app", Flavor: FlavorTrunkBased},
			{Name: "api-gateway", Flavor: FlavorGitHubFlow},
			{Name: "mobile", Flavor: FlavorReleaseTrain},
		},
		MemberNames:   demoMemberRoster[:5],
		ProjectTitles: []string{"Sprint 1", "Sprint 2"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeProject, IsTimeSeries: false},
			{Type: entityTypeCopilotMetrics, PerRunCount: 7, IsTimeSeries: true},
			{Type: entityTypeCopilotUser, IsTimeSeries: true},
		},
	},
	{
		OrgName:  "demo-gh-oss",
		Vendor:   vendorGitHub,
		PMTool:   pmToolGitHubIssues,
		Scenario: "Open-source project — fork-and-pull, external contributors",
		Repos: []RepoSpec{
			{Name: "core-lib", Flavor: FlavorForkAndPull},
			{Name: "cli", Flavor: FlavorForkAndPull},
			{Name: "website", Flavor: FlavorGitHubFlow},
		},
		MemberNames:   demoMemberRoster[:6],
		ProjectTitles: []string{"v2.0 Roadmap"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeProject, IsTimeSeries: false},
			{Type: entityTypeCopilotMetrics, PerRunCount: 7, IsTimeSeries: true},
			{Type: entityTypeCopilotUser, IsTimeSeries: true},
		},
	},

	// ── GitLab orgs ──────────────────────────────────────────────────────────
	{
		OrgName:  "demo-gl-regulated",
		Vendor:   vendorGitLab,
		PMTool:   pmToolGitLabIssues,
		Scenario: "Regulated industry — strict review gates, compliance-heavy repos",
		Repos: []RepoSpec{
			{Name: "ledger", Flavor: FlavorGitLabFlow},
			{Name: "compliance", Flavor: FlavorGitFlow},
			{Name: "reporting", Flavor: FlavorReleaseTrain},
		},
		MemberNames:   demoMemberRoster[:7],
		ProjectTitles: []string{"Compliance Sprint", "Audit Prep"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeIssue, IsTimeSeries: true},
		},
	},
	{
		OrgName:  "demo-gl-scaleup",
		Vendor:   vendorGitLab,
		PMTool:   pmToolGitLabIssues,
		Scenario: "Scale-up — mixed workflow, growing team",
		Repos: []RepoSpec{
			{Name: "checkout", Flavor: FlavorTrunkBased},
			{Name: "catalog", Flavor: FlavorGitLabFlow},
			{Name: "search", Flavor: FlavorReleaseTrain},
		},
		MemberNames:   demoMemberRoster[:6],
		ProjectTitles: []string{"Q4 Features", "Infra Hardening"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeIssue, IsTimeSeries: true},
		},
	},
	{
		OrgName:  "demo-gl-solo",
		Vendor:   vendorGitLab,
		PMTool:   pmToolGitLabIssues,
		Scenario: "Solo developer — direct push, minimal review",
		Repos: []RepoSpec{
			{Name: "side-project", Flavor: FlavorDirectPush},
			{Name: "scripts", Flavor: FlavorDirectPush},
		},
		MemberNames:   demoMemberRoster[:2],
		ProjectTitles: []string{},
		Entities: []EntitySpec{
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
			{Type: entityTypeIssue, IsTimeSeries: true},
		},
	},

	// ── Bitbucket orgs ───────────────────────────────────────────────────────
	{
		OrgName:  "demo-bb-enterprise",
		Vendor:   vendorBitbucket,
		PMTool:   pmToolJira,
		Scenario: "Enterprise — gitflow, heavy branching strategy",
		Repos: []RepoSpec{
			{Name: "ledger-core", Flavor: FlavorMonorepo},
			{Name: "platform", Flavor: FlavorGitFlow},
			{Name: "deploy", Flavor: FlavorReleaseTrain},
			{Name: "sdk", Flavor: FlavorGitHubFlow},
		},
		MemberNames:   demoMemberRoster[:8],
		ProjectTitles: []string{"Ledger Core", "Platform", "SDK"},
		Entities: []EntitySpec{
			// pr before commit: seedBitbucketCommits reads the seeded pr rows to
			// derive PR-correlated commits, so pr must be persisted first.
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
		},
	},
	{
		OrgName:  "demo-bb-startup",
		Vendor:   vendorBitbucket,
		PMTool:   pmToolJira,
		Scenario: "Startup on Bitbucket — trunk-based, small team",
		Repos: []RepoSpec{
			{Name: "dashboard", Flavor: FlavorTrunkBased},
			{Name: "worker", Flavor: FlavorGitHubFlow},
		},
		MemberNames:   demoMemberRoster[:5],
		ProjectTitles: []string{"MVP"},
		Entities: []EntitySpec{
			{Type: "pr", IsTimeSeries: true},
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
		},
	},
	{
		OrgName:  "demo-bb-solo",
		Vendor:   vendorBitbucket,
		PMTool:   "",
		Scenario: "Solo developer on Bitbucket — direct push",
		Repos: []RepoSpec{
			{Name: "prototype", Flavor: FlavorDirectPush},
			{Name: "tools", Flavor: FlavorDirectPush},
		},
		MemberNames:   demoMemberRoster[:2],
		ProjectTitles: []string{},
		Entities: []EntitySpec{
			// No pr entity: demo-bb-solo is the direct-push archetype, so its
			// activity is the commit stream alone.
			{Type: entityTypeCommit, IsTimeSeries: true},
			{Type: entityTypeMember, IsTimeSeries: false},
			{Type: entityTypeRepo, IsTimeSeries: false},
		},
	},
}

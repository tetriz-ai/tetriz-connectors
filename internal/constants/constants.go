// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package constants

// API group names used for rate limiting and base URL mapping.
const (
	// GitHub API groups.
	GroupGitHubGraphQL    = "github-graphql"
	GroupGitHubREST       = "github-rest"
	GroupGitHubRESTSearch = "github-rest-search"

	// Bitbucket API groups.
	GroupBitbucketREST = "bitbucket-rest"

	// Jira API groups.
	GroupJiraREST = "jira-rest"

	// Slack API groups.
	GroupSlackWeb = "slack-web"

	// GitLab API groups.
	GroupGitLabREST = "gitlab-rest"

	// Linear API groups.
	GroupLinearGraphQL = "linear-graphql"

	// Cursor API groups. Two groups over one host, because Cursor publishes a
	// different budget for the billed-events endpoint than for the rest of its
	// Admin API — 60/min against 20/min — and a group is the rate-limit pool.
	GroupCursorAdmin = "cursor-admin"
	GroupCursorUsage = "cursor-usage"

	// GroupAnthropicAnalytics is the single rate-limit pool for the Claude
	// Enterprise Analytics endpoints and the member roster. This is the one home for
	// why they share a pool; elsewhere points here.
	//
	// Anthropic states 60 requests per minute for the analytics family at
	// ORGANISATION level — not per key and not per endpoint. A group is the pool, so
	// four groups would each be granted that whole budget and collectively spend four
	// times it.
	//
	// The member roster is pooled with them on a weaker basis: it sits outside the
	// analytics family and its own cap is undocumented, so sharing the analytics
	// budget is the conservative reading rather than a vendor-stated one. If the
	// roster is later found to have its own budget, splitting it out only loosens
	// what we enforce.
	GroupAnthropicAnalytics = "anthropic-analytics"

	// GroupCodexAnalytics is the single rate-limit pool for the Codex Enterprise
	// Analytics endpoints. One group for the endpoint family, on the same reasoning
	// as the Anthropic pool above: a group is the pool, so a group per endpoint would
	// grant each one the whole budget.
	//
	// The rate behind this group is NOT a vendor figure. OpenAI publishes no limit for
	// this family at all, so the configured 30 requests per minute is borrowed from the
	// default a shipping third-party connector chose for the same endpoints and the
	// same reason. It is a conservative default standing in for a number nobody has
	// published, which is why raising it needs a measurement rather than an argument.
	GroupCodexAnalytics = "codex-analytics"
)

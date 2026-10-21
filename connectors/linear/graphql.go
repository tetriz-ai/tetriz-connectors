// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"fmt"
	"net/http"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const linearGraphQLEndpoint = "https://api.linear.app/graphql"

// Workspace-scoped query page sizes. Linear enforces a hard max of 250 per page;
// 100 keeps payloads manageable and within rate-limit budget.
const linearPageSize = 100

// linearIssuesPageSize is smaller than linearPageSize because the issues query
// selects deeply-nested objects (state, team, assignee, creator, project, cycle,
// parent, labels, attachments). Linear's per-query complexity cap (~10k points)
// scales with page size × nested selections (Linear estimates it structurally
// from the requested first: values, before execution), so 50 keeps a worst-case
// issue page within budget — verified against the live API with the attachments
// connection included.
const linearIssuesPageSize = 50

// queryFetchTeams fetches one page of Linear teams.
const queryFetchTeams = `
query FetchTeams($first: Int!, $after: String) {
  teams(first: $first, after: $after) {
    nodes {
      id
      name
      key
      description
      timezone
      createdAt
      updatedAt
      archivedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryFetchUsers fetches one page of Linear workspace members.
const queryFetchUsers = `
query FetchUsers($first: Int!, $after: String) {
  users(first: $first, after: $after, includeDisabled: true) {
    nodes {
      id
      name
      displayName
      email
      active
      admin
      createdAt
      updatedAt
      archivedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryFetchProjects fetches one page of Linear projects (cross-team initiatives).
const queryFetchProjects = `
query FetchProjects($first: Int!, $after: String) {
  projects(first: $first, after: $after) {
    nodes {
      id
      name
      description
      state
      url
      progress
      targetDate
      createdAt
      updatedAt
      archivedAt
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryFetchLabels fetches one page of Linear issue labels (workspace and team scoped).
const queryFetchLabels = `
query FetchLabels($first: Int!, $after: String) {
  issueLabels(first: $first, after: $after) {
    nodes {
      id
      name
      color
      description
      createdAt
      updatedAt
      archivedAt
      team { id key }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryDiscoverTeams is a lightweight query used by the per-team operations
// (fetch_issues, fetch_sprints) to enumerate current team IDs before paging.
// It selects only id to stay cheap. includeArchived: true is required — Linear's
// teams connection excludes archived teams by default, so without it a disbanded
// team's historical issues and cycles would never be discovered, hence never
// backfilled (the opposite of the intended "include archived teams" behavior).
const queryDiscoverTeams = `
query DiscoverTeams($first: Int!, $after: String) {
  teams(first: $first, after: $after, includeArchived: true) {
    nodes { id }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryFetchCycles fetches one page of cycles for a specific team.
// includeArchived returns archived cycles so completed/closed iterations
// remain visible for historical sprint analytics.
const queryFetchCycles = `
query FetchCycles($teamId: ID!, $first: Int!, $after: String) {
  cycles(filter: { team: { id: { eq: $teamId } } }, first: $first, after: $after, includeArchived: true) {
    nodes {
      id
      number
      name
      startsAt
      endsAt
      completedAt
      archivedAt
      updatedAt
      progress
      team { id key }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

// queryFetchIssues fetches one page of issues for a specific team, ordered by
// updatedAt and filtered incrementally by updatedAt > $since. includeArchived
// returns archived (soft-deleted) issues so the parser can set deleted_at.
//
// The attachments sub-connection is capped at first: 10 VCS-sourced links
// (github + gitlab) and is NOT sub-paginated (matching the GitHub issues
// connector's own closedByPullRequestsReferences cap) — a PM issue linking more
// than 10 PRs/MRs is rare, and the cap keeps per-issue query complexity within
// Linear's budget. pageInfo.hasNextPage is selected so the consumer can log when
// an issue does exceed the cap, making the (rare) dropped tail observable rather
// than silent.
const queryFetchIssues = `
query FetchIssues($teamId: ID!, $first: Int!, $after: String, $since: DateTimeOrDuration) {
  issues(filter: { team: { id: { eq: $teamId } }, updatedAt: { gt: $since } }, first: $first, after: $after, orderBy: updatedAt, includeArchived: true) {
    nodes {
      id
      identifier
      title
      description
      priority
      estimate
      dueDate
      createdAt
      updatedAt
      completedAt
      canceledAt
      archivedAt
      state { id name type }
      team { id key }
      assignee { id name email }
      creator { id name email }
      project { id name }
      cycle { id number name }
      parent { id identifier team { id key } }
      labels { nodes { id name } }
      attachments(first: 10, filter: { sourceType: { in: ["github", "gitlab"] } }) { nodes { url sourceType } pageInfo { hasNextPage } }
    }
    pageInfo { hasNextPage endCursor }
  }
}`

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// execGraphQL posts a GraphQL query to the Linear API via the supplied
// VendorGateway and unmarshals the response into dest.
// The caller is responsible for rate-group scoping (ForGroup) before calling.
func execGraphQL(ctx context.Context, gw sdk.VendorGateway, query string, vars map[string]any, dest any) error {
	req := graphqlRequest{Query: query, Variables: vars}
	if err := gw.DoJSON(ctx, http.MethodPost, linearGraphQLEndpoint, req, dest); err != nil {
		return fmt.Errorf("linear graphql: %w", err)
	}
	return nil
}

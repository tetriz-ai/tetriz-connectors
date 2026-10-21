// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"fmt"
	"strings"
	"time"
)

// Raw GraphQL query builders for GitHub API v4.
// These produce query strings used with VendorGateway.Do() against the GraphQL endpoint.
// We use raw strings instead of the githubv4 library so all HTTP goes through VendorGateway.

// graphQLNullCursor is the literal used for an empty GraphQL pagination cursor (first page).
const graphQLNullCursor = "null"

// buildPRsQuery builds a GraphQL query to fetch pull requests updated since a given time.
// The cursor parameter is the GraphQL pagination cursor (empty string for the first page).
// pageSize controls the number of PR nodes per page. states, when non-empty, narrows the
// query to the named GraphQL PullRequestState values (OPEN, CLOSED, MERGED); empty returns
// all states (GraphQL default).
func buildPRsQuery(owner, repo string, since time.Time, cursor string, pageSize int, states []string, individualScope bool) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	// PullRequest.commits is push-ordered and does not accept orderBy. Fetch a
	// bounded page and let the consumer's parser take MIN(authoredDate,
	// committedDate); first:1 would often pick a rebased branch head.
	statesClause := ""
	if len(states) > 0 {
		statesClause = fmt.Sprintf(", states: [%s]", strings.Join(states, ", "))
	}

	// requestedReviewer is a User|Team union. Resolving a Team node requires the
	// Organization:Members permission, which a repo-scoped credential lacks; when
	// a PR has a team reviewer GitHub FORBIDs that node and zeroes the whole page.
	// An individual tenant-of-one has no org teams to surface, and metrics consume
	// only reviewRequests.totalCount — so the individual variant keeps the count
	// and drops the org-scoped reviewer identity. Org tenants keep the full set.
	reviewRequestsBlock := `reviewRequests(first: 10) {
          totalCount
          nodes {
            requestedReviewer {
              ... on User { login databaseId }
              ... on Team { slug }
            }
          }
        }`
	if individualScope {
		reviewRequestsBlock = `reviewRequests(first: 10) { totalCount }`
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    pullRequests(first: %d, after: %s%s, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        title
        url
        state
        createdAt
        updatedAt
        mergedAt
        closedAt
        additions
        deletions
        changedFiles
        mergeable
        author { login ... on User { databaseId } }
        mergedBy { login }
        baseRefName
        headRefName
        comments { totalCount }
        reviews(first: 100) { totalCount nodes { author { login ... on User { databaseId } } state submittedAt } }
        reviewDecision
        %s
        body
        isDraft
        # merge_strategy derivation. parents.totalCount
        # disambiguates squash (1) from merge-commit (2); message feeds the squash-by-title heuristic.
        mergeCommit { message parents { totalCount } }
        # PR <-> Issue linkage. nameWithOwner is required so the
        # extractor builds canonical cross-repo issue_keys ("acme/payments#ISSUE-42").
        # first: 10 matches the issue-side query.
        closingIssuesReferences(first: 10) { nodes { number title state repository { nameWithOwner } } }
        isCrossRepository
        headRepositoryOwner { login }
        files(first: 100) { nodes { path additions deletions } pageInfo { hasNextPage } }
        commits(first: 100) {
          nodes {
            commit {
              authoredDate
              committedDate
            }
          }
          pageInfo { hasNextPage }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, owner, repo, pageSize, afterClause, statesClause, reviewRequestsBlock)
}

// buildCommitsQuery builds a GraphQL query to fetch commits from the default branch since a given time.
func buildCommitsQuery(owner, repo string, since time.Time, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: %d, after: %s, since: %q) {
            nodes {
              oid
              message
              authoredDate
              committedDate
              additions
              deletions
              changedFilesIfAvailable
              author {
                name
                email
                user { login ... on User { databaseId } }
              }
              parents { totalCount }
              associatedPullRequests(first: 5) { nodes { number } }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
  }
}`, owner, repo, pageSize, afterClause, since.Format(time.RFC3339))
}

// buildCommitsQueryForBranch builds a GraphQL query to fetch commits from a specific branch.
// Used when the admin configures a tracked_branch override.
func buildCommitsQueryForBranch(owner, repo, branch string, since time.Time, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    ref(qualifiedName: %q) {
      target {
        ... on Commit {
          history(first: %d, after: %s, since: %q) {
            nodes {
              oid
              message
              authoredDate
              committedDate
              additions
              deletions
              changedFilesIfAvailable
              author {
                name
                email
                user { login ... on User { databaseId } }
              }
              parents { totalCount }
              associatedPullRequests(first: 5) { nodes { number } }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
      }
    }
  }
}`, owner, repo, "refs/heads/"+branch, pageSize, afterClause, since.Format(time.RFC3339))
}

// buildPRNumbersQuery builds a lightweight GraphQL query that fetches PR numbers,
// updatedAt, author info, and the repository database ID. Used by fetch_reviews
// to collect the metadata needed by a consumer (self-PR detection,
// platform_repo_id) without the overhead of full PR payloads.
func buildPRNumbersQuery(owner, repo string, since time.Time, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    pullRequests(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        createdAt
        updatedAt
        author { login ... on User { databaseId } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, owner, repo, pageSize, afterClause)
}

// buildPRNumbersWithAuthorQuery builds a lightweight GraphQL query that fetches PR numbers,
// updatedAt, author info, and the repository database ID. Used by fetch_pr_comments to
// collect the metadata needed by a consumer (self-PR detection, platform_repo_id)
// without the overhead of full PR payloads.
func buildPRNumbersWithAuthorQuery(owner, repo string, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    pullRequests(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        createdAt
        updatedAt
        author { login ... on User { databaseId } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, owner, repo, pageSize, afterClause)
}

// buildIssuesQuery builds a GraphQL query to fetch issues updated since a
// given time. commentsPerIssue is the number of inline comment nodes fetched
// per issue; issues with more comments are paginated by fetchRemainingComments.
//
// Targets GitHub.com only. The selection set includes subIssues.totalCount
// which requires GitHub Enterprise Server v3.13+ — every other onboarded
// host is implicitly api.github.com (the connector hardcodes that base URL
// in the gateway). If GHES is ever
// brought into scope, gate subIssues behind a host-version check or accept
// the failure on older instances.
//
// Any field added to this selection set must also be declared on
// ghGQLIssueNode, or fetchIssuesForRepo's re-marshal silently drops it from the
// emitted record payload before a downstream consumer can read it.
func buildIssuesQuery(owner, repo string, since time.Time, cursor string, pageSize, commentsPerIssue int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    issues(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}, filterBy: {since: %q}) {
      nodes {
        number
        title
        url
        state
        stateReason
        createdAt
        updatedAt
        closedAt
        body
        repository { nameWithOwner }
        author { login ... on User { databaseId } }
        assignees(first: 5) { nodes { login ... on User { databaseId } } }
        labels(first: 10) { nodes { name } }
        milestone { number title state dueOn }
        # Native issue type (GitHub Issues 2024 feature). Optional on issues
        # — null when the team hasn't enabled native types. parseGitHub
        # prefers this over the projectItems "TaskType" field when both
        # are present.
        issueType { name }
        # Parent sub-issue reference. Used by ensureParentID to populate
        # a consumer's parent-issue link. Reads number + repo so the parent's
        # natural key reconstructs as "<owner>/<repo>#<n>".
        parent { number repository { nameWithOwner } }
        subIssues { totalCount }
        participants { totalCount }
        # PR <-> Issue linkage, issue side. The vendor_native
        # source — PRs that GitHub auto-detected close this issue (via closing
        # keyword in the PR body) plus manually-linked PRs. orderByState: true
        # surfaces open PRs first inside the 10-result window. Each entry is
        # tagged NATIVE_LINK_SOURCE_GITHUB; the GraphQL schema exposes no
        # per-edge provenance (keyword vs manual), so that axis stays unset.
        closedByPullRequestsReferences(
          first: 10,
          includeClosedPrs: true,
          orderByState: true
        ) {
          nodes {
            number
            repository { nameWithOwner }
            state
          }
        }
        comments(first: %d) {
          totalCount
          nodes {
            id
            body
            author { login ... on User { databaseId } }
            createdAt
            updatedAt
          }
          pageInfo { hasNextPage endCursor }
        }
        projectItems(first: 10) {
          nodes {
            project { title number }
            fieldValues(first: 50) {
              nodes {
                ... on ProjectV2ItemFieldSingleSelectValue {
                  name
                  field { ... on ProjectV2SingleSelectField { name } }
                }
                ... on ProjectV2ItemFieldTextValue {
                  text
                  field { ... on ProjectV2Field { name } }
                }
                ... on ProjectV2ItemFieldNumberValue {
                  number
                  field { ... on ProjectV2Field { name } }
                }
                ... on ProjectV2ItemFieldIterationValue {
                  title
                  startDate
                  duration
                  field { ... on ProjectV2IterationField { name } }
                }
              }
            }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, owner, repo, pageSize, afterClause, since.Format(time.RFC3339), commentsPerIssue)
}

// buildIssueCommentsTailQuery builds a GraphQL query to fetch additional
// comment pages for a specific issue when the initial fetch_issues response
// indicated comments.pageInfo.hasNextPage. The cursor is the endCursor from
// the previous page.
func buildIssueCommentsTailQuery(owner, repo string, issueNumber int, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}

	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    issue(number: %d) {
      comments(first: %d, after: %s) {
        nodes {
          id
          body
          author { login ... on User { databaseId } }
          createdAt
          updatedAt
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`, owner, repo, issueNumber, pageSize, afterClause)
}

// buildRepoMergeMethodsQuery returns a GraphQL query that fetches the three
// merge-method permission toggles for a single repository.
func buildRepoMergeMethodsQuery(owner, repo string) string {
	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    squashMergeAllowed
    mergeCommitAllowed
    rebaseMergeAllowed
  }
}`, owner, repo)
}

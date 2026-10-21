// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildPRsQuery_CommitsNodeShape pins the supplemental
// commits node:
//
//   - GitHub's PullRequest.commits does NOT accept orderBy (only first/last/before/
//     after). Earlier drafts included orderBy → every fetch_prs run failed with
//     "Field 'commits' doesn't accept argument 'orderBy'" and emitted zero records.
//   - PR.commits returns nodes in PUSH order, not commit-date order. With first:1
//     we end up with the rebase HEAD on rebased PRs, producing first_commit_at
//     dates AFTER opened_at.
//     We use first:100 and let the parser take MIN(authoredDate, committedDate)
//     over the returned set.
func TestBuildPRsQuery_CommitsNodeShape(t *testing.T) {
	query := buildPRsQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50, []string{"OPEN", "MERGED", "CLOSED"}, false)

	assert.Contains(t, query, "commits(first: 100)",
		"PR query must select first:100 commits so the parser can take MIN(authoredDate, committedDate) — first:1 returns the push-order head, not the chronological earliest")
	assert.NotContains(t, query, "orderBy: {field: COMMITTED_DATE",
		"PullRequest.commits does not accept orderBy — GitHub rejects with 'Field commits doesn't accept argument orderBy'")
}

// TestQueriesSelectAuthoredDate ensures every commit-fetching query asks for
// authoredDate alongside committedDate. Rebases REWRITE committedDate to the
// rebase time but PRESERVE authoredDate; for the "when did this work begin"
// anchor (Cycle Time) we want the earlier of the two. A rebase before merge
// can move committedDate weeks past opened_at.
func TestQueriesSelectAuthoredDate(t *testing.T) {
	prQuery := buildPRsQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50, []string{"OPEN", "MERGED", "CLOSED"}, false)
	commitsQuery := buildCommitsQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50)
	branchQuery := buildCommitsQueryForBranch("acme", "widgets", "feature/x", time.Now().Add(-90*24*time.Hour), "", 50)

	for _, tc := range []struct{ name, q string }{
		{"buildPRsQuery", prQuery},
		{"buildCommitsQuery", commitsQuery},
		{"buildCommitsQueryForBranch", branchQuery},
	} {
		assert.Contains(t, tc.q, "authoredDate", "%s must select authoredDate (rebase-stable)", tc.name)
		assert.Contains(t, tc.q, "committedDate", "%s must keep committedDate (used for MIN with authoredDate)", tc.name)
	}
}

// TestPRNumbersQueriesSelectCreatedAt ensures the two lightweight PR-list queries
// backing fetch_reviews / fetch_pr_comments select createdAt. The consumer's gates
// cross-day reviews/comments on the parent PR's opened_at, carried as the
// pr_created_at metadata sourced from this field.
func TestPRNumbersQueriesSelectCreatedAt(t *testing.T) {
	for _, tc := range []struct{ name, q string }{
		{"buildPRNumbersQuery", buildPRNumbersQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50)},
		{"buildPRNumbersWithAuthorQuery", buildPRNumbersWithAuthorQuery("acme", "widgets", "", 50)},
	} {
		assert.Contains(t, tc.q, "createdAt", "%s must select createdAt for the record's date anchor", tc.name)
	}
}

// TestBuildPRsQuery_AssociatedPullRequestsNotInPRQuery pins that
// associatedPullRequests is a field on Commit, not PullRequest. It belongs
// only on buildCommitsQuery's commit node.
func TestBuildPRsQuery_AssociatedPullRequestsNotInPRQuery(t *testing.T) {
	query := buildPRsQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50, []string{"OPEN", "MERGED", "CLOSED"}, false)
	assert.NotContains(t, query, "associatedPullRequests",
		"associatedPullRequests is a Commit field; should not appear in the PR query")
}

// TestBuildCommitsQuery_AssociatedPullRequests pins the
// commit→PR linkage node.
func TestBuildCommitsQuery_AssociatedPullRequests(t *testing.T) {
	query := buildCommitsQuery("acme", "widgets", time.Now().Add(-90*24*time.Hour), "", 50)
	assert.Contains(t, query, "associatedPullRequests(first:",
		"commit node must select associatedPullRequests for first_commit_at attribution")
	// Also confirm it's not buggy with surplus arguments
	occurrences := strings.Count(query, "associatedPullRequests")
	assert.Equal(t, 1, occurrences, "associatedPullRequests should appear exactly once")
}

// TestBuildPRsQuerySelectsRequestedReviewerDatabaseID pins the numeric user id in the
// requested-reviewer selection. It is the key the identity layer stores GitHub
// identities under, so a query that returns only the login leaves every requested
// reviewer unkeyable downstream — the consumer's fold then cannot resolve them to the
// identity a person actually maps to, and pending reviews go missing with no error.
func TestBuildPRsQuerySelectsRequestedReviewerDatabaseID(t *testing.T) {
	tests := []struct {
		name            string
		individualScope bool
		wantDatabaseID  bool
	}{
		// The individual-scope variant deliberately drops the reviewer identity
		// entirely (count only), so there is no id to select.
		{name: "org scope selects the reviewer identity", individualScope: false, wantDatabaseID: true},
		{name: "individual scope keeps only the count", individualScope: true, wantDatabaseID: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := buildPRsQuery("owner", "repo", time.Time{}, "", 50, nil, tt.individualScope)
			start := strings.Index(q, "reviewRequests")
			require.GreaterOrEqual(t, start, 0, "query must request reviewRequests")
			block := q[start:]
			if end := strings.Index(block, "comments"); end > 0 {
				block = block[:end]
			}
			if tt.wantDatabaseID {
				assert.Contains(t, block, "databaseId")
			} else {
				assert.NotContains(t, block, "databaseId")
			}
		})
	}
}

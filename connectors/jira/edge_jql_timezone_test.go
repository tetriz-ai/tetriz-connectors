// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

var reEdgeUpdatedLiteral = regexp.MustCompile(`updated >= "([^"]+)"`)

// TestEdge_JQLBoundaryHoldsInTheUserTimezone: Jira evaluates a JQL date literal
// such as "2026-05-01 12:00" in the searching user's profile time zone, not UTC
// (Atlassian, "JQL fields: Updated": results are relative to your configured
// time zone). Under OAuth 3LO the searching user is whoever authorized the
// connection. The connector renders the cursor's UTC wall clock with no zone, so
// for a user west of UTC the boundary lands hours after the cursor, and every
// issue updated in that gap is never fetched. The boundary Jira applies must not
// be later than the cursor's high-water mark.
func TestEdge_JQLBoundaryHoldsInTheUserTimezone(t *testing.T) {
	t.Parallel()
	userTZ, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	highWater := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	since := effectiveSince(highWater, defaultCursorOverlapWindow)

	check := func(t *testing.T, jql string) {
		t.Helper()
		m := reEdgeUpdatedLiteral.FindStringSubmatch(jql)
		require.Len(t, m, 2, "jql %q has no updated bound", jql)
		applied, err := time.ParseInLocation(jiraTimeFormat, m[1], userTZ)
		require.NoError(t, err)
		assert.False(t, applied.After(highWater),
			"Jira applies %q as %s for a user in %s, %s after the high-water mark %s: updates in that gap are never fetched",
			m[1], applied.UTC(), userTZ, applied.Sub(highWater), highWater)
	}

	t.Run("fetch_issues", func(t *testing.T) {
		t.Parallel()
		gw, captured := captureJQLGateway()
		_, err := fetchIssuesForProject(context.Background(), gw, testkit.NewChannelSink(),
			"fetch_issues", "org-1", "cloud-1", "PROJ", since, "", highWater, nil)
		require.NoError(t, err)
		check(t, *captured)
	})
	t.Run("fetch_comments and fetch_worklogs", func(t *testing.T) {
		t.Parallel()
		gw, captured := captureJQLGateway()
		_, err := fetchUpdatedIssueKeys(context.Background(), gw, "cloud-1", "PROJ", since)
		require.NoError(t, err)
		check(t, *captured)
	})
}

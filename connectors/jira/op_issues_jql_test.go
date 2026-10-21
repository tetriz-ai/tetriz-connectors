// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestFetchIssuesJQL_QuotesReservedWordProjectKey pins project-key quoting:
// a project key that is a reserved JQL word (e.g.
// "DO", "AND", "OR", "IN", "NOT", "ORDER") must be quoted so Atlassian does
// not reject the query with HTTP 400 ("... is a reserved JQL word"). Both
// issue-fetch JQL builders — fetchIssuesForProject (feeds fetch_issues) and
// fetchUpdatedIssueKeys (feeds fetch_comments + fetch_worklogs) — must produce
// a quoted key in both the since-zero and since-set branches. A reserved JQL
// word as a project key must be quoted, or Jira answers 400.
func TestFetchIssuesJQL_QuotesReservedWordProjectKey(t *testing.T) {
	t.Parallel()
	// A reserved JQL word as the project key.
	const reservedKey = "DO"
	since := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	sinceRendered := since.UTC().Format(jiraTimeFormat)

	tests := []struct {
		name    string
		since   time.Time
		wantJQL string
	}{
		{
			name:    "since zero — full scan",
			since:   time.Time{},
			wantJQL: `project = "DO" ORDER BY updated ASC`,
		},
		{
			name:    "since set — incremental",
			since:   since,
			wantJQL: fmt.Sprintf(`project = "DO" AND updated >= %q ORDER BY updated ASC`, sinceRendered),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// fetchIssuesForProject captures its JQL on the first (and only) page.
			t.Run("fetchIssuesForProject", func(t *testing.T) {
				t.Parallel()
				gw, captured := captureJQLGateway()
				_, err := fetchIssuesForProject(
					context.Background(), gw, testkit.NewChannelSink(),
					"fetch_issues", "org-1", "cloud-1", reservedKey,
					tt.since, "", time.Time{}, nil,
				)
				require.NoError(t, err)
				assert.Equal(t, tt.wantJQL, *captured)
			})

			t.Run("fetchUpdatedIssueKeys", func(t *testing.T) {
				t.Parallel()
				gw, captured := captureJQLGateway()
				_, err := fetchUpdatedIssueKeys(
					context.Background(), gw, "cloud-1", reservedKey, tt.since,
				)
				require.NoError(t, err)
				assert.Equal(t, tt.wantJQL, *captured)
			})
		})
	}
}

// captureJQLGateway returns a mock VendorGateway that records the JQL of the
// first JiraSearchIssuesRequest and returns an empty page so the fetch loop
// terminates after one call. The returned pointer holds the captured JQL.
func captureJQLGateway() (*mockVendorGateway, *string) {
	captured := new(string)
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			r, ok := req.(*connectorpb.JiraSearchIssuesRequest)
			if !ok {
				return nil, fmt.Errorf("unexpected request type %T", req)
			}
			*captured = r.GetJql()
			return searchIssuesResponse(nil, ""), nil
		},
	}
	return gw, captured
}

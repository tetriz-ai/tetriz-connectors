// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A comment or worklog with no id has no ExternalID of its own: emitting it
// would key every such item "comment-" or "worklog-" and collapse them. It is
// skipped, and the items with ids are still emitted.
func TestCommentsAndWorklogsWithoutIDAreSkipped(t *testing.T) {
	created := jiraTime{time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)}
	issues := makeIssues(t, 1, created.Time, time.Minute, 10001)

	comments := &commentsExecutor{t: t, projectKey: "PROJ", issues: issues,
		commentsByIssue: map[string]func() (*connectorpb.JiraListIssueCommentsResponse, error){
			"PROJ-1": func() (*connectorpb.JiraListIssueCommentsResponse, error) {
				return commentsResponse([]jiraComment{{ID: "", Created: created}, {ID: "", Created: created}, {ID: "c-1", Created: created}}, 3), nil
			},
		}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}).Execute(context.Background(), testParams(sink, &mockVendorGateway{executeFunc: comments.executeFunc()}, nil))
	require.NoError(t, err)
	require.Len(t, sink.Records(), 1)
	assert.Equal(t, "comment-c-1", sink.Records()[0].ExternalID)

	worklogs := &worklogsExecutor{t: t, projectKey: "PROJ", issues: issues,
		worklogsByIssue: map[string]func() (*connectorpb.JiraListIssueWorklogsResponse, error){
			"PROJ-1": func() (*connectorpb.JiraListIssueWorklogsResponse, error) {
				return worklogsResponse([]jiraWorklog{{ID: "", Created: created}, {ID: "w-1", Created: created}}, 2), nil
			},
		}}
	sink = testkit.NewChannelSink()
	_, err = (&FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}).Execute(context.Background(), testParams(sink, &mockVendorGateway{executeFunc: worklogs.executeFunc()}, nil))
	require.NoError(t, err)
	require.Len(t, sink.Records(), 1)
	assert.Equal(t, "worklog-w-1", sink.Records()[0].ExternalID)
}

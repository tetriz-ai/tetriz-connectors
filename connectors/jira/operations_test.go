// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- Mock VendorGateway ---

type mockVendorGateway struct {
	executeFunc func(ctx context.Context, req proto.Message) (proto.Message, error)
}

func (m *mockVendorGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not expected in unit tests")
}

func (m *mockVendorGateway) DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error {
	return fmt.Errorf("DoJSON not expected in unit tests")
}

func (m *mockVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) ForGroup(group string) sdk.VendorGateway {
	return m
}

func (m *mockVendorGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, req)
	}
	return nil, fmt.Errorf("unexpected Execute call: %T", req)
}

// --- Test helpers ---

func testParams(sink sdk.DataSink, gw sdk.VendorGateway, cursor *sdk.Cursor) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config:        validConfig(),
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
	}
}

func projectsResponse(projects []jiraProject, isLast bool) *connectorpb.JiraListProjectsResponse {
	projectBytes := make([][]byte, len(projects))
	for i, p := range projects {
		projectBytes[i] = mustJSON(p)
	}
	return &connectorpb.JiraListProjectsResponse{
		Projects: projectBytes,
		IsLast:   isLast,
	}
}

func searchIssuesResponse(issues []jiraIssue, nextPageToken string) *connectorpb.JiraSearchIssuesResponse {
	issueBytes := make([][]byte, len(issues))
	for i, issue := range issues {
		issueBytes[i] = mustJSON(issue)
	}
	return &connectorpb.JiraSearchIssuesResponse{
		Issues:        issueBytes,
		NextPageToken: nextPageToken,
	}
}

func commentsResponse(comments []jiraComment, total int32) *connectorpb.JiraListIssueCommentsResponse {
	commentBytes := make([][]byte, len(comments))
	for i, c := range comments {
		commentBytes[i] = mustJSON(c)
	}
	return &connectorpb.JiraListIssueCommentsResponse{
		Comments:   commentBytes,
		Total:      total,
		StartAt:    0,
		MaxResults: int32(len(comments)),
	}
}

func worklogsResponse(worklogs []jiraWorklog, total int32) *connectorpb.JiraListIssueWorklogsResponse {
	worklogBytes := make([][]byte, len(worklogs))
	for i, w := range worklogs {
		worklogBytes[i] = mustJSON(w)
	}
	return &connectorpb.JiraListIssueWorklogsResponse{
		Worklogs:   worklogBytes,
		Total:      total,
		StartAt:    0,
		MaxResults: int32(len(worklogs)),
	}
}

// --- FetchProjectsOp tests ---

func TestFetchProjectsOp_Execute(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return projectsResponse([]jiraProject{
				{ID: "10001", Key: "PROJ", Name: "My Project"},
				{ID: "10002", Key: "BACK", Name: "Backend"},
			}, true), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchProjectsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "project-10001", records[0].ExternalID)
	assert.Equal(t, "project-10002", records[1].ExternalID)
	assert.Equal(t, "PROJ", records[0].Metadata["project_key"])
	assert.Equal(t, "fetch_projects", records[0].OperationID)
	assert.Equal(t, "jira", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)
	assert.Equal(t, "abc123-def456", records[0].Metadata["cloud_id"])
}

func TestFetchProjectsOp_EmptyResponse(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return projectsResponse(nil, true), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchProjectsOp{}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)
	assert.Empty(t, sink.Records())
}

func TestFetchProjectsOp_APIError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return nil, fmt.Errorf("REST HTTP 403: Forbidden")
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchProjectsOp{}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

func TestFetchProjectsOp_InvalidConfig(t *testing.T) {
	gw := &mockVendorGateway{}
	sink := testkit.NewChannelSink()

	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: "org-test",
			Credentials:    json.RawMessage(`{invalid}`),
			Settings:       mustJSON(JiraSettings{CloudID: "abc123"}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	op := &FetchProjectsOp{}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
}

// --- FetchIssuesOp tests ---

func TestFetchIssuesOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{
					{ID: "10001", Key: "PROJ"},
				}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{
						ID:  "10100",
						Key: "PROJ-1",
						Fields: jiraIssueFields{
							Summary: "First issue",
							Created: jiraTime{now.Add(-time.Hour)},
							Updated: jiraTime{now},
						},
					},
				}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	// PROJ-1: created + snapshot = 2 records (no resolutiondate)
	require.Len(t, records, 2)
	assert.Equal(t, "10100#created", records[0].ExternalID)
	assert.Equal(t, "10100#snapshot", records[1].ExternalID)
	assert.Equal(t, "PROJ-1", records[0].Metadata["issue_key"])
	assert.Equal(t, "fetch_issues", records[0].OperationID)
	assert.Equal(t, "jira", records[0].ConnectorID)
	assert.Equal(t, "PROJ", records[0].Metadata["project"])
	assert.Equal(t, "abc123-def456", records[0].Metadata["cloud_id"])
	assert.Equal(t, "created", records[0].Metadata["event_type"])
	assert.Equal(t, "snapshot", records[1].Metadata["event_type"])
}

func TestFetchIssuesOp_ResolvedIssueEmitsThreeRecords(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	resDate := now.Add(-30 * time.Minute)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{
						ID:  "10101",
						Key: "PROJ-2",
						Fields: jiraIssueFields{
							Summary:        "Resolved issue",
							Created:        jiraTime{now.Add(-2 * time.Hour)},
							Updated:        jiraTime{now},
							ResolutionDate: &jiraTime{resDate},
						},
					},
				}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)

	records := sink.Records()
	// PROJ-2: created + snapshot + resolved = 3 records
	require.Len(t, records, 3)
	assert.Equal(t, "10101#created", records[0].ExternalID)
	assert.Equal(t, "10101#snapshot", records[1].ExternalID)
	assert.Equal(t, "10101#resolved", records[2].ExternalID)
	assert.Equal(t, "resolved", records[2].Metadata["event_type"])
}

func TestFetchIssuesOp_IncrementalSync(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-2 * time.Hour)

	var capturedJQL string
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				capturedJQL = r.GetJql()
				return searchIssuesResponse([]jiraIssue{}, ""), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	// Build cursor with a prior timestamp for PROJ
	cursorStates := map[string]projectCursorState{
		"PROJ": {Timestamp: since},
	}
	cursorParts := make(map[string]json.RawMessage)
	for k, v := range cursorStates {
		cursorParts[k] = mustJSON(v)
	}
	cursor := sdk.NewCompositeCursor(cursorParts)
	params := testParams(sink, gw, cursor)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	// JQL should contain the since timestamp filter
	assert.Contains(t, capturedJQL, "updated >=")
}

func TestFetchIssuesOp_APIError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			default:
				return nil, fmt.Errorf("search API error: 429 Too Many Requests")
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "429")
}

// --- FetchCommentsOp tests ---

func TestFetchCommentsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{ID: "10100", Key: "PROJ-1", Fields: jiraIssueFields{Updated: jiraTime{now}}},
				}, ""), nil
			case *connectorpb.JiraListIssueCommentsRequest:
				return commentsResponse([]jiraComment{
					{ID: "com-1", Created: jiraTime{now.Add(-time.Hour)}},
					{ID: "com-2", Created: jiraTime{now}},
				}, 2), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "comment-com-1", records[0].ExternalID)
	assert.Equal(t, "comment-com-2", records[1].ExternalID)
	assert.Equal(t, "fetch_comments", records[0].OperationID)
	assert.Equal(t, "PROJ", records[0].Metadata["project"])
	assert.Equal(t, "PROJ-1", records[0].Metadata[MetaKeyIssueKey])
}

func TestFetchCommentsOp_EmptyResponse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{ID: "10100", Key: "PROJ-1", Fields: jiraIssueFields{Updated: jiraTime{now}}},
				}, ""), nil
			case *connectorpb.JiraListIssueCommentsRequest:
				return commentsResponse(nil, 0), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)
	assert.Empty(t, sink.Records())
}

// --- FetchWorklogsOp tests ---

func TestFetchWorklogsOp_Execute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{ID: "10100", Key: "PROJ-1", Fields: jiraIssueFields{Updated: jiraTime{now}}},
				}, ""), nil
			case *connectorpb.JiraListIssueWorklogsRequest:
				return worklogsResponse([]jiraWorklog{
					{ID: "10001", Created: jiraTime{now.Add(-time.Hour)}},
					{ID: "10002", Created: jiraTime{now}},
				}, 2), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "worklog-10001", records[0].ExternalID)
	assert.Equal(t, "worklog-10002", records[1].ExternalID)
	assert.Equal(t, "fetch_worklogs", records[0].OperationID)
	assert.Equal(t, "PROJ", records[0].Metadata["project"])
	assert.Equal(t, "PROJ-1", records[0].Metadata[MetaKeyIssueKey])
}

func TestFetchWorklogsOp_EmptyResponse(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			case *connectorpb.JiraSearchIssuesRequest:
				return searchIssuesResponse([]jiraIssue{
					{ID: "10100", Key: "PROJ-1", Fields: jiraIssueFields{Updated: jiraTime{now}}},
				}, ""), nil
			case *connectorpb.JiraListIssueWorklogsRequest:
				return worklogsResponse(nil, 0), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	cursor, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, cursor)
	assert.Empty(t, sink.Records())
}

// --- FetchSprintsOp tests ---

func boardsResponse(boards []jiraBoard, isLast bool) *connectorpb.JiraListBoardsResponse {
	boardBytes := make([][]byte, len(boards))
	for i, b := range boards {
		boardBytes[i] = mustJSON(b)
	}
	return &connectorpb.JiraListBoardsResponse{Boards: boardBytes, IsLast: isLast}
}

func sprintsResponse(raws [][]byte, isLast bool) *connectorpb.JiraListSprintsResponse {
	return &connectorpb.JiraListSprintsResponse{Sprints: raws, IsLast: isLast}
}

func sprintJSON(id int64, name, state string) []byte {
	return []byte(fmt.Sprintf(
		`{"id":%d,"name":%q,"state":%q,"startDate":"2026-04-01T00:00:00.000Z","endDate":"2026-04-14T00:00:00.000Z"}`,
		id, name, state,
	))
}

func boardWithProject(id int64, name, boardType string, projectID int64, projectKey string) jiraBoard {
	b := jiraBoard{ID: id, Name: name, Type: boardType}
	b.Location = &struct {
		ProjectID  int64  `json:"projectId,omitempty"`
		ProjectKey string `json:"projectKey,omitempty"`
	}{ProjectID: projectID, ProjectKey: projectKey}
	return b
}

func TestFetchSprintsOp_Execute(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{
					boardWithProject(1, "Backend Board", "scrum", 10001, "PROJ"),
				}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				assert.Equal(t, int64(1), r.GetBoardId())
				return sprintsResponse([][]byte{
					sprintJSON(101, "Sprint 23", "active"),
					sprintJSON(102, "Sprint 24", "future"),
				}, true), nil
			default:
				return nil, fmt.Errorf("unexpected request type: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)
	ts, err := sdk.TimestampFromCursor(cursor)
	require.NoError(t, err)
	assert.False(t, ts.IsZero(), "timestamp cursor must be populated")

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "sprint-101", records[0].ExternalID)
	assert.Equal(t, "sprint-102", records[1].ExternalID)
	assert.Equal(t, "fetch_sprints", records[0].OperationID)
	assert.Equal(t, "jira", records[0].ConnectorID)
	assert.Equal(t, "org-test", records[0].OrganizationID)

	meta := records[0].Metadata
	assert.Equal(t, "abc123-def456", meta[MetaKeyCloudID])
	assert.Equal(t, "1", meta[MetaKeyBoardID])
	assert.Equal(t, "Backend Board", meta[MetaKeyBoardName])
	assert.Equal(t, "scrum", meta[MetaKeyBoardType])
	assert.Equal(t, "10001", meta[MetaKeyProjectID])
	assert.Equal(t, "PROJ", meta[MetaKeyProjectKey])
}

func TestFetchSprintsOp_MultipleBoards(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{
					boardWithProject(1, "Backend", "scrum", 10001, "PROJ"),
					boardWithProject(2, "Frontend", "scrum", 10002, "WEB"),
				}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				switch r.GetBoardId() {
				case 1:
					return sprintsResponse([][]byte{sprintJSON(101, "Sprint A", "active")}, true), nil
				case 2:
					return sprintsResponse([][]byte{sprintJSON(201, "Sprint B", "closed")}, true), nil
				}
				return nil, fmt.Errorf("unexpected board id %d", r.GetBoardId())
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "PROJ", records[0].Metadata[MetaKeyProjectKey])
	assert.Equal(t, "WEB", records[1].Metadata[MetaKeyProjectKey])
}

func TestFetchSprintsOp_SkipsBoardsThatDoNotSupportSprints(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{
					boardWithProject(1, "Kanban", "scrum", 10001, "PROJ"),
					boardWithProject(2, "Working", "scrum", 10002, "WEB"),
				}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				if r.GetBoardId() == 1 {
					return nil, &sdk.SyncError{
						ConnectorID: connectorID,
						OperationID: "fetch_sprints",
						Kind:        sdk.ErrKindInternal,
						HTTPStatus:  http.StatusBadRequest,
						Cause:       fmt.Errorf("HTTP 400: The board does not support sprints"),
					}
				}
				return sprintsResponse([][]byte{sprintJSON(201, "Sprint A", "active")}, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err, "400 with 'does not support sprints' body should not fail the whole run")

	records := sink.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "sprint-201", records[0].ExternalID)
}

// TestFetchSprintsOp_GenericBadRequestFailsRun guards CODE-007: a 400 whose
// body is NOT the "does not support sprints" marker must fail the run rather
// than be silently swallowed as a kanban-board skip. This is the case that
// surfaces real client-side bugs (bad cloud_id, URL-builder regression,
// Atlassian contract change).
func TestFetchSprintsOp_GenericBadRequestFailsRun(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{boardWithProject(1, "Backend", "scrum", 10001, "PROJ")}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_sprints",
					Kind:        sdk.ErrKindInternal,
					HTTPStatus:  http.StatusBadRequest,
					Cause:       fmt.Errorf("HTTP 400: invalid startAt parameter"),
				}
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err, "generic 400 must propagate so real bugs aren't masked as board-skip")
	assert.Contains(t, err.Error(), "fetch sprints for board 1")
}

func TestFetchSprintsOp_BoardListError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return nil, fmt.Errorf("REST HTTP 403: Forbidden")
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list boards")
}

func TestFetchSprintsOp_NonRetryableSprintError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{boardWithProject(1, "B", "scrum", 10001, "PROJ")}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				return nil, fmt.Errorf("REST HTTP 500: server exploded")
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch sprints for board 1")
}

func TestFetchSprintsOp_NoBoards(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse(nil, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	// Even with no boards the op succeeded, so it returns a fresh timestamp
	// cursor — there's no "some work didn't happen" signal to preserve.
	require.NotNil(t, cursor)
	ts, err := sdk.TimestampFromCursor(cursor)
	require.NoError(t, err)
	assert.False(t, ts.IsZero())
	assert.Empty(t, sink.Records())
}

func TestFetchSprintsOp_BoardWithoutProject(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{{ID: 9, Name: "Detached", Type: "scrum"}}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				return sprintsResponse([][]byte{sprintJSON(300, "Sprint", "active")}, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1)
	meta := records[0].Metadata
	assert.Equal(t, "9", meta[MetaKeyBoardID])
	_, hasProjectID := meta[MetaKeyProjectID]
	_, hasProjectKey := meta[MetaKeyProjectKey]
	assert.False(t, hasProjectID, "project_id must be absent when board has no location")
	assert.False(t, hasProjectKey, "project_key must be absent when board has no location")
}

// TestFetchSprintsOp_ReturnsTimestampCursor guards that on success the op
// returns a fresh timestamp cursor — matches fetch_projects, which also has
// no incremental filter on the vendor API.
func TestFetchSprintsOp_ReturnsTimestampCursor(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{
					boardWithProject(1, "Backend", "scrum", 10001, "PROJ"),
					boardWithProject(2, "Frontend", "scrum", 10002, "WEB"),
				}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				if r.GetBoardId() == 1 {
					return sprintsResponse([][]byte{sprintJSON(101, "S", "active")}, true), nil
				}
				return sprintsResponse([][]byte{sprintJSON(201, "S", "closed")}, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	before := time.Now().UTC()
	op := &FetchSprintsOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cursor)

	ts, err := sdk.TimestampFromCursor(cursor)
	require.NoError(t, err)
	assert.False(t, ts.Before(before), "cursor timestamp must be >= run start")
}

func TestFetchSprintsOp_InvalidConfig(t *testing.T) {
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: "org-test",
			Credentials:    json.RawMessage(`{invalid}`),
			Settings:       mustJSON(JiraSettings{CloudID: "abc123"}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: &mockVendorGateway{},
	}

	op := &FetchSprintsOp{}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
}

func TestFetchWorklogsOp_RateLimitError(t *testing.T) {
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			default:
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: "fetch_worklogs",
					Kind:        sdk.ErrKindRateLimit,
					Retryable:   true,
					Cause:       fmt.Errorf("rate limit exceeded"),
				}
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow}
	_, err := op.Execute(context.Background(), params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit")
}

// --- Settings-based project key filter tests ---

func TestFetchProjectsOp_ProjectKeyFilter(t *testing.T) {
	var capturedCloudID string

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListProjectsRequest:
				capturedCloudID = r.GetCloudId()
				return projectsResponse([]jiraProject{{ID: "10001", Key: "PROJ"}}, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchProjectsOp{}
	_, err := op.Execute(context.Background(), params)

	require.NoError(t, err)
	assert.Equal(t, "abc123-def456", capturedCloudID, "should pass cloud ID from settings")
}

// --- Cursor scaffolding tests ---

// TestProjectCursorStateUnmarshalJSON pins the reader: the
// {timestamp/page_token/since/last_issue_id} layout parses, and any other
// shape, a {ts/pt/sf} one included, is an error the caller drops.
func TestProjectCursorStateUnmarshalJSON(t *testing.T) {
	knownTime := time.Date(2026, 1, 16, 10, 23, 45, 0, time.UTC)
	earlierTime := time.Date(2026, 1, 14, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		rawJSON        string
		wantErr        bool
		wantTimestamp  time.Time
		wantPageToken  string
		wantSince      time.Time
		wantLastIssue  string
		assertExtraMsg string
	}{
		{
			name:          "new format with all fields",
			rawJSON:       fmt.Sprintf(`{"timestamp":"%s","page_token":"tok123","since":"%s","last_issue_id":"10042"}`, knownTime.Format(time.RFC3339Nano), earlierTime.Format(time.RFC3339Nano)),
			wantTimestamp: knownTime,
			wantPageToken: "tok123",
			wantSince:     earlierTime,
			wantLastIssue: "10042",
		},
		{
			name:          "new format worklogs/comments clean state",
			rawJSON:       fmt.Sprintf(`{"timestamp":"%s","last_issue_id":"10042"}`, knownTime.Format(time.RFC3339Nano)),
			wantTimestamp: knownTime,
			wantLastIssue: "10042",
		},
		{
			name:           "short-tag layout is rejected",
			rawJSON:        fmt.Sprintf(`{"ts":"%s","pt":"tok123","sf":"%s"}`, knownTime.Format(time.RFC3339Nano), earlierTime.Format(time.RFC3339Nano)),
			wantErr:        true,
			assertExtraMsg: "an entry in another layout has no timestamp, so the caller drops it",
		},
		{
			name:           "zero timestamp new format is rejected",
			rawJSON:        `{"timestamp":"0001-01-01T00:00:00Z","page_token":"x"}`,
			wantErr:        true,
			assertExtraMsg: "zero-Timestamp new must fall through to error so caller drops it",
		},
		{
			name:    "not an object",
			rawJSON: `"not-an-object"`,
			wantErr: true,
		},
		{
			name:           "unknown field in new format is ignored",
			rawJSON:        fmt.Sprintf(`{"timestamp":"%s","unknown_future_field":"x"}`, knownTime.Format(time.RFC3339Nano)),
			wantTimestamp:  knownTime,
			assertExtraMsg: "unknown fields must not break parsing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var state projectCursorState
			err := json.Unmarshal([]byte(tt.rawJSON), &state)
			if tt.wantErr {
				require.Error(t, err, tt.assertExtraMsg)
				return
			}
			require.NoError(t, err, tt.assertExtraMsg)
			assert.True(t, tt.wantTimestamp.Equal(state.Timestamp), "Timestamp mismatch: got %s want %s", state.Timestamp, tt.wantTimestamp)
			assert.Equal(t, tt.wantPageToken, state.PageToken)
			assert.True(t, tt.wantSince.Equal(state.Since), "Since mismatch: got %s want %s", state.Since, tt.wantSince)
			assert.Equal(t, tt.wantLastIssue, state.LastIssueID)
		})
	}
}

// TestProjectCursorStateMarshalOnlyEmitsNewTags pins the tag names Marshal writes.
func TestProjectCursorStateMarshalOnlyEmitsNewTags(t *testing.T) {
	state := projectCursorState{
		Timestamp:   time.Date(2026, 1, 16, 10, 23, 45, 0, time.UTC),
		PageToken:   "tok123",
		Since:       time.Date(2026, 1, 14, 8, 0, 0, 0, time.UTC),
		LastIssueID: "10042",
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	jsonStr := string(raw)
	assert.Contains(t, jsonStr, `"timestamp":`)
	assert.Contains(t, jsonStr, `"page_token":`)
	assert.Contains(t, jsonStr, `"since":`)
	assert.Contains(t, jsonStr, `"last_issue_id":`)
	assert.NotContains(t, jsonStr, `"ts":`)
	assert.NotContains(t, jsonStr, `"pt":`)
	assert.NotContains(t, jsonStr, `"sf":`)
}

// TestEffectiveSince pins the cursor overlap: zero cursor falls back to
// firstSyncSince (12-month lookback), non-zero cursor subtracts
// cursorOverlapWindow (5 minutes) as a defense-in-depth safety margin, plus
// maxJQLZoneOffset because Jira reads the JQL literal in the user's time zone.
func TestEffectiveSince(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		cursorTime    time.Time
		expectedSince func(now time.Time) time.Time
		assertMsg     string
	}{
		{
			name:       "zero cursor falls back to firstSyncSince (12-month lookback)",
			cursorTime: time.Time{},
			expectedSince: func(now time.Time) time.Time {
				return now.Add(-defaultLookback)
			},
			assertMsg: "zero cursor must use firstSyncSince, not now-5min",
		},
		{
			name:       "non-zero cursor subtracts cursorOverlapWindow",
			cursorTime: knownTime,
			expectedSince: func(_ time.Time) time.Time {
				return knownTime.Add(-defaultCursorOverlapWindow - maxJQLZoneOffset)
			},
			assertMsg: "5-minute overlap window plus the 14h JQL time-zone widening must be applied",
		},
		{
			name:       "non-zero cursor near day boundary underflows correctly",
			cursorTime: time.Date(2026, 5, 24, 0, 2, 0, 0, time.UTC),
			expectedSince: func(_ time.Time) time.Time {
				return time.Date(2026, 5, 23, 9, 57, 0, 0, time.UTC)
			},
			assertMsg: "must correctly underflow day boundary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			before := time.Now().UTC()
			got := effectiveSince(tt.cursorTime, defaultCursorOverlapWindow)
			after := time.Now().UTC()

			if tt.cursorTime.IsZero() {
				// firstSyncSince uses time.Now() so we only assert the value
				// is within [before-defaultLookback, after-defaultLookback].
				assert.False(t, got.After(after.Add(-defaultLookback)), tt.assertMsg)
				assert.False(t, got.Before(before.Add(-defaultLookback)), tt.assertMsg)
				return
			}
			want := tt.expectedSince(before)
			assert.True(t, want.Equal(got), "%s: got %s want %s", tt.assertMsg, got, want)
		})
	}
}

// TestCompareIssueIDs pins the numeric Jira-ID comparator. Comparing "10042"
// against "9999" by lexicographic order would give wrong answer ("1" < "9").
// The fallback to string compare exists for defensive purposes; tests pin
// both paths.
func TestCompareIssueIDs(t *testing.T) {
	tests := []struct {
		name    string
		leftID  string
		rightID string
		want    int
	}{
		{name: "numeric a less than b", leftID: "9999", rightID: "10042", want: -1},
		{name: "numeric a greater than b", leftID: "10042", rightID: "9999", want: 1},
		{name: "numeric equal", leftID: "10042", rightID: "10042", want: 0},
		{name: "numeric zero edge", leftID: "0", rightID: "1", want: -1},
		{name: "large monotonic IDs", leftID: "10500", rightID: "10999", want: -1},
		{name: "non-numeric fallback to string compare less", leftID: "abc", rightID: "xyz", want: -1},
		{name: "non-numeric fallback to string compare greater", leftID: "xyz", rightID: "abc", want: 1},
		{name: "one side non-numeric falls back to string", leftID: "10042", rightID: "abc", want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareIssueIDs(tt.leftID, tt.rightID)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestIsAlreadyProcessed pins the cross-run skip predicate that protects against
// duplicate emission on resume. The same-Timestamp branch is the boundary case:
// without an ID tiebreaker, we'd either miss new issues (using a strict >
// filter) or re-process completed ones (using >=).
func TestIsAlreadyProcessed(t *testing.T) {
	timeA := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	timeB := time.Date(2026, 5, 24, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		ref          issueRef
		cursorTime   time.Time
		cursorID     string
		wantSkipped  bool
		assertReason string
	}{
		{
			name:         "before cursor timestamp",
			ref:          issueRef{ID: "10000", UpdatedOn: timeA},
			cursorTime:   timeB,
			cursorID:     "",
			wantSkipped:  true,
			assertReason: "issue's UpdatedOn strictly before cursor: already processed",
		},
		{
			name:         "after cursor timestamp",
			ref:          issueRef{ID: "10000", UpdatedOn: timeB},
			cursorTime:   timeA,
			cursorID:     "",
			wantSkipped:  false,
			assertReason: "issue's UpdatedOn strictly after cursor: not processed",
		},
		{
			name:         "same timestamp, id less than cursor.id",
			ref:          issueRef{ID: "10000", UpdatedOn: timeA},
			cursorTime:   timeA,
			cursorID:     "10001",
			wantSkipped:  true,
			assertReason: "(T,id) <= (T,cursor.id) means already processed",
		},
		{
			name:         "same timestamp, id equal to cursor.id",
			ref:          issueRef{ID: "10001", UpdatedOn: timeA},
			cursorTime:   timeA,
			cursorID:     "10001",
			wantSkipped:  true,
			assertReason: "exact same (T, id) was the cursor — already processed",
		},
		{
			name:         "same timestamp, id greater than cursor.id",
			ref:          issueRef{ID: "10002", UpdatedOn: timeA},
			cursorTime:   timeA,
			cursorID:     "10001",
			wantSkipped:  false,
			assertReason: "higher numeric ID at same timestamp means newer issue — must process",
		},
		{
			name:         "same timestamp new issue with LOWER alphabetic key but HIGHER numeric ID is not skipped",
			ref:          issueRef{Key: "PROJ-499", ID: "10999", UpdatedOn: timeA},
			cursorTime:   timeA,
			cursorID:     "10500",
			wantSkipped:  false,
			assertReason: "ID tiebreaker (10999 > 10500) overrides alphabetic-key intuition",
		},
		{
			name:         "empty cursor id with matching timestamp does not skip",
			ref:          issueRef{ID: "10000", UpdatedOn: timeA},
			cursorTime:   timeA,
			cursorID:     "",
			wantSkipped:  false,
			assertReason: "empty cursor id means first-time write at this T; must process",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isAlreadyProcessed(tt.ref, tt.cursorTime, tt.cursorID)
			assert.Equal(t, tt.wantSkipped, got, tt.assertReason)
		})
	}
}

// TestExistingCursorStates pins the cursor-reader's drop-on-parse-failure
// behavior: a single corrupt per-project entry must not prevent other
// well-formed entries from being read and re-used.
func TestExistingCursorStates(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		cursorBuild func() *sdk.Cursor
		wantKeys    []string
		assertMsg   string
	}{
		{
			name:        "nil cursor returns empty map",
			cursorBuild: func() *sdk.Cursor { return nil },
			wantKeys:    nil,
		},
		{
			name: "mixed valid and invalid entries — invalid dropped, valid kept",
			cursorBuild: func() *sdk.Cursor {
				// "BACK" is a JSON number — the outer composite-cursor map
				// marshals cleanly, but the per-entry UnmarshalJSON into
				// projectCursorState fails (cannot unmarshal number into struct).
				// "ZEROTS" parses but its Timestamp is zero, hitting the second
				// drop branch.
				parts := map[string]json.RawMessage{
					"PROJ":   mustJSON(projectCursorState{Timestamp: knownTime, LastIssueID: "10001"}),
					"BACK":   json.RawMessage(`123`),
					"OTHER":  mustJSON(projectCursorState{Timestamp: knownTime}),
					"ZEROTS": json.RawMessage(`{"timestamp":"0001-01-01T00:00:00Z","last_issue_id":"x"}`),
				}
				return sdk.NewCompositeCursor(parts)
			},
			wantKeys:  []string{"PROJ", "OTHER"},
			assertMsg: "non-object entry and zero-Timestamp entry must be dropped; valid kept",
		},
		{
			name: "an entry in another layout is dropped like any malformed entry",
			cursorBuild: func() *sdk.Cursor {
				parts := map[string]json.RawMessage{
					"PROJ": json.RawMessage(fmt.Sprintf(`{"ts":"%s","pt":"tok","sf":"%s"}`, knownTime.Format(time.RFC3339Nano), knownTime.Add(-time.Hour).Format(time.RFC3339Nano))),
					"BACK": mustJSON(projectCursorState{Timestamp: knownTime, LastIssueID: "9999"}),
				}
				return sdk.NewCompositeCursor(parts)
			},
			wantKeys: []string{"BACK"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := existingCursorStates(context.Background(), tt.cursorBuild())
			gotKeys := make([]string, 0, len(got))
			for key := range got {
				gotKeys = append(gotKeys, key)
			}
			assert.ElementsMatch(t, tt.wantKeys, gotKeys, tt.assertMsg)
		})
	}
}

// TestBuildStateCursor pins the round-trip: a map of well-formed states
// marshals into a composite cursor that read-back via existingCursorStates
// returns the same map.
func TestBuildStateCursor(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)
	stateA := projectCursorState{Timestamp: knownTime, LastIssueID: "10042"}
	stateB := projectCursorState{Timestamp: knownTime.Add(-time.Hour), PageToken: "tok", Since: knownTime.Add(-2 * time.Hour)}

	in := map[string]projectCursorState{
		"PROJ": stateA,
		"BACK": stateB,
	}
	cursor := buildStateCursor(context.Background(), in)
	require.NotNil(t, cursor)

	out := existingCursorStates(context.Background(), cursor)
	require.Len(t, out, 2)
	assert.True(t, stateA.Timestamp.Equal(out["PROJ"].Timestamp))
	assert.Equal(t, stateA.LastIssueID, out["PROJ"].LastIssueID)
	assert.True(t, stateB.Timestamp.Equal(out["BACK"].Timestamp))
	assert.Equal(t, stateB.PageToken, out["BACK"].PageToken)
	assert.True(t, stateB.Since.Equal(out["BACK"].Since))
}

// TestBuildStateCursorSkipsEmptyState pins that a zero-Timestamp per-project
// state is omitted from the composite cursor — an empty project must not churn a
// zero-Timestamp entry — while a state with a non-zero Timestamp is kept. The
// skip mirrors existingCursorStates' read-side drop rule (see TestExistingCursorStates
// "ZEROTS"): a zero-Timestamp entry is dropped on read regardless of
// PageToken/LastIssueID, so keeping it on write would be discarded next run.
func TestBuildStateCursorSkipsEmptyState(t *testing.T) {
	knownTime := time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		state projectCursorState
		want  bool // whether the entry should survive into the cursor
	}{
		{
			name:  "fully-empty state is omitted",
			state: projectCursorState{},
			want:  false,
		},
		{
			name:  "zero Timestamp is omitted even with PageToken",
			state: projectCursorState{PageToken: "tok", Since: knownTime},
			want:  false,
		},
		{
			name:  "zero Timestamp is omitted even with LastIssueID",
			state: projectCursorState{LastIssueID: "10042"},
			want:  false,
		},
		{
			name:  "non-zero Timestamp is preserved",
			state: projectCursorState{Timestamp: knownTime},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cursor := buildStateCursor(context.Background(), map[string]projectCursorState{"PROJ": tt.state})
			require.NotNil(t, cursor)
			parts, err := sdk.CompositeFromCursor(cursor)
			require.NoError(t, err)
			_, present := parts["PROJ"]
			assert.Equal(t, tt.want, present)
		})
	}
}

// One board whose sprints cannot be read is skipped; the next board's sprints
// are still read and the run completes.
func TestFetchSprintsOp_OneFailingBoardIsSkipped(t *testing.T) {
	var read []int64
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.JiraListBoardsRequest:
				return boardsResponse([]jiraBoard{boardWithProject(1, "A", "scrum", 10001, "PROJ"), boardWithProject(2, "B", "scrum", 10002, "PROJ")}, true), nil
			case *connectorpb.JiraListSprintsRequest:
				read = append(read, r.GetBoardId())
				if r.GetBoardId() == 1 {
					return nil, fmt.Errorf("REST HTTP 500: server exploded")
				}
				return sprintsResponse(nil, true), nil
			default:
				return nil, fmt.Errorf("unexpected: %T", req)
			}
		},
	}
	_, err := (&FetchSprintsOp{}).Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, read, "the board after the failing one is read")
}

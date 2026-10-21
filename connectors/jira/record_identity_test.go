// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// runJiraOp executes op once against a gateway answering every list request
// with the given issues (and projects), and returns what it emitted.
func runJiraOp(t *testing.T, op sdk.APIFetchOperation, projects []jiraProject, issues []json.RawMessage) []*sdk.RawRecord {
	t.Helper()
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse(projects, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			return &connectorpb.JiraSearchIssuesResponse{Issues: toBytes(issues)}, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.BlobStore, _ = newJiraRecordingS3(t)
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	return sink.Records()
}

func toBytes(raws []json.RawMessage) [][]byte {
	out := make([][]byte, len(raws))
	for i, r := range raws {
		out[i] = r
	}
	return out
}

// jiraIssueJSON is an issue as the search API returns it, in project key/id.
func jiraIssueJSON(id, key, projectID, projectKey string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"id":%q,"key":%q,"fields":{
		"summary":"s","created":"2026-07-01T00:00:00.000+0000","updated":"2026-07-02T00:00:00.000+0000",
		"resolutiondate":"2026-07-03T00:00:00.000+0000",
		"description":{"type":"doc","version":1,"content":[]},
		"project":{"id":%q,"key":%q}}}`, id, key, projectID, projectKey))
}

// An issue moved from WORK to OPS gets a new key but keeps its numeric id, so
// fetch_issues and fetch_issues_data keep its ExternalIDs: a sink overwrites
// the record instead of forking it into a second row.
func TestIssueMovedToAnotherProjectKeepsItsExternalID(t *testing.T) {
	for name, op := range map[string]sdk.APIFetchOperation{
		"fetch_issues":      &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow},
		"fetch_issues_data": &FetchIssuesDataOp{},
	} {
		t.Run(name, func(t *testing.T) {
			before := runJiraOp(t, op, []jiraProject{{ID: "10001", Key: "WORK"}},
				[]json.RawMessage{jiraIssueJSON("1100001", "WORK-1", "10001", "WORK")})
			after := runJiraOp(t, op, []jiraProject{{ID: "10002", Key: "OPS"}},
				[]json.RawMessage{jiraIssueJSON("1100001", "OPS-7", "10002", "OPS")})
			require.NotEmpty(t, before)
			require.Len(t, after, len(before))
			for i := range before {
				assert.Equal(t, before[i].ExternalID, after[i].ExternalID, "a move must not change the ExternalID")
				assert.Contains(t, after[i].ExternalID, "1100001")
				assert.NotContains(t, after[i].ExternalID, "WORK")
			}
			if name == "fetch_issues" {
				assert.Equal(t, "OPS-7", after[0].Metadata[MetaKeyIssueKey], "the current key stays in metadata")
			} else {
				var p map[string]any
				require.NoError(t, json.Unmarshal(after[0].Payload, &p))
				assert.Equal(t, "OPS-7", p["issue_key"], "the current key stays in the payload")
			}
		})
	}
}

// A project whose key an admin renames keeps its ExternalID: it is the
// numeric project id.
func TestProjectKeyRenameKeepsItsExternalID(t *testing.T) {
	before := runJiraOp(t, &FetchProjectsOp{}, []jiraProject{{ID: "10001", Key: "WORK"}}, nil)
	after := runJiraOp(t, &FetchProjectsOp{}, []jiraProject{{ID: "10001", Key: "WRK"}}, nil)
	require.Len(t, before, 1)
	require.Len(t, after, 1)
	assert.Equal(t, "project-10001", before[0].ExternalID)
	assert.Equal(t, before[0].ExternalID, after[0].ExternalID)
	assert.Equal(t, "WRK", after[0].Metadata["project_key"])
}

// An issue or project without an id has no identity: it is skipped rather
// than emitted under a colliding ExternalID such as "#created" or "project-".
func TestRecordsWithoutAnIDAreSkipped(t *testing.T) {
	noID := []json.RawMessage{jiraIssueJSON("", "WORK-1", "10001", "WORK"), jiraIssueJSON("", "WORK-2", "10001", "WORK")}
	for name, op := range map[string]sdk.APIFetchOperation{
		"fetch_issues":      &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow},
		"fetch_issues_data": &FetchIssuesDataOp{},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Empty(t, runJiraOp(t, op, []jiraProject{{ID: "10001", Key: "WORK"}}, noID))
		})
	}
	t.Run("fetch_projects", func(t *testing.T) {
		assert.Empty(t, runJiraOp(t, &FetchProjectsOp{}, []jiraProject{{Key: "WORK"}, {Key: "OPS"}}, nil))
	})
}

// DataDate rule (sdk.RawRecord): current-state records carry none, so a
// date-keyed sink holds one row per item; event records are dated by the
// event. Before the rule, projects and sprints were dated by the fetch time,
// fetch_issues_data by fields.updated and #snapshot by fields.created.
func TestDataDateOnlyOnEventRecords(t *testing.T) {
	projects := []jiraProject{{ID: "10001", Key: "WORK"}}
	issues := []json.RawMessage{jiraIssueJSON("1100001", "WORK-1", "10001", "WORK")}

	for _, rec := range runJiraOp(t, &FetchProjectsOp{}, projects, nil) {
		assert.True(t, rec.DataDate.IsZero(), "fetch_projects %s", rec.ExternalID)
	}
	for _, rec := range runJiraOp(t, &FetchIssuesDataOp{}, projects, issues) {
		assert.True(t, rec.DataDate.IsZero(), "fetch_issues_data %s", rec.ExternalID)
	}
	recs := runJiraOp(t, &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow}, projects, issues)
	require.Len(t, recs, 3)
	want := map[string]time.Time{
		"created":  time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		"snapshot": {},
		"resolved": time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC),
	}
	for _, rec := range recs {
		ev := rec.Metadata["event_type"]
		assert.True(t, want[ev].Equal(rec.DataDate), "%s DataDate = %v, want %v", ev, rec.DataDate, want[ev])
	}

	// Sprints: a board with one sprint.
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListBoardsRequest:
			return &connectorpb.JiraListBoardsResponse{Boards: [][]byte{[]byte(`{"id":9001,"name":"B","type":"scrum"}`)}, IsLast: true}, nil
		case *connectorpb.JiraListSprintsRequest:
			return &connectorpb.JiraListSprintsResponse{Sprints: [][]byte{[]byte(`{"id":900103,"name":"S","state":"active"}`)}, IsLast: true}, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchSprintsOp{}).Execute(context.Background(), testParams(sink, gw, nil))
	require.NoError(t, err)
	sprints := sink.Records()
	require.Len(t, sprints, 1)
	assert.True(t, sprints[0].DataDate.IsZero(), "fetch_sprints is current state")
}

// A comment or worklog with no created time has no event date. Dating it by
// the fetch time would fork a date-keyed sink row per run.
func TestCommentAndWorklogWithoutCreatedHaveNoDataDate(t *testing.T) {
	issues := makeIssues(t, 1, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Minute, 10002)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "10000", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			return searchIssuesResponse(issues, ""), nil
		case *connectorpb.JiraListIssueCommentsRequest:
			return &connectorpb.JiraListIssueCommentsResponse{Comments: [][]byte{[]byte(`{"id":"c1"}`)}, Total: 1, MaxResults: 1}, nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			return &connectorpb.JiraListIssueWorklogsResponse{Worklogs: [][]byte{[]byte(`{"id":"w1"}`)}, Total: 1, MaxResults: 1}, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	for name, op := range map[string]sdk.APIFetchOperation{
		"fetch_comments": &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow},
		"fetch_worklogs": &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow},
	} {
		t.Run(name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			_, err := op.Execute(context.Background(), testParams(sink, gw, nil))
			require.NoError(t, err)
			recs := sink.Records()
			require.Len(t, recs, 1)
			assert.True(t, recs[0].DataDate.IsZero())
		})
	}
}

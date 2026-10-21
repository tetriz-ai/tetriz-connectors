// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// Vendor-shaped bodies from the Jira Cloud REST API v3 reference, with the
// nulls and expansions the simulator does not send.
const (
	schemaFixtureComment = `{
  "self": "https://acme.atlassian.net/rest/api/3/issue/10010/comment/10000",
  "id": "10000",
  "author": {"self": "https://acme.atlassian.net/rest/api/3/user?accountId=5b10a2844c20165700ede21g", "accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": false},
  "body": {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Lorem ipsum dolor sit amet."}]}]},
  "updateAuthor": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": false},
  "created": "2021-01-17T12:34:00.000+0000",
  "updated": "2021-01-18T23:45:00.000+0000",
  "jsdPublic": true,
  "visibility": {"identifier": "Administrators", "type": "role", "value": "Administrators"}
}`
	schemaFixtureWorklog = `{
  "self": "https://acme.atlassian.net/rest/api/3/issue/10010/worklog/10000",
  "id": "100028",
  "issueId": "10002",
  "author": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": false},
  "updateAuthor": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": false},
  "comment": {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": [{"type": "text", "text": "I did some work here."}]}]},
  "created": "2021-01-17T12:34:00.000+0000",
  "updated": "2021-01-18T23:45:00.000+0000",
  "started": "2021-01-17T12:34:00.000+0000",
  "timeSpent": "3h 20m",
  "timeSpentSeconds": 12000,
  "visibility": {"identifier": "276f955c-63d7-42c8-9520-92d01dca0625", "type": "group", "value": "jira-developers"}
}`
	schemaFixtureIssue = `{
  "expand": "renderedFields,names,schema,operations,editmeta,changelog,versionedRepresentations",
  "id": "10002",
  "self": "https://acme.atlassian.net/rest/api/3/issue/10002",
  "key": "ED-1",
  "fields": {
    "summary": "Main order flow broken",
    "created": "2021-01-17T12:34:00.000+0000",
    "updated": "2021-01-18T23:45:00.000+0000",
    "resolutiondate": null,
    "resolution": null,
    "duedate": null,
    "description": null,
    "assignee": null,
    "reporter": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": true, "timeZone": "Australia/Sydney"},
    "priority": {"name": "Major", "id": "3"},
    "status": {"name": "To Do", "statusCategory": {"key": "new", "name": "To Do"}},
    "issuetype": {"name": "Bug", "subtask": false},
    "project": {"id": "10000", "key": "ED", "name": "Edison"},
    "labels": [],
    "customfield_10020": null
  },
  "changelog": {
    "startAt": 0, "maxResults": 1, "total": 1,
    "histories": [{
      "id": "10001",
      "author": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Mia Krystof", "active": true},
      "created": "2021-01-18T23:45:00.000+0000",
      "items": [{"field": "status", "fieldtype": "jira", "from": "10000", "fromString": "Backlog", "to": "10001", "toString": "To Do"}]
    }]
  },
  "renderedFields": {"description": null, "created": "17/Jan/21 12:34 PM"}
}`
)

// TestSchemaFixtures runs the operations the simulator cannot serve, or
// serves without nulls, over vendor-shaped bodies, and validates every
// emitted payload against its schema.
func TestSchemaFixtures(t *testing.T) {
	issue := makeIssues(t, 1, time.Date(2021, 1, 18, 23, 45, 0, 0, time.UTC), time.Minute, 10002)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.JiraListProjectsRequest:
			return projectsResponse([]jiraProject{{ID: "10000", Key: "PROJ"}}, true), nil
		case *connectorpb.JiraSearchIssuesRequest:
			if r.GetFields() == issueFields {
				return &connectorpb.JiraSearchIssuesResponse{Issues: [][]byte{[]byte(schemaFixtureIssue)}}, nil
			}
			return searchIssuesResponse(issue, ""), nil
		case *connectorpb.JiraListIssueCommentsRequest:
			return &connectorpb.JiraListIssueCommentsResponse{Comments: [][]byte{[]byte(schemaFixtureComment)}, Total: 1, MaxResults: 1}, nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			return &connectorpb.JiraListIssueWorklogsResponse{Worklogs: [][]byte{[]byte(schemaFixtureWorklog)}, Total: 1, MaxResults: 1}, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	}}
	ops := map[string]sdk.APIFetchOperation{
		"fetch_comments": &FetchCommentsOp{cursorOverlap: defaultCursorOverlapWindow},
		"fetch_worklogs": &FetchWorklogsOp{cursorOverlap: defaultCursorOverlapWindow},
		"fetch_issues":   &FetchIssuesOp{cursorOverlap: defaultCursorOverlapWindow},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			if _, err := op.Execute(context.Background(), testParams(sink, gw, nil)); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			recs := sink.Records()
			if len(recs) == 0 {
				t.Fatalf("%s emitted no records", name)
			}
			for _, rec := range recs {
				if rec.OperationID != name {
					t.Errorf("%s: record %s has operationId %s", name, rec.ExternalID, rec.OperationID)
				}
				if err := schemas.Validate(string(connectorID), rec.OperationID, rec.Payload); err != nil {
					t.Errorf("%s: record %s: %v", name, rec.ExternalID, err)
				}
			}
		})
	}
}

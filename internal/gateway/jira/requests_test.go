// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

const testBaseURL = "https://api.atlassian.com"

func testBaseURLs() map[string]string {
	return map[string]string{
		constants.GroupJiraREST: testBaseURL,
	}
}

// --- Factory dispatch ---

func TestNewJiraVendorRequest_SearchIssues(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId:    "cloud-123",
		Jql:        "project = PROJ",
		MaxResults: 100,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestNewJiraVendorRequest_ListProjects(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListProjectsRequest{
		CloudId:    "cloud-123",
		MaxResults: 50,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestNewJiraVendorRequest_ListIssueComments(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListIssueCommentsRequest{
		CloudId:      "cloud-123",
		IssueIdOrKey: "PROJ-1",
		MaxResults:   50,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestNewJiraVendorRequest_ListIssueWorklogs(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListIssueWorklogsRequest{
		CloudId:      "cloud-123",
		IssueIdOrKey: "PROJ-1",
		MaxResults:   50,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestNewJiraVendorRequest_UnsupportedType(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	// Pass a type the factory doesn't know about.
	req := &connectorpb.JiraListProjectsResponse{}

	_, err := factory(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

// --- URL construction ---

func TestSearchIssuesVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId:    "my-cloud-id",
		Jql:        "project = PROJ",
		MaxResults: 100,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, testBaseURL+"/ex/jira/")
	assert.Contains(t, u, "my-cloud-id")
	assert.Contains(t, u, "/rest/api/3/search/jql")
}

func TestSearchIssuesVendorRequest_URL_EscapesCloudID(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	// Cloud IDs can contain slashes and special chars — these must be path-escaped.
	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId: "cloud/id with spaces",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	// Check raw URL string — url.Parse().Path would decode percent-encoding back to spaces.
	u := vr.URL()
	assert.NotContains(t, u, " ", "spaces must be percent-encoded in raw URL: %s", u)
}

func TestListProjectsVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListProjectsRequest{
		CloudId:    "my-cloud-id",
		StartAt:    0,
		MaxResults: 50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, testBaseURL+"/ex/jira/")
	assert.Contains(t, u, "my-cloud-id")
	assert.Contains(t, u, "/rest/api/3/project/search")
	assert.Contains(t, u, "startAt=0")
	assert.Contains(t, u, "maxResults=50")
}

func TestListProjectsVendorRequest_URL_DefaultMaxResults(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListProjectsRequest{
		CloudId:    "my-cloud-id",
		MaxResults: 0, // zero → should default to 50
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, "maxResults=50")
}

func TestListIssueCommentsVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListIssueCommentsRequest{
		CloudId:      "my-cloud-id",
		IssueIdOrKey: "PROJ-42",
		StartAt:      10,
		MaxResults:   50,
		OrderBy:      "created",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, "/ex/jira/my-cloud-id/rest/api/3/issue/PROJ-42/comment")
	assert.Contains(t, u, "startAt=10")
	assert.Contains(t, u, "maxResults=50")
	assert.Contains(t, u, "orderBy=created")
}

func TestListIssueWorklogsVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListIssueWorklogsRequest{
		CloudId:      "my-cloud-id",
		IssueIdOrKey: "PROJ-7",
		StartAt:      0,
		MaxResults:   50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, "/ex/jira/my-cloud-id/rest/api/3/issue/PROJ-7/worklog")
	assert.Contains(t, u, "startAt=0")
	assert.Contains(t, u, "maxResults=50")
}

// --- HTTP methods ---

func TestSearchIssuesVendorRequest_HTTPMethod(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraSearchIssuesRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, vr.HTTPMethod())
}

func TestListProjectsVendorRequest_HTTPMethod(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListProjectsRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
}

// --- Marshal ---

func TestSearchIssuesVendorRequest_Marshal_IncludesJQLAndMaxResults(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId:    "cloud-123",
		Jql:        "project = PROJ",
		MaxResults: 100,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)
	require.NotNil(t, body)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	assert.Equal(t, "project = PROJ", parsed["jql"])
	assert.InDelta(t, float64(100), parsed["maxResults"], 0)
}

func TestSearchIssuesVendorRequest_Marshal_IncludesNextPageToken(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId:       "cloud-123",
		NextPageToken: "token-xyz",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	assert.Equal(t, "token-xyz", parsed["nextPageToken"])
}

func TestSearchIssuesVendorRequest_Marshal_IncludesFields(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraSearchIssuesRequest{
		CloudId: "cloud-123",
		Fields:  "summary,status,updated",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	fields, ok := parsed["fields"].([]any)
	require.True(t, ok, "fields should be an array")
	assert.Len(t, fields, 3)
}

func TestListProjectsVendorRequest_Marshal_NilForGET(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListProjectsRequest{})
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body, "GET requests should return nil body")
}

// --- Response unmarshaling ---

func TestSearchIssuesVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraSearchIssuesRequest{})
	require.NoError(t, err)

	resp := vr.GetResponse()
	require.NotNil(t, resp)

	body := []byte(`{
		"issues": [
			{"id": "10001", "key": "PROJ-1"},
			{"id": "10002", "key": "PROJ-2"}
		],
		"maxResults": 100,
		"nextPageToken": "page-token-abc"
	}`)

	msg, err := resp.Unmarshal(body)
	require.NoError(t, err)

	searchResp, ok := msg.(*connectorpb.JiraSearchIssuesResponse)
	require.True(t, ok)
	assert.Len(t, searchResp.GetIssues(), 2)
	assert.Equal(t, int32(100), searchResp.GetMaxResults())
	assert.Equal(t, "page-token-abc", searchResp.GetNextPageToken())
}

func TestListProjectsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListProjectsRequest{})
	require.NoError(t, err)

	resp := vr.GetResponse()
	require.NotNil(t, resp)

	body := []byte(`{
		"values": [
			{"id": "10001", "key": "PROJ", "name": "My Project"},
			{"id": "10002", "key": "BACK", "name": "Backend"}
		],
		"total": 2,
		"isLast": true
	}`)

	msg, err := resp.Unmarshal(body)
	require.NoError(t, err)

	projResp, ok := msg.(*connectorpb.JiraListProjectsResponse)
	require.True(t, ok)
	assert.Len(t, projResp.GetProjects(), 2)
	assert.Equal(t, int32(2), projResp.GetTotal())
	assert.True(t, projResp.GetIsLast())
}

func TestListIssueCommentsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListIssueCommentsRequest{})
	require.NoError(t, err)

	resp := vr.GetResponse()
	require.NotNil(t, resp)

	body := []byte(`{
		"comments": [
			{"id": "com-1", "body": {"type": "doc"}},
			{"id": "com-2", "body": {"type": "doc"}}
		],
		"total": 2,
		"startAt": 0,
		"maxResults": 50
	}`)

	msg, err := resp.Unmarshal(body)
	require.NoError(t, err)

	commentResp, ok := msg.(*connectorpb.JiraListIssueCommentsResponse)
	require.True(t, ok)
	assert.Len(t, commentResp.GetComments(), 2)
	assert.Equal(t, int32(2), commentResp.GetTotal())
	assert.Equal(t, int32(50), commentResp.GetMaxResults())
}

func TestListIssueWorklogsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListIssueWorklogsRequest{})
	require.NoError(t, err)

	resp := vr.GetResponse()
	require.NotNil(t, resp)

	body := []byte(`{
		"worklogs": [
			{"id": "10001", "timeSpentSeconds": 3600},
			{"id": "10002", "timeSpentSeconds": 7200}
		],
		"total": 2,
		"startAt": 0,
		"maxResults": 50
	}`)

	msg, err := resp.Unmarshal(body)
	require.NoError(t, err)

	worklogResp, ok := msg.(*connectorpb.JiraListIssueWorklogsResponse)
	require.True(t, ok)
	assert.Len(t, worklogResp.GetWorklogs(), 2)
	assert.Equal(t, int32(2), worklogResp.GetTotal())
}

func TestVendorResponse_Unmarshal_InvalidJSON(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraSearchIssuesRequest{})
	require.NoError(t, err)

	resp := vr.GetResponse()
	_, err = resp.Unmarshal([]byte(`{invalid json`))
	require.Error(t, err)
}

// --- List Boards (Agile API) ---

func TestNewJiraVendorRequest_ListBoards(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListBoardsRequest{
		CloudId:    "cloud-123",
		MaxResults: 50,
		Type:       "scrum",
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestListBoardsVendorRequest_HTTPMethod(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListBoardsRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
}

func TestListBoardsVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListBoardsRequest{
		CloudId:    "my-cloud-id",
		StartAt:    20,
		MaxResults: 50,
		Type:       "scrum",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, testBaseURL+"/ex/jira/")
	assert.Contains(t, u, "my-cloud-id")
	assert.Contains(t, u, "/rest/agile/1.0/board")
	assert.Contains(t, u, "startAt=20")
	assert.Contains(t, u, "maxResults=50")
	assert.Contains(t, u, "type=scrum")
}

func TestListBoardsVendorRequest_URL_OmitsEmptyTypeFilter(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListBoardsRequest{
		CloudId:    "my-cloud-id",
		MaxResults: 50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.NotContains(t, u, "type=", "empty type must not be serialised")
}

func TestListBoardsVendorRequest_URL_DefaultMaxResults(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListBoardsRequest{
		CloudId:    "my-cloud-id",
		MaxResults: 0,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.Contains(t, vr.URL(), "maxResults=50")
}

func TestListBoardsVendorRequest_Marshal_NilForGET(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListBoardsRequest{})
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body)
}

func TestListBoardsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListBoardsRequest{})
	require.NoError(t, err)

	body := []byte(`{
		"values": [
			{"id": 1, "name": "Backend Board", "type": "scrum",
			 "location": {"projectId": 10001, "projectKey": "PROJ"}},
			{"id": 2, "name": "Frontend Board", "type": "scrum",
			 "location": {"projectId": 10002, "projectKey": "WEB"}}
		],
		"startAt": 0,
		"maxResults": 50,
		"isLast": true,
		"total": 2
	}`)

	msg, err := vr.GetResponse().Unmarshal(body)
	require.NoError(t, err)

	resp, ok := msg.(*connectorpb.JiraListBoardsResponse)
	require.True(t, ok)
	assert.Len(t, resp.GetBoards(), 2)
	assert.Equal(t, int32(2), resp.GetTotal())
	assert.True(t, resp.GetIsLast())
}

// --- List Sprints (Agile API) ---

func TestNewJiraVendorRequest_ListSprints(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListSprintsRequest{
		CloudId: "cloud-123",
		BoardId: 42,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotNil(t, vr)
}

func TestListSprintsVendorRequest_HTTPMethod(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListSprintsRequest{})
	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
}

func TestListSprintsVendorRequest_URL(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListSprintsRequest{
		CloudId:    "my-cloud-id",
		BoardId:    42,
		StartAt:    10,
		MaxResults: 50,
		State:      "active,closed",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, "/ex/jira/my-cloud-id/rest/agile/1.0/board/42/sprint")
	assert.Contains(t, u, "startAt=10")
	assert.Contains(t, u, "maxResults=50")
	// url.Values encodes the comma so we look for the key and "active" substring.
	assert.Contains(t, u, "state=")
	assert.Contains(t, u, "active")
	assert.Contains(t, u, "closed")
}

func TestListSprintsVendorRequest_URL_OmitsEmptyStateFilter(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListSprintsRequest{
		CloudId: "my-cloud-id",
		BoardId: 7,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.NotContains(t, vr.URL(), "state=", "empty state must not be serialised")
}

func TestListSprintsVendorRequest_URL_DefaultMaxResults(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())

	req := &connectorpb.JiraListSprintsRequest{
		CloudId:    "my-cloud-id",
		BoardId:    7,
		MaxResults: 0,
	}

	vr, err := factory(req)
	require.NoError(t, err)
	assert.Contains(t, vr.URL(), "maxResults=50")
}

func TestListSprintsVendorRequest_Marshal_NilForGET(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListSprintsRequest{})
	require.NoError(t, err)

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body)
}

func TestListSprintsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewJiraVendorRequest(testBaseURLs())
	vr, err := factory(&connectorpb.JiraListSprintsRequest{})
	require.NoError(t, err)

	body := []byte(`{
		"values": [
			{"id": 11, "state": "active", "name": "Sprint 23", "originBoardId": 1,
			 "startDate": "2026-04-01T00:00:00.000Z",
			 "endDate": "2026-04-14T00:00:00.000Z"},
			{"id": 12, "state": "future", "name": "Sprint 24", "originBoardId": 1}
		],
		"startAt": 0,
		"maxResults": 50,
		"isLast": true,
		"total": 2
	}`)

	msg, err := vr.GetResponse().Unmarshal(body)
	require.NoError(t, err)

	resp, ok := msg.(*connectorpb.JiraListSprintsResponse)
	require.True(t, ok)
	assert.Len(t, resp.GetSprints(), 2)
	assert.Equal(t, int32(2), resp.GetTotal())
	assert.True(t, resp.GetIsLast())
}

// --- Base URL trimming ---

func TestNewJiraVendorRequest_TrimsTrailingSlash(t *testing.T) {
	baseURLs := map[string]string{
		constants.GroupJiraREST: "https://api.atlassian.com/",
	}
	factory := NewJiraVendorRequest(baseURLs)

	vr, err := factory(&connectorpb.JiraListProjectsRequest{
		CloudId: "c123",
	})
	require.NoError(t, err)

	u := vr.URL()
	// Should not have double slashes
	assert.NotContains(t, u, "//ex", "trailing slash on base URL should be trimmed: %s", u)
}

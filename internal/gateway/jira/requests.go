// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// --- Search Issues (POST /ex/jira/{cloudId}/rest/api/3/search/jql) ---

type searchIssuesVendorRequest struct {
	req     *connectorpb.JiraSearchIssuesRequest
	baseURL string
}

func (r *searchIssuesVendorRequest) HTTPMethod() string { return http.MethodPost }

func (r *searchIssuesVendorRequest) SetHeaders(req *http.Request) *http.Request {
	req.Header.Set("Content-Type", "application/json")
	return req
}

func (r *searchIssuesVendorRequest) URL() string {
	return fmt.Sprintf("%s/ex/jira/%s/rest/api/3/search/jql",
		r.baseURL, url.PathEscape(r.req.GetCloudId()))
}

func (r *searchIssuesVendorRequest) Marshal() ([]byte, error) {
	body := map[string]any{
		"jql":        r.req.GetJql(),
		"maxResults": r.req.GetMaxResults(),
	}
	if expand := r.req.GetExpand(); expand != "" {
		body["expand"] = expand
	}
	if fields := r.req.GetFields(); fields != "" {
		body["fields"] = strings.Split(fields, ",")
	}
	if token := r.req.GetNextPageToken(); token != "" {
		body["nextPageToken"] = token
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshaling search-issues request body: %w", err)
	}
	return raw, nil
}

func (r *searchIssuesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &searchIssuesVendorResponse{}
}

type searchIssuesVendorResponse struct{}

func (r *searchIssuesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Issues        []json.RawMessage `json:"issues"`
		MaxResults    int32             `json:"maxResults"`
		NextPageToken string            `json:"nextPageToken"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraSearchIssuesResponse{
		MaxResults:    raw.MaxResults,
		NextPageToken: raw.NextPageToken,
	}
	for _, v := range raw.Issues {
		resp.Issues = append(resp.Issues, v)
	}
	return resp, nil
}

// --- List Projects (GET /ex/jira/{cloudId}/rest/api/3/project/search) ---

type listProjectsVendorRequest struct {
	req     *connectorpb.JiraListProjectsRequest
	baseURL string
}

func (r *listProjectsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listProjectsVendorRequest) URL() string {
	startAt := r.req.GetStartAt()
	maxResults := r.req.GetMaxResults()
	if maxResults == 0 {
		maxResults = 50
	}

	qv := url.Values{}
	qv.Set("startAt", fmt.Sprintf("%d", startAt))
	qv.Set("maxResults", fmt.Sprintf("%d", maxResults))

	return fmt.Sprintf("%s/ex/jira/%s/rest/api/3/project/search?%s",
		r.baseURL, url.PathEscape(r.req.GetCloudId()), qv.Encode())
}

func (r *listProjectsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listProjectsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listProjectsVendorResponse{}
}

type listProjectsVendorResponse struct{}

func (r *listProjectsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Total  int32             `json:"total"`
		IsLast bool              `json:"isLast"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraListProjectsResponse{
		Total:  raw.Total,
		IsLast: raw.IsLast,
	}
	for _, v := range raw.Values {
		resp.Projects = append(resp.Projects, v)
	}
	return resp, nil
}

// --- List Issue Comments (GET /ex/jira/{cloudId}/rest/api/3/issue/{issueIdOrKey}/comment) ---

type listIssueCommentsVendorRequest struct {
	req     *connectorpb.JiraListIssueCommentsRequest
	baseURL string
}

func (r *listIssueCommentsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listIssueCommentsVendorRequest) URL() string {
	maxResults := r.req.GetMaxResults()
	if maxResults == 0 {
		maxResults = 50
	}

	qv := url.Values{}
	qv.Set("startAt", fmt.Sprintf("%d", r.req.GetStartAt()))
	qv.Set("maxResults", fmt.Sprintf("%d", maxResults))
	if orderBy := r.req.GetOrderBy(); orderBy != "" {
		qv.Set("orderBy", orderBy)
	}

	return fmt.Sprintf("%s/ex/jira/%s/rest/api/3/issue/%s/comment?%s",
		r.baseURL, url.PathEscape(r.req.GetCloudId()),
		url.PathEscape(r.req.GetIssueIdOrKey()), qv.Encode())
}

func (r *listIssueCommentsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listIssueCommentsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listIssueCommentsVendorResponse{}
}

type listIssueCommentsVendorResponse struct{}

func (r *listIssueCommentsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Comments   []json.RawMessage `json:"comments"`
		Total      int32             `json:"total"`
		StartAt    int32             `json:"startAt"`
		MaxResults int32             `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraListIssueCommentsResponse{
		Total:      raw.Total,
		StartAt:    raw.StartAt,
		MaxResults: raw.MaxResults,
	}
	for _, v := range raw.Comments {
		resp.Comments = append(resp.Comments, v)
	}
	return resp, nil
}

// --- List Issue Worklogs (GET /ex/jira/{cloudId}/rest/api/3/issue/{issueIdOrKey}/worklog) ---

type listIssueWorklogsVendorRequest struct {
	req     *connectorpb.JiraListIssueWorklogsRequest
	baseURL string
}

func (r *listIssueWorklogsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listIssueWorklogsVendorRequest) URL() string {
	maxResults := r.req.GetMaxResults()
	if maxResults == 0 {
		maxResults = 50
	}

	qv := url.Values{}
	qv.Set("startAt", fmt.Sprintf("%d", r.req.GetStartAt()))
	qv.Set("maxResults", fmt.Sprintf("%d", maxResults))

	return fmt.Sprintf("%s/ex/jira/%s/rest/api/3/issue/%s/worklog?%s",
		r.baseURL, url.PathEscape(r.req.GetCloudId()),
		url.PathEscape(r.req.GetIssueIdOrKey()), qv.Encode())
}

func (r *listIssueWorklogsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listIssueWorklogsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listIssueWorklogsVendorResponse{}
}

type listIssueWorklogsVendorResponse struct{}

func (r *listIssueWorklogsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Worklogs   []json.RawMessage `json:"worklogs"`
		Total      int32             `json:"total"`
		StartAt    int32             `json:"startAt"`
		MaxResults int32             `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraListIssueWorklogsResponse{
		Total:      raw.Total,
		StartAt:    raw.StartAt,
		MaxResults: raw.MaxResults,
	}
	for _, v := range raw.Worklogs {
		resp.Worklogs = append(resp.Worklogs, v)
	}
	return resp, nil
}

// --- List Boards (Agile API — GET /ex/jira/{cloudId}/rest/agile/1.0/board) ---

type listBoardsVendorRequest struct {
	req     *connectorpb.JiraListBoardsRequest
	baseURL string
}

func (r *listBoardsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listBoardsVendorRequest) URL() string {
	maxResults := r.req.GetMaxResults()
	if maxResults == 0 {
		maxResults = 50
	}

	qv := url.Values{}
	qv.Set("startAt", fmt.Sprintf("%d", r.req.GetStartAt()))
	qv.Set("maxResults", fmt.Sprintf("%d", maxResults))
	if t := r.req.GetType(); t != "" {
		qv.Set("type", t)
	}

	return fmt.Sprintf("%s/ex/jira/%s/rest/agile/1.0/board?%s",
		r.baseURL, url.PathEscape(r.req.GetCloudId()), qv.Encode())
}

func (r *listBoardsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listBoardsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listBoardsVendorResponse{}
}

type listBoardsVendorResponse struct{}

func (r *listBoardsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values     []json.RawMessage `json:"values"`
		StartAt    int32             `json:"startAt"`
		MaxResults int32             `json:"maxResults"`
		IsLast     bool              `json:"isLast"`
		Total      int32             `json:"total"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraListBoardsResponse{
		StartAt:    raw.StartAt,
		MaxResults: raw.MaxResults,
		IsLast:     raw.IsLast,
		Total:      raw.Total,
	}
	for _, v := range raw.Values {
		resp.Boards = append(resp.Boards, v)
	}
	return resp, nil
}

// --- List Sprints for a Board (Agile API — GET /ex/jira/{cloudId}/rest/agile/1.0/board/{boardId}/sprint) ---

type listSprintsVendorRequest struct {
	req     *connectorpb.JiraListSprintsRequest
	baseURL string
}

func (r *listSprintsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listSprintsVendorRequest) URL() string {
	maxResults := r.req.GetMaxResults()
	if maxResults == 0 {
		maxResults = 50
	}

	qv := url.Values{}
	qv.Set("startAt", fmt.Sprintf("%d", r.req.GetStartAt()))
	qv.Set("maxResults", fmt.Sprintf("%d", maxResults))
	if s := r.req.GetState(); s != "" {
		qv.Set("state", s)
	}

	return fmt.Sprintf("%s/ex/jira/%s/rest/agile/1.0/board/%d/sprint?%s",
		r.baseURL, url.PathEscape(r.req.GetCloudId()), r.req.GetBoardId(), qv.Encode())
}

func (r *listSprintsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listSprintsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listSprintsVendorResponse{}
}

type listSprintsVendorResponse struct{}

func (r *listSprintsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values     []json.RawMessage `json:"values"`
		StartAt    int32             `json:"startAt"`
		MaxResults int32             `json:"maxResults"`
		IsLast     bool              `json:"isLast"`
		Total      int32             `json:"total"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding jira response: %w", err)
	}
	resp := &connectorpb.JiraListSprintsResponse{
		StartAt:    raw.StartAt,
		MaxResults: raw.MaxResults,
		IsLast:     raw.IsLast,
		Total:      raw.Total,
	}
	for _, v := range raw.Values {
		resp.Sprints = append(resp.Sprints, v)
	}
	return resp, nil
}

// --- Factory ---

// NewJiraVendorRequest returns a SyncRequestFactory that maps proto messages
// to Jira vendor requests. Auth is handled by the autoRefreshDoer in the
// HTTP transport layer, so request types don't set auth headers.
func NewJiraVendorRequest(baseURLs map[string]string) vendorapi.SyncRequestFactory {
	restBaseURL := strings.TrimSuffix(baseURLs[constants.GroupJiraREST], "/")

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		switch r := req.(type) {
		case *connectorpb.JiraSearchIssuesRequest:
			return &searchIssuesVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.JiraListProjectsRequest:
			return &listProjectsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.JiraListIssueCommentsRequest:
			return &listIssueCommentsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.JiraListIssueWorklogsRequest:
			return &listIssueWorklogsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.JiraListBoardsRequest:
			return &listBoardsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.JiraListSprintsRequest:
			return &listSprintsVendorRequest{req: r, baseURL: restBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

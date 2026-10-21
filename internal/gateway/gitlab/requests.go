// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

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

// --- List Projects ---

type listProjectsVendorRequest struct {
	req     *connectorpb.GitLabListProjectsRequest
	baseURL string
}

func (r *listProjectsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listProjectsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	qv.Set("include_subgroups", "false")
	qv.Set("with_shared", "false")
	return fmt.Sprintf("%s/groups/%d/projects?%s",
		r.baseURL, r.req.GetGroupId(), qv.Encode())
}

func (r *listProjectsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listProjectsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listProjectsVendorResponse{}
}

type listProjectsVendorResponse struct{}

func (r *listProjectsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListProjectsResponse{
		GitLabListProjectsResponse: &connectorpb.GitLabListProjectsResponse{},
	}
	for _, v := range raw {
		resp.Projects = append(resp.Projects, v)
	}
	return resp, nil
}

// gitLabListProjectsResponse wraps the generated proto type and implements
// nextPageSetter so Execute can inject the X-Next-Page header value.
type gitLabListProjectsResponse struct {
	*connectorpb.GitLabListProjectsResponse
}

func (r *gitLabListProjectsResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListProjectsResponse) unwrap() proto.Message {
	return r.GitLabListProjectsResponse
}

// --- List Commits ---

type listCommitsVendorRequest struct {
	req     *connectorpb.GitLabListCommitsRequest
	baseURL string
}

func (r *listCommitsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listCommitsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	if ts := r.req.GetSince(); ts != "" {
		qv.Set("since", ts)
	}
	if rn := r.req.GetRefName(); rn != "" {
		qv.Set("ref_name", rn)
	}
	if r.req.GetFirstParent() {
		qv.Set("first_parent", "true")
	}
	return fmt.Sprintf("%s/projects/%d/repository/commits?%s",
		r.baseURL, r.req.GetProjectId(), qv.Encode())
}

func (r *listCommitsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listCommitsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listCommitsVendorResponse{}
}

type listCommitsVendorResponse struct{}

func (r *listCommitsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListCommitsResponse{
		GitLabListCommitsResponse: &connectorpb.GitLabListCommitsResponse{},
	}
	for _, v := range raw {
		resp.Commits = append(resp.Commits, v)
	}
	return resp, nil
}

// gitLabListCommitsResponse wraps the generated proto type and implements
// nextPageSetter so Execute can inject the X-Next-Page header value.
type gitLabListCommitsResponse struct {
	*connectorpb.GitLabListCommitsResponse
}

func (r *gitLabListCommitsResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListCommitsResponse) unwrap() proto.Message {
	return r.GitLabListCommitsResponse
}

// --- List Issues ---

type listIssuesVendorRequest struct {
	req     *connectorpb.GitLabListIssuesRequest
	baseURL string
}

func (r *listIssuesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listIssuesVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	if ts := r.req.GetUpdatedAfter(); ts != "" {
		qv.Set("updated_after", ts)
	}
	return fmt.Sprintf("%s/groups/%d/issues?%s",
		r.baseURL, r.req.GetGroupId(), qv.Encode())
}

func (r *listIssuesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listIssuesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listIssuesVendorResponse{}
}

type listIssuesVendorResponse struct{}

func (r *listIssuesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListIssuesResponse{
		GitLabListIssuesResponse: &connectorpb.GitLabListIssuesResponse{},
	}
	for _, v := range raw {
		resp.Issues = append(resp.Issues, v)
	}
	return resp, nil
}

// gitLabListIssuesResponse wraps the generated proto type and implements
// nextPageSetter so Execute can inject the X-Next-Page header value.
type gitLabListIssuesResponse struct {
	*connectorpb.GitLabListIssuesResponse
}

func (r *gitLabListIssuesResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListIssuesResponse) unwrap() proto.Message {
	return r.GitLabListIssuesResponse
}

// --- List Notes ---

type listNotesVendorRequest struct {
	req     *connectorpb.GitLabListNotesRequest
	baseURL string
}

func (r *listNotesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listNotesVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	if ts := r.req.GetUpdatedAfter(); ts != "" {
		qv.Set("updated_after", ts)
	}
	return fmt.Sprintf("%s/projects/%d/merge_requests/%d/notes?%s",
		r.baseURL, r.req.GetProjectId(), r.req.GetMergeRequestIid(), qv.Encode())
}

func (r *listNotesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listNotesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listNotesVendorResponse{}
}

type listNotesVendorResponse struct{}

func (r *listNotesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListNotesResponse{
		GitLabListNotesResponse: &connectorpb.GitLabListNotesResponse{},
	}
	for _, v := range raw {
		resp.Notes = append(resp.Notes, v)
	}
	return resp, nil
}

// gitLabListNotesResponse wraps the generated proto type and implements
// nextPageSetter so Execute can inject the X-Next-Page header value.
type gitLabListNotesResponse struct {
	*connectorpb.GitLabListNotesResponse
}

func (r *gitLabListNotesResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListNotesResponse) unwrap() proto.Message {
	return r.GitLabListNotesResponse
}

// --- List Group Members ---

type listGroupMembersVendorRequest struct {
	req     *connectorpb.GitLabListGroupMembersRequest
	baseURL string
}

func (r *listGroupMembersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listGroupMembersVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	// /all includes inherited members (subgroup members visible at parent level)
	return fmt.Sprintf("%s/groups/%d/members/all?%s",
		r.baseURL, r.req.GetGroupId(), qv.Encode())
}

func (r *listGroupMembersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listGroupMembersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listGroupMembersVendorResponse{}
}

type listGroupMembersVendorResponse struct{}

func (r *listGroupMembersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListGroupMembersResponse{
		GitLabListGroupMembersResponse: &connectorpb.GitLabListGroupMembersResponse{},
	}
	for _, v := range raw {
		resp.Members = append(resp.Members, v)
	}
	return resp, nil
}

type gitLabListGroupMembersResponse struct {
	*connectorpb.GitLabListGroupMembersResponse
}

func (r *gitLabListGroupMembersResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListGroupMembersResponse) unwrap() proto.Message {
	return r.GitLabListGroupMembersResponse
}

// --- List Project Merge Requests ---

type listProjectMergeRequestsVendorRequest struct {
	req     *connectorpb.GitLabListProjectMergeRequestsRequest
	baseURL string
}

func (r *listProjectMergeRequestsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listProjectMergeRequestsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	if ts := r.req.GetUpdatedAfter(); ts != "" {
		qv.Set("updated_after", ts)
	}
	if r.req.GetWithStats() {
		qv.Set("with_merge_status_recheck", "true")
	}
	// order_by/sort must match the updated_after filter field, ascending, for
	// page-cursor resume (fetch_prs/fetch_prs_data's repoCursorState.PageCursor)
	// to be safe. GitLab's default is order_by=created_at&sort=desc — a
	// different field from the updated_after filter, so an MR that newly starts
	// matching the filter between an interrupted run and its resume can appear
	// at any page position (not appended at the end), shifting content across
	// already-fetched pages and permanently skipping MRs at the resume boundary.
	// Ascending updated_at guarantees a newly-qualifying MR always sorts after
	// whatever page the resume is currently on.
	qv.Set("order_by", "updated_at")
	qv.Set("sort", "asc")
	return fmt.Sprintf("%s/projects/%d/merge_requests?%s",
		r.baseURL, r.req.GetProjectId(), qv.Encode())
}

func (r *listProjectMergeRequestsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listProjectMergeRequestsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listProjectMergeRequestsVendorResponse{}
}

type listProjectMergeRequestsVendorResponse struct{}

func (r *listProjectMergeRequestsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListProjectMergeRequestsResponse{
		GitLabListProjectMergeRequestsResponse: &connectorpb.GitLabListProjectMergeRequestsResponse{},
	}
	for _, v := range raw {
		resp.MergeRequests = append(resp.MergeRequests, v)
	}
	return resp, nil
}

type gitLabListProjectMergeRequestsResponse struct {
	*connectorpb.GitLabListProjectMergeRequestsResponse
}

func (r *gitLabListProjectMergeRequestsResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListProjectMergeRequestsResponse) unwrap() proto.Message {
	return r.GitLabListProjectMergeRequestsResponse
}

// --- Get Commit Detail ---

type getCommitDetailVendorRequest struct {
	req     *connectorpb.GitLabGetCommitDetailRequest
	baseURL string
}

func (r *getCommitDetailVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getCommitDetailVendorRequest) URL() string {
	return fmt.Sprintf("%s/projects/%d/repository/commits/%s?stats=true",
		r.baseURL, r.req.GetProjectId(), url.PathEscape(r.req.GetSha()))
}

func (r *getCommitDetailVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getCommitDetailVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getCommitDetailVendorResponse{}
}

type getCommitDetailVendorResponse struct{}

func (r *getCommitDetailVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabGetCommitDetailResponse{Commit: body}, nil
}

// --- List MR Diffs ---

type listMRDiffsVendorRequest struct {
	req     *connectorpb.GitLabListMRDiffsRequest
	baseURL string
}

func (r *listMRDiffsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listMRDiffsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/projects/%d/merge_requests/%d/diffs?%s",
		r.baseURL, r.req.GetProjectId(), r.req.GetMergeRequestIid(), qv.Encode())
}

func (r *listMRDiffsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listMRDiffsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listMRDiffsVendorResponse{}
}

type listMRDiffsVendorResponse struct{}

func (r *listMRDiffsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &gitLabListMRDiffsResponse{
		GitLabListMRDiffsResponse: &connectorpb.GitLabListMRDiffsResponse{},
	}
	for _, v := range raw {
		resp.Diffs = append(resp.Diffs, v)
	}
	return resp, nil
}

type gitLabListMRDiffsResponse struct {
	*connectorpb.GitLabListMRDiffsResponse
}

func (r *gitLabListMRDiffsResponse) setNextPageURL(u string) {
	r.NextPageUrl = u
}

func (r *gitLabListMRDiffsResponse) unwrap() proto.Message {
	return r.GitLabListMRDiffsResponse
}

// --- List MR Closes-Issues ---

type listMRClosesIssuesVendorRequest struct {
	req     *connectorpb.GitLabListMRClosesIssuesRequest
	baseURL string
}

func (r *listMRClosesIssuesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listMRClosesIssuesVendorRequest) URL() string {
	// Deliberate single-page cap: this fetches one page of up to 100 issues and
	// does not follow X-Next-Page, so an MR that closes >100 issues loses the
	// overflow. Native linkage is best-effort (an unlinked issue degrades
	// gracefully to regex linkage), and >100 keyword-closing refs on one MR is
	// pathological, so the extra pagination round trips are not worth their cost
	// on GitLab's tightest rate budget.
	return fmt.Sprintf("%s/projects/%d/merge_requests/%d/closes_issues?per_page=100",
		r.baseURL, r.req.GetProjectId(), r.req.GetMergeRequestIid())
}

func (r *listMRClosesIssuesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listMRClosesIssuesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listMRClosesIssuesVendorResponse{}
}

type listMRClosesIssuesVendorResponse struct{}

func (r *listMRClosesIssuesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabListMRClosesIssuesResponse{Issues: body}, nil
}

// --- List MR Approvals ---

type listMRApprovalsVendorRequest struct {
	req     *connectorpb.GitLabListMRApprovalsRequest
	baseURL string
}

func (r *listMRApprovalsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listMRApprovalsVendorRequest) URL() string {
	return fmt.Sprintf("%s/projects/%d/merge_requests/%d/approvals",
		r.baseURL, r.req.GetProjectId(), r.req.GetIid())
}

func (r *listMRApprovalsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listMRApprovalsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listMRApprovalsVendorResponse{}
}

type listMRApprovalsVendorResponse struct{}

func (r *listMRApprovalsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabListMRApprovalsResponse{Approvals: body}, nil
}

// --- List Subgroups ---

type listSubgroupsVendorRequest struct {
	req     *connectorpb.GitLabListSubgroupsRequest
	baseURL string
}

func (r *listSubgroupsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listSubgroupsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/groups/%d/subgroups?%s",
		r.baseURL, r.req.GetGroupId(), qv.Encode())
}

func (r *listSubgroupsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listSubgroupsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listSubgroupsVendorResponse{}
}

type listSubgroupsVendorResponse struct{}

func (r *listSubgroupsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("unmarshal gitlab response: %w", err)
	}
	raw := make([][]byte, len(items))
	for i, item := range items {
		raw[i] = item
	}
	return &connectorpb.GitLabListSubgroupsResponse{Subgroups: raw}, nil
}

// --- List Group Direct Members ---

type listGroupDirectMembersVendorRequest struct {
	req     *connectorpb.GitLabListGroupDirectMembersRequest
	baseURL string
}

func (r *listGroupDirectMembersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listGroupDirectMembersVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	// /members (not /members/all) — returns only direct members, not inherited.
	return fmt.Sprintf("%s/groups/%d/members?%s",
		r.baseURL, r.req.GetGroupId(), qv.Encode())
}

func (r *listGroupDirectMembersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listGroupDirectMembersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listGroupDirectMembersVendorResponse{}
}

type listGroupDirectMembersVendorResponse struct{}

func (r *listGroupDirectMembersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("unmarshal gitlab response: %w", err)
	}
	raw := make([][]byte, len(items))
	for i, item := range items {
		raw[i] = item
	}
	return &connectorpb.GitLabListGroupDirectMembersResponse{Members: raw}, nil
}

// --- Get User ---

type getUserVendorRequest struct {
	req     *connectorpb.GitLabGetUserRequest
	baseURL string
}

func (r *getUserVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getUserVendorRequest) URL() string {
	return fmt.Sprintf("%s/users/%d", r.baseURL, r.req.GetUserId())
}

func (r *getUserVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getUserVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getUserVendorResponse{}
}

type getUserVendorResponse struct{}

func (r *getUserVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabGetUserResponse{User: body}, nil
}

// --- Get Project (config capture) ---

type getProjectVendorRequest struct {
	req     *connectorpb.GitLabGetProjectRequest
	baseURL string
}

func (r *getProjectVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *getProjectVendorRequest) URL() string {
	return fmt.Sprintf("%s/projects/%d", r.baseURL, r.req.GetProjectId())
}
func (r *getProjectVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *getProjectVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getProjectVendorResponse{}
}

type getProjectVendorResponse struct{}

func (r *getProjectVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabGetProjectResponse{RawJson: body}, nil
}

// --- Get Group (config capture) ---

type getGroupVendorRequest struct {
	req     *connectorpb.GitLabGetGroupRequest
	baseURL string
}

func (r *getGroupVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *getGroupVendorRequest) URL() string {
	return fmt.Sprintf("%s/groups/%d", r.baseURL, r.req.GetGroupId())
}
func (r *getGroupVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *getGroupVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getGroupVendorResponse{}
}

type getGroupVendorResponse struct{}

func (r *getGroupVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabGetGroupResponse{RawJson: body}, nil
}

// --- Get Project Approvals (config capture) ---

type getProjectApprovalsVendorRequest struct {
	req     *connectorpb.GitLabGetProjectApprovalsRequest
	baseURL string
}

func (r *getProjectApprovalsVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *getProjectApprovalsVendorRequest) URL() string {
	return fmt.Sprintf("%s/projects/%d/approvals", r.baseURL, r.req.GetProjectId())
}
func (r *getProjectApprovalsVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *getProjectApprovalsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getProjectApprovalsVendorResponse{}
}

type getProjectApprovalsVendorResponse struct{}

func (r *getProjectApprovalsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GitLabGetProjectApprovalsResponse{RawJson: body}, nil
}

// --- List Protected Branches (config capture) ---

type listProtectedBranchesVendorRequest struct {
	req     *connectorpb.GitLabListProtectedBranchesRequest
	baseURL string
}

func (r *listProtectedBranchesVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *listProtectedBranchesVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/projects/%d/protected_branches?%s", r.baseURL, r.req.GetProjectId(), qv.Encode())
}
func (r *listProtectedBranchesVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *listProtectedBranchesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listProtectedBranchesVendorResponse{}
}

type listProtectedBranchesVendorResponse struct{}

func (r *listProtectedBranchesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling protected branches: %w", err)
	}
	resp := &connectorpb.GitLabListProtectedBranchesResponse{}
	for _, v := range raw {
		resp.ProtectedBranches = append(resp.ProtectedBranches, v)
	}
	return resp, nil
}

// --- List MR Commits ---

// listMRCommitsVendorRequest fetches commits for a single MR.
// Used for first_commit_at enrichment — first_commit_at write side.
type listMRCommitsVendorRequest struct {
	req     *connectorpb.GitLabListMRCommitsRequest
	baseURL string
}

func (r *listMRCommitsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listMRCommitsVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 1
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	if ob := r.req.GetOrderBy(); ob != "" {
		qv.Set("order_by", ob)
	}
	if s := r.req.GetSort(); s != "" {
		qv.Set("sort", s)
	}
	return fmt.Sprintf("%s/projects/%s/merge_requests/%d/commits?%s",
		r.baseURL,
		url.PathEscape(r.req.GetProjectId()),
		r.req.GetMrIid(),
		qv.Encode())
}

func (r *listMRCommitsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listMRCommitsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listMRCommitsVendorResponse{}
}

type listMRCommitsVendorResponse struct{}

func (r *listMRCommitsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}
	resp := &connectorpb.GitLabListMRCommitsResponse{}
	for _, v := range raw {
		resp.Commits = append(resp.Commits, v)
	}
	return resp, nil
}

// --- Factory ---

// NewGitLabVendorRequest returns a SyncRequestFactory that maps proto messages
// to GitLab vendor requests. Auth is handled by the autoRefreshDoer in the
// HTTP transport layer, so request types don't set auth headers.
func NewGitLabVendorRequest(baseURLs map[string]string) vendorapi.SyncRequestFactory {
	restBaseURL := strings.TrimSuffix(baseURLs[constants.GroupGitLabREST], "/")

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		switch r := req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return &listProjectsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListProjectMergeRequestsRequest:
			return &listProjectMergeRequestsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListCommitsRequest:
			return &listCommitsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabGetCommitDetailRequest:
			return &getCommitDetailVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListIssuesRequest:
			return &listIssuesVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListNotesRequest:
			return &listNotesVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListGroupMembersRequest:
			return &listGroupMembersVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListMRDiffsRequest:
			return &listMRDiffsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListMRClosesIssuesRequest:
			return &listMRClosesIssuesVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListMRApprovalsRequest:
			return &listMRApprovalsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListSubgroupsRequest:
			return &listSubgroupsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListGroupDirectMembersRequest:
			return &listGroupDirectMembersVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabGetUserRequest:
			return &getUserVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListMRCommitsRequest:
			return &listMRCommitsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabGetProjectRequest:
			return &getProjectVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabGetGroupRequest:
			return &getGroupVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabGetProjectApprovalsRequest:
			return &getProjectApprovalsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GitLabListProtectedBranchesRequest:
			return &listProtectedBranchesVendorRequest{req: r, baseURL: restBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

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

// --- List Repos ---

type listReposVendorRequest struct {
	req     *connectorpb.BitbucketListReposRequest
	baseURL string
}

func (r *listReposVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listReposVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s?%s",
		r.baseURL, url.PathEscape(r.req.GetWorkspace()), qv.Encode())
}

func (r *listReposVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listReposVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listReposVendorResponse{}
}

type listReposVendorResponse struct{}

func (r *listReposVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListReposResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Repos = append(resp.Repos, v)
	}
	return resp, nil
}

// --- Get Single Repository ---

type getRepoVendorRequest struct {
	req     *connectorpb.BitbucketGetRepoRequest
	baseURL string
}

func (r *getRepoVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getRepoVendorRequest) URL() string {
	return fmt.Sprintf("%s/repositories/%s/%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()))
}

func (r *getRepoVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getRepoVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getRepoVendorResponse{}
}

type getRepoVendorResponse struct{}

func (r *getRepoVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	// The single-repo endpoint returns one repository object directly (no
	// `values` envelope) — the same shape as one element of the workspace
	// listing, so the connector's bbRepository unmarshals it unchanged.
	return &connectorpb.BitbucketGetRepoResponse{Repo: body}, nil
}

// --- Get Branch (config capture: merge strategies live on the branch object) ---

type getBranchVendorRequest struct {
	req     *connectorpb.BitbucketGetBranchRequest
	baseURL string
}

func (r *getBranchVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getBranchVendorRequest) URL() string {
	return fmt.Sprintf("%s/repositories/%s/%s/refs/branches/%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		url.PathEscape(r.req.GetBranch()))
}

func (r *getBranchVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getBranchVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getBranchVendorResponse{}
}

type getBranchVendorResponse struct{}

func (r *getBranchVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.BitbucketGetBranchResponse{RawJson: body}, nil
}

// --- List Branch Restrictions (config capture; needs repository:admin) ---

type listBranchRestrictionsVendorRequest struct {
	req     *connectorpb.BitbucketListBranchRestrictionsRequest
	baseURL string
}

func (r *listBranchRestrictionsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listBranchRestrictionsVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s/%s/branch-restrictions?%s",
		r.baseURL, url.PathEscape(r.req.GetWorkspace()), url.PathEscape(r.req.GetRepoSlug()), qv.Encode())
}

func (r *listBranchRestrictionsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listBranchRestrictionsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listBranchRestrictionsVendorResponse{}
}

type listBranchRestrictionsVendorResponse struct{}

func (r *listBranchRestrictionsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling branch restrictions: %w", err)
	}
	resp := &connectorpb.BitbucketListBranchRestrictionsResponse{NextPageUrl: raw.Next}
	for _, v := range raw.Values {
		resp.Restrictions = append(resp.Restrictions, v)
	}
	return resp, nil
}

// --- List Pull Requests ---

type listPullRequestsVendorRequest struct {
	req     *connectorpb.BitbucketListPullRequestsRequest
	baseURL string
}

func (r *listPullRequestsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPullRequestsVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 50
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))

	// Map proto enum to Bitbucket API state string.
	// When UNSPECIFIED, explicitly request all states because Bitbucket defaults
	// to returning only OPEN PRs when no state parameter is provided.
	if state := prStateString(r.req.GetState()); state != "" {
		qv.Set("state", state)
	} else {
		qv.Add("state", "OPEN")
		qv.Add("state", "MERGED")
		qv.Add("state", "DECLINED")
		qv.Add("state", "SUPERSEDED")
	}

	// Sort by updated_on descending so cursor-based incremental sync
	// (which stops at the first PR older than the cursor) works correctly.
	qv.Set("sort", "-updated_on")

	// Bitbucket's slim list response omits `participants` and `reviewers`. The
	// additive `+` field syntax tells the API to extend the default field set
	// for each value in the page, which is the only way to surface those
	// arrays without an N+1 per-PR GET. Without this, `reviews_requested`
	// metric input is permanently null.
	qv.Set("fields", "+values.participants,+values.reviewers")

	return fmt.Sprintf("%s/repositories/%s/%s/pullrequests?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		qv.Encode())
}

func prStateString(state connectorpb.BitbucketPullRequestState) string {
	switch state {
	case connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_OPEN:
		return "OPEN"
	case connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_MERGED:
		return "MERGED"
	case connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_DECLINED:
		return "DECLINED"
	case connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_SUPERSEDED:
		return "SUPERSEDED"
	default:
		return ""
	}
}

func (r *listPullRequestsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPullRequestsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listPullRequestsVendorResponse{}
}

type listPullRequestsVendorResponse struct{}

func (r *listPullRequestsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListPullRequestsResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.PullRequests = append(resp.PullRequests, v)
	}
	return resp, nil
}

// --- List Commits ---

type listCommitsVendorRequest struct {
	req     *connectorpb.BitbucketListCommitsRequest
	baseURL string
}

func (r *listCommitsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listCommitsVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	if inc := r.req.GetInclude(); inc != "" {
		qv.Set("include", inc)
	}
	return fmt.Sprintf("%s/repositories/%s/%s/commits?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		qv.Encode())
}

func (r *listCommitsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listCommitsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listCommitsVendorResponse{}
}

type listCommitsVendorResponse struct{}

func (r *listCommitsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListCommitsResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Commits = append(resp.Commits, v)
	}
	return resp, nil
}

// --- Get User ---

type getUserVendorRequest struct {
	baseURL string
}

func (r *getUserVendorRequest) HTTPMethod() string       { return http.MethodGet }
func (r *getUserVendorRequest) URL() string              { return r.baseURL + "/user" }
func (r *getUserVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getUserVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getUserVendorResponse{}
}

type getUserVendorResponse struct{}

func (r *getUserVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.BitbucketGetUserResponse{User: body}, nil
}

// --- Get PR Diffstat ---

type getPRDiffstatVendorRequest struct {
	req     *connectorpb.BitbucketGetPRDiffstatRequest
	baseURL string
}

func (r *getPRDiffstatVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getPRDiffstatVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 500
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s/%s/pullrequests/%d/diffstat?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		r.req.GetPrId(),
		qv.Encode())
}

func (r *getPRDiffstatVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getPRDiffstatVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getPRDiffstatVendorResponse{}
}

type getPRDiffstatVendorResponse struct{}

func (r *getPRDiffstatVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketGetPRDiffstatResponse{NextPageUrl: raw.Next}
	for _, v := range raw.Values {
		resp.DiffstatEntries = append(resp.DiffstatEntries, v)
	}
	return resp, nil
}

// --- Get Commit Diffstat ---

type getCommitDiffstatVendorRequest struct {
	req     *connectorpb.BitbucketGetCommitDiffstatRequest
	baseURL string
}

func (r *getCommitDiffstatVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getCommitDiffstatVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 500
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	// Bitbucket commit-diffstat endpoint takes the commit SHA in the path.
	// A single commit's diffstat is the diff against its first parent.
	return fmt.Sprintf("%s/repositories/%s/%s/diffstat/%s?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		url.PathEscape(r.req.GetCommitSha()),
		qv.Encode())
}

func (r *getCommitDiffstatVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getCommitDiffstatVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getCommitDiffstatVendorResponse{}
}

type getCommitDiffstatVendorResponse struct{}

func (r *getCommitDiffstatVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketGetCommitDiffstatResponse{NextPageUrl: raw.Next}
	for _, v := range raw.Values {
		resp.DiffstatEntries = append(resp.DiffstatEntries, v)
	}
	return resp, nil
}

// --- List PR Comments ---

type listPRCommentsVendorRequest struct {
	req     *connectorpb.BitbucketListPRCommentsRequest
	baseURL string
}

func (r *listPRCommentsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRCommentsVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s/%s/pullrequests/%d/comments?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		r.req.GetPrId(),
		qv.Encode())
}

func (r *listPRCommentsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRCommentsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listPRCommentsVendorResponse{}
}

type listPRCommentsVendorResponse struct{}

func (r *listPRCommentsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListPRCommentsResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Comments = append(resp.Comments, v)
	}
	return resp, nil
}

// --- List PR Activity ---

type listPRActivityVendorRequest struct {
	req     *connectorpb.BitbucketListPRActivityRequest
	baseURL string
}

func (r *listPRActivityVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRActivityVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s/%s/pullrequests/%d/activity?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		r.req.GetPrId(),
		qv.Encode())
}

func (r *listPRActivityVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRActivityVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listPRActivityVendorResponse{}
}

type listPRActivityVendorResponse struct{}

func (r *listPRActivityVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListPRActivityResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Activities = append(resp.Activities, v)
	}
	return resp, nil
}

// --- List Issues ---

type listIssuesVendorRequest struct {
	req     *connectorpb.BitbucketListIssuesRequest
	baseURL string
}

func (r *listIssuesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listIssuesVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/repositories/%s/%s/issues?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		qv.Encode())
}

func (r *listIssuesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listIssuesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listIssuesVendorResponse{}
}

type listIssuesVendorResponse struct{}

func (r *listIssuesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListIssuesResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Issues = append(resp.Issues, v)
	}
	return resp, nil
}

// --- List Workspace Members ---

type listWorkspaceMembersVendorRequest struct {
	req     *connectorpb.BitbucketListWorkspaceMembersRequest
	baseURL string
}

func (r *listWorkspaceMembersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listWorkspaceMembersVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	qv.Set("page", fmt.Sprintf("%d", page))
	return fmt.Sprintf("%s/workspaces/%s/members?%s",
		r.baseURL, url.PathEscape(r.req.GetWorkspace()), qv.Encode())
}

func (r *listWorkspaceMembersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listWorkspaceMembersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listWorkspaceMembersVendorResponse{}
}

type listWorkspaceMembersVendorResponse struct{}

func (r *listWorkspaceMembersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListWorkspaceMembersResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Members = append(resp.Members, v)
	}
	return resp, nil
}

// --- List PR Commits ---

// listPRCommitsVendorRequest fetches commits for a single PR.
// Used for first_commit_at enrichment — first_commit_at write side.
type listPRCommitsVendorRequest struct {
	req     *connectorpb.BitbucketListPRCommitsRequest
	baseURL string
}

func (r *listPRCommitsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRCommitsVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 1
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	if s := r.req.GetSort(); s != "" {
		qv.Set("sort", s)
	}
	return fmt.Sprintf("%s/repositories/%s/%s/pullrequests/%d/commits?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		r.req.GetPrId(),
		qv.Encode())
}

func (r *listPRCommitsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRCommitsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listPRCommitsVendorResponse{}
}

type listPRCommitsVendorResponse struct{}

func (r *listPRCommitsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket response: %w", err)
	}
	resp := &connectorpb.BitbucketListPRCommitsResponse{
		NextPageUrl: raw.Next,
	}
	for _, v := range raw.Values {
		resp.Commits = append(resp.Commits, v)
	}
	return resp, nil
}

// --- List Workspace Groups (REST v1.0) ---

type listWorkspaceGroupsVendorRequest struct {
	req     *connectorpb.BitbucketListWorkspaceGroupsRequest
	baseURL string // v1.0 base URL derived from the v2.0 base
}

func (r *listWorkspaceGroupsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listWorkspaceGroupsVendorRequest) URL() string {
	return fmt.Sprintf("%s/groups/%s/",
		r.baseURL, url.PathEscape(r.req.GetWorkspace()))
}

func (r *listWorkspaceGroupsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listWorkspaceGroupsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listWorkspaceGroupsVendorResponse{}
}

type listWorkspaceGroupsVendorResponse struct{}

func (r *listWorkspaceGroupsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	// The v1.0 groups API returns a flat JSON array of group objects (not paginated).
	var groups []json.RawMessage
	if err := json.Unmarshal(body, &groups); err != nil {
		return nil, fmt.Errorf("unmarshaling workspace groups: %w", err)
	}
	resp := &connectorpb.BitbucketListWorkspaceGroupsResponse{}
	for _, g := range groups {
		resp.Groups = append(resp.Groups, g)
	}
	return resp, nil
}

// --- Factory ---

// NewBitbucketVendorRequest returns a SyncRequestFactory that maps proto messages
// to Bitbucket vendor requests. Auth is handled by the autoRefreshDoer in the
// HTTP transport layer, so request types don't set auth headers.
// --- File History + raw Diff (defect-lineage PATH_RECONSTRUCTION) ---

// escapePathSegments escapes each slash-separated segment of a file path while keeping the
// slashes, so a nested path resolves as URL path segments rather than one %2F-escaped blob.
func escapePathSegments(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

type getFileHistoryVendorRequest struct {
	req     *connectorpb.BitbucketGetFileHistoryRequest
	baseURL string
}

func (r *getFileHistoryVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getFileHistoryVendorRequest) URL() string {
	pagelen := r.req.GetPagelen()
	if pagelen == 0 {
		pagelen = 100
	}
	qv := url.Values{}
	qv.Set("pagelen", fmt.Sprintf("%d", pagelen))
	// Bitbucket's /filehistory endpoint paginates by cursor (the `next` link), NOT by page
	// number: sending a `page` query param makes it reject the request with 400 "Invalid page".
	// Subsequent pages are followed via NextPageUrl, so no page param is set here.
	// renames defaults true on Bitbucket; only send the param to turn it off.
	if !r.req.GetRenames() {
		qv.Set("renames", "false")
	}
	// The file path is the URL path tail after the commit, not a query param —
	// escape each segment but keep the slashes so a nested path resolves.
	return fmt.Sprintf("%s/repositories/%s/%s/filehistory/%s/%s?%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		url.PathEscape(r.req.GetCommit()),
		escapePathSegments(r.req.GetPath()),
		qv.Encode())
}

func (r *getFileHistoryVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getFileHistoryVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getFileHistoryVendorResponse{}
}

type getFileHistoryVendorResponse struct{}

func (r *getFileHistoryVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Values []json.RawMessage `json:"values"`
		Next   string            `json:"next"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling bitbucket file-history response: %w", err)
	}
	resp := &connectorpb.BitbucketGetFileHistoryResponse{NextPageUrl: raw.Next}
	for _, v := range raw.Values {
		resp.Entries = append(resp.Entries, v)
	}
	return resp, nil
}

type getDiffVendorRequest struct {
	req     *connectorpb.BitbucketGetDiffRequest
	baseURL string
}

func (r *getDiffVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getDiffVendorRequest) URL() string {
	qv := url.Values{}
	if r.req.GetPath() != "" {
		qv.Set("path", r.req.GetPath())
	}
	if r.req.GetIgnoreWhitespace() {
		qv.Set("ignore_whitespace", "true")
	}
	u := fmt.Sprintf("%s/repositories/%s/%s/diff/%s",
		r.baseURL,
		url.PathEscape(r.req.GetWorkspace()),
		url.PathEscape(r.req.GetRepoSlug()),
		url.PathEscape(r.req.GetSpec()))
	if enc := qv.Encode(); enc != "" {
		u += "?" + enc
	}
	return u
}

func (r *getDiffVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getDiffVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getDiffVendorResponse{}
}

type getDiffVendorResponse struct{}

func (r *getDiffVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	// Bitbucket's diff endpoint returns text/plain unified diff, not JSON — carry it verbatim.
	return &connectorpb.BitbucketGetDiffResponse{RawDiff: body}, nil
}

func NewBitbucketVendorRequest(baseURLs map[string]string) vendorapi.SyncRequestFactory {
	restBaseURL := strings.TrimSuffix(baseURLs[constants.GroupBitbucketREST], "/")
	// The v1.0 groups API has no v2.0 equivalent. Derive the v1.0 base URL
	// from the configured v2.0 base (e.g. "https://api.bitbucket.org/2.0" → "/1.0").
	restV1BaseURL := strings.Replace(restBaseURL, "/2.0", "/1.0", 1)

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		switch r := req.(type) {
		case *connectorpb.BitbucketListReposRequest:
			return &listReposVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetRepoRequest:
			return &getRepoVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetBranchRequest:
			return &getBranchVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListBranchRestrictionsRequest:
			return &listBranchRestrictionsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListPullRequestsRequest:
			return &listPullRequestsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListCommitsRequest:
			return &listCommitsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetUserRequest:
			return &getUserVendorRequest{baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			return &getPRDiffstatVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetCommitDiffstatRequest:
			return &getCommitDiffstatVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListPRCommentsRequest:
			return &listPRCommentsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListPRActivityRequest:
			return &listPRActivityVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListIssuesRequest:
			return &listIssuesVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListWorkspaceMembersRequest:
			return &listWorkspaceMembersVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketListWorkspaceGroupsRequest:
			return &listWorkspaceGroupsVendorRequest{req: r, baseURL: restV1BaseURL}, nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return &listPRCommitsVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetFileHistoryRequest:
			return &getFileHistoryVendorRequest{req: r, baseURL: restBaseURL}, nil
		case *connectorpb.BitbucketGetDiffRequest:
			return &getDiffVendorRequest{req: r, baseURL: restBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

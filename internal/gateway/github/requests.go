// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ghgateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// authSetter implements vendorapi.VendorRequestWithAuth for GitHub requests.
// It sets the bearer token and standard GitHub API headers.
type authSetter struct {
	token string
}

func (a *authSetter) SetAuth(req *http.Request) *http.Request {
	if a.token != "" {
		req.Header.Set("Authorization", "bearer "+a.token)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	return req
}

// --- GraphQL ---

type graphQLVendorRequest struct {
	*authSetter
	req     *connectorpb.GitHubGraphQLRequest
	baseURL string
}

func (r *graphQLVendorRequest) HTTPMethod() string { return http.MethodPost }
func (r *graphQLVendorRequest) URL() string        { return r.baseURL }

func (r *graphQLVendorRequest) Marshal() ([]byte, error) {
	body, err := json.Marshal(map[string]string{"query": r.req.GetQuery()})
	if err != nil {
		return nil, fmt.Errorf("marshalling GraphQL query: %w", err)
	}
	return body, nil
}

func (r *graphQLVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &graphQLVendorResponse{}
}

type graphQLVendorResponse struct{}

func (r *graphQLVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"errors,omitempty"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling GraphQL response: %w", err)
	}
	resp := &connectorpb.GitHubGraphQLResponse{
		Data: raw.Data,
	}
	for _, e := range raw.Errors {
		resp.Errors = append(resp.Errors, &connectorpb.GitHubGraphQLError{
			Message: e.Message,
			Type:    e.Type,
		})
	}
	return resp, nil
}

// --- List Org Repos ---

type listOrgReposVendorRequest struct {
	*authSetter
	req     *connectorpb.ListOrgReposRequest
	baseURL string
}

func (r *listOrgReposVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listOrgReposVendorRequest) URL() string {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	page := r.req.GetPage()
	if page == 0 {
		page = 1
	}
	repoType := r.req.GetType()
	if repoType == "" {
		repoType = "all"
	}
	qv := url.Values{}
	qv.Set("per_page", fmt.Sprintf("%d", perPage))
	qv.Set("page", fmt.Sprintf("%d", page))
	qv.Set("type", repoType)
	return fmt.Sprintf("%s/orgs/%s/repos?%s",
		r.baseURL, url.PathEscape(r.req.GetOrganization()), qv.Encode())
}

func (r *listOrgReposVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listOrgReposVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listOrgReposVendorResponse{perPage: int(perPage)}
}

type listOrgReposVendorResponse struct {
	perPage int
}

func (r *listOrgReposVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var repos []json.RawMessage
	if err := json.Unmarshal(body, &repos); err != nil {
		return nil, fmt.Errorf("unmarshalling org repos response: %w", err)
	}
	resp := &connectorpb.ListOrgReposResponse{
		HasNextPage: len(repos) == r.perPage,
	}
	for _, repo := range repos {
		resp.Repos = append(resp.Repos, repo)
	}
	return resp, nil
}

// --- List User Repos ---

type listUserReposVendorRequest struct {
	*authSetter
	req     *connectorpb.ListUserReposRequest
	baseURL string
}

func (r *listUserReposVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listUserReposVendorRequest) URL() string {
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
	if visibility := r.req.GetVisibility(); visibility != "" {
		qv.Set("visibility", visibility)
	}
	return fmt.Sprintf("%s/user/repos?%s", r.baseURL, qv.Encode())
}

func (r *listUserReposVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listUserReposVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listUserReposVendorResponse{perPage: int(perPage)}
}

type listUserReposVendorResponse struct {
	perPage int
}

// Unmarshal parses GET /user/repos, which returns a bare JSON array of repos.
func (r *listUserReposVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var repos []json.RawMessage
	if err := json.Unmarshal(body, &repos); err != nil {
		return nil, fmt.Errorf("unmarshalling user repos response: %w", err)
	}
	resp := &connectorpb.ListUserReposResponse{
		HasNextPage: len(repos) == r.perPage,
	}
	for _, repo := range repos {
		resp.Repos = append(resp.Repos, repo)
	}
	return resp, nil
}

// --- List Installation Repos ---

type listInstallationReposVendorRequest struct {
	*authSetter
	req     *connectorpb.ListInstallationReposRequest
	baseURL string
}

func (r *listInstallationReposVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listInstallationReposVendorRequest) URL() string {
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
	return fmt.Sprintf("%s/installation/repositories?%s", r.baseURL, qv.Encode())
}

func (r *listInstallationReposVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listInstallationReposVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listInstallationReposVendorResponse{perPage: int(perPage)}
}

type listInstallationReposVendorResponse struct {
	perPage int
}

// Unmarshal parses GET /installation/repositories. Unlike /user/repos and
// /orgs/{org}/repos (bare arrays), this endpoint wraps the page under a
// top-level "repositories" key alongside "total_count"; the page is unwrapped
// before populating repos. has_next_page is the same len==per_page heuristic,
// counting the unwrapped array — not the envelope.
func (r *listInstallationReposVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var envelope struct {
		Repositories []json.RawMessage `json:"repositories"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshalling installation repos response: %w", err)
	}
	resp := &connectorpb.ListInstallationReposResponse{
		HasNextPage: len(envelope.Repositories) == r.perPage,
	}
	for _, repo := range envelope.Repositories {
		resp.Repos = append(resp.Repos, repo)
	}
	return resp, nil
}

// --- Get Authenticated User ---

// tokenExpirationHeader is GitHub's response header carrying a fine-grained
// PAT's expiry. Absent for non-expiring tokens and for GitHub App installation
// tokens.
const tokenExpirationHeader = "github-authentication-token-expiration"

// tokenExpirationLayouts are the timestamp formats GitHub has used for the
// token-expiration header. The space-separated "UTC"-suffixed form is current;
// RFC1123 is kept as a fallback so a format flip does not silently drop expiry.
var tokenExpirationLayouts = []string{"2006-01-02 15:04:05 MST", time.RFC1123, time.RFC3339}

// parseTokenExpiration converts the raw header value to a Timestamp, returning
// nil when the header is empty or unparseable — expiry is best-effort metadata,
// never a hard failure of the user-identity resolution.
func parseTokenExpiration(raw string) *timestamppb.Timestamp {
	if raw == "" {
		return nil
	}
	var lastErr error
	for _, layout := range tokenExpirationLayouts {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return timestamppb.New(t.UTC())
		}
		lastErr = err
	}
	// Best-effort: a malformed expiry must not fail identity resolution, but a
	// miss across every known layout signals GitHub changed the format — log it
	// so the drop is observable, not silent. The raw value is a vendor-controlled
	// header, so it is truncated to bound the log line.
	logger.Warn(
		"ghgateway: token-expiration header did not match any known layout, dropping expiry",
		zap.String("raw", truncateForLog(raw)),
		zap.Error(lastErr),
	)
	return nil
}

// maxLoggedHeaderLen caps a vendor-controlled header value before it reaches a
// log line, so a pathological multi-KB header cannot bloat the log.
const maxLoggedHeaderLen = 64

func truncateForLog(s string) string {
	if len(s) > maxLoggedHeaderLen {
		return s[:maxLoggedHeaderLen] + "…(truncated)"
	}
	return s
}

// getAuthenticatedUserVendorRequest builds GET /user. GetAuthenticatedUserRequest
// carries no fields (the identity is fixed by the auth token), so the request is
// not stored — only the base URL is needed.
type getAuthenticatedUserVendorRequest struct {
	*authSetter
	baseURL string
}

func (r *getAuthenticatedUserVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getAuthenticatedUserVendorRequest) URL() string {
	return fmt.Sprintf("%s/user", r.baseURL)
}

func (r *getAuthenticatedUserVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getAuthenticatedUserVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getAuthenticatedUserVendorResponse{}
}

type getAuthenticatedUserVendorResponse struct{}

// Unmarshal parses GET /user's body. credential_expires_at is always nil on
// this path — the expiry lives in a response header this method does not
// receive; UnmarshalWithHeaders populates it.
func (r *getAuthenticatedUserVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	msg, err := r.UnmarshalWithHeaders(body, nil)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling authenticated user: %w", err)
	}
	return msg, nil
}

// UnmarshalWithHeaders parses the body and reads the fine-grained PAT expiry
// from the github-authentication-token-expiration response header (empty header
// → empty expiry). A nil header is treated as absent.
func (r *getAuthenticatedUserVendorResponse) UnmarshalWithHeaders(body []byte, header http.Header) (proto.Message, error) {
	var raw struct {
		Login string `json:"login"`
		ID    int64  `json:"id"`
		Email string `json:"email"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling authenticated user response: %w", err)
	}
	return &connectorpb.GetAuthenticatedUserResponse{
		Login:               raw.Login,
		Id:                  raw.ID,
		Email:               raw.Email,
		CredentialExpiresAt: parseTokenExpiration(header.Get(tokenExpirationHeader)),
	}, nil
}

// --- List PR Reviews ---

type listPRReviewsVendorRequest struct {
	*authSetter
	req     *connectorpb.ListPRReviewsRequest
	baseURL string
}

func (r *listPRReviewsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRReviewsVendorRequest) URL() string {
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
	return fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews?%s",
		r.baseURL,
		url.PathEscape(r.req.GetOwner()),
		url.PathEscape(r.req.GetRepo()),
		r.req.GetPrNumber(),
		qv.Encode())
}

func (r *listPRReviewsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRReviewsVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listPRReviewsVendorResponse{perPage: int(perPage)}
}

type listPRReviewsVendorResponse struct {
	perPage int
}

func (r *listPRReviewsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var reviews []json.RawMessage
	if err := json.Unmarshal(body, &reviews); err != nil {
		return nil, fmt.Errorf("unmarshalling pull request reviews response: %w", err)
	}
	resp := &connectorpb.ListPRReviewsResponse{
		HasNextPage: len(reviews) == r.perPage,
	}
	for _, rev := range reviews {
		resp.Reviews = append(resp.Reviews, rev)
	}
	return resp, nil
}

// --- PR Review Comments ---

type listPRCommentsVendorRequest struct {
	*authSetter
	req     *connectorpb.ListPRCommentsRequest
	baseURL string
}

func (r *listPRCommentsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRCommentsVendorRequest) URL() string {
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
	return fmt.Sprintf("%s/repos/%s/%s/pulls/%d/comments?%s",
		r.baseURL,
		url.PathEscape(r.req.GetOwner()),
		url.PathEscape(r.req.GetRepo()),
		r.req.GetPrNumber(),
		qv.Encode())
}

func (r *listPRCommentsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRCommentsVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listPRCommentsVendorResponse{perPage: int(perPage)}
}

type listPRCommentsVendorResponse struct {
	perPage int
}

func (r *listPRCommentsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var comments []json.RawMessage
	if err := json.Unmarshal(body, &comments); err != nil {
		return nil, fmt.Errorf("unmarshalling PR comments response: %w", err)
	}
	resp := &connectorpb.ListPRCommentsResponse{
		HasNextPage: len(comments) == r.perPage,
	}
	for _, c := range comments {
		resp.Comments = append(resp.Comments, c)
	}
	return resp, nil
}

// --- List PR Files ---

type listPRFilesVendorRequest struct {
	*authSetter
	req     *connectorpb.ListPRFilesRequest
	baseURL string
}

func (r *listPRFilesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listPRFilesVendorRequest) URL() string {
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
	return fmt.Sprintf("%s/repos/%s/%s/pulls/%d/files?%s",
		r.baseURL,
		url.PathEscape(r.req.GetOwner()),
		url.PathEscape(r.req.GetRepo()),
		r.req.GetPrNumber(),
		qv.Encode())
}

func (r *listPRFilesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listPRFilesVendorRequest) GetResponse() vendorapi.VendorResponse {
	perPage := r.req.GetPerPage()
	if perPage == 0 {
		perPage = 100
	}
	return &listPRFilesVendorResponse{perPage: int(perPage)}
}

type listPRFilesVendorResponse struct {
	perPage int
}

func (r *listPRFilesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var files []json.RawMessage
	if err := json.Unmarshal(body, &files); err != nil {
		return nil, fmt.Errorf("unmarshalling PR files response: %w", err)
	}
	resp := &connectorpb.ListPRFilesResponse{
		HasNextPage: len(files) == r.perPage,
	}
	for _, f := range files {
		resp.Files = append(resp.Files, f)
	}
	return resp, nil
}

// --- Copilot Metrics ---

type copilotMetricsVendorRequest struct {
	*authSetter
	req     *connectorpb.CopilotMetricsRequest
	baseURL string
}

func (r *copilotMetricsVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *copilotMetricsVendorRequest) URL() string {
	return fmt.Sprintf("%s/orgs/%s/copilot/metrics",
		r.baseURL, url.PathEscape(r.req.GetOrganization()))
}

func (r *copilotMetricsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *copilotMetricsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &copilotMetricsVendorResponse{}
}

type copilotMetricsVendorResponse struct{}

func (r *copilotMetricsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var metrics []json.RawMessage
	if err := json.Unmarshal(body, &metrics); err != nil {
		return nil, fmt.Errorf("unmarshalling copilot metrics response: %w", err)
	}
	resp := &connectorpb.CopilotMetricsResponse{}
	for _, m := range metrics {
		resp.Metrics = append(resp.Metrics, m)
	}
	return resp, nil
}

// --- Copilot User Usage ---

type copilotUserUsageVendorRequest struct {
	*authSetter
	req     *connectorpb.CopilotUserUsageRequest
	baseURL string
}

func (r *copilotUserUsageVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *copilotUserUsageVendorRequest) URL() string {
	return fmt.Sprintf("%s/orgs/%s/copilot/usage/user",
		r.baseURL, url.PathEscape(r.req.GetOrganization()))
}

func (r *copilotUserUsageVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *copilotUserUsageVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &copilotUserUsageVendorResponse{}
}

type copilotUserUsageVendorResponse struct{}

func (r *copilotUserUsageVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var userMetrics []json.RawMessage
	if err := json.Unmarshal(body, &userMetrics); err != nil {
		return nil, fmt.Errorf("unmarshalling copilot user usage response: %w", err)
	}
	resp := &connectorpb.CopilotUserUsageResponse{}
	for _, um := range userMetrics {
		resp.UserMetrics = append(resp.UserMetrics, um)
	}
	return resp, nil
}

// --- Get Repository (REST) ---

type getRepositoryVendorRequest struct {
	*authSetter
	req     *connectorpb.GetRepositoryRequest
	baseURL string
}

func (r *getRepositoryVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getRepositoryVendorRequest) URL() string {
	return fmt.Sprintf("%s/repos/%s/%s",
		r.baseURL, url.PathEscape(r.req.GetOwner()), url.PathEscape(r.req.GetRepo()))
}

func (r *getRepositoryVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getRepositoryVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getRepositoryVendorResponse{}
}

type getRepositoryVendorResponse struct{}

func (r *getRepositoryVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GetRepositoryResponse{RawJson: body}, nil
}

// --- Get effective branch rules (REST) ---

type getBranchRulesVendorRequest struct {
	*authSetter
	req     *connectorpb.GetBranchRulesRequest
	baseURL string
}

func (r *getBranchRulesVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *getBranchRulesVendorRequest) URL() string {
	return fmt.Sprintf("%s/repos/%s/%s/rules/branches/%s",
		r.baseURL,
		url.PathEscape(r.req.GetOwner()),
		url.PathEscape(r.req.GetRepo()),
		url.PathEscape(r.req.GetBranch()))
}

func (r *getBranchRulesVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *getBranchRulesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &getBranchRulesVendorResponse{}
}

type getBranchRulesVendorResponse struct{}

func (r *getBranchRulesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	return &connectorpb.GetBranchRulesResponse{RawJson: body}, nil
}

// --- Factory ---

// NewGitHubVendorRequest returns a SyncRequestFactory that maps proto messages to
// GitHub vendor requests. The factory closure captures the token and base URLs.
func NewGitHubVendorRequest(token string, baseURLs map[string]string) vendorapi.SyncRequestFactory {
	graphQLBaseURL := baseURLs[constants.GroupGitHubGraphQL]
	restBaseURL := baseURLs[constants.GroupGitHubREST]

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		auth := &authSetter{token: token}
		switch r := req.(type) {
		case *connectorpb.GitHubGraphQLRequest:
			return &graphQLVendorRequest{authSetter: auth, req: r, baseURL: graphQLBaseURL}, nil
		case *connectorpb.ListOrgReposRequest:
			return &listOrgReposVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.ListUserReposRequest:
			return &listUserReposVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.ListInstallationReposRequest:
			return &listInstallationReposVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GetAuthenticatedUserRequest:
			return &getAuthenticatedUserVendorRequest{authSetter: auth, baseURL: restBaseURL}, nil
		case *connectorpb.ListPRReviewsRequest:
			return &listPRReviewsVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.ListPRCommentsRequest:
			return &listPRCommentsVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.ListPRFilesRequest:
			return &listPRFilesVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.CopilotMetricsRequest:
			return &copilotMetricsVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.CopilotUserUsageRequest:
			return &copilotUserUsageVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GetRepositoryRequest:
			return &getRepositoryVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		case *connectorpb.GetBranchRulesRequest:
			return &getBranchRulesVendorRequest{authSetter: auth, req: r, baseURL: restBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

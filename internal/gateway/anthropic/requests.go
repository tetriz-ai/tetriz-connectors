// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropicgateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// anthropicVersion pins the vendor's dated API contract. Anthropic requires the
// header on every request and treats its absence as an error rather than defaulting
// to the newest contract, which is the behaviour that makes pinning safe: a new
// contract cannot silently change a response shape under us.
const anthropicVersion = "2023-06-01"

// authHeaders implements vendorapi.VendorRequestWithAuth for every Anthropic
// request. Both headers are set together because both are mandatory on every call,
// and a request that carries the key without the version is rejected by the vendor
// for a reason that reads nothing like a missing header.
//
// The key goes in x-api-key, not Authorization: an Anthropic key presented as a
// bearer token is refused. Anthropic has more than one credential family behind that
// same header and they are not interchangeable, so one gateway holds one family's
// key — this one holds the Enterprise Analytics key.
type authHeaders struct {
	apiKey string
}

func (a *authHeaders) SetAuth(req *http.Request) *http.Request {
	if a.apiKey != "" {
		req.Header.Set("x-api-key", a.apiKey)
	}
	req.Header.Set("anthropic-version", anthropicVersion)
	return req
}

// Every request type must satisfy the auth interface, or vendorapi's optional
// type-assertion silently ships them unauthenticated. Asserted at compile time
// because that failure is invisible to a passing test suite.
var (
	_ vendorapi.VendorRequestWithAuth = (*analyticsUsersVendorRequest)(nil)
	_ vendorapi.VendorRequestWithAuth = (*analyticsSkillsVendorRequest)(nil)
	_ vendorapi.VendorRequestWithAuth = (*userCostReportVendorRequest)(nil)
	_ vendorapi.VendorRequestWithAuth = (*userUsageReportVendorRequest)(nil)
	_ vendorapi.VendorRequestWithAuth = (*organizationMembersVendorRequest)(nil)
)

// --- /v1/organizations/analytics/users ---

type analyticsUsersVendorRequest struct {
	*authHeaders
	req     *connectorpb.AnthropicAnalyticsUsersRequest
	baseURL string
}

func (r *analyticsUsersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *analyticsUsersVendorRequest) URL() string {
	q := url.Values{}
	q.Set("date", r.req.GetDate())
	addLimit(q, r.req.GetLimit())
	addPage(q, r.req.GetPage())
	return r.baseURL + "/v1/organizations/analytics/users?" + q.Encode()
}

func (r *analyticsUsersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *analyticsUsersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &analyticsUsersVendorResponse{}
}

type analyticsUsersVendorResponse struct{}

func (r *analyticsUsersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Data     []json.RawMessage `json:"data"`
		NextPage string            `json:"next_page"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling anthropic analytics users response: %w", err)
	}
	resp := &connectorpb.AnthropicAnalyticsUsersResponse{NextPage: raw.NextPage}
	for _, d := range raw.Data {
		resp.Data = append(resp.Data, d)
	}
	return resp, nil
}

// --- /v1/organizations/analytics/skills ---

type analyticsSkillsVendorRequest struct {
	*authHeaders
	req     *connectorpb.AnthropicAnalyticsSkillsRequest
	baseURL string
}

func (r *analyticsSkillsVendorRequest) HTTPMethod() string { return http.MethodGet }

// URL fixes group_by to user_id rather than taking it from the request. The
// endpoint is skill-grained by default, and the grouping is what makes a row
// attributable to one user — the invariant every consumer downstream assumes. It is
// also a parameter a pagination cursor is bound to, so a caller able to vary it
// between pages could turn a mid-sequence page into a 400.
func (r *analyticsSkillsVendorRequest) URL() string {
	q := url.Values{}
	q.Set("date", r.req.GetDate())
	q.Add("group_by[]", "user_id")
	addLimit(q, r.req.GetLimit())
	addPage(q, r.req.GetPage())
	return r.baseURL + "/v1/organizations/analytics/skills?" + q.Encode()
}

func (r *analyticsSkillsVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *analyticsSkillsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &analyticsSkillsVendorResponse{}
}

type analyticsSkillsVendorResponse struct{}

func (r *analyticsSkillsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Data     []json.RawMessage `json:"data"`
		NextPage string            `json:"next_page"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling anthropic analytics skills response: %w", err)
	}
	resp := &connectorpb.AnthropicAnalyticsSkillsResponse{NextPage: raw.NextPage}
	for _, d := range raw.Data {
		resp.Data = append(resp.Data, d)
	}
	return resp, nil
}

// --- /v1/organizations/analytics/user_cost_report ---

type userCostReportVendorRequest struct {
	*authHeaders
	req     *connectorpb.AnthropicAnalyticsUserCostReportRequest
	baseURL string
}

func (r *userCostReportVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *userCostReportVendorRequest) URL() string {
	return r.baseURL + "/v1/organizations/analytics/user_cost_report?" +
		reportQuery(r.req.GetStartingAt(), r.req.GetEndingAt(), r.req.GetLimit(), r.req.GetPage()).Encode()
}

func (r *userCostReportVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *userCostReportVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &userCostReportVendorResponse{}
}

type userCostReportVendorResponse struct{}

func (r *userCostReportVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw reportEnvelope
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling anthropic user cost report response: %w", err)
	}
	resp := &connectorpb.AnthropicAnalyticsUserCostReportResponse{
		NextPage:        raw.NextPage,
		HasMore:         raw.HasMore,
		DataRefreshedAt: parseVendorTime(raw.DataRefreshedAt),
		OrganizationId:  raw.OrganizationID,
	}
	for _, d := range raw.Data {
		resp.Data = append(resp.Data, d)
	}
	return resp, nil
}

// --- /v1/organizations/analytics/user_usage_report ---

type userUsageReportVendorRequest struct {
	*authHeaders
	req     *connectorpb.AnthropicAnalyticsUserUsageReportRequest
	baseURL string
}

func (r *userUsageReportVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *userUsageReportVendorRequest) URL() string {
	return r.baseURL + "/v1/organizations/analytics/user_usage_report?" +
		reportQuery(r.req.GetStartingAt(), r.req.GetEndingAt(), r.req.GetLimit(), r.req.GetPage()).Encode()
}

func (r *userUsageReportVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *userUsageReportVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &userUsageReportVendorResponse{}
}

type userUsageReportVendorResponse struct{}

func (r *userUsageReportVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw reportEnvelope
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling anthropic user usage report response: %w", err)
	}
	resp := &connectorpb.AnthropicAnalyticsUserUsageReportResponse{
		NextPage:        raw.NextPage,
		HasMore:         raw.HasMore,
		DataRefreshedAt: parseVendorTime(raw.DataRefreshedAt),
		OrganizationId:  raw.OrganizationID,
	}
	for _, d := range raw.Data {
		resp.Data = append(resp.Data, d)
	}
	return resp, nil
}

// --- /v1/organizations/users ---

type organizationMembersVendorRequest struct {
	*authHeaders
	req     *connectorpb.AnthropicOrganizationMembersRequest
	baseURL string
}

func (r *organizationMembersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *organizationMembersVendorRequest) URL() string {
	q := url.Values{}
	if afterID := r.req.GetAfterId(); afterID != "" {
		q.Set("after_id", afterID)
	}
	addLimit(q, r.req.GetLimit())
	if len(q) == 0 {
		return r.baseURL + "/v1/organizations/users"
	}
	return r.baseURL + "/v1/organizations/users?" + q.Encode()
}

func (r *organizationMembersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *organizationMembersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &organizationMembersVendorResponse{}
}

type organizationMembersVendorResponse struct{}

func (r *organizationMembersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Data    []json.RawMessage `json:"data"`
		FirstID string            `json:"first_id"`
		LastID  string            `json:"last_id"`
		HasMore bool              `json:"has_more"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling anthropic organization members response: %w", err)
	}
	resp := &connectorpb.AnthropicOrganizationMembersResponse{
		FirstId: raw.FirstID,
		LastId:  raw.LastID,
		HasMore: raw.HasMore,
	}
	for _, d := range raw.Data {
		resp.Data = append(resp.Data, d)
	}
	return resp, nil
}

// --- shared query and envelope helpers ---

// addLimit omits a zero rather than sending limit=0. The vendor reads an absent
// limit as its own default and a zero as a request for an empty page.
func addLimit(q url.Values, limit int32) {
	if limit > 0 {
		q.Set("limit", strconv.Itoa(int(limit)))
	}
}

// addPage omits an empty cursor, which is how the first page is asked for.
func addPage(q url.Values, page string) {
	if page != "" {
		q.Set("page", page)
	}
}

// reportQuery builds the query the two report endpoints share. bucket_width and
// group_by are fixed here rather than carried on the request: daily buckets grouped
// by product and model are what make a row attributable to a product and a model
// instead of an undifferentiated per-user total, and both are parameters a
// pagination cursor is bound to.
//
// No products[] filter is sent. Filtering to Claude Code would discard the breadth
// this ingestion exists to gain, and grouping by product recovers the Claude Code
// slice from the response anyway.
func reportQuery(startingAt, endingAt *timestamppb.Timestamp, limit int32, page string) url.Values {
	q := url.Values{}
	q.Set("starting_at", formatVendorTime(startingAt))
	if endingAt != nil {
		q.Set("ending_at", formatVendorTime(endingAt))
	}
	q.Set("bucket_width", "1d")
	q.Add("group_by[]", "product")
	q.Add("group_by[]", "model")
	addLimit(q, limit)
	addPage(q, page)
	return q
}

// formatVendorTime renders a timestamp the way the vendor's contract expects.
//
// A pagination cursor is bound to the exact query that issued it, so one window must
// render to the same bytes on every page of a sequence. What guarantees that is the
// TYPE, not this function: timestamppb discards the caller's location and AsTime
// always returns UTC, so no caller can introduce an offset here. That is the reason
// these fields are timestamps rather than the strings the vendor's own documentation
// shows — a string would put the rendering back in the caller's hands. This function
// only fixes the layout, which is why there is no .UTC() call to be found: it would
// be a no-op.
//
// Note the layout carries no fractional seconds, so two instants inside the same
// second render identically. Harmless against daily buckets; worth knowing before
// reusing this for a finer window.
func formatVendorTime(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339)
}

// parseVendorTime reads a vendor timestamp, returning nil for an absent or
// unparseable value rather than an error. That is deliberate for the one field this
// serves: data_refreshed_at only bounds a re-read, so a nil leaves the re-read
// unbounded — it re-reads a window it might have skipped, which is the harmless
// direction. Failing the whole page instead would discard rows that are themselves
// fine.
//
// Unparseable is logged rather than passed over in silence: degrading is the safe
// direction, but a vendor that changed this field's format would otherwise cost us
// every re-read bound with nothing to read that from. An absent value is not logged
// — the vendor omits it legitimately.
//
// The log carries the offending value, because time.Parse quotes its input and that
// is what makes the failure diagnosable. That is a deliberate exception to this
// package's rule that a vendor payload never travels in an error, and it holds on a
// real distinction: this is a single timestamp field, not a data row, so it cannot
// carry the member addresses and per-user amounts that rule exists to contain.
func parseVendorTime(v string) *timestamppb.Timestamp {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		logger.ErrorNoCtx("anthropicgateway: unparseable data_refreshed_at, re-read bound dropped",
			zap.Error(err))
		return nil
	}
	return timestamppb.New(t)
}

// reportEnvelope is the shape both report endpoints return around their rows.
type reportEnvelope struct {
	Data            []json.RawMessage `json:"data"`
	NextPage        string            `json:"next_page"`
	HasMore         bool              `json:"has_more"`
	DataRefreshedAt string            `json:"data_refreshed_at"`
	OrganizationID  string            `json:"organization_id"`
}

// --- Factory ---

// NewAnthropicVendorRequest returns a SyncRequestFactory mapping proto messages to
// Anthropic vendor requests. The closure captures the API key and base URLs.
//
// All five requests resolve against one group, which is deliberate rather than an
// omission; constants.GroupAnthropicAnalytics records why.
func NewAnthropicVendorRequest(apiKey string, baseURLs map[string]string) vendorapi.SyncRequestFactory {
	baseURL := baseURLs[constants.GroupAnthropicAnalytics]

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		auth := &authHeaders{apiKey: apiKey}
		switch r := req.(type) {
		case *connectorpb.AnthropicAnalyticsUsersRequest:
			return &analyticsUsersVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		case *connectorpb.AnthropicAnalyticsSkillsRequest:
			return &analyticsSkillsVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		case *connectorpb.AnthropicAnalyticsUserCostReportRequest:
			return &userCostReportVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		case *connectorpb.AnthropicAnalyticsUserUsageReportRequest:
			return &userUsageReportVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		case *connectorpb.AnthropicOrganizationMembersRequest:
			return &organizationMembersVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

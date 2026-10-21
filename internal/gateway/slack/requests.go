// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackgateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// authSetter implements vendorapi.VendorRequestWithAuth for Slack requests.
type authSetter struct {
	token string
}

func (a *authSetter) SetAuth(req *http.Request) *http.Request {
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

// --- users.list ---

type listUsersVendorRequest struct {
	*authSetter
	req     *connectorpb.SlackListUsersRequest
	baseURL string
}

func (r *listUsersVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *listUsersVendorRequest) URL() string {
	qv := url.Values{}
	qv.Set("limit", fmt.Sprintf("%d", r.req.GetLimit()))
	if r.req.GetCursor() != "" {
		qv.Set("cursor", r.req.GetCursor())
	}
	return fmt.Sprintf("%s/users.list?%s", r.baseURL, qv.Encode())
}
func (r *listUsersVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *listUsersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listUsersVendorResponse{}
}

type listUsersVendorResponse struct{}

func (r *listUsersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		OK               bool              `json:"ok"`
		Members          []json.RawMessage `json:"members"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling slack list users response: %w", err)
	}
	resp := &connectorpb.SlackListUsersResponse{
		Ok:         raw.OK,
		Error:      raw.Error,
		NextCursor: raw.ResponseMetadata.NextCursor,
	}
	for _, m := range raw.Members {
		resp.Members = append(resp.Members, m)
	}
	return resp, nil
}

// --- conversations.list ---

type listConversationsVendorRequest struct {
	*authSetter
	req     *connectorpb.SlackListConversationsRequest
	baseURL string
}

func (r *listConversationsVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *listConversationsVendorRequest) URL() string {
	qv := url.Values{}
	if r.req.GetTypes() != "" {
		qv.Set("types", r.req.GetTypes())
	}
	if r.req.GetExcludeArchived() {
		qv.Set("exclude_archived", "true")
	}
	qv.Set("limit", fmt.Sprintf("%d", r.req.GetLimit()))
	if r.req.GetCursor() != "" {
		qv.Set("cursor", r.req.GetCursor())
	}
	return fmt.Sprintf("%s/conversations.list?%s", r.baseURL, qv.Encode())
}
func (r *listConversationsVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *listConversationsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listConversationsVendorResponse{}
}

type listConversationsVendorResponse struct{}

func (r *listConversationsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		OK               bool              `json:"ok"`
		Channels         []json.RawMessage `json:"channels"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling slack list conversations response: %w", err)
	}
	resp := &connectorpb.SlackListConversationsResponse{
		Ok:         raw.OK,
		Error:      raw.Error,
		NextCursor: raw.ResponseMetadata.NextCursor,
	}
	for _, ch := range raw.Channels {
		resp.Channels = append(resp.Channels, ch)
	}
	return resp, nil
}

// --- conversations.history ---

type conversationsHistoryVendorRequest struct {
	*authSetter
	req     *connectorpb.SlackConversationsHistoryRequest
	baseURL string
}

func (r *conversationsHistoryVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *conversationsHistoryVendorRequest) URL() string {
	qv := url.Values{}
	qv.Set("channel", r.req.GetChannel())
	qv.Set("limit", fmt.Sprintf("%d", r.req.GetLimit()))
	if r.req.GetOldest() != "" {
		qv.Set("oldest", r.req.GetOldest())
	}
	if r.req.GetLatest() != "" {
		qv.Set("latest", r.req.GetLatest())
	}
	if r.req.GetCursor() != "" {
		qv.Set("cursor", r.req.GetCursor())
	}
	return fmt.Sprintf("%s/conversations.history?%s", r.baseURL, qv.Encode())
}
func (r *conversationsHistoryVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *conversationsHistoryVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &conversationsHistoryVendorResponse{}
}

type conversationsHistoryVendorResponse struct{}

func (r *conversationsHistoryVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		OK               bool              `json:"ok"`
		Messages         []json.RawMessage `json:"messages"`
		HasMore          bool              `json:"has_more"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling slack conversations history response: %w", err)
	}
	resp := &connectorpb.SlackConversationsHistoryResponse{
		Ok:         raw.OK,
		Error:      raw.Error,
		HasMore:    raw.HasMore,
		NextCursor: raw.ResponseMetadata.NextCursor,
	}
	for _, m := range raw.Messages {
		resp.Messages = append(resp.Messages, m)
	}
	return resp, nil
}

// --- conversations.replies ---

type conversationsRepliesVendorRequest struct {
	*authSetter
	req     *connectorpb.SlackConversationsRepliesRequest
	baseURL string
}

func (r *conversationsRepliesVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *conversationsRepliesVendorRequest) URL() string {
	qv := url.Values{}
	qv.Set("channel", r.req.GetChannel())
	qv.Set("ts", r.req.GetTs())
	qv.Set("limit", fmt.Sprintf("%d", r.req.GetLimit()))
	if r.req.GetCursor() != "" {
		qv.Set("cursor", r.req.GetCursor())
	}
	return fmt.Sprintf("%s/conversations.replies?%s", r.baseURL, qv.Encode())
}
func (r *conversationsRepliesVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *conversationsRepliesVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &conversationsRepliesVendorResponse{}
}

type conversationsRepliesVendorResponse struct{}

func (r *conversationsRepliesVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		OK               bool              `json:"ok"`
		Messages         []json.RawMessage `json:"messages"`
		HasMore          bool              `json:"has_more"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling slack conversations replies response: %w", err)
	}
	resp := &connectorpb.SlackConversationsRepliesResponse{
		Ok:         raw.OK,
		Error:      raw.Error,
		HasMore:    raw.HasMore,
		NextCursor: raw.ResponseMetadata.NextCursor,
	}
	for _, m := range raw.Messages {
		resp.Messages = append(resp.Messages, m)
	}
	return resp, nil
}

// --- admin.analytics.getFile ---
// Special: returns NDJSON (newline-delimited JSON), not standard JSON.

type adminAnalyticsVendorRequest struct {
	*authSetter
	req     *connectorpb.SlackAdminAnalyticsRequest
	baseURL string
}

func (r *adminAnalyticsVendorRequest) HTTPMethod() string { return http.MethodGet }
func (r *adminAnalyticsVendorRequest) URL() string {
	qv := url.Values{}
	qv.Set("type", r.req.GetType())
	qv.Set("date", r.req.GetDate())
	return fmt.Sprintf("%s/admin.analytics.getFile?%s", r.baseURL, qv.Encode())
}
func (r *adminAnalyticsVendorRequest) Marshal() ([]byte, error) { return nil, nil }
func (r *adminAnalyticsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &adminAnalyticsVendorResponse{}
}

type adminAnalyticsVendorResponse struct{}

func (r *adminAnalyticsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	// Slack admin.analytics.getFile returns NDJSON on success, but can return
	// standard JSON error responses (e.g. {"ok":false,"error":"paid_teams_only"})
	// with HTTP 200. Detect JSON errors before attempting NDJSON parsing.
	var errResp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	// The body is NDJSON on success and a single JSON error object on failure. A failed
	// Unmarshal here is the expected NDJSON path, not a fault: we deliberately use the
	// nil-vs-non-nil outcome only as a JSON-error-object probe and fall through to NDJSON
	// on the miss. The cause cannot propagate (NDJSON success would become a hard error)
	// and there is no ctx to log it (interface-fixed Unmarshal(body []byte) signature).
	if err := json.Unmarshal(body, &errResp); err == nil && !errResp.OK && errResp.Error != "" { // probe miss = expected NDJSON body; discriminator-only use
		return &connectorpb.SlackAdminAnalyticsResponse{
			Ok:    false,
			Error: errResp.Error,
		}, nil
	}

	resp := &connectorpb.SlackAdminAnalyticsResponse{Ok: true}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > 0 {
			lineCopy := make([]byte, len(line))
			copy(lineCopy, line)
			resp.Records = append(resp.Records, lineCopy)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning slack ndjson response: %w", err)
	}
	return resp, nil
}

// --- Factory ---

// NewSlackVendorRequest returns a SyncRequestFactory that maps proto messages to
// Slack vendor requests. The factory closure captures the token and base URLs.
func NewSlackVendorRequest(token string, baseURLs map[string]string) vendorapi.SyncRequestFactory {
	webBaseURL := baseURLs[constants.GroupSlackWeb]

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		auth := &authSetter{token: token}
		switch r := req.(type) {
		case *connectorpb.SlackListUsersRequest:
			return &listUsersVendorRequest{authSetter: auth, req: r, baseURL: webBaseURL}, nil
		case *connectorpb.SlackListConversationsRequest:
			return &listConversationsVendorRequest{authSetter: auth, req: r, baseURL: webBaseURL}, nil
		case *connectorpb.SlackConversationsHistoryRequest:
			return &conversationsHistoryVendorRequest{authSetter: auth, req: r, baseURL: webBaseURL}, nil
		case *connectorpb.SlackConversationsRepliesRequest:
			return &conversationsRepliesVendorRequest{authSetter: auth, req: r, baseURL: webBaseURL}, nil
		case *connectorpb.SlackAdminAnalyticsRequest:
			return &adminAnalyticsVendorRequest{authSetter: auth, req: r, baseURL: webBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

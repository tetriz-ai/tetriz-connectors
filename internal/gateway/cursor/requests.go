// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursorgateway

import (
	"encoding/json"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// authSetter implements vendorapi.VendorRequestWithAuth for Cursor requests.
//
// Cursor authenticates with HTTP Basic, the API key as the username and an EMPTY
// password — the only vendor here that does, so there is no sibling gateway to
// copy. SetBasicAuth is used rather than a hand-built header because it owns the
// base64 encoding and the exact `Basic ` prefix; assembling that by hand is how
// an off-by-one in the separator or a missing colon ships silently.
type authSetter struct {
	apiKey string
}

func (a *authSetter) SetAuth(req *http.Request) *http.Request {
	if a.apiKey != "" {
		req.SetBasicAuth(a.apiKey, "")
	}
	return req
}

// --- /teams/members ---

type listTeamMembersVendorRequest struct {
	*authSetter
	baseURL string
}

func (r *listTeamMembersVendorRequest) HTTPMethod() string { return http.MethodGet }

func (r *listTeamMembersVendorRequest) URL() string {
	return r.baseURL + "/teams/members"
}

func (r *listTeamMembersVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *listTeamMembersVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &listTeamMembersVendorResponse{}
}

type listTeamMembersVendorResponse struct{}

func (r *listTeamMembersVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		TeamMembers []json.RawMessage `json:"teamMembers"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling cursor list team members response: %w", err)
	}
	resp := &connectorpb.CursorListTeamMembersResponse{}
	for _, m := range raw.TeamMembers {
		resp.TeamMembers = append(resp.TeamMembers, m)
	}
	return resp, nil
}

// --- /teams/filtered-usage-events ---

type filteredUsageEventsVendorRequest struct {
	*authSetter
	req     *connectorpb.CursorFilteredUsageEventsRequest
	baseURL string
}

func (r *filteredUsageEventsVendorRequest) HTTPMethod() string { return http.MethodPost }

// SetHeaders declares the body's media type. vendorapi.Handle does not infer it —
// only the DoJSON helper does — so a JSON POST that omits this reaches the vendor
// with no Content-Type, and a server that binds JSON only for that type rejects the
// request before it ever looks at the credential.
func (r *filteredUsageEventsVendorRequest) SetHeaders(req *http.Request) *http.Request {
	req.Header.Set("Content-Type", "application/json")
	return req
}

func (r *filteredUsageEventsVendorRequest) URL() string {
	return r.baseURL + "/teams/filtered-usage-events"
}

// Marshal builds the request body. Fields are omitted when zero rather than sent as
// 0: the vendor treats an absent pageSize as "default 100", and sending 0 would ask
// for an empty page instead.
func (r *filteredUsageEventsVendorRequest) Marshal() ([]byte, error) {
	body := map[string]any{
		"startDate": r.req.GetStartDate(),
		"endDate":   r.req.GetEndDate(),
	}
	if p := r.req.GetPage(); p > 0 {
		body["page"] = p
	}
	if ps := r.req.GetPageSize(); ps > 0 {
		body["pageSize"] = ps
	}
	out, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshalling cursor filtered usage events request: %w", err)
	}
	return out, nil
}

func (r *filteredUsageEventsVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &filteredUsageEventsVendorResponse{}
}

type filteredUsageEventsVendorResponse struct{}

func (r *filteredUsageEventsVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		UsageEvents           []json.RawMessage `json:"usageEvents"`
		TotalUsageEventsCount int64             `json:"totalUsageEventsCount"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling cursor filtered usage events response: %w", err)
	}
	resp := &connectorpb.CursorFilteredUsageEventsResponse{
		TotalUsageEventsCount: raw.TotalUsageEventsCount,
	}
	for _, e := range raw.UsageEvents {
		resp.UsageEvents = append(resp.UsageEvents, e)
	}
	return resp, nil
}

// --- Factory ---

// NewCursorVendorRequest returns a SyncRequestFactory that maps proto messages to
// Cursor vendor requests. The factory closure captures the API key and base URLs.
//
// The two endpoints resolve against different groups on purpose: they carry
// different published caps (20/min on the Admin API, 60/min on usage events), and
// the group is the rate-limit pool. Both point at the same host today, so a mix-up
// would not surface as a broken URL — it would surface as the wrong budget.
func NewCursorVendorRequest(apiKey string, baseURLs map[string]string) vendorapi.SyncRequestFactory {
	adminBaseURL := baseURLs[constants.GroupCursorAdmin]
	usageBaseURL := baseURLs[constants.GroupCursorUsage]

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		auth := &authSetter{apiKey: apiKey}
		switch r := req.(type) {
		case *connectorpb.CursorListTeamMembersRequest:
			return &listTeamMembersVendorRequest{authSetter: auth, baseURL: adminBaseURL}, nil
		case *connectorpb.CursorFilteredUsageEventsRequest:
			return &filteredUsageEventsVendorRequest{authSetter: auth, req: r, baseURL: usageBaseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

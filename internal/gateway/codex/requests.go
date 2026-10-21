// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codexgateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

// authHeaders implements vendorapi.VendorRequestWithAuth for every Codex request.
//
// The key is presented as an Authorization bearer token. That is the whole of the
// authentication contract: unlike the Anthropic sibling there is no dated version
// header to pin, and unlike it the key goes in Authorization rather than a
// vendor-specific header — the two are not interchangeable in either direction.
type authHeaders struct {
	apiKey string
}

func (a *authHeaders) SetAuth(req *http.Request) *http.Request {
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	return req
}

// Every request type must satisfy the auth interface, or vendorapi's optional
// type-assertion silently ships them unauthenticated. Asserted at compile time
// because that failure is invisible to a passing test suite.
var _ vendorapi.VendorRequestWithAuth = (*usageVendorRequest)(nil)

// --- /v1/analytics/codex/workspaces/{workspace_id}/usage ---

type usageVendorRequest struct {
	*authHeaders
	req     *connectorpb.CodexUsageRequest
	baseURL string
}

func (r *usageVendorRequest) HTTPMethod() string { return http.MethodGet }

// URL puts the workspace identifier in the path, escaped. It is administrator-supplied
// rather than derived from the credential, so it is the one segment here that can
// carry an unexpected character, and an unescaped one would change which endpoint is
// addressed rather than merely fail.
func (r *usageVendorRequest) URL() string {
	q := url.Values{}
	// The window bounds are Unix seconds on the wire. They are sent unconditionally,
	// including a zero: an absent bound would ask the vendor for its own default
	// window, which is a different question from the one the day-walk asks and would
	// be recorded as if it were the answer.
	q.Set("start_time", strconv.FormatInt(r.req.GetStartTime(), 10))
	q.Set("end_time", strconv.FormatInt(r.req.GetEndTime(), 10))
	// A zero limit is omitted rather than sent as limit=0. The vendor reads an absent
	// limit as its own default and a zero as a request for an empty page.
	if limit := r.req.GetLimit(); limit > 0 {
		q.Set("limit", strconv.Itoa(int(limit)))
	}
	// An empty cursor is how the first page is asked for.
	if page := r.req.GetPage(); page != "" {
		q.Set("page", page)
	}
	return r.baseURL + "/v1/analytics/codex/workspaces/" +
		url.PathEscape(r.req.GetWorkspaceId()) + "/usage?" + q.Encode()
}

func (r *usageVendorRequest) Marshal() ([]byte, error) { return nil, nil }

func (r *usageVendorRequest) GetResponse() vendorapi.VendorResponse {
	return &usageVendorResponse{}
}

type usageVendorResponse struct{}

// Unmarshal reads the flat envelope: has_more and next_page sit alongside data
// rather than nested under a page object. Rows are carried through as bytes, so a
// field the vendor adds survives into the stored payload untouched instead of being
// dropped at this boundary.
//
// The error carries no part of the body. A row holds developer addresses and
// per-developer counters, and an error string travels into logs.
func (r *usageVendorResponse) Unmarshal(body []byte) (proto.Message, error) {
	var raw struct {
		Data     *[]json.RawMessage `json:"data"`
		HasMore  *bool              `json:"has_more"`
		NextPage string             `json:"next_page"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("unmarshalling codex usage response: %w", err)
	}
	// The envelope this design reads is flat: data, has_more and next_page at the top
	// level. One third-party client describes them nested under a "page" object instead,
	// and the vendor publishes no schema to settle which is current. Both fields are
	// pointers so an absent key is distinguishable from a zero value: without that, a
	// nested response unmarshals cleanly into zero rows and no next page, and the walk
	// reports the day covered having read nothing. Refusing is what makes a moved shape
	// a visible failure rather than a silent under-count.
	if raw.Data == nil || raw.HasMore == nil {
		return nil, fmt.Errorf(
			"codex usage response is not the flat envelope this connector reads: " +
				"expected top-level \"data\" and \"has_more\"")
	}
	resp := &connectorpb.CodexUsageResponse{
		HasMore:  *raw.HasMore,
		NextPage: raw.NextPage,
	}
	for _, d := range *raw.Data {
		resp.UsageRows = append(resp.UsageRows, d)
	}
	return resp, nil
}

// --- Factory ---

// NewCodexVendorRequest returns a SyncRequestFactory mapping proto messages to Codex
// vendor requests. The closure captures the API key and base URLs.
//
// One request type resolves against one group. A second endpoint joins that same
// group rather than declaring its own; constants.GroupCodexAnalytics records why.
func NewCodexVendorRequest(apiKey string, baseURLs map[string]string) vendorapi.SyncRequestFactory {
	baseURL := baseURLs[constants.GroupCodexAnalytics]

	return func(req proto.Message) (vendorapi.VendorRequest, error) {
		auth := &authHeaders{apiKey: apiKey}
		switch r := req.(type) {
		case *connectorpb.CodexUsageRequest:
			return &usageVendorRequest{authHeaders: auth, req: r, baseURL: baseURL}, nil
		default:
			return nil, fmt.Errorf("unsupported request type: %T", req)
		}
	}
}

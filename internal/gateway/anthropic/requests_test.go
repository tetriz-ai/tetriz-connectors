// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropicgateway

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

const testBaseURL = "https://api.anthropic.com"

func testFactory(t *testing.T, apiKey string) func(proto.Message) (*http.Request, string) {
	t.Helper()
	factory := NewAnthropicVendorRequest(apiKey, map[string]string{
		constants.GroupAnthropicAnalytics: testBaseURL,
	})
	return func(msg proto.Message) (*http.Request, string) {
		t.Helper()
		vendorReq, err := factory(msg)
		require.NoErrorf(t, err, "factory rejected %T", msg)
		rawURL := vendorReq.URL()
		httpReq, err := http.NewRequest(vendorReq.HTTPMethod(), rawURL, nil) //nolint:noctx // header assertions only
		require.NoError(t, err)
		if withAuth, ok := vendorReq.(interface {
			SetAuth(*http.Request) *http.Request
		}); ok {
			httpReq = withAuth.SetAuth(httpReq)
		}
		return httpReq, rawURL
	}
}

// TestEveryRequestCarriesTheKeyAndTheVersion covers the whole family in one pass.
// Both headers are mandatory on every Anthropic call, and the failure modes differ:
// a missing key is a clear 401, while a missing version is rejected for a reason
// that reads nothing like a missing header — so the version is the one a partial
// implementation loses silently.
//
// The key belongs in x-api-key. An Anthropic key presented as a bearer token is
// refused, so asserting the header name rather than merely its presence is the point.
func TestEveryRequestCarriesTheKeyAndTheVersion(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "sk-ant-api01-test")
	for _, msg := range allFiveRequests() {
		req, _ := build(msg)
		assert.Equalf(t, "sk-ant-api01-test", req.Header.Get("x-api-key"), "%T", msg)
		assert.Equalf(t, anthropicVersion, req.Header.Get("anthropic-version"), "%T", msg)
		assert.Emptyf(t, req.Header.Get("Authorization"),
			"%T must not present the key as a bearer token; the vendor refuses it", msg)
	}
}

// TestAnEmptyKeyIsNotSentAsAnEmptyHeader keeps a misconfiguration legible. An empty
// x-api-key would be a header the vendor rejects as malformed rather than as
// unauthenticated, which sends an operator looking at the wrong layer.
func TestAnEmptyKeyIsNotSentAsAnEmptyHeader(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "")
	req, _ := build(&connectorpb.AnthropicOrganizationMembersRequest{})
	_, present := req.Header["X-Api-Key"]
	assert.False(t, present, "an absent credential must omit the header, not send it empty")
	assert.Equal(t, anthropicVersion, req.Header.Get("anthropic-version"),
		"the version is not conditional on the credential")
}

// TestRequestURLs pins each endpoint's path and the parameters the request builder
// fixes rather than accepting from a caller. Those fixed parameters are not style:
// a pagination cursor is bound to the exact query that issued it, so a parameter a
// caller could vary between pages makes a mid-sequence 400 reachable.
func TestRequestURLs(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "key")

	tests := []struct {
		name      string
		msg       proto.Message
		wantPath  string
		wantQuery map[string][]string
		absent    []string
	}{
		{
			name:     "users takes the day and omits an unset limit and cursor",
			msg:      &connectorpb.AnthropicAnalyticsUsersRequest{Date: "2026-08-01"},
			wantPath: "/v1/organizations/analytics/users",
			wantQuery: map[string][]string{
				"date": {"2026-08-01"},
			},
			// A zero limit must be absent, not limit=0: the vendor reads absent as its
			// own default and zero as a request for an empty page.
			absent: []string{"limit", "page"},
		},
		{
			name: "users passes an explicit limit and cursor through",
			msg: &connectorpb.AnthropicAnalyticsUsersRequest{
				Date: "2026-08-01", Limit: 1000, Page: "cursor-abc",
			},
			wantPath: "/v1/organizations/analytics/users",
			wantQuery: map[string][]string{
				"date":  {"2026-08-01"},
				"limit": {"1000"},
				"page":  {"cursor-abc"},
			},
		},
		{
			name:     "skills fixes the user grouping the per-user invariant depends on",
			msg:      &connectorpb.AnthropicAnalyticsSkillsRequest{Date: "2026-08-01"},
			wantPath: "/v1/organizations/analytics/skills",
			wantQuery: map[string][]string{
				"date":       {"2026-08-01"},
				"group_by[]": {"user_id"},
			},
		},
		{
			name: "the cost report fixes the daily bucket and the product-and-model grouping",
			msg: &connectorpb.AnthropicAnalyticsUserCostReportRequest{
				StartingAt: timestamppb.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)),
				EndingAt:   timestamppb.New(time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)),
			},
			wantPath: "/v1/organizations/analytics/user_cost_report",
			wantQuery: map[string][]string{
				"starting_at":  {"2026-08-01T00:00:00Z"},
				"ending_at":    {"2026-08-08T00:00:00Z"},
				"bucket_width": {"1d"},
				"group_by[]":   {"product", "model"},
			},
			// No products[] filter: filtering to Claude Code would discard the breadth
			// this ingestion exists to gain, and grouping by product recovers that slice.
			absent: []string{"products[]"},
		},
		{
			name: "the usage report takes the same shape on its own path",
			msg: &connectorpb.AnthropicAnalyticsUserUsageReportRequest{
				StartingAt: timestamppb.New(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)),
			},
			wantPath: "/v1/organizations/analytics/user_usage_report",
			wantQuery: map[string][]string{
				"starting_at":  {"2026-08-01T00:00:00Z"},
				"bucket_width": {"1d"},
				"group_by[]":   {"product", "model"},
			},
			// An unset ending_at is omitted so the vendor applies its own default
			// window rather than being handed an empty timestamp.
			absent: []string{"ending_at"},
		},
		{
			name:      "the member roster's first page carries no query at all",
			msg:       &connectorpb.AnthropicOrganizationMembersRequest{},
			wantPath:  "/v1/organizations/users",
			wantQuery: map[string][]string{},
			absent:    []string{"after_id", "limit"},
		},
		{
			name: "the member roster resumes on after_id, a third pagination shape",
			msg: &connectorpb.AnthropicOrganizationMembersRequest{
				AfterId: "user_abc", Limit: 1000,
			},
			wantPath: "/v1/organizations/users",
			wantQuery: map[string][]string{
				"after_id": {"user_abc"},
				"limit":    {"1000"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, rawURL := build(tt.msg)

			parsed, err := url.Parse(rawURL)
			require.NoError(t, err)
			assert.Equal(t, testBaseURL, parsed.Scheme+"://"+parsed.Host)
			assert.Equal(t, tt.wantPath, parsed.Path)

			// The assertions below go through Go's own decoder, so they cannot see
			// how the brackets were encoded. This is the repo's first bracket-array
			// parameter, so pin the raw bytes too — the vendor sees these, not the
			// decoded form.
			if len(tt.wantQuery["group_by[]"]) > 0 {
				for _, v := range tt.wantQuery["group_by[]"] {
					assert.Containsf(t, rawURL, "group_by%5B%5D="+v, "raw encoding of group_by[]=%s", v)
				}
			}

			q := parsed.Query()
			for key, want := range tt.wantQuery {
				assert.Equalf(t, want, q[key], "query parameter %q", key)
			}
			for _, key := range tt.absent {
				assert.NotContainsf(t, q, key, "%q must be omitted, not sent empty or zero", key)
			}
			assert.Lenf(t, q, len(tt.wantQuery),
				"unexpected query parameters: got %v, expected exactly %v", q, tt.wantQuery)
		})
	}
}

// TestResponsesCarryVendorObjectsVerbatim is the storage contract, asserted at the
// boundary that could break it. Each response keeps its rows as raw bytes, so a
// field Anthropic adds later survives ingestion without a code change — and, more
// importantly, two things are never parsed here: an amount is a decimal string in
// fractional cents, where parsing into a binary float loses precision on figures
// running to millions; and several metrics are nullable in a way that separates
// "zero" from "not applicable", which a parser flattening to zero erases.
func TestResponsesCarryVendorObjectsVerbatim(t *testing.T) {
	t.Parallel()

	t.Run("the activity shape keeps unknown fields and reports its cursor", func(t *testing.T) {
		t.Parallel()
		body := []byte(`{"data":[{"user":{"id":"user_1"},"a_field_added_later":{"nested":true}}],"next_page":"cursor-2"}`)
		msg, err := (&analyticsUsersVendorResponse{}).Unmarshal(body)
		require.NoError(t, err)
		resp, ok := msg.(*connectorpb.AnthropicAnalyticsUsersResponse)
		require.True(t, ok)
		require.Len(t, resp.GetData(), 1)
		assert.JSONEq(t,
			`{"user":{"id":"user_1"},"a_field_added_later":{"nested":true}}`,
			string(resp.GetData()[0]))
		assert.Equal(t, "cursor-2", resp.GetNextPage())
	})

	t.Run("an absent cursor is the activity shape's only end signal", func(t *testing.T) {
		t.Parallel()
		msg, err := (&analyticsSkillsVendorResponse{}).Unmarshal([]byte(`{"data":[]}`))
		require.NoError(t, err)
		resp, ok := msg.(*connectorpb.AnthropicAnalyticsSkillsResponse)
		require.True(t, ok)
		assert.Empty(t, resp.GetNextPage())
		assert.Empty(t, resp.GetData(), "an empty page is valid, not an error")
	})

	t.Run("the report shape preserves a fractional-cent amount as its exact string", func(t *testing.T) {
		t.Parallel()
		body := []byte(`{"data":[{"amount":"41280.000000","cost_type":null}],` +
			`"next_page":"c2","has_more":true,"data_refreshed_at":"2026-08-25T10:00:00Z",` +
			`"organization_id":"org_abc"}`)
		msg, err := (&userCostReportVendorResponse{}).Unmarshal(body)
		require.NoError(t, err)
		resp, ok := msg.(*connectorpb.AnthropicAnalyticsUserCostReportResponse)
		require.True(t, ok)
		require.Len(t, resp.GetData(), 1)
		// The exact digits survive, and the explicit null is still a null rather than
		// having become a zero.
		assert.Contains(t, string(resp.GetData()[0]), `"41280.000000"`)
		assert.Contains(t, string(resp.GetData()[0]), `"cost_type":null`)
		assert.True(t, resp.GetHasMore())
		assert.Equal(t, "c2", resp.GetNextPage())
		assert.Equal(t,
			time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC),
			resp.GetDataRefreshedAt().AsTime())
		assert.Equal(t, "org_abc", resp.GetOrganizationId())
	})

	t.Run("the usage report reads the same envelope on its own type", func(t *testing.T) {
		t.Parallel()
		body := []byte(`{"data":[{"total_tokens":10}],"has_more":false,"data_refreshed_at":"2026-08-25T10:00:00Z"}`)
		msg, err := (&userUsageReportVendorResponse{}).Unmarshal(body)
		require.NoError(t, err)
		resp, ok := msg.(*connectorpb.AnthropicAnalyticsUserUsageReportResponse)
		require.True(t, ok)
		assert.False(t, resp.GetHasMore())
		assert.Equal(t,
			time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC),
			resp.GetDataRefreshedAt().AsTime())
	})

	t.Run("the roster reports the ids that bound its page", func(t *testing.T) {
		t.Parallel()
		body := []byte(`{"data":[{"id":"user_1","email":"a@example.com"}],` +
			`"first_id":"user_1","last_id":"user_9","has_more":true}`)
		msg, err := (&organizationMembersVendorResponse{}).Unmarshal(body)
		require.NoError(t, err)
		resp, ok := msg.(*connectorpb.AnthropicOrganizationMembersResponse)
		require.True(t, ok)
		require.Len(t, resp.GetData(), 1)
		assert.Equal(t, "user_1", resp.GetFirstId())
		// last_id is what a following request passes as after_id.
		assert.Equal(t, "user_9", resp.GetLastId())
		assert.True(t, resp.GetHasMore())
	})
}

// TestMalformedBodyIsAnError keeps a truncated or non-JSON body from reading as an
// empty page, which a caller would otherwise checkpoint as a completed window and
// never re-read.
//
// It also pins that the error does not carry the body. These responses hold member
// email addresses and per-user spend, and naming the operation is enough to locate a
// failure without quoting one.
//
// This is not the only path a body could travel: ClassifyHTTPErrorWithHeaders in
// vendorapi appends the whole body to its message on every non-2xx, when the body is
// 2048 bytes or fewer — it does not truncate, it omits anything larger. That is out of
// this package's hands and narrow in practice, since the payloads carrying addresses
// and amounts arrive with 200s and a 4xx body is an error envelope, but it is the path
// to check before concluding a response cannot reach the logs.
func TestMalformedBodyIsAnError(t *testing.T) {
	t.Parallel()

	responses := []struct {
		name string
		call func([]byte) (proto.Message, error)
	}{
		{"users", (&analyticsUsersVendorResponse{}).Unmarshal},
		{"skills", (&analyticsSkillsVendorResponse{}).Unmarshal},
		{"cost report", (&userCostReportVendorResponse{}).Unmarshal},
		{"usage report", (&userUsageReportVendorResponse{}).Unmarshal},
		{"members", (&organizationMembersVendorResponse{}).Unmarshal},
	}

	// A body that is both malformed and full of things that must not be logged.
	const secretish = `{"data":[{"email":"person@example.com","amount":"41280.000000"}`

	for _, r := range responses {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			_, err := r.call([]byte(secretish))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshalling anthropic",
				"the error must name the operation so a failure is locatable")
			assert.NotContains(t, err.Error(), "person@example.com",
				"a vendor payload must not travel in an error; this is how responses reach the logs")
			assert.NotContains(t, err.Error(), "41280.000000")
		})
	}
}

// TestUnsupportedRequestTypeIsRejected pins the factory's default branch. A new
// endpoint's request type must fail loudly here rather than resolving to whichever
// case happens to be first.
func TestUnsupportedRequestTypeIsRejected(t *testing.T) {
	t.Parallel()

	factory := NewAnthropicVendorRequest("key", map[string]string{
		constants.GroupAnthropicAnalytics: testBaseURL,
	})
	_, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

// TestDataRefreshedAtDegradesToUnsetRatherThanFailing pins the lenient direction. The
// field only bounds a re-read, so an absent or malformed value must leave it unset —
// re-reading a window we could have skipped — rather than failing a page whose rows
// are themselves fine.
func TestDataRefreshedAtDegradesToUnsetRatherThanFailing(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"data":[{"amount":"1.0"}]}`,
		`{"data":[{"amount":"1.0"}],"data_refreshed_at":""}`,
		`{"data":[{"amount":"1.0"}],"data_refreshed_at":"not-a-timestamp"}`,
	} {
		msg, err := (&userCostReportVendorResponse{}).Unmarshal([]byte(body))
		require.NoErrorf(t, err, "body %s must not fail the page", body)
		resp, ok := msg.(*connectorpb.AnthropicAnalyticsUserCostReportResponse)
		require.True(t, ok)
		assert.Nilf(t, resp.GetDataRefreshedAt(), "body %s", body)
		assert.Len(t, resp.GetData(), 1, "the rows still arrive")
	}
}

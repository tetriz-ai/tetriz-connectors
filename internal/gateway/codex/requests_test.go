// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codexgateway

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

const testBaseURL = "https://api.chatgpt.com"

func testFactory(t *testing.T, apiKey string) func(proto.Message) (*http.Request, string) {
	t.Helper()
	factory := NewCodexVendorRequest(apiKey, map[string]string{
		constants.GroupCodexAnalytics: testBaseURL,
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

// TestTheKeyIsPresentedAsABearerToken pins the header name and its prefix. This is
// the whole authentication contract, and it is the one place this gateway diverges
// from the Anthropic sibling it is otherwise a copy of: a key placed in that sibling's
// vendor-specific header is simply not sent as credentials here, which the vendor
// reports as an unauthenticated request rather than as a misplaced header.
func TestTheKeyIsPresentedAsABearerToken(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "sk-proj-test")
	req, _ := build(&connectorpb.CodexUsageRequest{WorkspaceId: "ws-1"})

	assert.Equal(t, "Bearer sk-proj-test", req.Header.Get("Authorization"))
	assert.Empty(t, req.Header.Get("x-api-key"),
		"the key belongs in Authorization; a vendor-specific header is not read here")
}

// TestAnEmptyKeyIsNotSentAsAnEmptyHeader keeps a misconfiguration legible. A bare
// "Bearer " would be a header the vendor rejects as malformed rather than as
// unauthenticated, which sends an operator looking at the wrong layer.
func TestAnEmptyKeyIsNotSentAsAnEmptyHeader(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "")
	req, _ := build(&connectorpb.CodexUsageRequest{WorkspaceId: "ws-1"})
	_, present := req.Header["Authorization"]
	assert.False(t, present, "an absent credential must omit the header, not send a bare Bearer")
}

// TestUsageRequestURL pins the path shape and the query the operation varies. The
// window bounds are Unix seconds rather than a rendered time, so there is no layout
// for a caller to disagree with between pages of one cursor sequence.
func TestUsageRequestURL(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "key")

	tests := []struct {
		name      string
		msg       *connectorpb.CodexUsageRequest
		wantPath  string
		wantQuery map[string][]string
		absent    []string
	}{
		{
			name: "a day's window with an unset limit and cursor",
			msg: &connectorpb.CodexUsageRequest{
				WorkspaceId: "ws-1", StartTime: 1767225600, EndTime: 1767311999,
			},
			wantPath: "/v1/analytics/codex/workspaces/ws-1/usage",
			wantQuery: map[string][]string{
				"start_time": {"1767225600"},
				"end_time":   {"1767311999"},
			},
			// A zero limit must be absent, not limit=0: the vendor reads absent as its
			// own default and zero as a request for an empty page.
			absent: []string{"limit", "page"},
		},
		{
			name: "an explicit page size and cursor are passed through",
			msg: &connectorpb.CodexUsageRequest{
				WorkspaceId: "ws-1", StartTime: 1767225600, EndTime: 1767311999,
				Limit: 100, Page: "cursor-abc",
			},
			wantPath: "/v1/analytics/codex/workspaces/ws-1/usage",
			wantQuery: map[string][]string{
				"start_time": {"1767225600"},
				"end_time":   {"1767311999"},
				"limit":      {"100"},
				"page":       {"cursor-abc"},
			},
		},
		{
			// A zero bound is a real instant, not an absent one. Omitting it would ask
			// the vendor for its own default window and the answer would be stored as
			// if it were the window requested.
			name:     "a zero window bound is still sent",
			msg:      &connectorpb.CodexUsageRequest{WorkspaceId: "ws-1"},
			wantPath: "/v1/analytics/codex/workspaces/ws-1/usage",
			wantQuery: map[string][]string{
				"start_time": {"0"},
				"end_time":   {"0"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, rawURL := build(tt.msg)
			parsed, err := url.Parse(rawURL)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPath, parsed.Path)
			q := parsed.Query()
			for k, want := range tt.wantQuery {
				assert.Equalf(t, want, q[k], "query param %q", k)
			}
			for _, k := range tt.absent {
				assert.NotContainsf(t, q, k, "query param %q must be omitted, not sent empty", k)
			}
		})
	}
}

// TestWorkspaceIdIsEscapedIntoThePath is the guard on an administrator-supplied path
// segment. The identifier is pasted by a human and cannot be validated against the
// credential, so an unescaped slash or dot-segment would address a different endpoint
// than the one intended rather than merely fail.
func TestWorkspaceIdIsEscapedIntoThePath(t *testing.T) {
	t.Parallel()

	build := testFactory(t, "key")
	_, rawURL := build(&connectorpb.CodexUsageRequest{WorkspaceId: "ws/../../admin"})
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)

	assert.Equal(t, "/v1/analytics/codex/workspaces/ws%2F..%2F..%2Fadmin/usage", parsed.EscapedPath(),
		"a pasted identifier must not be able to leave its path segment")
}

// TestUnsupportedRequestTypeIsRejected pins the factory's default branch. A message it
// cannot map must be refused rather than silently building a request against an empty
// path, which would reach the transport as a host-less URL.
func TestUnsupportedRequestTypeIsRejected(t *testing.T) {
	t.Parallel()

	factory := NewCodexVendorRequest("key", map[string]string{constants.GroupCodexAnalytics: testBaseURL})
	_, err := factory(&connectorpb.CodexUsageResponse{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

// TestUsageResponseUnmarshal pins the flat envelope and the pass-through of rows. A
// row is carried as bytes so a field the vendor adds survives into the stored payload
// untouched — parsing it into typed fields here is what would drop it.
func TestUsageResponseUnmarshal(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"object": "list",
		"data": [
			{"user_id":"u-1","actor":{"email":null},"messages":3,"unknown_future":"kept"},
			{"user_id":"u-2","actor":{"email":"dev@example.com"},"messages":null}
		],
		"has_more": true,
		"next_page": "cursor-next"
	}`)

	msg, err := (&usageVendorResponse{}).Unmarshal(body)
	require.NoError(t, err)
	resp, ok := msg.(*connectorpb.CodexUsageResponse)
	require.True(t, ok)

	require.Len(t, resp.GetUsageRows(), 2)
	assert.True(t, resp.GetHasMore())
	assert.Equal(t, "cursor-next", resp.GetNextPage())
	assert.Contains(t, string(resp.GetUsageRows()[0]), `"unknown_future":"kept"`,
		"a field the vendor adds must survive this boundary untouched")
	assert.Contains(t, string(resp.GetUsageRows()[1]), `"messages":null`,
		"a null counter must stay null; flattening it to zero would erase not-applicable")
}

// TestUsageResponseUnmarshal_EmptyPageIsNotAnError pins the quiet-day case. A
// workspace with no activity in the window returns no rows, which is data rather than
// a failure — treating it as one would fail a run over an idle day.
func TestUsageResponseUnmarshal_EmptyPageIsNotAnError(t *testing.T) {
	t.Parallel()

	msg, err := (&usageVendorResponse{}).Unmarshal([]byte(`{"object":"list","data":[],"has_more":false}`))
	require.NoError(t, err)
	resp, ok := msg.(*connectorpb.CodexUsageResponse)
	require.True(t, ok)
	assert.Empty(t, resp.GetUsageRows())
	assert.False(t, resp.GetHasMore())
	assert.Empty(t, resp.GetNextPage())
}

// TestUsageResponseUnmarshal_ErrorCarriesNoBody keeps a vendor payload out of the
// logs. A row holds developer addresses and per-developer counters, and an error
// string travels wherever the error is logged.
func TestUsageResponseUnmarshal_ErrorCarriesNoBody(t *testing.T) {
	t.Parallel()

	_, err := (&usageVendorResponse{}).Unmarshal([]byte(`{"data": [{"actor":{"email":"dev@example.com"`))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "dev@example.com")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ghgateway

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// TestParseTokenExpiration covers the best-effort header parse: a valid layout
// yields a timestamp, an empty header yields nil with no log, and a value that
// matches no known layout yields nil AND emits an observable warning (the
// swallowed-error guard — a format flip must not silently drop expiry).
func TestParseTokenExpiration(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantNil   bool
		wantUnix  int64
		wantWarn  bool
		warnHasOK bool // warn carries the raw value field
	}{
		{
			name:    "empty header → nil, no warn",
			raw:     "",
			wantNil: true,
		},
		{
			name:     "current space-separated UTC layout",
			raw:      "2026-12-31 23:59:59 UTC",
			wantNil:  false,
			wantUnix: time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC).Unix(),
		},
		{
			name:     "RFC3339 fallback layout",
			raw:      "2026-12-31T23:59:59Z",
			wantNil:  false,
			wantUnix: time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC).Unix(),
		},
		{
			name:      "unknown layout → nil + warn",
			raw:       "not-a-timestamp",
			wantNil:   true,
			wantWarn:  true,
			warnHasOK: true,
		},
		{
			name:      "over-long unknown layout → nil + warn with truncated raw",
			raw:       strings.Repeat("x", maxLoggedHeaderLen+16),
			wantNil:   true,
			wantWarn:  true,
			warnHasOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.WarnLevel)
			prev := logger.Log
			logger.Log = zap.New(core)
			t.Cleanup(func() { logger.Log = prev })

			got := parseTokenExpiration(tt.raw)

			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.Equal(t, tt.wantUnix, got.AsTime().Unix())
			}

			warns := recorded.All()
			if tt.wantWarn {
				require.Len(t, warns, 1, "exactly one warn expected on an unparseable header")
				if tt.warnHasOK {
					// Assert against the truncated value so this exercises
					// truncateForLog end-to-end, not just the no-op short path.
					assert.Equal(t, truncateForLog(tt.raw), warns[0].ContextMap()["raw"], "warn must carry the (truncated) raw header value")
				}
			} else {
				assert.Empty(t, warns, "no warn expected on a parseable or empty header")
			}
		})
	}
}

// TestTruncateForLog guards the vendor-controlled-value bound: short strings
// pass through, an over-long string is capped with a marker.
func TestTruncateForLog(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "short value untouched", in: "2026-12-31 23:59:59 UTC", want: "2026-12-31 23:59:59 UTC"},
		{name: "exactly at cap untouched", in: string(make([]byte, maxLoggedHeaderLen)), want: string(make([]byte, maxLoggedHeaderLen))},
		{name: "over cap truncated", in: string(make([]byte, maxLoggedHeaderLen+10)), want: string(make([]byte, maxLoggedHeaderLen)) + "…(truncated)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, truncateForLog(tt.in))
		})
	}
}

func testBaseURLs() map[string]string {
	return map[string]string{
		constants.GroupGitHubGraphQL: "https://api.github.com/graphql",
		constants.GroupGitHubREST:    "https://api.github.com",
	}
}

// --- authSetter tests ---

func TestAuthSetter_BearerToken(t *testing.T) {
	auth := &authSetter{token: "ghp_test-token"}
	req, _ := http.NewRequest("GET", "https://example.com", nil)
	auth.SetAuth(req)
	assert.Equal(t, "bearer ghp_test-token", req.Header.Get("Authorization"))
	assert.Equal(t, "application/vnd.github+json", req.Header.Get("Accept"))
	assert.Equal(t, "2022-11-28", req.Header.Get("X-GitHub-Api-Version"))
}

func TestAuthSetter_EmptyToken(t *testing.T) {
	auth := &authSetter{token: ""}
	req, _ := http.NewRequest("GET", "https://example.com", nil)
	auth.SetAuth(req)
	assert.Empty(t, req.Header.Get("Authorization"))
	assert.Equal(t, "application/vnd.github+json", req.Header.Get("Accept"))
}

// --- GraphQL request tests ---

func TestGraphQLVendorRequest(t *testing.T) {
	factory := NewGitHubVendorRequest("test-token", testBaseURLs())

	vendorReq, err := factory(&connectorpb.GitHubGraphQLRequest{Query: `{ viewer { login } }`})
	require.NoError(t, err)
	assert.Equal(t, "POST", vendorReq.HTTPMethod())
	assert.Equal(t, "https://api.github.com/graphql", vendorReq.URL())

	body, err := vendorReq.Marshal()
	require.NoError(t, err)
	assert.Contains(t, string(body), `"query":"{ viewer { login } }"`)
}

func TestGraphQLVendorResponse_Unmarshal(t *testing.T) {
	resp := &graphQLVendorResponse{}
	protoMsg, err := resp.Unmarshal([]byte(`{"data":{"repository":{"name":"core"}},"errors":[{"message":"rate limited","type":"RATE_LIMITED"}]}`))
	require.NoError(t, err)

	ghResp := protoMsg.(*connectorpb.GitHubGraphQLResponse)
	assert.Contains(t, string(ghResp.Data), "repository")
	assert.Len(t, ghResp.Errors, 1)
	assert.Equal(t, "rate limited", ghResp.Errors[0].Message)
}

// --- ListOrgRepos request tests ---

func TestListOrgReposVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListOrgReposRequest{Organization: "acme"})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "/orgs/acme/repos")
	assert.Contains(t, u, "per_page=100")
	assert.Contains(t, u, "page=1")
	assert.Contains(t, u, "type=all")
}

func TestListOrgReposVendorRequest_URLWithPage(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListOrgReposRequest{
		Organization: "acme",
		Page:         3,
		PerPage:      50,
		Type:         "sources",
	})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "page=3")
	assert.Contains(t, u, "per_page=50")
	assert.Contains(t, u, "type=sources")
}

func TestListOrgReposVendorResponse_HasNextPage(t *testing.T) {
	resp := &listOrgReposVendorResponse{perPage: 2}

	// Full page = has next
	protoMsg, err := resp.Unmarshal([]byte(`[{"full_name":"a/b"},{"full_name":"c/d"}]`))
	require.NoError(t, err)
	assert.True(t, protoMsg.(*connectorpb.ListOrgReposResponse).HasNextPage)
	assert.Len(t, protoMsg.(*connectorpb.ListOrgReposResponse).Repos, 2)

	// Partial page = no next
	resp2 := &listOrgReposVendorResponse{perPage: 2}
	protoMsg2, err := resp2.Unmarshal([]byte(`[{"full_name":"a/b"}]`))
	require.NoError(t, err)
	assert.False(t, protoMsg2.(*connectorpb.ListOrgReposResponse).HasNextPage)
}

// --- ListUserRepos request tests ---

func TestListUserReposVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListUserReposRequest{})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "/user/repos")
	assert.Contains(t, u, "per_page=100")
	assert.Contains(t, u, "page=1")
	// visibility omitted when empty (vendor default).
	assert.NotContains(t, u, "visibility=")
	assert.Equal(t, "GET", vendorReq.HTTPMethod())
}

func TestListUserReposVendorRequest_URLWithParams(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListUserReposRequest{
		Visibility: "private",
		Page:       2,
		PerPage:    50,
	})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "page=2")
	assert.Contains(t, u, "per_page=50")
	assert.Contains(t, u, "visibility=private")
}

func TestListUserReposVendorResponse_BareArray(t *testing.T) {
	// /user/repos returns a bare array (same shape as /orgs/{org}/repos).
	resp := &listUserReposVendorResponse{perPage: 2}
	protoMsg, err := resp.Unmarshal([]byte(`[{"full_name":"me/a"},{"full_name":"me/b"}]`))
	require.NoError(t, err)
	typed := protoMsg.(*connectorpb.ListUserReposResponse)
	assert.Len(t, typed.Repos, 2)
	assert.True(t, typed.HasNextPage)

	resp2 := &listUserReposVendorResponse{perPage: 2}
	protoMsg2, err := resp2.Unmarshal([]byte(`[{"full_name":"me/a"}]`))
	require.NoError(t, err)
	assert.False(t, protoMsg2.(*connectorpb.ListUserReposResponse).HasNextPage)
}

// --- ListInstallationRepos request tests ---

func TestListInstallationReposVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListInstallationReposRequest{Page: 3, PerPage: 25})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "/installation/repositories")
	assert.Contains(t, u, "page=3")
	assert.Contains(t, u, "per_page=25")
	assert.Equal(t, "GET", vendorReq.HTTPMethod())
}

func TestListInstallationReposVendorResponse_WrappedEnvelope(t *testing.T) {
	// /installation/repositories wraps the page under a "repositories" key
	// alongside "total_count" — the parser must unwrap before counting.
	resp := &listInstallationReposVendorResponse{perPage: 2}
	protoMsg, err := resp.Unmarshal([]byte(`{"total_count":7,"repositories":[{"full_name":"org/a"},{"full_name":"org/b"}]}`))
	require.NoError(t, err)
	typed := protoMsg.(*connectorpb.ListInstallationReposResponse)
	assert.Len(t, typed.Repos, 2)
	assert.True(t, typed.HasNextPage, "len(repositories)==per_page means another page")

	// Partial page (fewer than per_page) = no next page.
	resp2 := &listInstallationReposVendorResponse{perPage: 2}
	protoMsg2, err := resp2.Unmarshal([]byte(`{"total_count":1,"repositories":[{"full_name":"org/a"}]}`))
	require.NoError(t, err)
	assert.False(t, protoMsg2.(*connectorpb.ListInstallationReposResponse).HasNextPage)
}

// --- GetAuthenticatedUser request tests ---

func TestGetAuthenticatedUserVendorRequest_URLAndAuth(t *testing.T) {
	t.Parallel()
	factory := NewGitHubVendorRequest("ghp_pat", testBaseURLs())

	vendorReq, err := factory(&connectorpb.GetAuthenticatedUserRequest{})
	require.NoError(t, err)
	assert.Equal(t, "GET", vendorReq.HTTPMethod())
	assert.Equal(t, "https://api.github.com/user", vendorReq.URL())

	httpReq, _ := http.NewRequest("GET", vendorReq.URL(), nil)
	withAuth := vendorReq.(*getAuthenticatedUserVendorRequest)
	withAuth.SetAuth(httpReq)
	assert.Equal(t, "bearer ghp_pat", httpReq.Header.Get("Authorization"))
}

func TestGetAuthenticatedUserVendorResponse_BodyAndHeaderExpiry(t *testing.T) {
	t.Parallel()
	const body = `{"login":"octocat","id":583231,"email":"octocat@example.com"}`

	withExpiry := http.Header{}
	withExpiry.Set(tokenExpirationHeader, "2026-12-31 23:59:59 UTC")
	withUnparseable := http.Header{}
	withUnparseable.Set(tokenExpirationHeader, "not-a-timestamp")

	wantExpiry := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)

	tests := []struct {
		name       string
		header     http.Header
		withHeader bool
		wantExpiry *time.Time // nil → CredentialExpiresAt must be nil
	}{
		{
			name:       "body only — expiry nil (Unmarshal path)",
			withHeader: false,
		},
		{
			name:       "header carries the fine-grained PAT expiry",
			header:     withExpiry,
			withHeader: true,
			wantExpiry: &wantExpiry,
		},
		{
			name:       "expiry header absent — nil (non-expiring token)",
			header:     http.Header{},
			withHeader: true,
		},
		{
			name:       "unparseable header — nil, never a hard failure",
			header:     withUnparseable,
			withHeader: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := &getAuthenticatedUserVendorResponse{}
			var (
				msg proto.Message
				err error
			)
			if tt.withHeader {
				msg, err = resp.UnmarshalWithHeaders([]byte(body), tt.header)
			} else {
				msg, err = resp.Unmarshal([]byte(body))
			}
			require.NoError(t, err)
			typed := msg.(*connectorpb.GetAuthenticatedUserResponse)
			assert.Equal(t, "octocat", typed.GetLogin())
			assert.Equal(t, int64(583231), typed.GetId())
			assert.Equal(t, "octocat@example.com", typed.GetEmail())
			if tt.wantExpiry == nil {
				assert.Nil(t, typed.GetCredentialExpiresAt())
			} else {
				require.NotNil(t, typed.GetCredentialExpiresAt())
				assert.Equal(t, tt.wantExpiry.Unix(), typed.GetCredentialExpiresAt().AsTime().Unix())
			}
		})
	}
}

// --- ListPRReviews request tests ---

func TestListPRReviewsVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.ListPRReviewsRequest{
		Owner:    "acme",
		Repo:     "widgets",
		PrNumber: 42,
	})
	require.NoError(t, err)
	u := vendorReq.URL()
	assert.Contains(t, u, "/repos/acme/widgets/pulls/42/reviews")
	assert.Contains(t, u, "per_page=100")
}

// --- Copilot request tests ---

func TestCopilotMetricsVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.CopilotMetricsRequest{Organization: "acme"})
	require.NoError(t, err)
	assert.Contains(t, vendorReq.URL(), "/orgs/acme/copilot/metrics")
	assert.Equal(t, "GET", vendorReq.HTTPMethod())
}

func TestCopilotUserUsageVendorRequest_URL(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	vendorReq, err := factory(&connectorpb.CopilotUserUsageRequest{Organization: "acme"})
	require.NoError(t, err)
	assert.Contains(t, vendorReq.URL(), "/orgs/acme/copilot/usage/user")
}

// --- Factory tests ---

func TestNewGitHubVendorRequest_AllTypes(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	validRequests := []proto.Message{
		&connectorpb.GitHubGraphQLRequest{Query: "{ viewer { login } }"},
		&connectorpb.ListOrgReposRequest{Organization: "acme"},
		&connectorpb.ListUserReposRequest{},
		&connectorpb.ListInstallationReposRequest{},
		&connectorpb.GetAuthenticatedUserRequest{},
		&connectorpb.ListPRReviewsRequest{Owner: "acme", Repo: "widgets", PrNumber: 1},
		&connectorpb.CopilotMetricsRequest{Organization: "acme"},
		&connectorpb.CopilotUserUsageRequest{Organization: "acme"},
	}
	for _, req := range validRequests {
		vendorReq, err := factory(req)
		require.NoError(t, err, "factory should handle %T", req)
		assert.NotNil(t, vendorReq)
		assert.NotEmpty(t, vendorReq.URL())
	}
}

func TestNewGitHubVendorRequest_UnknownType(t *testing.T) {
	factory := NewGitHubVendorRequest("test", testBaseURLs())

	_, err := factory(&connectorpb.GitHubGraphQLResponse{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

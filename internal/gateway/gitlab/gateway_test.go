// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth"

	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// mockConfigStore is a test double for ConnectorConfigStore.
type mockConfigStore struct {
	getCfg     *sdk.ConnectorConfig
	getErr     error
	getFn      func(ctx context.Context, orgID string, connectorID sdk.ConnectorID)
	saveErr    error
	saveFn     func(ctx context.Context, cfg *sdk.ConnectorConfig)
	saveCalled int
}

func (m *mockConfigStore) Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	if m.getFn != nil {
		m.getFn(ctx, orgID, connectorID)
	}
	return m.getCfg, m.getErr
}

func (m *mockConfigStore) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	m.saveCalled++
	if m.saveFn != nil {
		m.saveFn(ctx, cfg)
	}
	return m.saveErr
}

const testOrgID = "org-gitlab-test-123"

// mockDoer records requests and returns a canned response.
type mockDoer struct {
	statusCode int
	body       string
	headers    http.Header
	lastReq    *http.Request
}

func (d *mockDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.lastReq = req
	h := d.headers
	if h == nil {
		h = make(http.Header)
	}
	return &http.Response{
		StatusCode: d.statusCode,
		Body:       io.NopCloser(strings.NewReader(d.body)),
		Header:     h,
	}, nil
}

// sequenceDoer returns different responses for each call.
type sequenceDoer struct {
	responses []struct {
		statusCode int
		body       string
	}
	calls   int
	lastReq *http.Request
}

func (d *sequenceDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.lastReq = req
	idx := d.calls
	if idx >= len(d.responses) {
		idx = len(d.responses) - 1
	}
	d.calls++
	return &http.Response{
		StatusCode: d.responses[idx].statusCode,
		Body:       io.NopCloser(strings.NewReader(d.responses[idx].body)),
		Header:     make(http.Header),
	}, nil
}

// newTestAutoRefreshDoer creates an autoRefreshDoer with a test token endpoint.
func newTestAutoRefreshDoer(
	inner *mockDoer,
	rc *redis.Client,
	creds ConnectorConfigStore,
	tokenServerURL string,
) *autoRefreshDoer {
	return &autoRefreshDoer{
		inner:         inner,
		cache:         authredis.NewTokenCache(rc),
		locker:        authredis.NewLocker(rc),
		tx:            store.PostgresTxRunner{},
		configDAO:     creds,
		clientID:      "test-client-id",
		clientSecret:  "test-client-secret",
		orgID:         testOrgID,
		tokenEndpoint: tokenServerURL,
	}
}

// newCredsJSON marshals GitLabCredentials to JSON for test setup.
func newCredsJSON(t *testing.T, refreshToken string) []byte {
	t.Helper()
	b, err := json.Marshal(glconfig.GitLabCredentials{OAuthRefreshToken: refreshToken})
	require.NoError(t, err)
	return b
}

// --- autoRefreshDoer tests ---

func TestAutoRefreshDoer_UsesCache(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed cached access token — second call should hit Redis, no token exchange.
	_ = mr.Set(glconfig.AccessTokenKey(testOrgID), "cached-gl-token")

	inner := &mockDoer{statusCode: 200, body: `[{"id": 1}]`}
	doer := NewAutoRefreshDoer(inner, auth.RefreshDeps{Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc), Creds: nil, Tx: store.PostgresTxRunner{}}, "cid", "csecret", testOrgID)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	resp, err := doer.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer cached-gl-token", inner.lastReq.Header.Get("Authorization"))
}

func TestAutoRefreshDoer_RefreshesOnCacheMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// No cached token — triggers refresh.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		// GitLab uses body params — no Basic auth.
		assert.NoError(t, r.ParseForm())
		assert.Equal(t, "test-client-id", r.FormValue("client_id"))
		assert.Equal(t, "test-client-secret", r.FormValue("client_secret"))
		assert.Equal(t, "refresh_token", r.FormValue("grant_type"))
		assert.Equal(t, "old-refresh-token", r.FormValue("refresh_token"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "fresh-access-token",
			"refresh_token": "new-refresh-token",
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, "old-refresh-token"),
		},
		getFn: func(_ context.Context, orgID string, connectorID sdk.ConnectorID) {
			assert.Equal(t, testOrgID, orgID)
			assert.Equal(t, sdk.ConnectorID("gitlab"), connectorID)
		},
		saveFn: func(_ context.Context, cfg *sdk.ConnectorConfig) {
			var creds glconfig.GitLabCredentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			// New refresh token ALWAYS persisted.
			assert.Equal(t, "new-refresh-token", creds.OAuthRefreshToken)
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[{"id": 1}]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer fresh-access-token", inner.lastReq.Header.Get("Authorization"))

	// Verify access token is cached in Redis.
	cached, err := rc.Get(context.Background(), glconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "fresh-access-token", cached)
}

func TestAutoRefreshDoer_RefreshUsesBodyParams_NotBasicAuth(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	var capturedReq *http.Request
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedReq = r
		assert.NoError(t, r.ParseForm())

		// Respond successfully so the doer proceeds.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-token-xyz",
			"refresh_token": "rotated-refresh-token",
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, "current-refresh-token"),
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)

	require.NotNil(t, capturedReq)

	// Assert NO Basic auth header is present.
	authHeader := capturedReq.Header.Get("Authorization")
	assert.Empty(t, authHeader, "GitLab must NOT use Basic auth for token refresh")

	// Assert client_id and client_secret are in the POST body.
	assert.Equal(t, "test-client-id", capturedReq.FormValue("client_id"))
	assert.Equal(t, "test-client-secret", capturedReq.FormValue("client_secret"))
}

func TestAutoRefreshDoer_AlwaysPersistsRefreshToken(t *testing.T) {
	// Even when the new refresh token is the same as the old one, configDAO.Save
	// must be called (GitLab always rotates — same token value would be unusual
	// but we always persist unconditionally).
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	sameRefreshToken := "same-refresh-token"
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "new-access-token",
			"refresh_token": sameRefreshToken, // same value as current
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, sameRefreshToken),
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)

	// Save must have been called exactly once even though refresh token value is identical.
	assert.Equal(t, 1, creds.saveCalled, "configDAO.Save must always be called (GitLab always rotates)")
}

func TestAutoRefreshDoer_On401_InvalidatesCacheAndRetries(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed a stale cached access token.
	_ = mr.Set(glconfig.AccessTokenKey(testOrgID), "stale-gl-token")

	// Mock token endpoint for refresh after 401.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "refreshed-gl-token",
			"refresh_token": "new-refresh-token-after-401",
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, "original-refresh-token"),
		},
	}

	// Inner doer returns 401 first, then 200.
	sd := &sequenceDoer{
		responses: []struct {
			statusCode int
			body       string
		}{
			{http.StatusUnauthorized, `{"message": "401 Unauthorized"}`},
			{http.StatusOK, `[{"id": 42}]`},
		},
	}

	d := &autoRefreshDoer{
		inner:         sd,
		cache:         authredis.NewTokenCache(rc),
		locker:        authredis.NewLocker(rc),
		tx:            store.PostgresTxRunner{},
		configDAO:     creds,
		clientID:      "test-client-id",
		clientSecret:  "test-client-secret",
		orgID:         testOrgID,
		tokenEndpoint: tokenServer.URL,
	}

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, sd.calls, "expected 2 calls (initial + retry after 401)")

	// Retry must use the refreshed token.
	assert.Equal(t, "Bearer refreshed-gl-token", sd.lastReq.Header.Get("Authorization"))

	// Stale token must be evicted and new one cached.
	cached, err := rc.Get(context.Background(), glconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "refreshed-gl-token", cached)
}

func TestAutoRefreshDoer_EmptyRefreshTokenInResponse_ReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Token endpoint returns a response with no refresh_token.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "some-access-token",
			// refresh_token intentionally absent
			"expires_in": 7200,
		})
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, "current-refresh-token"),
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	_, err := d.Do(context.Background(), req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "gitlab: empty refresh token in response")
}

func TestAutoRefreshDoer_RefreshFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Token endpoint returns 400 with OAuth invalid_grant body.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"refresh token revoked"}`))
	}))
	defer tokenServer.Close()

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, "expired-refresh-token"),
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	// Typed SyncError from the OAuth refresh classifier (see
	// gateway/vendorapi/oauth.go). The orchestrator branches on its kind,
	// not the string, and pauses the connector as auth_revoked on a
	// terminal auth error.
	assert.Contains(t, err.Error(), "token refresh outcome=invalid_grant status=400")

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.False(t, syncErr.Retryable, "invalid_grant is terminal until human reconnect")
	assert.True(t, syncErr.Terminal, "invalid_grant must SkipRetry, not burn MaxRetry")
}

// TestAutoRefreshDoer_MissingRefreshToken verifies a disconnected/wiped
// connector (empty refresh token) fails Terminal so the scheduler SkipRetries
// instead of looping MaxRetry until the org reconnects.
func TestAutoRefreshDoer_MissingRefreshToken(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: testOrgID,
			Credentials:    newCredsJSON(t, ""), // no refresh token
		},
	}

	inner := &mockDoer{statusCode: 200, body: `[]`}
	d := newTestAutoRefreshDoer(inner, rc, creds, "http://unused")

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://gitlab.com/api/v4/groups/1/projects", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "want a sdk.SyncError")
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal, "no refresh token is terminal until the org reconnects")
}

// errReturningDoer surfaces a fixed error, simulating autoRefreshDoer returning
// a dead-credential/invalid_grant *sdk.SyncError from g.Doer.Do.
type errReturningDoer struct{ err error }

func (d *errReturningDoer) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, d.err
}

// TestExecute_PreservesTerminalSyncError guards that Execute preserves an
// upstream Terminal *sdk.SyncError instead of shadowing it under a fresh
// ErrKindVendorError. gitlab's Execute replicates vendorapi.Handle by hand;
// without the errors.As preservation the scheduler's errors.As would find the
// outer (non-terminal) error and burn MaxRetry on a dead credential. It covers
// the gap the doer-level tests miss (they never go through Execute).
func TestExecute_PreservesTerminalSyncError(t *testing.T) {
	terminal := &sdk.SyncError{
		Kind:     sdk.ErrKindAuth,
		Terminal: true,
		Cause:    errors.New("gitlab: no refresh token (reconnect required)"),
	}
	gw := &GitLabVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			// Wrapped exactly as autoRefreshDoer.Do does ("token refresh after 401: %w").
			Doer:    &errReturningDoer{err: fmt.Errorf("token refresh after 401: %w", terminal)},
			Factory: NewGitLabVendorRequest(map[string]string{constants.GroupGitLabREST: "https://gitlab.com/api/v4"}),
		},
	}

	_, err := gw.Execute(context.Background(), &connectorpb.GitLabListProjectsRequest{GroupId: 1})
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.True(t, syncErr.Terminal, "Execute must preserve the upstream Terminal SyncError, not shadow it")
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
}

// --- Request URL tests ---

func TestGitLabRequestFactory_ListProjects(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListProjectsRequest{
		GroupId: 42,
		Page:    2,
		PerPage: 50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "https://gitlab.com/api/v4/groups/42/projects")
	assert.Contains(t, vr.URL(), "per_page=50")
	assert.Contains(t, vr.URL(), "page=2")
	assert.Contains(t, vr.URL(), "include_subgroups=false")
	assert.Contains(t, vr.URL(), "with_shared=false")

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body)
}

func TestGitLabRequestFactory_ListProjects_Defaults(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListProjectsRequest{GroupId: 10}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Contains(t, vr.URL(), "per_page=100")
	assert.Contains(t, vr.URL(), "page=1")
}

func TestGitLabRequestFactory_ListCommits(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListCommitsRequest{
		ProjectId: 123,
		Since:     "2024-06-01T00:00:00Z",
		Page:      3,
		PerPage:   100,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/projects/123/repository/commits")
	assert.Contains(t, vr.URL(), "since=2024-06-01T00%3A00%3A00Z")
	assert.Contains(t, vr.URL(), "page=3")
}

func TestGitLabRequestFactory_ListCommits_NoSince(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListCommitsRequest{ProjectId: 7}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.NotContains(t, vr.URL(), "since")
}

// TestGitLabRequestFactory_ListProjectMergeRequests_OrderByMatchesFilterField
// guards the page-cursor resume invariant: order_by/sort must match the
// updated_after filter field (ascending), or an MR that newly starts matching
// the filter between an interrupted run and its resume can land at any page
// position — not appended at the end — shifting content across already-fetched
// pages and permanently skipping MRs at the resume boundary. GitLab's own
// default (order_by=created_at&sort=desc) does not satisfy this.
func TestGitLabRequestFactory_ListProjectMergeRequests_OrderByMatchesFilterField(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListProjectMergeRequestsRequest{
		ProjectId:    77,
		UpdatedAfter: "2024-01-01T00:00:00Z",
		Page:         3,
		PerPage:      50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/projects/77/merge_requests")
	assert.Contains(t, vr.URL(), "updated_after=2024-01-01T00%3A00%3A00Z")
	assert.Contains(t, vr.URL(), "page=3")
	assert.Contains(t, vr.URL(), "per_page=50")
	assert.Contains(t, vr.URL(), "order_by=updated_at",
		"order_by must match the updated_after filter field, or page-cursor resume can skip MRs")
	assert.Contains(t, vr.URL(), "sort=asc",
		"ascending sort guarantees a newly-qualifying MR sorts after the current resume page, never before it")
}

func TestGitLabRequestFactory_ListMRClosesIssues(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	req := &connectorpb.GitLabListMRClosesIssuesRequest{
		ProjectId:       88,
		MergeRequestIid: 56,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/projects/88/merge_requests/56/closes_issues")
	assert.Contains(t, vr.URL(), "per_page=100")

	// The raw array body is passed through verbatim for a consumer's extractor.
	body := []byte(`[{"iid":17,"references":{"full":"g/p#17"}}]`)
	msg, err := vr.GetResponse().Unmarshal(body)
	require.NoError(t, err)
	resp, ok := msg.(*connectorpb.GitLabListMRClosesIssuesResponse)
	require.True(t, ok)
	assert.Equal(t, body, resp.GetIssues())
}

func TestGitLabRequestFactory_UnsupportedType(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "https://gitlab.com/api/v4",
	})

	_, err := factory(&connectorpb.GitLabListProjectsResponse{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

func TestSimulatorGitLabBaseURLs_RewritesGitlabREST(t *testing.T) {
	got, err := simulatorGitLabBaseURLs(map[string]gateway.APIEndpointConfig{
		constants.GroupGitLabREST: {Group: constants.GroupGitLabREST, BaseURL: "https://gitlab.com/api/v4"},
	}, "http://localhost:8080")
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8080/gitlab/api/v4", got[constants.GroupGitLabREST])
}

func TestGitLabRequestFactory_SimulatorBase(t *testing.T) {
	factory := NewGitLabVendorRequest(map[string]string{
		constants.GroupGitLabREST: "http://localhost:8080/gitlab/api/v4",
	})

	req := &connectorpb.GitLabListProjectsRequest{GroupId: 91001, Page: 1, PerPage: 100}
	vr, err := factory(req)
	require.NoError(t, err)
	assert.Contains(t, vr.URL(), "http://localhost:8080/gitlab/api/v4/groups/91001/projects")
}

// --- Response unmarshal tests ---

func TestListProjectsResponse_Unmarshal(t *testing.T) {
	resp := &listProjectsVendorResponse{}
	body := `[{"id": 1, "name": "project-one"}, {"id": 2, "name": "project-two"}]`

	msg, err := resp.Unmarshal([]byte(body))
	require.NoError(t, err)

	projectsResp := msg.(*gitLabListProjectsResponse)
	assert.Len(t, projectsResp.Projects, 2)
	assert.Empty(t, projectsResp.NextPageUrl) // populated via Execute, not Unmarshal
}

func TestListCommitsResponse_Unmarshal(t *testing.T) {
	resp := &listCommitsVendorResponse{}
	body := `[{"id": "abc123", "message": "initial commit"}]`

	msg, err := resp.Unmarshal([]byte(body))
	require.NoError(t, err)

	commitsResp := msg.(*gitLabListCommitsResponse)
	assert.Len(t, commitsResp.Commits, 1)
	assert.Empty(t, commitsResp.NextPageUrl)
}

func TestListProjectsResponse_Unmarshal_Empty(t *testing.T) {
	resp := &listProjectsVendorResponse{}
	msg, err := resp.Unmarshal([]byte(`[]`))
	require.NoError(t, err)

	projectsResp := msg.(*gitLabListProjectsResponse)
	assert.Empty(t, projectsResp.Projects)
}

// --- buildNextPageURL tests ---

func TestBuildNextPageURL(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		nextPage string
		want     string
	}{
		{
			name:     "adds page param",
			baseURL:  "https://gitlab.com/api/v4/groups/1/projects?per_page=100",
			nextPage: "2",
			want:     "https://gitlab.com/api/v4/groups/1/projects?page=2&per_page=100",
		},
		{
			name:     "replaces existing page param",
			baseURL:  "https://gitlab.com/api/v4/groups/1/projects?page=1&per_page=50",
			nextPage: "2",
			want:     "https://gitlab.com/api/v4/groups/1/projects?page=2&per_page=50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildNextPageURL(tt.baseURL, tt.nextPage)
			// Parse both to compare query params regardless of order.
			gotParsed, err := url.Parse(got)
			require.NoError(t, err)
			wantParsed, err := url.Parse(tt.want)
			require.NoError(t, err)
			assert.Equal(t, wantParsed.Query().Get("page"), gotParsed.Query().Get("page"))
			assert.Equal(t, wantParsed.Query().Get("per_page"), gotParsed.Query().Get("per_page"))
			assert.Equal(t, wantParsed.Path, gotParsed.Path)
		})
	}
}

// CompareAndSwapCredentials stands in for the store's swap: it records the
// write through Save, as the refresher's persist, and always swaps.
func (m *mockConfigStore) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, _ time.Time, credentials json.RawMessage) (bool, error) {
	if err := m.Save(ctx, &sdk.ConnectorConfig{OrganizationID: orgID, ConnectorID: connectorID, Credentials: credentials}); err != nil {
		return false, err
	}
	return true, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/tetriz-ai/tetriz-connectors/auth"

	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	os.Exit(testguard.Run(m))
}

const testOrgID = "org-test-123"

// --- Test doubles ---

// mockConfigStore is a test double for ConnectorConfigStore.
type mockConfigStore struct {
	getCfg  *sdk.ConnectorConfig
	getErr  error
	getFn   func(ctx context.Context, orgID string, connectorID sdk.ConnectorID)
	saveErr error
	saveFn  func(ctx context.Context, cfg *sdk.ConnectorConfig)
}

func (m *mockConfigStore) Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	if m.getFn != nil {
		m.getFn(ctx, orgID, connectorID)
	}
	return m.getCfg, m.getErr
}

func (m *mockConfigStore) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if m.saveFn != nil {
		m.saveFn(ctx, cfg)
	}
	return m.saveErr
}

// mockDoer records requests and returns a canned response.
type mockDoer struct {
	statusCode int
	body       string
	lastReq    *http.Request
}

func (d *mockDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.lastReq = req
	return &http.Response{
		StatusCode: d.statusCode,
		Body:       io.NopCloser(strings.NewReader(d.body)),
		Header:     make(http.Header),
	}, nil
}

// sequenceDoer returns different responses for each successive call.
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

func newTestAutoRefreshDoer(
	inner vendorapi.HTTPDoer,
	rc *redis.Client,
	creds ConnectorConfigStore,
	tokenEndpoint string,
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
		tokenEndpoint: tokenEndpoint,
	}
}

// --- autoRefreshDoer tests ---

func TestAutoRefreshDoer_CacheHit(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed cached access token.
	_ = mr.Set(jiraconfig.AccessTokenKey(testOrgID), "cached-access-token")

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	doer := NewAutoRefreshDoer(inner, auth.RefreshDeps{Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc), Creds: nil, Tx: store.PostgresTxRunner{}}, "cid", "csecret", testOrgID)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/ex/jira/abc/rest/api/3/project/search", nil)
	resp, err := doer.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer cached-access-token", inner.lastReq.Header.Get("Authorization"))
}

func TestAutoRefreshDoer_CacheMiss_RefreshSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Mock Atlassian token endpoint.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		// Jira sends credentials in the request body (not Basic auth).
		assert.NoError(t, r.ParseForm())
		assert.Equal(t, "refresh_token", r.Form.Get("grant_type"))
		assert.Equal(t, "test-client-id", r.Form.Get("client_id"))
		assert.Equal(t, "test-client-secret", r.Form.Get("client_secret"))
		assert.Equal(t, "old-refresh-token", r.Form.Get("refresh_token"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "fresh-access-token",
			"refresh_token": "new-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{
		OAuthRefreshToken: "old-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
		// Verify Get is called with correct org and connector ID (inside RunInScope).
		getFn: func(_ context.Context, orgID string, connectorID sdk.ConnectorID) {
			assert.Equal(t, testOrgID, orgID)
			assert.Equal(t, sdk.ConnectorID("jira"), connectorID)
		},
		// Verify the new rotating refresh token is persisted.
		saveFn: func(_ context.Context, cfg *sdk.ConnectorConfig) {
			var creds jiraconfig.JiraCredentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			assert.Equal(t, "new-refresh-token", creds.OAuthRefreshToken)
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/ex/jira/abc/rest/api/3/project/search", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer fresh-access-token", inner.lastReq.Header.Get("Authorization"))

	// Verify the new access token is cached in Redis.
	cached, err := rc.Get(context.Background(), jiraconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "fresh-access-token", cached)
}

func TestAutoRefreshDoer_RotatingRefreshToken_Persisted(t *testing.T) {
	// Jira invalidates old refresh tokens on exchange — verify the new one is persisted.
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	var savedCreds jiraconfig.JiraCredentials

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "new-access-token",
			"refresh_token": "rotated-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{
		OAuthRefreshToken: "initial-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
		saveFn: func(_ context.Context, cfg *sdk.ConnectorConfig) {
			require.NoError(t, json.Unmarshal(cfg.Credentials, &savedCreds))
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/something", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	assert.Equal(t, "rotated-refresh-token", savedCreds.OAuthRefreshToken,
		"rotating refresh token must be persisted on every exchange")
}

func TestAutoRefreshDoer_401Retry(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed a stale cached token.
	_ = mr.Set(jiraconfig.AccessTokenKey(testOrgID), "stale-token")

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "refreshed-token",
			"refresh_token": "new-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{
		OAuthRefreshToken: "original-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
	}

	// Inner doer returns 401 first, then 200.
	sd := &sequenceDoer{
		responses: []struct {
			statusCode int
			body       string
		}{
			{http.StatusUnauthorized, `{"error": "unauthorized"}`},
			{http.StatusOK, `{"ok": true}`},
		},
	}

	d := newTestAutoRefreshDoer(sd, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/something", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, sd.calls, "expected 2 calls: initial + retry after 401")

	// Retry should use the refreshed token.
	assert.Equal(t, "Bearer refreshed-token", sd.lastReq.Header.Get("Authorization"))

	// Stale token must be evicted; refreshed token must be cached.
	cached, err := rc.Get(context.Background(), jiraconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "refreshed-token", cached)
}

func TestAutoRefreshDoer_RefreshFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Token endpoint returns error (e.g. invalid refresh token).
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid_grant", "error_description": "Refresh token is invalid"}`))
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{
		OAuthRefreshToken: "expired-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/something", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	// Typed SyncError from the OAuth refresh classifier. The token endpoint
	// returns {"error":"invalid_grant"} so outcome maps to ErrKindAuth with
	// Retryable=false regardless of HTTP status. The orchestrator branches on
	// the kind, not the string, and pauses the connector as auth_revoked on a
	// terminal auth error.
	assert.Contains(t, err.Error(), "token refresh outcome=invalid_grant status=400")

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.False(t, syncErr.Retryable, "invalid_grant is terminal until human reconnect")
	assert.True(t, syncErr.Terminal, "invalid_grant must SkipRetry, not burn MaxRetry")
}

func TestAutoRefreshDoer_LockContention_CachePopulatedByOther(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Simulate another instance holding the refresh lock and having populated the cache.
	_ = mr.Set(jiraconfig.RefreshLockKey(testOrgID), "1")
	_ = mr.Set(jiraconfig.AccessTokenKey(testOrgID), "token-from-other-instance")

	inner := &mockDoer{statusCode: 200, body: `{}`}
	d := newTestAutoRefreshDoer(inner, rc, nil, "http://unused")

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/something", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// Should use the token cached by the other instance (picked up after the lock wait).
	assert.Equal(t, "Bearer token-from-other-instance", inner.lastReq.Header.Get("Authorization"))
}

func TestAutoRefreshDoer_MissingRefreshToken(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Config has no refresh token.
	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "jira",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, "http://unused")

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.atlassian.com/something", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no refresh token")
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "want a sdk.SyncError")
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal, "no refresh token is terminal until the org reconnects")
}

// TestNewJiraVendorGatewayForSimulator_RewritesBase locks the routing every demo
// Jira request depends on: the jira-rest group base is rewritten onto the
// simulator service root under a /jira prefix, so a request built by the gateway's
// factory targets "<root>/jira/ex/jira/{cloudId}/rest/...". A nil rate-limiter
// creds is safe here — the creds is only touched on Acquire, which this test does
// not exercise.
func TestNewJiraVendorGatewayForSimulator_RewritesBase(t *testing.T) {
	gw, err := NewJiraVendorGatewayForSimulator(&mockDoer{statusCode: 200}, "simulator-token", nil, "http://sim:8080")
	require.NoError(t, err)

	vr, err := gw.Factory(&connectorpb.JiraListProjectsRequest{CloudId: "demo-gh-enterprise"})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(vr.URL(), "http://sim:8080/jira/ex/jira/demo-gh-enterprise/"),
		"simulator gateway must rewrite the jira-rest base onto <root>/jira; got %s", vr.URL())
}

func TestSimulatorBearerDoer_SetsTokenWhenAbsent(t *testing.T) {
	inner := &mockDoer{statusCode: 200, body: "{}"}
	d := &simulatorBearerDoer{inner: inner, token: "simulator-token"}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://sim:8080/jira/x", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer simulator-token", inner.lastReq.Header.Get("Authorization"))
}

// A pre-set Authorization header wins — the simulator doer only fills a missing
// one, so it never clobbers auth a caller deliberately supplied.
func TestSimulatorBearerDoer_DoesNotOverwriteExistingAuth(t *testing.T) {
	inner := &mockDoer{statusCode: 200, body: "{}"}
	d := &simulatorBearerDoer{inner: inner, token: "simulator-token"}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://sim:8080/jira/x", nil)
	req.Header.Set("Authorization", "Bearer preexisting")
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer preexisting", inner.lastReq.Header.Get("Authorization"))
}

// CompareAndSwapCredentials stands in for the store's swap: it records the
// write through Save, as the refresher's persist, and always swaps.
func (m *mockConfigStore) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, _ time.Time, credentials json.RawMessage) (bool, error) {
	if err := m.Save(ctx, &sdk.ConnectorConfig{OrganizationID: orgID, ConnectorID: connectorID, Credentials: credentials}); err != nil {
		return false, err
	}
	return true, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

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

const testOrgID = "org-test-123"

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

// --- autoRefreshDoer tests ---

func TestAutoRefreshDoer_CacheHit(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed cached access token.
	_ = mr.Set(bbconfig.AccessTokenKey(testOrgID), "cached-token-abc")

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	doer := NewAutoRefreshDoer(inner, auth.RefreshDeps{Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc), Creds: nil, Tx: store.PostgresTxRunner{}}, "cid", "csecret", testOrgID)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)
	resp, err := doer.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer cached-token-abc", inner.lastReq.Header.Get("Authorization"))
}

func TestAutoRefreshDoer_CacheMiss_RefreshSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Mock Bitbucket token endpoint.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		// Verify Basic auth.
		user, pass, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "test-client-id", user)
		assert.Equal(t, "test-client-secret", pass)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "fresh-access-token",
			"refresh_token": "new-refresh-token",
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(bbconfig.BitbucketCredentials{
		OAuthRefreshToken: "old-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
		getFn: func(_ context.Context, orgID string, connectorID sdk.ConnectorID) {
			assert.Equal(t, testOrgID, orgID)
			assert.Equal(t, sdk.ConnectorID("bitbucket"), connectorID)
		},
		saveFn: func(_ context.Context, cfg *sdk.ConnectorConfig) {
			var creds bbconfig.BitbucketCredentials
			require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
			assert.Equal(t, "new-refresh-token", creds.OAuthRefreshToken)
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "Bearer fresh-access-token", inner.lastReq.Header.Get("Authorization"))

	// Verify access token is cached in Redis.
	cached, err := rc.Get(context.Background(), bbconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "fresh-access-token", cached)
}

func TestAutoRefreshDoer_401Retry(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Seed a stale cached access token.
	_ = mr.Set(bbconfig.AccessTokenKey(testOrgID), "stale-token")

	// Mock token endpoint for refresh after 401.
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "refreshed-token",
			"refresh_token": "new-refresh-token",
			"expires_in":    7200,
		})
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(bbconfig.BitbucketCredentials{
		OAuthRefreshToken: "original-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
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

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, sd.calls, "expected 2 calls (initial + retry)")

	// Verify the retry used the refreshed token.
	assert.Equal(t, "Bearer refreshed-token", sd.lastReq.Header.Get("Authorization"))

	// Verify stale token was evicted and new one cached.
	cached, err := rc.Get(context.Background(), bbconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Equal(t, "refreshed-token", cached)
}

func TestAutoRefreshDoer_RefreshFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Mock token endpoint that returns 401 (invalid refresh token).
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": "invalid_grant"}`))
	}))
	defer tokenServer.Close()

	credsJSON, _ := json.Marshal(bbconfig.BitbucketCredentials{
		OAuthRefreshToken: "expired-refresh-token",
	})
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, tokenServer.URL)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	// Error shape: typed *sdk.SyncError wrapping the
	// classifier outcome rather than a bare "token refresh failed with
	// status N" string. The token endpoint returns
	// {"error":"invalid_grant"} so the classifier maps to
	// RefreshOutcomeInvalidGrant regardless of HTTP status.
	assert.Contains(t, err.Error(), "token refresh outcome=invalid_grant status=401")

	// And the typed SyncError is reachable via errors.As: the orchestrator
	// branches on its kind without parsing strings, and pauses the connector
	// as auth_revoked on a terminal auth error.
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

	credsJSON, _ := json.Marshal(bbconfig.BitbucketCredentials{}) // no refresh token
	creds := &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: testOrgID,
			Credentials:    credsJSON,
		},
	}

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	d := newTestAutoRefreshDoer(inner, rc, creds, "http://unused")

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "want a sdk.SyncError")
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal, "no refresh token is terminal until the org reconnects")
}

func TestAutoRefreshDoer_LockContention(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Simulate lock held by another instance.
	_ = mr.Set(bbconfig.RefreshLockKey(testOrgID), "1")

	// Also seed the cache (as if the other instance completed refresh).
	_ = mr.Set(bbconfig.AccessTokenKey(testOrgID), "token-from-other-instance")

	inner := &mockDoer{statusCode: 200, body: `{"ok": true}`}
	d := newTestAutoRefreshDoer(inner, rc, nil, "http://unused")

	req, _ := http.NewRequestWithContext(context.Background(), "GET", "https://api.bitbucket.org/2.0/repositories", nil)

	// The first getAccessToken will fail to acquire lock, sleep, then find the cache populated.
	// But first, there's no cache (lock is held), so let's clear the cache to test the fallback.
	rc.Del(context.Background(), bbconfig.AccessTokenKey(testOrgID))

	// With lock held and no cache, getAccessToken should wait and then check cache.
	// Set the cache after a brief delay (simulating the other instance completing).
	_ = mr.Set(bbconfig.AccessTokenKey(testOrgID), "token-from-other-instance")

	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, "Bearer token-from-other-instance", inner.lastReq.Header.Get("Authorization"))
}

// --- Request factory tests ---

func TestBitbucketRequestFactory_ListRepos(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListReposRequest{
		Workspace: "acme",
		Page:      2,
		Pagelen:   50,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "https://api.bitbucket.org/2.0/repositories/acme")
	assert.Contains(t, vr.URL(), "pagelen=50")
	assert.Contains(t, vr.URL(), "page=2")

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body)
}

// TestBitbucketRequestFactory_SimulatorBase locks the simulator routing: with
// the simulator REST base (service-root + "/bitbucket/2.0", as
// NewBitbucketVendorGatewayForSimulator composes it), the factory must produce
// simulator URLs under /bitbucket/2.0, and the v1.0 groups route must derive
// /bitbucket/1.0 — the "/2.0"→"/1.0" replace must survive the /bitbucket prefix, or
// workspace-group enrichment would 404 against the simulator.
func TestBitbucketRequestFactory_SimulatorBase(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "http://localhost:8080/bitbucket/2.0",
	})

	repos, err := factory(&connectorpb.BitbucketListReposRequest{Workspace: "demo-bb-enterprise"})
	require.NoError(t, err)
	assert.Contains(t, repos.URL(), "http://localhost:8080/bitbucket/2.0/repositories/demo-bb-enterprise")

	groups, err := factory(&connectorpb.BitbucketListWorkspaceGroupsRequest{Workspace: "demo-bb-enterprise"})
	require.NoError(t, err)
	assert.Contains(t, groups.URL(), "http://localhost:8080/bitbucket/1.0/groups/demo-bb-enterprise",
		"v1.0 groups base must derive from the simulator's /bitbucket/2.0 base")
}

func TestBitbucketRequestFactory_GetRepo(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketGetRepoRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Equal(t, "https://api.bitbucket.org/2.0/repositories/acme/widgets", vr.URL())

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body)

	// The single-repo endpoint returns the bare repository object; the response
	// passes the raw body through as `repo` for the connector to unmarshal.
	msg, err := vr.GetResponse().Unmarshal([]byte(`{"uuid":"{u}","mainbranch":{"name":"master"}}`))
	require.NoError(t, err)
	resp, ok := msg.(*connectorpb.BitbucketGetRepoResponse)
	require.True(t, ok)
	assert.JSONEq(t, `{"uuid":"{u}","mainbranch":{"name":"master"}}`, string(resp.GetRepo()))
}

func TestBitbucketRequestFactory_ListRepos_Defaults(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListReposRequest{
		Workspace: "acme",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Contains(t, vr.URL(), "pagelen=100")
	assert.Contains(t, vr.URL(), "page=1")
}

func TestBitbucketRequestFactory_ListPullRequests(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListPullRequestsRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		State:     connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_MERGED,
		Page:      1,
		Pagelen:   25,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/repositories/acme/widgets/pullrequests")
	assert.Contains(t, vr.URL(), "state=MERGED")
	assert.Contains(t, vr.URL(), "pagelen=25")
}

func TestBitbucketRequestFactory_ListPullRequests_UnspecifiedState(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListPullRequestsRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		Page:      1,
		Pagelen:   50,
		// State intentionally left as UNSPECIFIED (zero value).
	}

	vr, err := factory(req)
	require.NoError(t, err)

	u := vr.URL()
	assert.Contains(t, u, "/repositories/acme/widgets/pullrequests")
	// When state is unspecified, all four states should be requested explicitly.
	assert.Contains(t, u, "state=OPEN")
	assert.Contains(t, u, "state=MERGED")
	assert.Contains(t, u, "state=DECLINED")
	assert.Contains(t, u, "state=SUPERSEDED")
	// Must sort by updated_on descending for cursor-based sync.
	assert.Contains(t, u, "sort=-updated_on")
	// PR list must request participants + reviewers expansion so reviews_requested
	// metric input is populated without an N+1 per-PR fetch.
	assert.Contains(t, u, "fields=")
	assert.Contains(t, u, "values.participants")
	assert.Contains(t, u, "values.reviewers")
}

func TestBitbucketRequestFactory_GetCommitDiffstat(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketGetCommitDiffstatRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		CommitSha: "deadbeefcafe",
		Page:      1,
		Pagelen:   500,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	u := vr.URL()
	assert.Contains(t, u, "/repositories/acme/widgets/diffstat/deadbeefcafe")
	assert.Contains(t, u, "pagelen=500")
	assert.Contains(t, u, "page=1")
}

func TestBitbucketRequestFactory_ListCommits(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListCommitsRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		Page:      3,
		Pagelen:   100,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/repositories/acme/widgets/commits")
	assert.Contains(t, vr.URL(), "page=3")
}

func TestBitbucketRequestFactory_ListCommits_WithInclude(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListCommitsRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		Page:      1,
		Pagelen:   100,
		Include:   "develop",
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Contains(t, vr.URL(), "/repositories/acme/widgets/commits")
	assert.Contains(t, vr.URL(), "include=develop")
}

func TestBitbucketRequestFactory_ListCommits_WithoutInclude(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketListCommitsRequest{
		Workspace: "acme",
		RepoSlug:  "widgets",
		Page:      1,
		Pagelen:   100,
	}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.NotContains(t, vr.URL(), "include=")
}

func TestBitbucketRequestFactory_GetUser(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	req := &connectorpb.BitbucketGetUserRequest{}

	vr, err := factory(req)
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Equal(t, "https://api.bitbucket.org/2.0/user", vr.URL())
}

func TestBitbucketRequestFactory_UnsupportedType(t *testing.T) {
	factory := NewBitbucketVendorRequest(map[string]string{
		"bitbucket-rest": "https://api.bitbucket.org/2.0",
	})

	_, err := factory(&connectorpb.BitbucketListReposResponse{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

// --- Response unmarshal tests ---

func TestListReposResponse_Unmarshal(t *testing.T) {
	resp := &listReposVendorResponse{}
	body := `{"values": [{"slug": "repo1"}, {"slug": "repo2"}], "next": "https://api.bitbucket.org/2.0/repositories/acme?page=2"}`

	msg, err := resp.Unmarshal([]byte(body))
	require.NoError(t, err)

	reposResp := msg.(*connectorpb.BitbucketListReposResponse)
	assert.Len(t, reposResp.Repos, 2)
	assert.Equal(t, "https://api.bitbucket.org/2.0/repositories/acme?page=2", reposResp.NextPageUrl)
}

func TestListPullRequestsResponse_Unmarshal(t *testing.T) {
	resp := &listPullRequestsVendorResponse{}
	body := `{"values": [{"id": 1, "title": "PR 1"}], "next": ""}`

	msg, err := resp.Unmarshal([]byte(body))
	require.NoError(t, err)

	prResp := msg.(*connectorpb.BitbucketListPullRequestsResponse)
	assert.Len(t, prResp.PullRequests, 1)
	assert.Empty(t, prResp.NextPageUrl)
}

func TestGetUserResponse_Unmarshal(t *testing.T) {
	resp := &getUserVendorResponse{}
	body := `{"uuid": "{user-uuid}", "display_name": "Test User"}`

	msg, err := resp.Unmarshal([]byte(body))
	require.NoError(t, err)

	userResp := msg.(*connectorpb.BitbucketGetUserResponse)
	assert.Contains(t, string(userResp.User), "user-uuid")
}

func TestPRStateString(t *testing.T) {
	tests := []struct {
		state connectorpb.BitbucketPullRequestState
		want  string
	}{
		{connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_OPEN, "OPEN"},
		{connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_MERGED, "MERGED"},
		{connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_DECLINED, "DECLINED"},
		{connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_SUPERSEDED, "SUPERSEDED"},
		{connectorpb.BitbucketPullRequestState_BITBUCKET_PULL_REQUEST_STATE_UNSPECIFIED, ""},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.state), func(t *testing.T) {
			assert.Equal(t, tt.want, prStateString(tt.state))
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

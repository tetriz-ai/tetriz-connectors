// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineargateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const testOrgID = "org-test-123"

// testEnc is a deterministic AES-256-GCM encryptor so seeded ciphertext
// round-trips with the doer's decrypt path.
var testEnc = mustTestEncryptor()

func mustTestEncryptor() cryptutil.Encryptor {
	enc, err := cryptutil.NewAESGCMEncryptor([]byte("0123456789abcdef0123456789abcdef")) // 32 bytes
	if err != nil {
		panic(err)
	}
	return enc
}

// getResult is one canned Get response.
type getResult struct {
	cfg *sdk.ConnectorConfig
	err error
}

// mockConfigStore is a test double for ConnectorConfigStore. When getQueue is
// non-empty its entries are returned in order (then it falls back to getCfg/getErr),
// so a test can model the connector config changing mid-refresh (e.g. a disconnect
// between the refresh-token read and the persist re-read).
type mockConfigStore struct {
	getCfg    *sdk.ConnectorConfig
	getErr    error
	getQueue  []getResult
	saveFn    func(cfg *sdk.ConnectorConfig)
	saveCalls int
	// last is the row the most recent Get returned: the stored row that
	// CompareAndSwapCredentials fences against.
	last *sdk.ConnectorConfig
}

func (m *mockConfigStore) Get(_ context.Context, _ string, _ sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	cfg, err := m.getCfg, m.getErr
	if len(m.getQueue) > 0 {
		r := m.getQueue[0]
		m.getQueue = m.getQueue[1:]
		cfg, err = r.cfg, r.err
	}
	if err == nil {
		m.last = cfg
	}
	return cfg, err
}

// CompareAndSwapCredentials swaps when the stored row's
// CredentialsUpdatedAt still equals expected, and records the swap as a save.
func (m *mockConfigStore) CompareAndSwapCredentials(_ context.Context, _ string, _ sdk.ConnectorID, expected time.Time, credentials json.RawMessage) (bool, error) {
	if m.last == nil || !m.last.CredentialsUpdatedAt.Equal(expected) {
		return false, nil
	}
	m.last.Credentials = credentials
	m.saveCalls++
	if m.saveFn != nil {
		m.saveFn(m.last)
	}
	return true, nil
}

func (m *mockConfigStore) Save(_ context.Context, cfg *sdk.ConnectorConfig) error {
	m.saveCalls++
	if m.saveFn != nil {
		m.saveFn(cfg)
	}
	return nil
}

// sequenceDoer returns canned status codes in order, recording each request.
type sequenceDoer struct {
	codes []int
	reqs  []*http.Request
	i     int
}

func (d *sequenceDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	d.reqs = append(d.reqs, req.Clone(req.Context()))
	code := 200
	if d.i < len(d.codes) {
		code = d.codes[d.i]
	}
	d.i++
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("body")), Header: make(http.Header)}, nil
}

func authHeader(req *http.Request) string { return req.Header.Get("Authorization") }

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

// seedCachedToken stores token in Redis encrypted at rest, as the doer does.
func seedCachedToken(t *testing.T, rc *redis.Client, token string) {
	t.Helper()
	ct, err := testEnc.Encrypt([]byte(token))
	require.NoError(t, err)
	require.NoError(t, rc.Set(context.Background(), linearconfig.AccessTokenKey(testOrgID), ct, 0).Err())
}

// refreshServer answers the refresh-token exchange with the given tokens and
// asserts the request shape; hits counts how many times it was called.
func refreshServer(t *testing.T, accessTok, refreshTok string, hits *int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*hits++
		assert.Equal(t, http.MethodPost, r.Method)
		assert.NoError(t, r.ParseForm())
		assert.Equal(t, "refresh_token", r.PostForm.Get("grant_type"))
		assert.Equal(t, "test-client-id", r.PostForm.Get("client_id"))
		assert.Equal(t, "test-client-secret", r.PostForm.Get("client_secret"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  accessTok,
			"refresh_token": refreshTok,
			"expires_in":    86399,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestDoer(inner vendorapi.HTTPDoer, rc *redis.Client, creds ConnectorConfigStore, enc cryptutil.Encryptor, tokenEndpoint string) *autoRefreshDoer {
	return &autoRefreshDoer{
		inner:         inner,
		cache:         authredis.NewTokenCache(rc),
		locker:        authredis.NewLocker(rc),
		tx:            store.PostgresTxRunner{},
		enc:           enc,
		configDAO:     creds,
		clientID:      "test-client-id",
		clientSecret:  "test-client-secret",
		orgID:         testOrgID,
		tokenEndpoint: tokenEndpoint,
	}
}

func storeWithRefreshToken(t *testing.T, refreshTok string, saveFn func(*sdk.ConnectorConfig)) *mockConfigStore {
	t.Helper()
	credsJSON, err := json.Marshal(linearconfig.LinearCredentials{AccessToken: "stored-access", RefreshToken: refreshTok})
	require.NoError(t, err)
	return &mockConfigStore{
		getCfg: &sdk.ConnectorConfig{ConnectorID: "linear", OrganizationID: testOrgID, Credentials: credsJSON},
		saveFn: saveFn,
	}
}

// Cache hit → use the cached (decrypted) token; never call the refresh endpoint.
func TestAutoRefreshDoer_CacheHit(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	seedCachedToken(t, rc, "cached-tok")
	var hits int
	srv := refreshServer(t, "should-not-be-used", "", &hits)
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, &mockConfigStore{}, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer cached-tok", authHeader(inner.reqs[0]))
	assert.Equal(t, 0, hits, "cached token must not trigger a refresh")
}

// Cache miss → exchange the stored refresh token, persist the rotated tokens,
// cache the new access token encrypted at rest, and authenticate with it.
func TestAutoRefreshDoer_CacheMiss_RefreshSuccess(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)

	var saved linearconfig.LinearCredentials
	creds := storeWithRefreshToken(t, "old-refresh", func(cfg *sdk.ConnectorConfig) {
		require.NoError(t, json.Unmarshal(cfg.Credentials, &saved))
	})
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, hits)
	assert.Equal(t, "Bearer fresh-tok", authHeader(inner.reqs[0]))

	// Rotated tokens persisted to credentials.
	assert.Equal(t, "fresh-tok", saved.AccessToken)
	assert.Equal(t, "rotated-refresh", saved.RefreshToken, "rotating refresh token must be persisted")

	// Access token cached ENCRYPTED, not plaintext.
	raw, err := rc.Get(context.Background(), linearconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.NotEqual(t, "fresh-tok", raw, "token must be encrypted at rest")
	plain, err := testEnc.Decrypt([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, "fresh-tok", string(plain))
}

// 401 → discard the cached token, refresh, and retry once with the new token.
func TestAutoRefreshDoer_401Retry(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	seedCachedToken(t, rc, "stale-tok")
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)
	creds := storeWithRefreshToken(t, "old-refresh", nil)
	inner := &sequenceDoer{codes: []int{401, 200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	resp, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	require.Len(t, inner.reqs, 2)
	assert.Equal(t, "Bearer stale-tok", authHeader(inner.reqs[0]))
	assert.Equal(t, "Bearer fresh-tok", authHeader(inner.reqs[1]))
	assert.Equal(t, 1, hits)
}

// No refresh token in credentials (revoked/wiped) → fail closed with ErrKindAuth,
// so the orchestrator does not loop.
func TestAutoRefreshDoer_NoRefreshToken_FailsClosed(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "x", "y", &hits)
	creds := storeWithRefreshToken(t, "", nil) // empty refresh token
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.Error(t, err)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "want a sdk.SyncError")
	assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
	assert.True(t, syncErr.Terminal, "no refresh token is terminal until the org reconnects")
	assert.Equal(t, 0, hits, "no refresh exchange without a refresh token")
}

// Local plaintext mode (nil encryptor): refresh and authenticate WITHOUT caching
// (no plaintext token in Redis) and without panicking on the nil encryptor.
func TestAutoRefreshDoer_NilEncryptor_NoCaching(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)
	creds := storeWithRefreshToken(t, "old-refresh", nil)
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, nil, srv.URL) // nil enc

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, "Bearer fresh-tok", authHeader(inner.reqs[0]))

	_, getErr := rc.Get(context.Background(), linearconfig.AccessTokenKey(testOrgID)).Result()
	assert.ErrorIs(t, getErr, redis.Nil, "plaintext mode must not cache the token")
}

func liveCfg(t *testing.T) *sdk.ConnectorConfig {
	t.Helper()
	creds, err := json.Marshal(linearconfig.LinearCredentials{AccessToken: "stored-access", RefreshToken: "old-refresh"})
	require.NoError(t, err)
	return &sdk.ConnectorConfig{ConnectorID: "linear", OrganizationID: testOrgID, Credentials: creds}
}

// A disconnect that soft-deletes the row mid-refresh (persist re-read returns
// ErrRecordNotFound) must NOT persist — otherwise the upsert would resurrect the
// disconnected connection with fresh tokens.
func TestAutoRefreshDoer_DisconnectedMidRefresh_SkipsSave(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)
	creds := &mockConfigStore{getQueue: []getResult{
		{cfg: liveCfg(t)},             // load refresh token
		{err: gorm.ErrRecordNotFound}, // persist re-read: row soft-deleted by disconnect
	}}
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 0, creds.saveCalls, "must not resurrect a disconnected connection")
}

// A disconnect that wipes credentials to "{}" mid-refresh (before soft-delete)
// must likewise skip the persist.
func TestAutoRefreshDoer_WipedCredsMidRefresh_SkipsSave(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)
	wiped := &sdk.ConnectorConfig{ConnectorID: "linear", OrganizationID: testOrgID, Credentials: []byte("{}")}
	creds := &mockConfigStore{getQueue: []getResult{
		{cfg: liveCfg(t)}, // load refresh token
		{cfg: wiped},      // persist re-read: credentials wiped by disconnect
	}}
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 0, creds.saveCalls, "must not resurrect a wiped connection")
}

// A plaintext entry in Redis (one this doer never writes when it has an
// encryptor) is a cache miss: it is not sent as the bearer token, and the
// doer refreshes.
func TestAutoRefreshDoer_PlaintextEntryIsAMiss(t *testing.T) {
	t.Parallel()
	rc := newTestRedis(t)
	require.NoError(t, rc.Set(context.Background(), linearconfig.AccessTokenKey(testOrgID), "planted-plaintext", 0).Err())
	var hits int
	srv := refreshServer(t, "fresh-tok", "rotated-refresh", &hits)
	creds := storeWithRefreshToken(t, "old-refresh", func(*sdk.ConnectorConfig) {})
	inner := &sequenceDoer{codes: []int{200}}
	d := newTestDoer(inner, rc, creds, testEnc, srv.URL)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err := d.Do(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, hits, "the plaintext entry is a miss, so the doer refreshes")
	assert.Equal(t, "Bearer fresh-tok", authHeader(inner.reqs[0]))
}

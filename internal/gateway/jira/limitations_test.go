// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

// Tests of the token cache: what the refresher stores, and how.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const limOrg = "00000000-0000-4000-8000-00000000dddd"

func limDoer(rc *redis.Client, creds ConnectorConfigStore, endpoint string) *autoRefreshDoer {
	return &autoRefreshDoer{
		inner: &mockDoer{statusCode: 200, body: `{}`},
		cache: authredis.NewTokenCache(rc), locker: authredis.NewLocker(rc),
		tx: store.NoopTxRunner{}, configDAO: creds,
		clientID: "id", clientSecret: "secret", orgID: limOrg, tokenEndpoint: endpoint,
	}
}

func limSeed(t *testing.T, mem *memstore.Store) {
	t.Helper()
	credsJSON, _ := json.Marshal(jiraconfig.JiraCredentials{OAuthRefreshToken: "refresh-1"})
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "jira", Enabled: true, Credentials: credsJSON}))
}

// Given the credentials encryptor, the refresher caches the access token as
// the credentials' envelope, never in the clear, and the next read decrypts it.
func TestAccessTokenIsCachedEncrypted(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "plain-access", "expires_in": 3600})
	}))
	defer tokenServer.Close()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	enc, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)
	d := NewAutoRefreshDoer(nil, auth.RefreshDeps{
		Cache: authredis.NewTokenCache(rc), Locker: authredis.NewLocker(rc),
		Creds: mem, Tx: store.NoopTxRunner{}, Encryptor: enc,
	}, "id", "secret", limOrg).(*autoRefreshDoer)
	d.tokenEndpoint = tokenServer.URL

	_, err = d.refreshToken(context.Background())
	require.NoError(t, err)
	v, err := mr.Get(jiraconfig.AccessTokenKey(limOrg))
	require.NoError(t, err)
	assert.True(t, cryptutil.IsEnvelope([]byte(v)), "the cached value is an envelope")
	assert.NotContains(t, v, "plain-access")
	got, err := d.cache.Get(context.Background(), jiraconfig.AccessTokenKey(limOrg))
	require.NoError(t, err)
	assert.Equal(t, "plain-access", got)
}

// The refresh lock is released even when the request context is cancelled
// during the exchange, so other refreshes for the org do not wait out its TTL.
func TestCancelledRefreshReleasesLock(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	ctx, cancel := context.WithCancel(context.Background())
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "a", "expires_in": 3600})
	}))
	defer tokenServer.Close()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	_, _ = limDoer(rc, mem, tokenServer.URL).refreshToken(ctx)
	assert.False(t, mr.Exists(jiraconfig.RefreshLockKey(limOrg)), "the cancelled refresh releases its lock")
}

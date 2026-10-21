// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineargateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A reconnect to another workspace that lands during the token exchange wins:
// the refresh does not cache the replaced workspace's access token.
func TestRefreshDoesNotCacheAReplacedAccountToken(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	seed, _ := json.Marshal(linearconfig.LinearCredentials{AccessToken: "stored-access", RefreshToken: "refresh-1"})
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: testOrgID, ConnectorID: "linear", Enabled: true, Credentials: seed}))
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fresh, _ := json.Marshal(linearconfig.LinearCredentials{AccessToken: "account-B-access", RefreshToken: "account-B-grant"})
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: testOrgID, ConnectorID: "linear", Enabled: true, Credentials: fresh}))
		mr.Del(linearconfig.AccessTokenKey(testOrgID)) // the reconnect forgets the cached token
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "account-A-access", "refresh_token": "account-A-rotated", "expires_in": 86399})
	}))
	defer srv.Close()

	_, err := newTestDoer(&sequenceDoer{codes: []int{200}}, rc, mem, testEnc, srv.URL).refreshToken(ctx)
	require.NoError(t, err)
	got, _ := mr.Get(linearconfig.AccessTokenKey(testOrgID))
	require.Empty(t, got, "the replaced workspace's token must not be cached after a reconnect")
	cfg, err := mem.Get(ctx, testOrgID, "linear")
	require.NoError(t, err)
	require.Contains(t, string(cfg.Credentials), "account-B-grant", "the reconnect's credentials stand")
}

// reconnectAfterSwap is the store with a reconnect that lands after the
// refresh's swap and its read-back, just before the cache check reads the row.
type reconnectAfterSwap struct {
	ConnectorConfigStore
	getsAfterSwap int
	swapped       bool
	reconnect     func()
}

func (r *reconnectAfterSwap) CompareAndSwapCredentials(ctx context.Context, org string, c sdk.ConnectorID, expected time.Time, creds json.RawMessage) (bool, error) {
	ok, err := r.ConnectorConfigStore.CompareAndSwapCredentials(ctx, org, c, expected, creds)
	r.swapped = r.swapped || ok
	return ok, err //nolint:wrapcheck // test double
}

func (r *reconnectAfterSwap) Get(ctx context.Context, org string, c sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	if r.swapped {
		r.getsAfterSwap++
		if r.getsAfterSwap == 2 && r.reconnect != nil {
			r.reconnect()
		}
	}
	return r.ConnectorConfigStore.Get(ctx, org, c) //nolint:wrapcheck // test double
}

// A reconnect that lands after the refresh stored its rotated token, but
// before the access token is cached, wins: its cache forget finds nothing
// yet, and the cache check sees the moved credential fence and drops the
// replaced account's token.
func TestReconnectAfterTheSwapDropsTheCachedToken(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	seed, _ := json.Marshal(linearconfig.LinearCredentials{AccessToken: "stored-access", RefreshToken: "refresh-1"})
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: testOrgID, ConnectorID: "linear", Enabled: true, Credentials: seed}))
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "account-A-access", "refresh_token": "account-A-rotated", "expires_in": 3600, "token_type": "Bearer"})
	}))
	defer srv.Close()
	store := &reconnectAfterSwap{ConnectorConfigStore: mem}
	store.reconnect = func() {
		fresh, _ := json.Marshal(linearconfig.LinearCredentials{AccessToken: "account-B-access", RefreshToken: "account-B-grant"})
		_ = mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: testOrgID, ConnectorID: "linear", Enabled: true, Credentials: fresh})
		// Its cache forget ran before the refresh set the entry, so it
		// forgot nothing: only the fence check can drop the stale entry.
	}

	_, err := newTestDoer(&sequenceDoer{codes: []int{200}}, rc, store, testEnc, srv.URL).refreshToken(ctx)
	require.NoError(t, err)
	require.True(t, store.swapped, "the refresh stored its rotated token")
	got, _ := mr.Get(linearconfig.AccessTokenKey(testOrgID))
	require.Empty(t, got, "the replaced account's token must not be cached after a reconnect")
}

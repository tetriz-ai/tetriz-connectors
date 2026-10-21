// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// raceTokenServer answers the refresh exchange after running during, which
// stands for a write that lands while the exchange is in flight.
func raceTokenServer(t *testing.T, during func()) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if during != nil {
			during()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "a-2", "refresh_token": "refresh-2", "expires_in": 3600, "token_type": "Bearer"})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func storedRefreshToken(t *testing.T, mem *memstore.Store) (*sdk.ConnectorConfig, string) {
	t.Helper()
	cfg, err := mem.Get(context.Background(), limOrg, "jira")
	require.NoError(t, err)
	var creds jiraconfig.JiraCredentials
	require.NoError(t, json.Unmarshal(cfg.Credentials, &creds))
	return cfg, creds.OAuthRefreshToken
}

// A reconnect that lands during the refresh stands: its credentials and
// settings are kept, and the rotated token is dropped and not cached.
func TestRefreshKeepsAReconnectThatLandsDuringTheExchange(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	fresh, _ := json.Marshal(jiraconfig.JiraCredentials{OAuthRefreshToken: "fresh-grant"})
	srv := raceTokenServer(t, func() {
		require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "jira", Enabled: true,
			Credentials: fresh, Settings: json.RawMessage(`{"cloud_id":"cloud-2"}`)}))
		_, err := mem.ResetHealthState(context.Background(), limOrg, "jira") // a credential write advances credentials_updated_at
		require.NoError(t, err)
	})
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	cfg, token := storedRefreshToken(t, mem)
	assert.Equal(t, "fresh-grant", token, "the reconnect's credentials survive")
	assert.JSONEq(t, `{"cloud_id":"cloud-2"}`, string(cfg.Settings), "the reconnect's settings survive")
	assert.False(t, mr.Exists(jiraconfig.AccessTokenKey(limOrg)), "the dropped grant's access token is not cached")
}

// A disable that lands during the refresh stands: the refresh writes only the
// credentials.
func TestRefreshKeepsADisableThatLandsDuringTheExchange(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	srv := raceTokenServer(t, func() {
		cfg, err := mem.Get(context.Background(), limOrg, "jira")
		require.NoError(t, err)
		cfg.Enabled = false // config put --disabled
		require.NoError(t, mem.Save(context.Background(), cfg))
	})
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	cfg, token := storedRefreshToken(t, mem)
	assert.False(t, cfg.Enabled, "the operator's disable survives")
	assert.Equal(t, "refresh-2", token, "the rotated token is stored")
}

// A disconnect that lands during the refresh stands: the refresh does not
// bring the connector back.
func TestRefreshDoesNotReviveADisconnect(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	srv := raceTokenServer(t, func() { require.NoError(t, mem.Delete(context.Background(), limOrg, "jira")) })
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	_, err = mem.Get(context.Background(), limOrg, "jira")
	assert.Error(t, err, "the disconnected connector stays disconnected")
}

// cancelOnWrite is the store under a run whose deadline fires between the
// exchange and the write: its credential writes fail on a cancelled context.
type cancelOnWrite struct {
	*memstore.Store
	cancel context.CancelFunc
}

func (c cancelOnWrite) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	c.cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.Store.Save(ctx, cfg)
}

func (c cancelOnWrite) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expected time.Time, credentials json.RawMessage) (bool, error) {
	c.cancel()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return c.Store.CompareAndSwapCredentials(ctx, orgID, connectorID, expected, credentials)
}

// The rotated token is stored even when the run's context ends after the
// exchange: the vendor has already rotated it, so losing the write would
// leave a dead token stored.
func TestRefreshStoresTheRotatedTokenAfterTheRunIsCancelled(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := raceTokenServer(t, nil)
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})

	_, _ = limDoer(rc, cancelOnWrite{Store: mem, cancel: cancel}, srv.URL).refreshToken(ctx)
	_, token := storedRefreshToken(t, mem)
	assert.Equal(t, "refresh-2", token, "the rotated token is stored")
}

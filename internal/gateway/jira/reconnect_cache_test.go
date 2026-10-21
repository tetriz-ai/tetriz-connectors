// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A refresh that stores no rotated token still does not cache its access token
// when a reconnect to another account lands during the exchange: the
// reconnect's credential write moves the fence, so the entry is not kept.
func TestNonRotatingRefreshDoesNotCacheAReplacedAccountToken(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fresh, _ := json.Marshal(jiraconfig.JiraCredentials{OAuthRefreshToken: "account-B-grant"})
		require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "jira", Enabled: true, Credentials: fresh}))
		_, _ = mem.ResetHealthState(context.Background(), limOrg, "jira")
		mr.Del(jiraconfig.AccessTokenKey(limOrg)) // resetConnectorHealth forgets the cached token
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "account-A-access", "expires_in": 3600})
	}))
	defer srv.Close()

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	got, _ := mr.Get(jiraconfig.AccessTokenKey(limOrg))
	require.Empty(t, got, "the replaced account's token must not be cached after a reconnect")
}

// Without a concurrent write the refreshed token is cached as before.
func TestNonRotatingRefreshCachesItsToken(t *testing.T) {
	mem := memstore.New()
	limSeed(t, mem)
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "account-A-access", "expires_in": 3600})
	}))
	defer srv.Close()

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	got, _ := mr.Get(jiraconfig.AccessTokenKey(limOrg))
	require.Equal(t, "account-A-access", got)
}

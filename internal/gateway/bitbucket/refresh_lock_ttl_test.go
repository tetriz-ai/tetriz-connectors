// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

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

	"github.com/tetriz-ai/tetriz-connectors/auth"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
)

// The refresh lock outlives the token exchange and the writes that follow it
// under the lock, so it cannot expire while its holder still exchanges or
// stores a rotated refresh token.
func TestRefreshLockOutlivesTheExchangeAndPersist(t *testing.T) {
	require.Greater(t, refreshLockTTL, refreshExchangeTimeout+auth.PersistTimeout)
	mem := memstore.New()
	limSeed(t, mem)
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	var held time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		held = mr.TTL(bbconfig.RefreshLockKey(limOrg))
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "fresh-access", "refresh_token": "refresh-2", "expires_in": 3600, "token_type": "bearer"})
	}))
	defer srv.Close()

	_, err := limDoer(rc, mem, srv.URL).refreshToken(context.Background())
	require.NoError(t, err)
	require.Equal(t, refreshLockTTL, held, "the refresh takes the lock with refreshLockTTL")
}

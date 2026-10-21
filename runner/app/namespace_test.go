// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	rlstore "github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
	"github.com/tetriz-ai/tetriz-connectors/internal/tokencache"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// writeEveryKind writes one key through each Redis-backed store in d and
// returns the keys Redis then holds.
func writeEveryKind(t *testing.T, mr *miniredis.Miniredis, d GatewayDeps) []string {
	t.Helper()
	ctx := context.Background()
	_, _, err := d.RateLimits.AddMaxN(ctx, "rl", 1, 10, int64(time.Minute))
	require.NoError(t, err)
	reached, err := d.RateLimits.IsWindowsCountReached(ctx, []*ratelimiter.WindowParams{{Key: "rl-multi", FromTime: time.Now().Add(-time.Minute), ToTime: time.Now(), MaxRequest: 10}}, false)
	require.NoError(t, err)
	require.False(t, reached)
	require.NoError(t, d.Refresh.Cache.Set(ctx, "token", "v", time.Minute))
	got, err := d.Refresh.Cache.Get(ctx, "token")
	require.NoError(t, err)
	assert.Equal(t, "v", got)
	_, held, err := d.Refresh.Locker.TryLock(ctx, "token-lock", time.Minute)
	require.NoError(t, err)
	require.True(t, held)
	require.NoError(t, d.BitbucketCircuit.Set(ctx, "circuit", "1", time.Minute).Err())
	h := http.Header{}
	h.Set("X-RateLimit-Remaining", "10")
	h.Set("X-RateLimit-Limit", "100")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	h.Set("X-RateLimit-Resource", "core")
	require.NoError(t, d.Budgets["github"].UpdateFromResponse(ctx, "00000000-0000-4000-8000-000000000001", h))
	keys := mr.Keys()
	sort.Strings(keys)
	return keys
}

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// With no namespace the gateways write exactly the keys the Redis stores
// write unwrapped, the names from before namespaces existed.
func TestEmptyNamespaceKeepsGatewayKeyNames(t *testing.T) {
	mr, rdb := newRedis(t)
	got := writeEveryKind(t, mr, RedisGatewayDeps(Config{}, rdb, nil, store.NoopTxRunner{}))

	mr2, rdb2 := newRedis(t)
	raw := GatewayDeps{
		RateLimits:       rlstore.NewSlidingWindowLogWithRedis(rdb2),
		Budgets:          map[sdk.ConnectorID]*ratelimiter.BudgetLimiter{"github": ratelimiter.NewBudgetLimiter(rdb2, ratelimiter.GitHubAdapter{})},
		Refresh:          auth.RefreshDeps{Cache: authredis.NewTokenCache(rdb2), Locker: authredis.NewLocker(rdb2)},
		BitbucketCircuit: rdb2,
	}
	want := writeEveryKind(t, mr2, raw)
	assert.Equal(t, want, got)
	for _, k := range []string{"rl", "rl-multi", "token", "token-lock", "circuit"} {
		assert.Contains(t, got, k)
	}
}

// With a namespace every key the gateways write carries it.
func TestNamespacePrefixesEveryGatewayKey(t *testing.T) {
	mr, rdb := newRedis(t)
	plain := writeEveryKind(t, mr, RedisGatewayDeps(Config{}, rdb, nil, store.NoopTxRunner{}))
	mr2, rdb2 := newRedis(t)
	keys := writeEveryKind(t, mr2, RedisGatewayDeps(Config{Namespace: "e2e"}, rdb2, nil, store.NoopTxRunner{}))
	want := make([]string, len(plain))
	for i, k := range plain {
		want[i] = "e2e:" + k
	}
	assert.Equal(t, want, keys)

	// PutConfig forgets the cached token under the namespaced key.
	key, ok := tokencache.Key("gitlab", "org")
	require.True(t, ok)
	require.NoError(t, mr2.Set("e2e:"+key, "cached"))
	require.NoError(t, tokencache.ForgetFromCache(context.Background(), RedisGatewayDeps(Config{Namespace: "e2e"}, rdb2, nil, store.NoopTxRunner{}).Refresh.Cache, "gitlab", "org"))
	assert.False(t, mr2.Exists("e2e:"+key))
}

func TestValidateNamespace(t *testing.T) {
	for _, ok := range []string{"", "prod", "team-a.eu_1"} {
		assert.NoError(t, validateNamespace(ok), ok)
	}
	for _, bad := range []string{"a b", "a:b", "{a}", "-a", "ü"} {
		assert.Error(t, validateNamespace(bad), bad)
	}
}

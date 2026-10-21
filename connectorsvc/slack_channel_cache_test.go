// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
)

const cacheOrg = "22222222-2222-2222-2222-222222222222"

// newTestCache builds a cache over an in-process miniredis client — see
// newTestRedisClient (secret_broker_test.go), the single source of the
// miniredis setup shared across this package's tests.
func newTestCache(t *testing.T) *connectorsvc.SlackChannelCache {
	return connectorsvc.NewSlackChannelCache(newTestRedisClient(t))
}

// TestSlackChannelCacheMiss proves Get reports found=false with no error when
// nothing has been cached for the org yet (the cold-start case).
func TestSlackChannelCacheMiss(t *testing.T) {
	cache := newTestCache(t)
	channels, refreshedAt, found, err := cache.Get(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, channels)
	require.True(t, refreshedAt.IsZero())
}

// TestSlackChannelCacheSetThenGet proves a written entry round-trips exactly,
// including the refreshedAt stamp.
func TestSlackChannelCacheSetThenGet(t *testing.T) {
	cache := newTestCache(t)
	want := []*connectorpb.SlackChannel{
		{Id: "C1", Name: "general", IsPrivate: false},
		{Id: "C2", Name: "secret-ops", IsPrivate: true},
	}
	stamp := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)

	err := cache.Set(context.Background(), cacheOrg, want, stamp)
	require.NoError(t, err)

	got, refreshedAt, found, err := cache.Get(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, stamp.Equal(refreshedAt))
	require.Len(t, got, 2)
	require.Equal(t, "C1", got[0].GetId())
	require.Equal(t, "general", got[0].GetName())
	require.False(t, got[0].GetIsPrivate())
	require.True(t, got[1].GetIsPrivate())
}

// TestSlackChannelCacheDelete proves Delete removes a cached entry so a
// subsequent Get reports a miss — the grant-invalidation/purge invalidation path.
func TestSlackChannelCacheDelete(t *testing.T) {
	cache := newTestCache(t)
	require.NoError(t, cache.Set(context.Background(), cacheOrg, []*connectorpb.SlackChannel{{Id: "C1", Name: "general"}}, time.Now()))

	_, _, found, err := cache.Get(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, found, "entry must exist before delete")

	require.NoError(t, cache.Delete(context.Background(), cacheOrg))

	_, _, found, err = cache.Get(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.False(t, found, "Get must report a miss after Delete")
}

// TestSlackChannelCacheTryLockRefreshExclusive proves a second concurrent
// TryLockRefresh for the same org fails while the first lock is held — the
// singleflight guarantee that prevents duplicate background refreshes.
func TestSlackChannelCacheTryLockRefreshExclusive(t *testing.T) {
	cache := newTestCache(t)

	release, ok, err := cache.TryLockRefresh(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, ok, "first lock attempt must succeed")

	_, ok2, err := cache.TryLockRefresh(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.False(t, ok2, "second concurrent lock attempt must fail while the first is held")

	release()

	_, ok3, err := cache.TryLockRefresh(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, ok3, "lock must be acquirable again after release")
}

// TestSlackChannelCacheIsRefreshInFlight proves the peek reports the lock's
// held/not-held state without acquiring it — no lock, no held lock, held then
// released.
func TestSlackChannelCacheIsRefreshInFlight(t *testing.T) {
	cache := newTestCache(t)

	inFlight, err := cache.IsRefreshInFlight(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.False(t, inFlight, "no lock has been acquired yet")

	release, ok, err := cache.TryLockRefresh(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, ok)

	inFlight, err = cache.IsRefreshInFlight(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, inFlight, "the lock is held")

	release()

	inFlight, err = cache.IsRefreshInFlight(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.False(t, inFlight, "the lock was released")
}

// TestSlackChannelCacheKeysAreOrgScoped proves two different orgs' cache
// entries and locks don't collide.
func TestSlackChannelCacheKeysAreOrgScoped(t *testing.T) {
	cache := newTestCache(t)
	otherOrg := "33333333-3333-3333-3333-333333333333"

	require.NoError(t, cache.Set(context.Background(), cacheOrg, []*connectorpb.SlackChannel{{Id: "C1", Name: "a"}}, time.Now()))

	_, _, found, err := cache.Get(context.Background(), otherOrg)
	require.NoError(t, err)
	require.False(t, found, "a different org must not see cacheOrg's entry")

	_, ok, err := cache.TryLockRefresh(context.Background(), cacheOrg)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = cache.TryLockRefresh(context.Background(), otherOrg)
	require.NoError(t, err)
	require.True(t, ok, "a different org's lock must be independently acquirable")
}

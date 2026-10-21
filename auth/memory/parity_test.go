// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memory_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	authredis "github.com/tetriz-ai/tetriz-connectors/internal/auth/redis"
)

// cacheBackend is a token cache and locker on one keyspace, as the gateways'
// auth.RefreshDeps get them, plus a clock that expires TTLs.
type cacheBackend struct {
	cache   auth.TokenCache
	locker  auth.Locker
	advance func(time.Duration)
}

// The same cases run on Redis (miniredis) and on memory.Store, so the
// in-process cache and locker cannot drift from the Redis ones.
func TestParityWithRedis(t *testing.T) {
	backends := map[string]func(t *testing.T) cacheBackend{
		"redis": func(t *testing.T) cacheBackend {
			mr := miniredis.RunT(t)
			rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = rc.Close() })
			return cacheBackend{authredis.NewTokenCache(rc), authredis.NewLocker(rc), mr.FastForward}
		},
		"memory": func(*testing.T) cacheBackend {
			c := &clock{t: time.Unix(1000, 0)}
			var mu sync.Mutex
			s := &memory.Store{Now: func() time.Time { mu.Lock(); defer mu.Unlock(); return c.now() }}
			return cacheBackend{s, s, func(d time.Duration) { mu.Lock(); defer mu.Unlock(); c.t = c.t.Add(d) }}
		},
	}
	ctx := context.Background()
	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			b := mk(t)
			_, err := b.cache.Get(ctx, "tok")
			assert.True(t, errors.Is(err, auth.ErrCacheMiss))
			require.NoError(t, b.cache.Set(ctx, "tok", "v1", 2*time.Second))
			require.NoError(t, b.cache.Set(ctx, "forever", "f", 0))
			v, err := b.cache.Get(ctx, "tok")
			require.NoError(t, err)
			assert.Equal(t, "v1", v)
			b.advance(2 * time.Second)
			_, err = b.cache.Get(ctx, "tok")
			assert.True(t, errors.Is(err, auth.ErrCacheMiss), "expired")
			v, err = b.cache.Get(ctx, "forever")
			require.NoError(t, err)
			assert.Equal(t, "f", v, "a zero TTL never expires")
			require.NoError(t, b.cache.Delete(ctx, "forever"))
			require.NoError(t, b.cache.Delete(ctx, "missing"))
			_, err = b.cache.Get(ctx, "forever")
			assert.True(t, errors.Is(err, auth.ErrCacheMiss))

			first, ok, err := b.locker.TryLock(ctx, "lock", time.Second)
			require.NoError(t, err)
			require.True(t, ok)
			_, ok, err = b.locker.TryLock(ctx, "lock", time.Second)
			require.NoError(t, err)
			assert.False(t, ok, "held")
			b.advance(time.Second)
			second, ok, err := b.locker.TryLock(ctx, "lock", time.Minute)
			require.NoError(t, err)
			require.True(t, ok, "an expired lease can be retaken")
			require.NoError(t, first.Unlock(ctx))
			_, ok, _ = b.locker.TryLock(ctx, "lock", time.Minute)
			assert.False(t, ok, "a stale holder cannot release its successor's lock")
			require.NoError(t, second.Unlock(ctx))

			var winners atomic.Int32
			var wg sync.WaitGroup
			for range 32 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, ok, err := b.locker.TryLock(ctx, "lock", time.Minute); err == nil && ok {
						winners.Add(1)
					}
				}()
			}
			wg.Wait()
			assert.Equal(t, int32(1), winners.Load(), "exactly one concurrent winner")

			_, ok, err = b.locker.TryLock(ctx, "nolease", 0)
			require.NoError(t, err)
			require.True(t, ok)
			b.advance(time.Hour)
			_, ok, _ = b.locker.TryLock(ctx, "nolease", time.Second)
			assert.False(t, ok, "a zero-TTL lock does not expire")
		})
	}
}

// Expired entries leave memory even when nothing reads them again.
func TestSweepDropsUnreadExpiredEntries(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1000, 0)}
	s := &memory.Store{Now: c.now}
	require.NoError(t, s.Set(ctx, "a", "v", time.Second))
	_, _, err := s.TryLock(ctx, "l", time.Second)
	require.NoError(t, err)
	require.NoError(t, s.Set(ctx, "keep", "v", 0))
	c.t = c.t.Add(2 * time.Minute)
	require.NoError(t, s.Set(ctx, "b", "v", time.Hour))
	assert.Equal(t, 2, s.Len(), "a and l were swept")
}

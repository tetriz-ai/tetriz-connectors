// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeBackend is a CircuitBreakerStore under test and a clock that expires its
// TTLs: Redis (miniredis) and the in-memory store, with the same expectations.
type storeBackend struct {
	store   CircuitBreakerStore
	advance func(time.Duration)
}

func forEachCBBackend(t *testing.T, fn func(t *testing.T, b storeBackend)) {
	t.Helper()
	t.Run("redis", func(t *testing.T) {
		mr := miniredis.RunT(t)
		rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rc.Close() })
		fn(t, storeBackend{store: rc, advance: mr.FastForward})
	})
	t.Run("memory", func(t *testing.T) {
		clk := clockwork.NewFakeClock()
		fn(t, storeBackend{store: NewInMemoryCircuitBreakerStore(clk), advance: clk.Advance})
	})
}

// The store primitives the breaker relies on answer the same on both stores.
func TestCircuitBreakerStoreParity(t *testing.T) {
	forEachCBBackend(t, func(t *testing.T, b storeBackend) {
		ctx := context.Background()
		s := b.store

		assert.Equal(t, time.Duration(-2), s.PTTL(ctx, "k").Val(), "missing key")
		assert.False(t, s.Expire(ctx, "k", time.Minute).Val(), "EXPIRE on a missing key")
		assert.Equal(t, int64(1), s.Incr(ctx, "k").Val())
		assert.Equal(t, time.Duration(-1), s.PTTL(ctx, "k").Val(), "no TTL yet")
		assert.True(t, s.Expire(ctx, "k", 1500*time.Millisecond).Val())
		assert.Equal(t, time.Second, s.PTTL(ctx, "k").Val(), "EXPIRE truncates to seconds")
		assert.Equal(t, int64(2), s.Incr(ctx, "k").Val())
		assert.Equal(t, time.Second, s.PTTL(ctx, "k").Val(), "INCR keeps the TTL")
		b.advance(time.Second)
		assert.Equal(t, int64(0), s.Exists(ctx, "k").Val(), "expired")

		ok, err := s.SetNX(ctx, "n", "1", 1500*time.Millisecond).Result()
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 1500*time.Millisecond, s.PTTL(ctx, "n").Val(), "SET PX keeps milliseconds")
		assert.False(t, s.SetNX(ctx, "n", "2", time.Minute).Val(), "held")
		b.advance(1500 * time.Millisecond)
		assert.True(t, s.SetNX(ctx, "n", "3", 0).Val(), "free once expired")
		assert.Equal(t, time.Duration(-1), s.PTTL(ctx, "n").Val())

		require.NoError(t, s.Set(ctx, "n", 7, 10*time.Second).Err())
		require.NoError(t, s.Set(ctx, "n", "v", redis.KeepTTL).Err())
		assert.Equal(t, 10*time.Second, s.PTTL(ctx, "n").Val(), "KEEPTTL")
		assert.Error(t, s.Incr(ctx, "n").Err(), "not an integer")
		assert.True(t, s.Expire(ctx, "n", 0).Val())
		assert.Equal(t, int64(0), s.Exists(ctx, "n").Val(), "a zero TTL deletes")

		s.Set(ctx, "a", "1", 0)
		s.Set(ctx, "b", "1", 0)
		assert.Equal(t, int64(3), s.Exists(ctx, "a", "b", "a", "zz").Val())
		assert.Equal(t, int64(2), s.Del(ctx, "a", "b", "zz").Val())
	})
}

func TestInMemoryCircuitBreakerStoreSweeps(t *testing.T) {
	ctx := context.Background()
	clk := clockwork.NewFakeClock()
	s := NewInMemoryCircuitBreakerStore(clk)
	s.Set(ctx, "a", "1", time.Second)
	s.Set(ctx, "b", "1", 0)
	clk.Advance(time.Minute)
	s.Set(ctx, "c", "1", 0)
	assert.Equal(t, 2, s.keyCount(), "the expired key was swept without being read")
}

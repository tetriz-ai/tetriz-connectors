// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memory_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// This package had no tests, so it carries its own guard.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

// An entry is gone at exactly its expiry instant, a zero TTL never expires,
// and a lease that expired can be taken by a new holder whose lock the stale
// holder cannot release.
func TestEdge_MemoryExpiryAndStaleUnlock(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1000, 0)}
	s := &memory.Store{Now: c.now}

	require.NoError(t, s.Set(ctx, "tok", "v", time.Second))
	require.NoError(t, s.Set(ctx, "forever", "v", 0))
	c.t = c.t.Add(time.Second)
	_, err := s.Get(ctx, "tok")
	assert.True(t, errors.Is(err, auth.ErrCacheMiss), "an entry is expired at its expiry instant")
	c.t = c.t.Add(1000 * time.Hour)
	v, err := s.Get(ctx, "forever")
	require.NoError(t, err)
	assert.Equal(t, "v", v)

	first, ok, err := s.TryLock(ctx, "lock", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, _ = s.TryLock(ctx, "lock", time.Second)
	assert.False(t, ok, "a held lock is not granted twice")
	c.t = c.t.Add(time.Second)
	second, ok, err := s.TryLock(ctx, "lock", time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "an expired lease can be retaken")
	require.NoError(t, first.Unlock(ctx))
	_, ok, _ = s.TryLock(ctx, "lock", time.Minute)
	assert.False(t, ok, "a stale holder released its successor's lock")
	require.NoError(t, second.Unlock(ctx))
	_, ok, _ = s.TryLock(ctx, "lock", time.Minute)
	assert.True(t, ok)
}

// Under contention exactly one of many goroutines holds the lock.
func TestEdge_MemoryLockIsExclusiveUnderContention(t *testing.T) {
	s := &memory.Store{}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, _ := s.TryLock(context.Background(), "k", time.Minute); ok {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), winners.Load())
}

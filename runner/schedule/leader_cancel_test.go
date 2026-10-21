// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *Scheduler) isLeading() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manager != nil
}

func waitFor(t *testing.T, within time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// Cancelling Start's context ends the leader term within one extension
// interval: the periodic task manager stops and the leader lock is released.
func TestCancelEndsTheLeaderTerm(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	s, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, s.Start(ctx))
	require.True(t, waitFor(t, 10*time.Second, func() bool { return mr.Exists(cfg.LockKey) && s.isLeading() }), "leader lock never acquired")

	cancel()
	assert.True(t, waitFor(t, cfg.LockTTL/3, func() bool { return !mr.Exists(cfg.LockKey) && !s.isLeading() }),
		"after cancel the lock is released and the manager stopped")

	require.NoError(t, s.Stop(context.Background()))
	require.NoError(t, s.Stop(context.Background()), "Stop stays idempotent")
	assert.False(t, mr.Exists(cfg.LockKey))
}

// A standby takes over within one retry interval after the leader's context
// is cancelled, well before the lock's TTL would free it.
func TestStandbyTakesOverAfterLeaderCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.LockRetry = 300 * time.Millisecond

	a, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	actx, acancel := context.WithCancel(context.Background())
	require.NoError(t, a.Start(actx))
	require.True(t, waitFor(t, 10*time.Second, a.isLeading), "a never led")

	b, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	require.NoError(t, b.Start(bctx))
	time.Sleep(2 * cfg.LockRetry)
	require.False(t, b.isLeading(), "b stands by while a leads")

	acancel()
	assert.True(t, waitFor(t, 2*cfg.LockRetry, b.isLeading), "b takes over after a is cancelled")
	assert.False(t, a.isLeading())

	require.NoError(t, a.Stop(context.Background()))
	require.NoError(t, b.Stop(context.Background()))
}

// A lone leader keeps its lock past LockTTL by extending it, and a standby
// never leads while it does. miniredis only expires keys when told, so the
// test advances its clock with the wall clock.
func TestLeaderHoldsTheLockPastItsTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.LockTTL = 300 * time.Millisecond
	cfg.LockRetry = 50 * time.Millisecond
	a, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	require.NoError(t, a.Start(context.Background()))
	require.True(t, waitFor(t, 10*time.Second, a.isLeading), "a never led")
	b, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	require.NoError(t, b.Start(context.Background()))

	for i := 0; i < 20; i++ { // two seconds, several TTLs
		time.Sleep(50 * time.Millisecond)
		mr.FastForward(50 * time.Millisecond)
		require.False(t, b.isLeading(), "the standby led while the leader was alive (step %d)", i)
	}
	assert.True(t, a.isLeading())
	assert.True(t, mr.Exists(cfg.LockKey))
	require.NoError(t, b.Stop(context.Background()))
	require.NoError(t, a.Stop(context.Background()))
}

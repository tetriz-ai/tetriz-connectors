// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// fakeDistLock is a test double for DistLock.
type fakeDistLock struct {
	lockFn   func() error
	unlockFn func() (bool, error)
	extendFn func() (bool, error)

	unlockCalled atomic.Bool
}

func (f *fakeDistLock) Lock() error {
	if f.lockFn != nil {
		return f.lockFn()
	}
	return nil
}

func (f *fakeDistLock) Unlock() (bool, error) {
	f.unlockCalled.Store(true)
	if f.unlockFn != nil {
		return f.unlockFn()
	}
	return true, nil
}

func (f *fakeDistLock) Extend() (bool, error) {
	if f.extendFn != nil {
		return f.extendFn()
	}
	return true, nil
}

// fakeDistLockFactory is a test double for DistLockFactory.
type fakeDistLockFactory struct {
	lock DistLock
}

func (f *fakeDistLockFactory) NewMutex(_ string, _ ...redsync.Option) DistLock {
	return f.lock
}

func newSuccessLockFactory() (*fakeDistLockFactory, *fakeDistLock) {
	lock := &fakeDistLock{}
	return &fakeDistLockFactory{lock: lock}, lock
}

// newTestConfig returns Options with test defaults.
func newTestConfig() Options {
	cfg := Options{
		SyncInterval:    5 * time.Minute,
		Concurrency:     5,
		DefaultTimeout:  10 * time.Minute,
		LockKey:         "scheduler-lock",
		LockTTL:         30 * time.Second,
		LockRetry:       10 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}
	return cfg
}

func newTestConfigWithLockRetry(retryInterval time.Duration) Options {
	cfg := Options{
		SyncInterval:    5 * time.Minute,
		Concurrency:     5,
		DefaultTimeout:  10 * time.Minute,
		LockKey:         "scheduler-lock",
		LockTTL:         30 * time.Second,
		LockRetry:       retryInterval,
		ShutdownTimeout: 30 * time.Second,
	}
	return cfg
}

// testRedisClient returns a *redis.Client for TETRIZ_TEST_REDIS_ADDR, or
// localhost:6379. These tests never call Start, so the asynq worker never dials.
func testRedisClient() *redis.Client {
	addr := os.Getenv("TETRIZ_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return redis.NewClient(&redis.Options{Addr: addr})
}

// ---------------------------------------------------------------------------
// Tests: Lifecycle
// ---------------------------------------------------------------------------

func TestNewScheduler_AcquiresLock_StopReleasesIt(t *testing.T) {
	t.Parallel()

	lockFactory, lock := newSuccessLockFactory()
	mux := asynq.NewServeMux()

	s, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil, // no sources
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))

	time.Sleep(50 * time.Millisecond)

	require.NoError(t, s.Stop(context.Background()))
	assert.True(t, lock.unlockCalled.Load(), "lock should be released on stop")
}

func TestNewScheduler_RetriesUntilLockAcquired(t *testing.T) {
	t.Parallel()

	var attempt atomic.Int32
	lock := &fakeDistLock{
		lockFn: func() error {
			n := attempt.Add(1)
			if n <= 2 {
				return errors.New("lock held")
			}
			return nil
		},
	}
	lockFactory := &fakeDistLockFactory{lock: lock}
	mux := asynq.NewServeMux()

	s, err := NewScheduler(
		context.Background(),
		newTestConfigWithLockRetry(1*time.Millisecond),
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))

	time.Sleep(50 * time.Millisecond)

	assert.GreaterOrEqual(t, attempt.Load(), int32(3), "should have retried at least 3 times")
	require.NoError(t, s.Stop(context.Background()))
}

func TestNewScheduler_ContextCancelledDuringRetry(t *testing.T) {
	t.Parallel()

	lock := &fakeDistLock{
		lockFn: func() error { return errors.New("lock held") },
	}
	lockFactory := &fakeDistLockFactory{lock: lock}
	mux := asynq.NewServeMux()

	ctx, cancel := context.WithCancel(context.Background())

	s, err := NewScheduler(
		ctx,
		newTestConfigWithLockRetry(1*time.Millisecond),
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(ctx))

	cancel()

	require.NoError(t, s.Stop(context.Background()))
}

func TestExtendLock_FailureTriggersShutdown(t *testing.T) {
	t.Parallel()

	var extendCalled atomic.Bool
	lock := &fakeDistLock{
		extendFn: func() (bool, error) {
			extendCalled.Store(true)
			return false, errors.New("redis down")
		},
	}
	lockFactory := &fakeDistLockFactory{lock: lock}
	mux := asynq.NewServeMux()

	cfg := Options{
		SyncInterval:    5 * time.Minute,
		Concurrency:     5,
		DefaultTimeout:  10 * time.Minute,
		LockKey:         "scheduler-lock",
		LockTTL:         3 * time.Millisecond,
		LockRetry:       10 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}

	s, err := NewScheduler(
		context.Background(),
		cfg,
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))

	// Wait for extendLock goroutine to call Extend and trigger shutdown.
	// The goroutine path involves real Redis I/O (PeriodicTaskManager
	// creation/start) which can be slow, so poll instead of a fixed sleep.
	require.Eventually(t, extendCalled.Load, 5*time.Second, 1*time.Millisecond,
		"extend should have been called")
	require.Eventually(t, lock.unlockCalled.Load, 5*time.Second, 1*time.Millisecond,
		"lock should be released after extend failure")

	// Clean up.
	require.NoError(t, s.Stop(context.Background()))
}

func TestStop_IsIdempotent(t *testing.T) {
	t.Parallel()

	lockFactory, _ := newSuccessLockFactory()
	mux := asynq.NewServeMux()

	s, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))

	require.NoError(t, s.Stop(context.Background()))

	// Second stop should not panic.
	assert.NotPanics(t, func() {
		_ = s.Stop(context.Background())
	})
}

// TestNewScheduler_DoesNotStartWorker: construction must not launch the asynq worker; without Start the worker is
// idle, the registry holds the instance, and a teardown via Stop is clean.
func TestNewScheduler_DoesNotStartWorker(t *testing.T) {
	t.Parallel()

	lockFactory, lock := newSuccessLockFactory()
	mux := asynq.NewServeMux()

	s, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)

	// Without Start, the lock-acquisition goroutine never runs, so the lock
	// must remain untouched even after a generous wait.
	time.Sleep(50 * time.Millisecond)
	assert.False(t, lock.unlockCalled.Load(), "Unlock must not be called before Start")

	require.NoError(t, s.Stop(context.Background()))
}

// TestStartAll_StartsRegisteredSchedulers verifies the registry-driven
// post-health start path used by the host's server bootstrap.
func TestStartAll_StartsRegisteredSchedulers(t *testing.T) {
	registryMu.Lock()
	saved := registry
	registry = nil
	registryMu.Unlock()
	t.Cleanup(func() {
		registryMu.Lock()
		registry = saved
		registryMu.Unlock()
	})

	lockFactory, lock := newSuccessLockFactory()
	mux := asynq.NewServeMux()
	s, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil,
		mux,
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)

	require.NoError(t, StartAll(context.Background()))
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, s.Stop(context.Background()))
	assert.True(t, lock.unlockCalled.Load(), "Stop must release the lock acquired by StartAll")
}

// TestStopAll_StopsRegisteredSchedulers pins that on
// shutdown the server must drain every registered scheduler (StopAll) before the
// shared Redis client closes, or asynq's subscriber goroutine panics on a nil
// pubsub message. StopAll must therefore stop all registered schedulers, release
// their leader locks, empty the registry, and be a safe no-op when called again.
func TestStopAll_StopsRegisteredSchedulers(t *testing.T) {
	registryMu.Lock()
	saved := registry
	registry = nil
	registryMu.Unlock()
	t.Cleanup(func() {
		registryMu.Lock()
		registry = saved
		registryMu.Unlock()
	})

	lockFactory, lock := newSuccessLockFactory()
	// NewScheduler self-registers into the package registry; the test drives
	// StartAll/StopAll through that registry, so the returned instance is unused.
	_, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil,
		asynq.NewServeMux(),
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)

	require.NoError(t, StartAll(context.Background()))
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, StopAll(context.Background()))

	registryMu.Lock()
	remaining := len(registry)
	registryMu.Unlock()
	assert.Zero(t, remaining, "StopAll must unregister every scheduler")
	assert.True(t, lock.unlockCalled.Load(), "StopAll must drain the scheduler and release its leader lock")

	// StopAll on a now-empty registry is a no-op and must not panic.
	assert.NotPanics(t, func() { _ = StopAll(context.Background()) })
}

// TestStart_AfterStop_IsNoOp guards the shutdown-race hole: Stop latches the
// scheduler, so a late Start — the fire-and-forget StartAll goroutine firing
// after StopAll on shutdown — must be a no-op and must NOT boot a worker against
// the about-to-close Redis client.
func TestStart_AfterStop_IsNoOp(t *testing.T) {
	t.Parallel()

	lockFactory, _ := newSuccessLockFactory()
	s, err := NewScheduler(
		context.Background(),
		newTestConfig(),
		testRedisClient(),
		nil,
		asynq.NewServeMux(),
		nil, // no leader workers
		lockFactory,
	)
	require.NoError(t, err)

	require.NoError(t, s.Stop(context.Background()))  // shutdown latches first
	require.NoError(t, s.Start(context.Background())) // late Start must no-op

	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	assert.False(t, started, "Start after Stop must not boot the worker")
}

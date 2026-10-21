// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redsync/redsync/v4"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// takenLock acquires, then reports on its first extension that another
// instance holds the lock now: Extend returns (false, nil).
type takenLock struct {
	locks, extends *atomic.Int32
}

func (l takenLock) Lock() error           { l.locks.Add(1); return nil }
func (l takenLock) Unlock() (bool, error) { return false, nil }
func (l takenLock) Extend() (bool, error) {
	l.extends.Add(1)
	return false, nil
}

type takenLockFactory struct{ locks, extends atomic.Int32 }

func (f *takenLockFactory) NewMutex(string, ...redsync.Option) DistLock {
	return takenLock{locks: &f.locks, extends: &f.extends}
}

// A leader whose extension reports the lock taken (false, no error) steps
// down: the periodic task manager shuts down and the loop re-enters election.
func TestLeaderStepsDownWhenExtendReportsTheLockTaken(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.LockTTL = 300 * time.Millisecond
	cfg.LockRetry = 50 * time.Millisecond
	f := &takenLockFactory{}
	s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{okDefs()}, asynq.NewServeMux(), nil, f)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.Start(ctx))
	// Terms are counted, not sampled: a new term starts only after the last one
	// ended, and every extension fails, so three acquisitions with at least two
	// failed extensions mean the leader stepped down twice.
	require.True(t, waitFor(t, 10*time.Second, func() bool { return f.locks.Load() >= 3 }), "the loop re-entered election after stepping down")
	assert.GreaterOrEqual(t, f.extends.Load(), int32(2), "each term ended at its first failed extension")
	cancel()
	require.NoError(t, s.Stop(context.Background()))
}

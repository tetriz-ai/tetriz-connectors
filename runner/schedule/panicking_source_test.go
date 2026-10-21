// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// panicSource panics on its panicOn-th read, or on every read when panicOn is 0.
type panicSource struct {
	calls   atomic.Int64
	panicOn int64
}

func (p *panicSource) Name() string { return "panicky" }
func (p *panicSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	n := p.calls.Add(1)
	if p.panicOn == 0 || n == p.panicOn {
		panic("assignment to entry in nil map") // what a buggy source's nil-map write raises
	}
	return []ScheduleDefinition{{ID: "p/1", JobType: "x", CronExpression: "* * * * *", Queue: "default"}}, nil
}

func okDefs() *countingSource {
	return &countingSource{name: "ok", defs: []ScheduleDefinition{{ID: "ok/1", JobType: "x", CronExpression: "* * * * *", Queue: "default"}}}
}

// A source that panics on the first read is skipped: a leader still leads and
// registers the other source's schedule.
func TestPanickingSourceOnFirstReadIsSkipped(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig()
	cfg.LockTTL = 600 * time.Millisecond
	cfg.LockRetry = 100 * time.Millisecond
	mk := func() *Scheduler {
		rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{okDefs(), &panicSource{}}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
		require.NoError(t, err)
		return s
	}
	a, b := mk(), mk()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, a.Start(ctx))
	require.NoError(t, b.Start(ctx))
	require.True(t, waitFor(t, 5*time.Second, func() bool { return a.isLeading() || b.isLeading() }), "no instance leads")
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		mr.FastForward(100 * time.Millisecond)
		require.True(t, a.isLeading() || b.isLeading(), "leadership lost at step %d", i)
	}
	assert.Equal(t, float64(1), testutil.ToFloat64(schedulerPeriodicTasks), "the ok source's schedule is registered")
	cancel()
	require.NoError(t, a.Stop(context.Background()))
	require.NoError(t, b.Stop(context.Background()))
}

// A source that panics on a later sync, inside asynq's own goroutine, is
// skipped the same way: the process survives and the leader keeps leading.
func TestPanickingSourceOnLaterSyncIsSkipped(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.SyncInterval = 200 * time.Millisecond
	src := &panicSource{panicOn: 2}
	s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{okDefs(), src}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.Start(ctx))
	require.True(t, waitFor(t, 5*time.Second, s.isLeading))
	require.True(t, waitFor(t, 5*time.Second, func() bool { return src.calls.Load() >= 3 }), "the sync after the panic ran")
	assert.True(t, s.isLeading())
	cancel()
	require.NoError(t, s.Stop(context.Background()))
}

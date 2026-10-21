// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"errors"
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

type countingSource struct {
	name  string
	defs  []ScheduleDefinition
	calls atomic.Int64
}

func (c *countingSource) Name() string { return c.name }
func (c *countingSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	c.calls.Add(1)
	return c.defs, nil
}

// One source's invalid definition is dropped: the leader still starts, the
// valid definition is registered, and sources are read once per sync, not in
// a loop.
func TestInvalidDefinitionDoesNotStopScheduling(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	good := &countingSource{name: "good", defs: []ScheduleDefinition{{ID: "good/1", JobType: "x", CronExpression: "* * * * *", Queue: "default"}}}
	bad := &countingSource{name: "bad", defs: []ScheduleDefinition{
		{ID: "bad/1", JobType: "x", CronExpression: "", Queue: "default"},
		{ID: "bad/2", JobType: "x", CronExpression: "not a cron", Queue: "default"},
		{ID: "bad/3", JobType: "", CronExpression: "* * * * *", Queue: "default"},
	}}
	before := testutil.ToFloat64(schedulerInvalidDefinitionsTotal.WithLabelValues("bad"))
	s, err := NewScheduler(context.Background(), newTestConfig(), rc, []ScheduleSource{good, bad}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.Start(ctx))
	require.True(t, waitFor(t, 10*time.Second, s.isLeading), "the leader starts despite the invalid definitions")
	time.Sleep(time.Second)
	assert.Equal(t, int64(1), good.calls.Load(), "one read per sync interval")
	assert.Equal(t, float64(1), testutil.ToFloat64(schedulerPeriodicTasks), "only the valid definition is registered")
	assert.Equal(t, float64(3), testutil.ToFloat64(schedulerInvalidDefinitionsTotal.WithLabelValues("bad"))-before)
	cancel()
	require.NoError(t, s.Stop(context.Background()))
}

// A leader term whose manager fails to start waits LockRetry before trying
// again, rather than re-acquiring the lock in a tight loop.
func TestFailedManagerStartWaitsLockRetry(t *testing.T) {
	var starts atomic.Int64
	orig := startManager
	startManager = func(*asynq.PeriodicTaskManager) error { starts.Add(1); return errors.New("start failed") }
	t.Cleanup(func() { startManager = orig })

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.LockRetry = 300 * time.Millisecond
	s, err := NewScheduler(context.Background(), cfg, rc, nil, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, s.Start(ctx))
	time.Sleep(time.Second)
	cancel()
	require.NoError(t, s.Stop(context.Background()))
	got := starts.Load()
	assert.GreaterOrEqual(t, got, int64(2), "the term is retried")
	assert.LessOrEqual(t, got, int64(5), "at most one attempt per LockRetry in one second")
}

// A negative timeout is an invalid definition, not a silent fall back to the
// default deadline.
func TestNegativeTimeoutIsAnInvalidDefinition(t *testing.T) {
	src := &countingSource{name: "negative-timeout", defs: []ScheduleDefinition{
		{ID: "neg/1", JobType: "x", CronExpression: "* * * * *", Timeout: -time.Second},
		{ID: "neg/2", JobType: "x", CronExpression: "* * * * *", Timeout: time.Second},
	}}
	before := testutil.ToFloat64(schedulerInvalidDefinitionsTotal.WithLabelValues("negative-timeout"))
	cfgs, err := NewConfigProvider([]ScheduleSource{src}, time.Minute, nil).GetConfigs()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	assert.Equal(t, "x", cfgs[0].Task.Type())
	assert.Equal(t, float64(1), testutil.ToFloat64(schedulerInvalidDefinitionsTotal.WithLabelValues("negative-timeout"))-before)
}

// A lock lost while the manager starts ends the term: the new manager is shut
// down and the instance does not lead.
func TestLockLostDuringManagerStartStepsDown(t *testing.T) {
	var sched *Scheduler
	var lost atomic.Bool
	orig := startManager
	startManager = func(m *asynq.PeriodicTaskManager) error {
		if err := m.Start(); err != nil {
			return err
		}
		if lost.CompareAndSwap(false, true) {
			sched.mu.Lock()
			sched.leaderMutex = nil // what extendLock's endTerm leaves after a failed extension
			sched.mu.Unlock()
		}
		return nil
	}
	t.Cleanup(func() { startManager = orig })

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := newTestConfig()
	cfg.LockRetry = 100 * time.Millisecond
	s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{okDefs()}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	sched = s
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.Start(ctx))
	require.True(t, waitFor(t, 5*time.Second, lost.Load), "the manager started")
	leading := false
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		leading = leading || s.isLeading()
	}
	assert.False(t, leading, "the instance stepped down instead of leading without the lock")
	cancel()
	require.NoError(t, s.Stop(context.Background()))
}

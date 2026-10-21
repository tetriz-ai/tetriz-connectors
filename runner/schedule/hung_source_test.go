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

// hungSource ignores its context until released.
type hungSource struct{ release chan struct{} }

func (hungSource) Name() string { return "hung" }
func (h hungSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	<-h.release
	return nil, nil
}

// ctxSource answers at once, but fails when its context has already ended, as
// a database-backed source does.
type ctxSource struct{ id string }

func (c ctxSource) Name() string { return "db-" + c.id }
func (c ctxSource) FetchSchedules(ctx context.Context) ([]ScheduleDefinition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []ScheduleDefinition{{ID: c.id, JobType: "x", CronExpression: "* * * * *", Queue: "default"}}, nil
}

func hasTaskID(cfgs []*asynq.PeriodicTaskConfig, id string) bool {
	for _, c := range cfgs {
		for _, o := range c.Opts {
			if o.Type() == asynq.TaskIDOpt && o.Value() == id {
				return true
			}
		}
	}
	return false
}

// A source that ignores its context is abandoned at its deadline: the leader
// starts and registers the working source's schedule on its first read.
func TestHungSourceDoesNotBlockScheduling(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig()
	cfg.LockTTL = 600 * time.Millisecond
	cfg.LockRetry = 100 * time.Millisecond
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	h := hungSource{release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	ok := okDefs()
	s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{h, ok}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, s.Start(ctx))
	require.True(t, waitFor(t, 3*time.Second, s.isLeading), "the leader starts despite the hung source")
	assert.GreaterOrEqual(t, ok.calls.Load(), int64(1), "the working source was read")
	assert.Equal(t, float64(1), testutil.ToFloat64(schedulerPeriodicTasks), "its schedule is registered")
	cancel()
	require.NoError(t, s.Stop(context.Background()))
}

// Each source has its own first-read deadline, so a slow source listed first
// does not use up the time of the source after it.
func TestSlowFirstSourceDoesNotStarveTheNext(t *testing.T) {
	p := NewConfigProvider([]ScheduleSource{slowSource{d: time.Second}, ctxSource{id: "connector/1"}}, 0, nil)
	p.boundNextRead(300 * time.Millisecond)
	first, err := p.GetConfigs()
	require.NoError(t, err)
	assert.True(t, hasTaskID(first, "connector/1"), "the second source is registered on the first read")
	assert.True(t, hasTaskID(first, "s/1"), "the slow source's definitions, returned at its deadline, are kept")
}

// countingHungSource ignores its context until released and counts its fetches.
type countingHungSource struct {
	release chan struct{}
	calls   atomic.Int64
}

func (*countingHungSource) Name() string { return "counting-hung" }
func (h *countingHungSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	h.calls.Add(1)
	<-h.release
	return nil, nil
}

// An abandoned fetch's goroutine is not reclaimed until the fetch returns: the
// provider stops waiting for it but cannot stop it, and it does not start a
// second fetch of that source meanwhile.
func TestLimitation_AbandonedFetchIsNotReclaimed(t *testing.T) {
	h := &countingHungSource{release: make(chan struct{})}
	p := NewConfigProvider([]ScheduleSource{h}, 0, nil)
	for i := 0; i < 3; i++ {
		p.boundNextRead(50 * time.Millisecond)
		_, err := p.GetConfigs()
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), h.calls.Load(), "the first fetch is still running, and no second one started")
	close(h.release)
	require.Eventually(t, func() bool { return !p.inflight[0].Load() }, 2*time.Second, 10*time.Millisecond, "the goroutine ends only when the fetch returns")
}

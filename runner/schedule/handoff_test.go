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

// Fires that fall due while no instance leads are not replayed: after a
// handoff the new leader starts the schedule afresh. Fires are counted as
// enqueues (the queue's size plus its processed total), not as handler runs,
// so a tick enqueued before the handoff and run late on a loaded host does
// not read as a fire in the gap.
func TestLimitation_HandoffDoesNotReplayMissedFires(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	insp := asynq.NewInspectorFromRedisClient(rc)
	enqueued := func() int {
		q, err := insp.GetQueueInfo("default")
		if err != nil {
			return 0 // the queue does not exist until the first enqueue
		}
		return q.Size + q.ProcessedTotal
	}
	mux := asynq.NewServeMux()
	mux.HandleFunc("tick", func(context.Context, *asynq.Task) error { return nil })
	src := &countingSource{name: "tick", defs: []ScheduleDefinition{{ID: "tick/1", JobType: "tick", CronExpression: "@every 1s", Queue: "default"}}}
	cfg := newTestConfig()
	cfg.ShutdownTimeout = 2 * time.Second

	a, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{src}, mux, nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	actx, acancel := context.WithCancel(context.Background())
	require.NoError(t, a.Start(actx))
	require.True(t, waitFor(t, 10*time.Second, a.isLeading), "a never led")
	time.Sleep(2500 * time.Millisecond)
	require.GreaterOrEqual(t, enqueued(), 1, "the schedule fired while a led")

	// Leader a steps down; for 2.5 seconds no instance leads, then b starts.
	acancel()
	require.True(t, waitFor(t, 5*time.Second, func() bool { return !a.isLeading() }))
	atStepDown := enqueued()
	time.Sleep(2500 * time.Millisecond)
	assert.Equal(t, atStepDown, enqueued(), "nothing is enqueued while no instance leads")

	b, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{src}, mux, nil, NewDistLockFactory(rc))
	require.NoError(t, err)
	require.NoError(t, b.Start(context.Background()))
	require.True(t, waitFor(t, 10*time.Second, b.isLeading), "b never took over")
	atTakeover := enqueued()
	time.Sleep(900 * time.Millisecond)
	assert.LessOrEqual(t, enqueued()-atTakeover, 1, "the new leader does not replay the missed fires")
	require.NoError(t, b.Stop(context.Background()))
	require.NoError(t, a.Stop(context.Background()))
}

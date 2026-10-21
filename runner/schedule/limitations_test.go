// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

// Tests of the scheduler's retry, cleanup and retry-count rules. The MaxRetry
// test pins a behaviour the README lists under "Before you rely on it".

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The scheduler caps a Retry-After hint at one hour, the cap the built-in
// gateways apply: a custom gateway's ten-day hint waits an hour plus jitter.
// A hint under the cap is honoured.
func TestRetryAfterHintIsCappedAtOneHour(t *testing.T) {
	const tenDays = 10 * 24 * 60 * 60
	got := retryDelayFromVendorHint(0, &sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: tenDays}, nil)
	assert.GreaterOrEqual(t, got, time.Hour)
	assert.Less(t, got, time.Hour+retryJitterWindow)

	got = retryDelayFromVendorHint(0, &sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: 600}, nil)
	assert.GreaterOrEqual(t, got, 10*time.Minute)
	assert.Less(t, got, 10*time.Minute+retryJitterWindow)
}

// The archived-task cleaner pages through every archived task of a queue:
// one tick removes all 250, more than two of its pages and eight of asynq's
// default 30-task page.
func TestArchivedCleanupRemovesEveryArchivedTask(t *testing.T) {
	saved := archivedCleanupInterval
	archivedCleanupInterval = 300 * time.Millisecond
	defer func() { archivedCleanupInterval = saved }()

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}
	client := asynq.NewClient(opt)
	defer func() { _ = client.Close() }()
	insp := asynq.NewInspector(opt)
	defer func() { _ = insp.Close() }()
	for i := 0; i < 250; i++ {
		_, err := client.Enqueue(asynq.NewTask("lim", nil), asynq.Queue("lim"), asynq.TaskID(fmt.Sprintf("t%03d", i)))
		require.NoError(t, err)
	}
	n, err := insp.ArchiveAllPendingTasks("lim")
	require.NoError(t, err)
	require.Equal(t, 250, n)

	lockFactory, _ := newSuccessLockFactory()
	opts := newTestConfig()
	opts.Queues = map[string]int{"lim": 1}
	s, err := NewScheduler(context.Background(), opts, rc, nil, asynq.NewServeMux(), nil, lockFactory)
	require.NoError(t, err)
	defer func() { _ = s.Stop(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.cleanupArchivedTasks(ctx); close(done) }()
	time.Sleep(450 * time.Millisecond) // one tick at 300ms, the next at 600ms
	cancel()
	<-done

	info, err := insp.GetQueueInfo("lim")
	require.NoError(t, err)
	assert.Zero(t, info.Archived)
}

// A schedule definition cannot ask for zero retries: MaxRetry zero or below
// becomes the default of 3.
func TestLimitation_NonPositiveMaxRetryBecomesThree(t *testing.T) {
	for _, mr := range []int{0, -1} {
		p := NewConfigProvider([]ScheduleSource{&mockScheduleSource{name: "s", defs: []ScheduleDefinition{{
			ID: "x/1", JobType: "connector.sync", CronExpression: "*/5 * * * *", MaxRetry: mr,
		}}}}, time.Minute, nil)
		cfgs, err := p.GetConfigs()
		require.NoError(t, err)
		require.Len(t, cfgs, 1)
		var got any
		for _, o := range cfgs[0].Opts {
			if o.Type() == asynq.MaxRetryOpt {
				got = o.Value()
			}
		}
		assert.Equal(t, 3, got, "MaxRetry %d", mr)
	}
}

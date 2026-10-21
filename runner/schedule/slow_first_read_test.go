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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// slowSource takes d to answer, or until its context ends.
type slowSource struct{ d time.Duration }

func (s slowSource) Name() string { return "slow" }
func (s slowSource) FetchSchedules(ctx context.Context) ([]ScheduleDefinition, error) {
	select {
	case <-time.After(s.d):
	case <-ctx.Done():
	}
	return []ScheduleDefinition{{ID: "s/1", JobType: "x", CronExpression: "* * * * *", Queue: "default"}}, nil
}

// A first read longer than LockTTL does not lose the lock: the lock is
// extended from acquisition, the first read is cut at half the TTL, and one
// instance leads throughout.
func TestSlowFirstReadKeepsOneLeader(t *testing.T) {
	core, recorded := observer.New(zapcore.WarnLevel)
	prev := logger.Log
	logger.Log = zap.New(core)
	t.Cleanup(func() { logger.Log = prev })

	mr := miniredis.RunT(t)
	cfg := newTestConfig()
	cfg.LockTTL = 600 * time.Millisecond
	cfg.LockRetry = 100 * time.Millisecond
	mk := func() *Scheduler {
		rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		s, err := NewScheduler(context.Background(), cfg, rc, []ScheduleSource{slowSource{2 * time.Second}}, asynq.NewServeMux(), nil, NewDistLockFactory(rc))
		require.NoError(t, err)
		return s
	}
	a, b := mk(), mk()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, a.Start(ctx))
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, b.Start(ctx))

	both := 0
	for i := 0; i < 80; i++ { // four seconds, several TTLs, the first read included
		time.Sleep(50 * time.Millisecond)
		mr.FastForward(50 * time.Millisecond)
		if a.isLeading() && b.isLeading() {
			both++
		}
	}
	assert.Zero(t, both, "two managers ran at once")
	assert.Zero(t, recorded.FilterMessageSnippet("lost leader lock").Len(), "the lock was lost")
	assert.NotZero(t, recorded.FilterMessageSnippet("first schedule read hit its deadline").Len(), "the first read is bounded below LockTTL")
	assert.True(t, a.isLeading() || b.isLeading(), "one instance leads")
	cancel()
	require.NoError(t, a.Stop(context.Background()))
	require.NoError(t, b.Stop(context.Background()))
}

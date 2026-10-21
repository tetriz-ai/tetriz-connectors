// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package heartbeat

import (
	"context"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

// The shorthand the package documents ("@every 30s") survives the
// scheduler's validation and registers one task on the default queue, which
// the runner's worker drains.
func TestEdge_EveryShorthandRegistersThroughTheProvider(t *testing.T) {
	cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{NewSource("@every 30s")}, 0, nil).GetConfigs()
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	assert.Equal(t, "@every 30s", cfgs[0].Cronspec)
	assert.Equal(t, JobType, cfgs[0].Task.Type())
	_, drained := schedule.DefaultQueues()[Queue]
	assert.True(t, drained, "the heartbeat queue must be one the worker drains")
}

// A cron the scheduler cannot parse is dropped, not registered, and does not
// fail the whole sync.
func TestEdge_InvalidCronIsDroppedNotFatal(t *testing.T) {
	for _, c := range []string{"every 30s", "", "*/5 * * *", "@every"} {
		cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{NewSource(c)}, 0, nil).GetConfigs()
		require.NoError(t, err, c)
		assert.Empty(t, cfgs, c)
	}
}

// A task with an empty object runs (message is empty); a task with no body
// at all is an error rather than a silent success.
func TestEdge_HandlerEmptyBodies(t *testing.T) {
	h := NewHandler()
	require.NoError(t, h.ProcessTask(context.Background(), asynq.NewTask(JobType, []byte(`{}`))))
	require.NoError(t, h.ProcessTask(context.Background(), asynq.NewTask(JobType, []byte(`{"message":"x","extra":1}`))))
	assert.Error(t, h.ProcessTask(context.Background(), asynq.NewTask(JobType, nil)))
}

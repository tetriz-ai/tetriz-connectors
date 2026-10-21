// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package heartbeat

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

func TestSource_FetchSchedules(t *testing.T) {
	src := NewSource("@every 1m")

	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)

	def := defs[0]
	assert.Equal(t, "heartbeat/ping", def.ID)
	assert.Equal(t, JobType, def.JobType)
	assert.Equal(t, "@every 1m", def.CronExpression)
	assert.Equal(t, schedule.QueueDefault, def.Queue)

	var p Payload
	require.NoError(t, json.Unmarshal(def.Payload, &p))
	assert.Equal(t, "ping", p.Message)
}

func TestSource_ImplementsInterface(t *testing.T) {
	var _ schedule.ScheduleSource = (*Source)(nil)
}

func TestHandler_ProcessTask(t *testing.T) {
	h := NewHandler()

	payload, _ := json.Marshal(Payload{Message: "hello"})
	task := asynq.NewTask(JobType, payload)

	core, recorded := observer.New(zapcore.InfoLevel)
	prev := logger.Log
	logger.Log = zap.New(core)
	t.Cleanup(func() { logger.Log = prev })

	require.NoError(t, h.ProcessTask(context.Background(), task))
	logs := recorded.FilterMessage("heartbeat").All()
	require.Len(t, logs, 1, "the handler logs one heartbeat per task")
	assert.Equal(t, "hello", logs[0].ContextMap()["message"])
}

func TestHandler_InvalidPayload(t *testing.T) {
	h := NewHandler()

	task := asynq.NewTask(JobType, []byte("not json"))

	err := h.ProcessTask(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "heartbeat: unmarshal payload")
}

func TestHandler_ImplementsInterface(t *testing.T) {
	var _ asynq.Handler = (*Handler)(nil)
}

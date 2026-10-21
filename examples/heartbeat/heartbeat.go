// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package heartbeat is a periodic job of your own on the scheduler.
//
// A job needs two things:
//
//  1. A ScheduleSource, which returns the periodic jobs to register.
//  2. An asynq.Handler, which runs each job when it fires.
//
// Pass the source to schedule.NewScheduler with the other sources, and
// register the handler on the scheduler's ServeMux:
//
//	source := heartbeat.NewSource("@every 30s")
//	mux.Handle(heartbeat.JobType, heartbeat.NewHandler())
package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

const (
	JobType = "example.heartbeat"
	Queue   = schedule.QueueDefault
)

// Payload is the JSON body carried by each heartbeat task.
type Payload struct {
	Message string `json:"message"`
}

// --- ScheduleSource ----------------------------------------------------------

// Source implements schedule.ScheduleSource.
// It returns a single heartbeat job running on the given cron expression.
type Source struct {
	cron    string
	message string
}

// NewSource creates a heartbeat source.
// cron is any 5-field cron expression or asynq shorthand like "@every 30s".
func NewSource(cron string) *Source {
	return &Source{cron: cron, message: "ping"}
}

func (s *Source) Name() string { return "heartbeat" }

func (s *Source) FetchSchedules(_ context.Context) ([]schedule.ScheduleDefinition, error) {
	payload, err := json.Marshal(Payload{Message: s.message})
	if err != nil {
		return nil, fmt.Errorf("heartbeat: marshal payload: %w", err)
	}

	return []schedule.ScheduleDefinition{
		{
			ID:             "heartbeat/ping",
			JobType:        JobType,
			CronExpression: s.cron,
			Payload:        payload,
			Queue:          Queue,
		},
	}, nil
}

// --- Handler -----------------------------------------------------------------

// Handler implements asynq.Handler for heartbeat tasks.
type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	var p Payload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("heartbeat: unmarshal payload: %w", err)
	}

	logger.Info(ctx, "heartbeat",
		zap.String("message", p.Message),
		zap.Time("fired_at", time.Now().UTC()),
	)
	return nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package handler

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

// LoggingMiddleware logs task start, completion/error, and duration.
func LoggingMiddleware(logger *zap.Logger) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			start := time.Now()
			logger.Info("scheduler: task started",
				zap.String("task_type", t.Type()),
			)

			err := next.ProcessTask(ctx, t)
			duration := time.Since(start)

			if err != nil {
				logger.Error("scheduler: task failed",
					zap.String("task_type", t.Type()),
					zap.Duration("duration", duration),
					zap.Error(err),
				)
			} else {
				logger.Info("scheduler: task completed",
					zap.String("task_type", t.Type()),
					zap.Duration("duration", duration),
				)
			}
			if err != nil {
				return fmt.Errorf("processing task %s: %w", t.Type(), err)
			}
			return nil
		})
	}
}

// ContextEnricher is a function that enriches the context before a task is processed.
type ContextEnricher func(ctx context.Context, t *asynq.Task) context.Context

// ContextMiddleware applies a list of enrichers to the context before each task runs.
// Use this to inject cross-cutting values (trace_id, etc.) without a new middleware per value.
func ContextMiddleware(enrichers ...ContextEnricher) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			for _, enrich := range enrichers {
				ctx = enrich(ctx, t)
			}
			return next.ProcessTask(ctx, t)
		})
	}
}

// WithTraceID is a ContextEnricher that generates a fresh trace_id for each task execution
// so all downstream logs include it automatically via GetCtxLogFields.
func WithTraceID() ContextEnricher {
	return func(ctx context.Context, _ *asynq.Task) context.Context {
		return reqctx.CtxWithTraceID(ctx, uuid.New().String())
	}
}

// RecoveryMiddleware catches panics in handlers, logs the stack trace, and returns an error.
func RecoveryMiddleware(logger *zap.Logger) asynq.MiddlewareFunc {
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("scheduler: task panicked",
						zap.String("task_type", t.Type()),
						zap.Any("panic", r),
						zap.String("stack", string(debug.Stack())),
					)
					err = fmt.Errorf("scheduler: panic in task %s: %v", t.Type(), r)
				}
			}()
			return next.ProcessTask(ctx, t)
		})
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// ---------------------------------------------------------------------------
// Tests: LoggingMiddleware
// ---------------------------------------------------------------------------

func TestLoggingMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handlerErr error
		wantLogs   []string // Expected log messages (substrings)
		wantLevel  string   // Expected level of the second log entry
	}{
		{
			name:       "logs start and success",
			handlerErr: nil,
			wantLogs:   []string{"scheduler: task started", "scheduler: task completed"},
			wantLevel:  "INFO",
		},
		{
			name:       "logs start and failure",
			handlerErr: errors.New("processing failed"),
			wantLogs:   []string{"scheduler: task started", "scheduler: task failed"},
			wantLevel:  "ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			core, logs := observer.New(zap.DebugLevel)
			logger := zap.New(core)

			handler := asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
				return tt.handlerErr
			})

			middleware := LoggingMiddleware(logger)
			wrapped := middleware(handler)

			task := asynq.NewTask("test.task", nil)
			err := wrapped.ProcessTask(context.Background(), task)

			if tt.handlerErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.GreaterOrEqual(t, logs.Len(), 2, "expected at least 2 log entries")
			assert.Contains(t, logs.All()[0].Message, tt.wantLogs[0])
			assert.Contains(t, logs.All()[1].Message, tt.wantLogs[1])
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: RecoveryMiddleware
// ---------------------------------------------------------------------------

func TestRecoveryMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		handler   asynq.Handler
		wantErr   bool
		wantPanic bool
	}{
		{
			name: "catches panic and returns error",
			handler: asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
				panic("something went wrong")
			}),
			wantErr:   true,
			wantPanic: true,
		},
		{
			name: "passthrough on success",
			handler: asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
				return nil
			}),
			wantErr: false,
		},
		{
			name: "passthrough on error",
			handler: asynq.HandlerFunc(func(_ context.Context, _ *asynq.Task) error {
				return errors.New("handler error")
			}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			core, logs := observer.New(zap.DebugLevel)
			logger := zap.New(core)

			middleware := RecoveryMiddleware(logger)
			wrapped := middleware(tt.handler)

			task := asynq.NewTask("test.task", nil)

			// The recovery middleware should never let a panic escape.
			assert.NotPanics(t, func() {
				err := wrapped.ProcessTask(context.Background(), task)
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})

			if tt.wantPanic {
				// Should have logged the panic.
				require.GreaterOrEqual(t, logs.Len(), 1)
				assert.Contains(t, logs.All()[0].Message, "scheduler: task panicked")
			}
		})
	}
}

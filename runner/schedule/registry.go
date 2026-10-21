// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Schedulers register themselves with a process-wide registry on construction,
// so a process can defer Start until it is ready to serve.
var (
	registryMu sync.Mutex
	registry   []*Scheduler
)

func registerForStart(s *Scheduler) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, s)
}

func unregisterForStart(s *Scheduler) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for i, r := range registry {
		if r == s {
			registry = append(registry[:i], registry[i+1:]...)
			return
		}
	}
}

// StartAll starts every Scheduler registered in this process. Call it once the
// process is ready to serve. Returns the first error encountered; remaining
// schedulers are still attempted so a partial failure does not silently leave
// healthy schedulers idle.
func StartAll(ctx context.Context) error {
	registryMu.Lock()
	snapshot := append([]*Scheduler(nil), registry...)
	registryMu.Unlock()

	var firstErr error
	for _, s := range snapshot {
		if err := s.Start(ctx); err != nil {
			logger.Error(ctx, "scheduler: StartAll failed for an instance", zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	logger.Info(ctx, "scheduler: StartAll completed", zap.Int("count", len(snapshot)))
	return firstErr
}

// StopAll gracefully stops every registered Scheduler. Symmetric to StartAll;
// each Stop drains its asynq worker and is idempotent even if StartAll never ran.
//
// Callers MUST invoke StopAll *before* closing the shared Redis client: closing
// it while the worker is live triggers an unrecovered panic on asynq's subscriber
// goroutine that crashes the process.
func StopAll(ctx context.Context) error {
	registryMu.Lock()
	snapshot := append([]*Scheduler(nil), registry...)
	registryMu.Unlock()

	var firstErr error
	for _, s := range snapshot {
		if err := s.Stop(ctx); err != nil {
			logger.Error(ctx, "scheduler: StopAll failed for an instance", zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	logger.Info(ctx, "scheduler: StopAll completed", zap.Int("count", len(snapshot)))
	return firstErr
}

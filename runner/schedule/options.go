// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

// DefaultTaskTimeout is a periodic task's deadline when its definition sets
// none, and the deadline a manual sync of such an operation runs under.
const DefaultTaskTimeout = 10 * time.Minute

// Options configures a Scheduler. Zero fields take the defaults noted on
// each.
type Options struct {
	// Namespace, when set, prefixes the leader lock key and every queue name
	// (the main worker's, the leader workers' and each definition's) with
	// "Namespace:", so schedulers in different namespaces can share one
	// Redis. Empty keeps the names unprefixed.
	Namespace string
	// LockKey is the Redis key of the leader lock, before Namespace. Default
	// "scheduler-lock".
	LockKey string
	// LockTTL is the leader lock's expiry; the leader extends it every
	// LockTTL/3. Default 30s.
	LockTTL time.Duration
	// LockRetry is how long a standby waits between attempts to take the
	// lock. Default 10s.
	LockRetry time.Duration
	// SyncInterval is how often the leader rebuilds the periodic task set
	// from the schedule sources. Default 3m.
	SyncInterval time.Duration
	// Concurrency is the main worker's concurrency. Default 10.
	Concurrency int
	// ShutdownTimeout bounds the main worker's drain and Stop's wait.
	// Default 30s.
	ShutdownTimeout time.Duration
	// DefaultTimeout is a periodic task's deadline when its definition sets
	// none. Default 10m.
	DefaultTimeout time.Duration
	// Queues maps each queue the main worker drains to its weight. Default
	// DefaultQueues().
	Queues map[string]int
	// SourceScope, when set, wraps each schedule source's fetch: it returns
	// the context the source runs with and a finish function that receives
	// the source's error. The Postgres runner sets it to a system-wide
	// tenant scope.
	SourceScope func(ctx context.Context) (context.Context, func(err error) error)
	// Metrics, when set, receives the scheduler's queue collector, which reports
	// asynq's queue sizes and task counts. Nil registers it nowhere.
	Metrics prometheus.Registerer
}

func (o Options) withDefaults() Options {
	if o.LockKey == "" {
		o.LockKey = "scheduler-lock"
	}
	if o.LockTTL <= 0 {
		o.LockTTL = 30 * time.Second
	}
	if o.LockRetry <= 0 {
		o.LockRetry = 10 * time.Second
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 3 * time.Minute
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 10
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 30 * time.Second
	}
	if o.DefaultTimeout <= 0 {
		o.DefaultTimeout = DefaultTaskTimeout
	}
	if o.Queues == nil {
		o.Queues = DefaultQueues()
	}
	return o
}

// LeaderWorker is an extra asynq server that only the leader runs, for work
// that must be bounded globally rather than per pod. A fresh server starts
// each leader term and shuts down when the term ends.
type LeaderWorker struct {
	// Name labels the worker in logs.
	Name string
	// Mux routes the worker's task types.
	Mux *asynq.ServeMux
	// Queues maps the worker's queues to weights. A queue here should not
	// also be in Options.Queues, or the main worker drains it too.
	Queues map[string]int
	// Concurrency returns the worker's concurrency at term start; a value
	// below 1, or a nil func, means 1.
	Concurrency func() int
	// ShutdownGrace bounds the drain on lock loss. Keep it short, so a
	// re-elected pod's worker and this one overlap briefly. Default 1s.
	ShutdownGrace time.Duration
}

func (w LeaderWorker) concurrency() int {
	if w.Concurrency != nil {
		if n := w.Concurrency(); n > 0 {
			return n
		}
	}
	return 1
}

func (w LeaderWorker) grace() time.Duration {
	if w.ShutdownGrace > 0 {
		return w.ShutdownGrace
	}
	return time.Second
}

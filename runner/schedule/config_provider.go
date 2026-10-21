// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// fetchTimeout is the per-source timeout for FetchSchedules calls.
const fetchTimeout = 30 * time.Second

// abandonGrace is how long past its deadline a fetch may take to return
// before GetConfigs abandons it: a fetch that honours ctx returns within it.
const abandonGrace = 100 * time.Millisecond

// defaultMaxRetry is the maximum number of retries for periodic tasks when the
// source does not specify one. Periodic tasks are re-enqueued on their cron
// schedule, so high retry counts are wasteful — a failed task with asynq.TaskID
// uniqueness blocks new enqueues until retries are exhausted. 3 retries with
// exponential backoff resolve within ~1 minute. Once exhausted, asynq archives
// the task; the leader's cleanupArchivedTasks loop deletes it so the next cron
// tick can re-enqueue fresh.
const defaultMaxRetry = 3

// ConfigProvider implements asynq.PeriodicTaskConfigProvider.
// It aggregates all registered ScheduleSources into asynq PeriodicTaskConfig entries.
type ConfigProvider struct {
	sources        []ScheduleSource
	defaultTimeout time.Duration
	scope          func(ctx context.Context) (context.Context, func(err error) error)
	// nextReadBound, when positive, bounds each source's fetch in the next
	// GetConfigs call; the call clears it. The scheduler sets it for a new
	// leader's first read.
	nextReadBound atomic.Int64
	// inflight marks a source whose abandoned fetch has not returned yet, so
	// a later sync does not start a second one.
	inflight []atomic.Bool
	// namespace prefixes each definition's queue; see Options.Namespace.
	namespace string

	// lastGood holds, per source, the definitions of its last successful
	// fetch. A source whose fetch fails keeps them, so one failed read does
	// not unregister that source's tasks. Guarded by lastGoodMu.
	lastGoodMu sync.Mutex
	lastGood   [][]ScheduleDefinition
}

// WithNamespace puts every definition's queue in namespace ns (see
// Options.Namespace); a definition that names no queue goes to ns's
// QueueDefault. Empty ns leaves the queues as the definitions name them.
func (p *ConfigProvider) WithNamespace(ns string) *ConfigProvider {
	p.namespace = ns
	return p
}

// queueFor returns the queue a definition's task is enqueued on, and false
// for asynq's default queue.
func (p *ConfigProvider) queueFor(def ScheduleDefinition) (string, bool) {
	q := def.Queue
	if q == "" {
		if p.namespace == "" {
			return "", false
		}
		q = QueueDefault
	}
	return Namespaced(p.namespace, q), true
}

// boundNextRead bounds each source's fetch in the next GetConfigs call to d.
func (p *ConfigProvider) boundNextRead(d time.Duration) { p.nextReadBound.Store(int64(d)) }

// NewConfigProvider creates a ConfigProvider that aggregates the given
// sources. defaultTimeout is the deadline of a definition that sets none
// (zero for no deadline). scope, when non-nil, wraps each source's fetch;
// see Options.SourceScope.
func NewConfigProvider(sources []ScheduleSource, defaultTimeout time.Duration, scope func(ctx context.Context) (context.Context, func(err error) error)) *ConfigProvider {
	return &ConfigProvider{
		sources:        sources,
		defaultTimeout: defaultTimeout,
		scope:          scope,
		inflight:       make([]atomic.Bool, len(sources)),
		lastGood:       make([][]ScheduleDefinition, len(sources)),
	}
}

// errSourceAbandoned marks a source whose fetch overran its deadline.
var errSourceAbandoned = errors.New("schedule source did not return by its deadline")

// errSourceStillRunning marks a source whose previously abandoned fetch is
// still running.
var errSourceStillRunning = errors.New("schedule source's previous fetch is still running")

type fetchResult struct {
	defs []ScheduleDefinition
	err  error
	late bool // the fetch ended at or after its deadline
}

// fetchSource runs one source's fetch in its own goroutine under timeout. A
// fetch that has not returned abandonGrace after its deadline is abandoned:
// GetConfigs goes on without it, and the goroutine finishes its scope when the
// fetch returns. Until then the source is skipped.
func (p *ConfigProvider) fetchSource(i int, src ScheduleSource, timeout time.Duration) fetchResult {
	if !p.inflight[i].CompareAndSwap(false, true) {
		return fetchResult{err: errSourceStillRunning}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	done := make(chan fetchResult, 1)
	go func() {
		defer cancel()
		defer p.inflight[i].Store(false)
		// Schedule discovery reads across all tenants; the scope hook (a
		// system-wide store transaction for Postgres) wraps the fetch.
		srcCtx, finish := ctx, func(error) error { return nil }
		if p.scope != nil {
			srcCtx, finish = p.scope(ctx)
		}
		defs, err := fetchRecovering(srcCtx, src)
		if finalizeErr := finish(err); finalizeErr != nil {
			logger.Warn("scheduler: failed to finalize schedule-fetch transaction",
				zap.String("source", src.Name()), zap.Error(finalizeErr))
		}
		done <- fetchResult{defs: defs, err: err, late: ctx.Err() != nil}
	}()
	timer := time.NewTimer(timeout + abandonGrace)
	defer timer.Stop()
	select {
	case r := <-done:
		return r
	case <-timer.C:
		return fetchResult{err: errSourceAbandoned, late: true}
	}
}

// GetConfigs is called by asynq.PeriodicTaskManager at SyncInterval.
// It queries all sources and converts ScheduleDefinitions to asynq.PeriodicTaskConfig.
// A source whose fetch fails is logged and keeps the definitions of its last
// successful fetch, so a failed read does not unregister its tasks; a source
// that has never fetched successfully contributes none. A definition
// with an empty ID or job type, or a cron expression asynq cannot parse, is
// dropped with an ERROR log and a count in scheduler_invalid_definitions_total:
// asynq rejects the whole set when one entry is invalid, so one bad definition
// would otherwise stop every schedule.
func (p *ConfigProvider) GetConfigs() ([]*asynq.PeriodicTaskConfig, error) {
	start := time.Now()
	// Observe the full aggregation cost regardless of how GetConfigs returns
	// (an early-warning gauge). The task-count gauge is set from len(seen)
	// just before the return, once dedup is final.
	defer func() {
		schedulerRefreshDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	// Use a map to deduplicate by ID — last writer wins.
	seen := make(map[string]*asynq.PeriodicTaskConfig)

	defaultTimeout := p.defaultTimeout
	failedSources := 0

	timeout := fetchTimeout
	bound := time.Duration(p.nextReadBound.Swap(0))
	if bound > 0 && bound < timeout {
		timeout = bound
	}

	// Every source is fetched at once, each under its own deadline, so a slow
	// or hung source costs only its own schedules.
	results := make([]fetchResult, len(p.sources))
	var wg sync.WaitGroup
	for i, src := range p.sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = p.fetchSource(i, src, timeout)
		}()
	}
	wg.Wait()

	p.lastGoodMu.Lock()
	defer p.lastGoodMu.Unlock()
	for i, src := range p.sources {
		defs, err := results[i].defs, results[i].err
		if err == nil {
			p.lastGood[i] = defs
		}
		if bound > 0 && results[i].late {
			logger.Warn("scheduler: the first schedule read hit its deadline for this source; it is read again at the next sync",
				zap.String("source", src.Name()), zap.Duration("deadline", timeout))
		}
		if err != nil {
			failedSources++
			switch {
			case errors.Is(err, errSourcePanicked):
				// logged at ERROR where it was recovered
			case bound > 0 && results[i].late:
				// logged above
			case errors.Is(err, errSourceAbandoned):
				logger.Warn("scheduler: abandoned a schedule source that did not return by its deadline; a fetch must honour its context",
					zap.String("source", src.Name()), zap.Duration("deadline", timeout))
			default:
				logger.Warn("scheduler: failed to fetch schedules from source",
					zap.String("source", src.Name()),
					zap.Error(err),
				)
			}
			// Keep the last good set: a failed read is not a request to
			// unregister the source's tasks.
			defs = p.lastGood[i]
			logger.Warn("scheduler: keeping the source's last good schedules until a fetch succeeds",
				zap.String("source", src.Name()), zap.Int("kept", len(defs)))
		}

		for _, def := range defs {
			if err := validateDefinition(def); err != nil {
				schedulerInvalidDefinitionsTotal.WithLabelValues(src.Name()).Inc()
				logger.ErrorNoCtx("scheduler: dropping an invalid schedule definition",
					zap.String("source", src.Name()), zap.String("id", def.ID), zap.Error(err))
				continue
			}
			opts := []asynq.Option{
				asynq.TaskID(def.ID),
			}
			if q, ok := p.queueFor(def); ok {
				opts = append(opts, asynq.Queue(q))
			}
			maxRetry := def.MaxRetry
			if maxRetry <= 0 {
				maxRetry = defaultMaxRetry
			}
			opts = append(opts, asynq.MaxRetry(maxRetry))
			timeout := def.Timeout
			if timeout == 0 && defaultTimeout > 0 {
				timeout = defaultTimeout
			}
			if timeout > 0 {
				opts = append(opts, asynq.Timeout(timeout))
			}

			seen[def.ID] = &asynq.PeriodicTaskConfig{
				Cronspec: def.CronExpression,
				Task:     asynq.NewTask(def.JobType, def.Payload),
				Opts:     opts,
			}
		}
	}

	if len(p.sources) > 0 && failedSources == len(p.sources) {
		logger.Warn("scheduler: all schedule sources failed, keeping their last good schedules",
			zap.Int("source_count", len(p.sources)),
		)
	}

	schedulerPeriodicTasks.Set(float64(len(seen)))

	configs := make([]*asynq.PeriodicTaskConfig, 0, len(seen))
	for _, cfg := range seen {
		configs = append(configs, cfg)
	}
	return configs, nil
}

// validateDefinition reports why asynq could not register def: it needs an
// ID, a job type and a cron expression its parser (robfig/cron's standard
// five-field parser with descriptors) accepts.
func validateDefinition(def ScheduleDefinition) error {
	switch {
	case def.ID == "":
		return errors.New("empty ID")
	case def.JobType == "":
		return errors.New("empty job type")
	case def.CronExpression == "":
		return errors.New("empty cron expression")
	case def.Timeout < 0:
		return fmt.Errorf("negative timeout %s", def.Timeout)
	}
	if _, err := cron.ParseStandard(def.CronExpression); err != nil {
		return fmt.Errorf("cron expression %q: %w", def.CronExpression, err)
	}
	return nil
}

// errSourcePanicked marks a source whose FetchSchedules panicked.
var errSourcePanicked = errors.New("schedule source panicked")

// fetchRecovering calls src.FetchSchedules and turns a panic, in the source or
// in anything it calls such as a host's TenantDirectory, into an error, so one
// source cannot stop the leader or, on a later sync inside asynq, the process.
func fetchRecovering(ctx context.Context, src ScheduleSource) (defs []ScheduleDefinition, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorNoCtx("scheduler: schedule source panicked; its schedules are skipped this sync",
				zap.String("source", src.Name()), zap.Any("panic", r), zap.Stack("stack"))
			defs, err = nil, fmt.Errorf("%w: %s: %v", errSourcePanicked, src.Name(), r)
		}
	}()
	return src.FetchSchedules(ctx) //nolint:wrapcheck // the caller logs with the source name
}

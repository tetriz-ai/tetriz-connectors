// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/hibiken/asynq"
	asynqmetrics "github.com/hibiken/asynq/x/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/waitgroup"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// archivedCleanupInterval controls how often the leader scans for archived
// tasks that block periodic re-enqueue. When asynq exhausts retries it moves
// the task to the archived (dead) queue. Because the task still holds its
// TaskID, the PeriodicTaskManager's next cron tick gets ErrDuplicateTask and
// silently skips — permanently blocking that schedule. This cleanup loop
// deletes archived tasks so the next cron tick can re-enqueue fresh.
var archivedCleanupInterval = 60 * time.Second

// Scheduler manages the asynq worker (all pods) and the PeriodicTaskManager (leader only).
// Leader election uses redsync distributed locking.
type Scheduler struct {
	opts    Options
	manager *asynq.PeriodicTaskManager // nil until leader lock acquired
	worker  *asynq.Server
	mux     *asynq.ServeMux
	// leaderWorkers are started per leader term, after the periodic task
	// manager, and shut down concurrently when the term ends.
	leaderWorkers  []LeaderWorker
	lockFactory    DistLockFactory
	configProvider *ConfigProvider
	// redisClient is shared by the asynq servers and the Inspector; the
	// PeriodicTaskManager opens a connection of its own from its options.
	redisClient *redis.Client
	inspector   *asynq.Inspector
	// shutdownTimeout bounds the Stop() wait for background goroutines. It is the
	// SAME configured budget handed to the asynq worker (asynq.Config.ShutdownTimeout
	// in NewScheduler), so the wg wait and the worker drain share one coherent,
	// tunable deadline instead of the shared 30s package default.
	shutdownTimeout time.Duration

	mu          sync.Mutex // protects manager, leaderMutex, started, stopped and cancel
	leaderMutex DistLock
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	started     bool
	stopped     bool // set by Stop; latches Start into a no-op after shutdown begins
}

// NewScheduler wires a Scheduler. It does NOT start the asynq worker: call
// Start (or StartAll) once the process is ready to serve.
//
// The Scheduler is added to the package-level registry, which StartAll and
// StopAll act on; Stop removes it.
func NewScheduler(
	ctx context.Context,
	opts Options,
	redisClient *redis.Client,
	sources []ScheduleSource,
	mux *asynq.ServeMux,
	leaderWorkers []LeaderWorker,
	lockFactory DistLockFactory,
) (*Scheduler, error) {
	_ = ctx // ctx is unused; Start binds the lifecycle context.
	opts = opts.withDefaults()
	opts.LockKey = Namespaced(opts.Namespace, opts.LockKey)
	opts.Queues = namespacedQueues(opts.Namespace, opts.Queues)
	if opts.Namespace != "" {
		lws := make([]LeaderWorker, len(leaderWorkers))
		for i, lw := range leaderWorkers {
			lw.Queues = namespacedQueues(opts.Namespace, lw.Queues)
			lws[i] = lw
		}
		leaderWorkers = lws
	}
	worker := asynq.NewServerFromRedisClient(redisClient, asynq.Config{
		Concurrency:     opts.Concurrency,
		ShutdownTimeout: opts.ShutdownTimeout,
		Queues:          opts.Queues,
		// Honor vendor backoff signals on retry. When a handler returns an
		// error carrying a RetryAfterHinter (a sdk.SyncError parsed from the
		// vendor's Retry-After header on 429/503), that wait is the floor for
		// the retry delay instead of the default exponential schedule.
		RetryDelayFunc: retryDelayFromVendorHint,
	})

	cp := NewConfigProvider(sources, opts.DefaultTimeout, opts.SourceScope).WithNamespace(opts.Namespace)

	inspector := asynq.NewInspectorFromRedisClient(redisClient)
	if opts.Metrics != nil {
		// Register returns AlreadyRegisteredError when another scheduler already
		// put a queue collector on the same registerer. Safe to ignore: the first
		// registration wins and keeps collecting.
		if err := opts.Metrics.Register(asynqmetrics.NewQueueMetricsCollector(inspector)); err != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(err, &are) {
				return nil, fmt.Errorf("scheduler: register metrics: %w", err)
			}
		}
	}

	s := &Scheduler{
		opts:            opts,
		worker:          worker,
		mux:             mux,
		leaderWorkers:   leaderWorkers,
		lockFactory:     lockFactory,
		configProvider:  cp,
		redisClient:     redisClient,
		inspector:       inspector,
		shutdownTimeout: opts.ShutdownTimeout,
	}

	registerForStart(s)
	return s, nil
}

// Start binds the asynq worker to Redis and kicks off leader election. Idempotent.
// Cancelling ctx ends leader election: a leader shuts its periodic task manager
// down and releases the leader lock, so another instance can take over. The
// worker keeps running until Stop, which must still be called.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	// Reject Start once Stop has begun, so a Start that races shutdown cannot
	// boot a worker against a Redis client that is about to close. The
	// rejection is logged, because it otherwise leaves no trace.
	if s.stopped {
		s.mu.Unlock()
		logger.Info(ctx, "scheduler: Start rejected after Stop; skipping worker boot")
		return nil
	}
	if s.started {
		s.mu.Unlock()
		return nil
	}

	// Hold the lock across worker.Start so Stop sees either no worker (latched out
	// above) or a fully-booted one to drain — never a half-started server, whose
	// asynq Shutdown would panic. asynq.Server.Start is non-blocking (verified
	// v0.26.0: it spawns background goroutines and returns, no Redis dial), so the
	// lock is held only briefly.
	if err := s.worker.Start(s.mux); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: start worker: %w", err)
	}
	s.started = true
	lifecycleCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	s.mu.Unlock()

	logger.Info(ctx, "scheduler: worker started on this pod")

	goroutine.RunWithCtx(lifecycleCtx, func(ctx context.Context) {
		defer s.wg.Done()
		s.runLeader(ctx)
	})

	return nil
}

// runLeader runs the leader election loop: acquire lock → start manager → extend.
// If the lock is lost, the manager is shut down and the loop re-enters leader
// election. Runs until the context is cancelled.
func (s *Scheduler) runLeader(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			logger.Info(ctx, "scheduler: leader loop exiting on context cancellation", zap.Error(err))
			return
		}
		s.runLeaderOnce(ctx)
	}
}

// runLeaderOnce performs a single leader term: acquire lock, start manager,
// extend until lost or cancelled. Returns when the term ends.
func (s *Scheduler) runLeaderOnce(ctx context.Context) {
	// A leader-only term publishes the cron-entry / periodic-task gauges (GetConfigs runs
	// only under the lock). Clear them when the term ends so a stepped-down pod stops
	// exporting a frozen snapshot that would win a cross-pod max(); the next
	// leader repopulates them. Fires on every exit, including a term that never won the lock
	// (a harmless no-op then).
	defer resetLeaderCronMetrics()

	mutex := s.lockFactory.NewMutex(s.opts.LockKey,
		redsync.WithExpiry(s.opts.LockTTL),
	)

	// Acquire the distributed leader lock, retrying until success or cancellation.
	for {
		err := mutex.Lock()
		if err == nil {
			logger.Info(ctx, "scheduler: acquired leader lock, starting periodic task manager")
			break
		}
		logger.Info(ctx, "scheduler: another instance is leader, retrying...",
			zap.Duration("retry_in", s.opts.LockRetry),
			zap.Error(err),
		)
		select {
		case <-ctx.Done():
			logger.Info(ctx, "scheduler: context cancelled during lock acquisition")
			return
		case <-time.After(s.opts.LockRetry):
		}
	}

	s.mu.Lock()
	s.leaderMutex = mutex
	s.mu.Unlock()

	// Extend the lock from the moment it is held, not after the manager's first
	// read: a first read longer than LockTTL would otherwise lose the lock while
	// this instance goes on to start a manager. stopTerm ends the extension,
	// which releases the lock and shuts down any manager (endTerm).
	termCtx, termCancel := context.WithCancel(ctx)
	defer termCancel()
	extendDone := make(chan struct{})
	goroutine.RunWithCtx(termCtx, func(ctx context.Context) {
		defer close(extendDone)
		s.extendLock(ctx)
	})
	stopTerm := func() {
		termCancel()
		<-extendDone
	}

	// The manager gets a connection of its own, made from the shared client's
	// options: asynq's periodic scheduler closes its client on Shutdown, which
	// on a shared client fails and logs an error. The shared client is closed
	// once, by its owner.
	manager, err := asynq.NewPeriodicTaskManager(asynq.PeriodicTaskManagerOpts{
		RedisConnOpt:               ownConnOpt(s.redisClient),
		PeriodicTaskConfigProvider: s.configProvider,
		SyncInterval:               s.opts.SyncInterval,
	})
	if err != nil {
		logger.Error(ctx, "scheduler: failed to create periodic task manager", zap.Error(err))
		stopTerm()
		s.waitLockRetry(ctx)
		return
	}
	// Bound the manager's first read below the lock TTL, so a slow source
	// cannot hold the term open past the lock's expiry.
	s.configProvider.boundNextRead(s.opts.LockTTL / 2)
	if err := s.startManager(manager); err != nil {
		logger.Error(ctx, "scheduler: failed to start periodic task manager", zap.Error(err))
		stopTerm()
		s.waitLockRetry(ctx)
		return
	}

	s.mu.Lock()
	if s.leaderMutex == nil {
		// The lock was lost while the manager started: step down.
		s.mu.Unlock()
		manager.Shutdown()
		stopTerm()
		return
	}
	s.manager = manager
	s.mu.Unlock()

	logger.Info(ctx, "scheduler: leader started with periodic task manager")

	// Start each leader-only worker for this term. A fresh server per term:
	// asynq.Server.Shutdown is terminal, so a re-elected leader builds a new
	// one. If one fails to start, the leader steps down (shuts the manager and
	// every worker started so far, releases the lock) so the leader loop
	// re-elects and retries the whole term: a queue that rides only that
	// worker would otherwise be stranded while this pod holds the lock.
	started := make([]*asynq.Server, 0, len(s.leaderWorkers))
	for _, lw := range s.leaderWorkers {
		if lw.Mux == nil {
			continue
		}
		srv := asynq.NewServerFromRedisClient(s.redisClient, asynq.Config{
			Concurrency:     lw.concurrency(),
			ShutdownTimeout: lw.grace(),
			Queues:          lw.Queues,
			RetryDelayFunc:  retryDelayFromVendorHint,
		})
		if err := srv.Start(lw.Mux); err != nil {
			logger.Error(ctx, "scheduler: failed to start leader worker; stepping down so the term is retried",
				zap.String("worker", lw.Name), zap.Error(err))
			s.mu.Lock()
			if s.manager != nil {
				s.manager.Shutdown()
				s.manager = nil
			}
			s.mu.Unlock()
			for _, w := range started {
				w.Shutdown()
			}
			stopTerm()
			return
		}
		started = append(started, srv)
		logger.Info(ctx, "scheduler: leader worker started",
			zap.String("worker", lw.Name), zap.Int("concurrency", lw.concurrency()))
	}

	// Start archived task cleanup in a background goroutine for this leader term.
	// Cancelled when leaderCtx is done (lock lost or shutdown).
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	cleanupDone := make(chan struct{})
	goroutine.RunWithCtx(leaderCtx, func(ctx context.Context) {
		defer close(cleanupDone)
		s.cleanupArchivedTasks(ctx)
	})

	// The lock is extended until ctx is cancelled or the lock is lost.
	<-extendDone
	leaderCancel()
	// Shut every leader worker down concurrently, not one after another: each
	// blocks up to its own grace, and the lock is already released, so a
	// re-elected pod may start its own workers meanwhile. Concurrent shutdown
	// bounds that overlap to the longest grace rather than their sum. The
	// servers are owned by this goroutine and never stored on the Scheduler,
	// so Stop cannot race a second Shutdown against this one.
	var shutdownWG sync.WaitGroup
	for _, w := range started {
		if w == nil {
			continue
		}
		srv := w
		shutdownWG.Add(1)
		goroutine.RunWithCtx(ctx, func(context.Context) {
			defer shutdownWG.Done()
			srv.Shutdown()
		})
	}
	// Each Shutdown blocks up to its own worker grace, so the fan-out finishes within
	// the longest grace. The shutdownTimeout bound only stops a wedged Shutdown from
	// blocking the leader term's exit forever; a grace longer than it is cut short.
	waitgroup.SafeWait(&shutdownWG, s.shutdownTimeout) //nolint:contextcheck // fan-out shutdown wait is bounded by a timeout, not the leader ctx (already cancelled via leaderCancel above)
	<-cleanupDone
}

// Stop gracefully shuts down the worker and manager, releases the leader lock.
// Safe to call even if Start was never invoked.
func (s *Scheduler) Stop(ctx context.Context) error {
	unregisterForStart(s)

	// Latch stopped and read cancel under the lock so a concurrent Start (the
	// fire-and-forget StartAll goroutine) is serialized with this shutdown.
	s.mu.Lock()
	s.stopped = true
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if !waitgroup.SafeWait(&s.wg, s.shutdownTimeout) { //nolint:contextcheck // shutdown wait uses a timeout; request ctx already cancelled above via s.cancel()
		logger.WarnWithCtx(ctx, "scheduler: timed out waiting for background goroutines to finish during shutdown")
	}

	s.mu.Lock()
	if s.manager != nil {
		s.manager.Shutdown()
		s.manager = nil
	}

	if s.leaderMutex != nil {
		if _, err := s.leaderMutex.Unlock(); err != nil {
			logger.WarnWithCtx(ctx, "scheduler: failed to release leader lock during shutdown", zap.Error(err))
		}
		s.leaderMutex = nil
	}
	wasStarted := s.started
	s.mu.Unlock()

	// Shutdown worker only if Start booted it; calling Shutdown on a never-
	// started asynq.Server panics on its internal channel close.
	if wasStarted {
		s.worker.Shutdown()
	}

	// The inspector shares redisClient, which its owner closes; asynq refuses
	// to close a shared connection, so the inspector is left open here.

	logger.Info(ctx, "scheduler: stopped")
	return nil
}

// cleanupArchivedTasks periodically deletes archived (dead) tasks from all
// known queues. This unblocks the PeriodicTaskManager which cannot re-enqueue
// a task if an archived task with the same TaskID exists.
func (s *Scheduler) cleanupArchivedTasks(ctx context.Context) {
	// Clean every lane any worker drains, so a lane left out cannot accumulate
	// archived tasks and permanently block its re-enqueue.
	queues := s.registeredQueues()
	ticker := time.NewTicker(archivedCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range queues {
				tasks, err := s.listAllArchived(q)
				if err != nil {
					if errors.Is(err, asynq.ErrQueueNotFound) {
						continue // queue not yet created in Redis — nothing to clean
					}
					logger.WarnWithCtx(ctx, "scheduler: failed to list archived tasks",
						zap.String("queue", q), zap.Error(err))
					if len(tasks) == 0 {
						continue
					}
					// The pages read before the failure are still deleted.
				}
				for _, t := range tasks {
					if err := s.inspector.DeleteTask(q, t.ID); err != nil {
						logger.WarnWithCtx(ctx, "scheduler: failed to delete archived task",
							zap.String("queue", q), zap.String("task_id", t.ID), zap.Error(err))
						continue
					}
					logger.Info(ctx, "scheduler: deleted archived task, unblocking re-enqueue",
						zap.String("queue", q), zap.String("task_id", t.ID))
				}
			}
		}
	}
}

// archivedPageSize is how many archived tasks the cleaner reads per page.
const archivedPageSize = 100

// listAllArchived reads every archived task of queue q, page by page, so a
// queue with more archived tasks than one page is cleaned in one pass. The
// pages are all read before any task is deleted, because a delete shifts the
// later tasks onto earlier pages. On an error it returns the tasks of the
// pages read before it.
func (s *Scheduler) listAllArchived(q string) ([]*asynq.TaskInfo, error) {
	var all []*asynq.TaskInfo
	for page := 1; ; page++ {
		tasks, err := s.inspector.ListArchivedTasks(q, asynq.PageSize(archivedPageSize), asynq.Page(page))
		if err != nil {
			return all, fmt.Errorf("list archived tasks of %s, page %d: %w", q, page, err)
		}
		all = append(all, tasks...)
		if len(tasks) < archivedPageSize {
			return all, nil
		}
	}
}

// registeredQueues returns every queue of the main worker and of every
// leader worker.
func (s *Scheduler) registeredQueues() []string {
	queues := make([]string, 0, len(s.opts.Queues))
	for q := range s.opts.Queues {
		queues = append(queues, q)
	}
	for _, lw := range s.leaderWorkers {
		for q := range lw.Queues {
			queues = append(queues, q)
		}
	}
	return queues
}

// ownConnOpt describes a new connection to the server c talks to.
func ownConnOpt(c *redis.Client) asynq.RedisClientOpt {
	o := c.Options()
	return asynq.RedisClientOpt{
		Network:      o.Network,
		Addr:         o.Addr,
		Username:     o.Username,
		Password:     o.Password,
		DB:           o.DB,
		DialTimeout:  o.DialTimeout,
		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,
		PoolSize:     o.PoolSize,
		TLSConfig:    o.TLSConfig,
	}
}

// startManager starts the periodic task manager; tests replace it.
var startManager = func(m *asynq.PeriodicTaskManager) error { return m.Start() }

func (s *Scheduler) startManager(m *asynq.PeriodicTaskManager) error { return startManager(m) }

// waitLockRetry waits LockRetry, or until ctx ends, after a leader term that
// failed to start. Without it a persistent start failure re-acquires the lock
// and re-reads every schedule source in a tight loop.
func (s *Scheduler) waitLockRetry(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(s.opts.LockRetry):
	}
}

// extendLock periodically extends the leader lock to maintain ownership.
// When the lock cannot be extended or ctx is cancelled, the term ends: the
// periodic task manager is shut down, the lock is released, and the method
// returns.
func (s *Scheduler) extendLock(ctx context.Context) {
	ticker := time.NewTicker(s.opts.LockTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.endTerm(ctx, "scheduler: context cancelled, stepping down as leader")
			return
		case <-ticker.C:
			s.mu.Lock()
			mutex := s.leaderMutex
			s.mu.Unlock()

			if mutex == nil {
				return
			}

			if ok, err := mutex.Extend(); err != nil || !ok {
				logger.WarnWithCtx(ctx, "scheduler: lost leader lock, will re-enter election",
					zap.Error(err),
				)
				s.endTerm(ctx, "")
				return
			}
		}
	}
}

// endTerm shuts the periodic task manager down and releases the leader lock.
// redsync's Unlock deletes the key only while it still holds this instance's
// token, so a lock another instance has since taken is left alone. Stop calls
// the same steps and finds nothing left to do.
func (s *Scheduler) endTerm(ctx context.Context, msg string) {
	if msg != "" {
		logger.Info(ctx, msg)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.manager != nil {
		s.manager.Shutdown()
		s.manager = nil
	}
	if s.leaderMutex != nil {
		if _, err := s.leaderMutex.Unlock(); err != nil {
			logger.WarnWithCtx(ctx, "scheduler: failed to release leader lock", zap.Error(err))
		}
		s.leaderMutex = nil
	}
}

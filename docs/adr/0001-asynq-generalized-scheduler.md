# ADR-0001: Asynq-based scheduler

## Status

Accepted.

## Context

A scheduler that runs cron jobs inside the process holding the leader lock
has four problems:

- only the leader does any work;
- adding a job type means changing the scheduler;
- reconciling schedules is hand-written diffing;
- there is no retry, queue priority or dead-letter handling.

The scheduler has to separate deciding when a job runs from running it, let
more than one job type plug in, and spread execution over every process.

## Decision

Build the scheduler on [hibiken/asynq](https://github.com/hibiken/asynq), a
Redis-backed task queue, in two layers:

1. **Scheduler core** (`runner/schedule`): domain-agnostic. It owns the asynq
   worker, leader election and periodic enqueueing.
2. **Sources and handlers**: a job type supplies a `ScheduleSource` and an
   `asynq.Handler`. The connector job type lives in
   `runner/schedule/connectorsource`.

```
Every process:
  asynq.Server (worker) -- processes tasks from the Redis queues
      +-- connector sync handler   (job type "connector.sync")
      +-- any other registered handler

Leader only (Redis lock via redsync):
  ConfigProvider -- merges every ScheduleSource
      +-- ConnectorScheduleSource (reads connector configs)
  asynq.PeriodicTaskManager -- enqueues tasks on their cron schedules
      +-- LeaderWorker slices (optional, started and stopped with each term)
```

The leader only enqueues. Every process, the leader included, executes.

### Core abstractions

```go
type ScheduleSource interface {
    Name() string
    FetchSchedules(ctx context.Context) ([]ScheduleDefinition, error)
}
```

A `ScheduleDefinition` carries an ID, job type, cron expression, opaque
payload, queue, retry count and timeout. The core never imports a domain
type.

`ConfigProvider` adapts the sources to asynq's `PeriodicTaskConfigProvider`:
it merges them, de-duplicates by ID (the last source wins), logs and skips a
failing source, and applies the default timeout to definitions without one.
asynq calls it every `SyncInterval` and reconciles the registered tasks by
hash, so no hand-written diffing is needed.

### Lifecycle

1. `NewScheduler` wires the scheduler. It does not start the worker, so a
   handler that calls a local server cannot run before that server listens.
2. `Start` (or `StartAll` for every registered scheduler) starts the worker
   and a goroutine that retries the leader lock every `LockRetry` until it
   wins or the context ends.
3. The leader starts the periodic task manager and any `LeaderWorker`
   slices, and extends the lock every `LockTTL / 3`.
4. If an extension fails, the leader stops the manager and its slices and
   releases the lock; another process takes over.
5. `Stop` cancels the lifecycle, waits for the goroutine, shuts down the
   manager and worker, and releases the lock.

### Options

The host passes plain `schedule.Options`:

| Field | Default | Meaning |
|---|---|---|
| `SyncInterval` | 3m | How often the task manager re-reads the sources |
| `Concurrency` | 10 | asynq worker concurrency |
| `DefaultTimeout` | 10m | Timeout for tasks that set none |
| `LockKey` | `scheduler-lock` | Redis key of the leader lock |
| `LockTTL` | 30s | Lock lifetime, extended every third of it |
| `LockRetry` | 10s | How often a standby retries the lock |
| `ShutdownTimeout` | 30s | Grace for in-flight tasks on shutdown |
| `Queues` | `connector-sync` 6, `default` 3, `individual-connector-sync` 1 | Weighted queues |

### Failure handling

| Case | Behaviour |
|---|---|
| Redis unavailable | `Start` does not dial Redis, so it succeeds; no task runs and no process leads until Redis returns |
| Lock held elsewhere | The goroutine retries until the context ends |
| Lock extension fails | The leader steps down |
| One source fails | Logged and skipped; the others still contribute |
| Every source fails | An empty set is returned and asynq unregisters every task until a later pass succeeds (a pinned limitation) |
| Handler panics | The recovery middleware logs the stack and returns an error |
| Task exceeds its timeout | asynq fails it and retries per its retry count |

## Consequences

Easier:

- A new job type is a source and a handler; the core does not change.
- Execution spreads over every process.
- Retry, timeout and queue weights come from asynq.

Harder or deferred:

- Redis carries both scheduling and execution; a Redis outage stops both.
- Archived tasks are not counted in any metric, and the cleaner reads one page of 30
  archived tasks per queue per interval (a pinned limitation).
- Fires missed during a leader handoff are not replayed, and the lock has
  no fencing token, so a split brain can enqueue twice.

### Adding a job type

1. Choose a job type name and queue.
2. Implement `schedule.ScheduleSource`, returning `[]ScheduleDefinition`.
3. Implement an `asynq.Handler` and register it on the `asynq.ServeMux`
   passed to `NewScheduler`.
4. Pass the source to `NewScheduler`. `examples/heartbeat` is a complete
   example.

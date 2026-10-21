# Architecture decision records

Each record states one decision, the context that forced it and what it
costs. Code comments cite them by number and section, so "ADR-0004 §4"
points at a section here.

| Record | Decision |
|---|---|
| [0001](0001-asynq-generalized-scheduler.md) | Schedule with asynq: one leader enqueues, every process executes |
| [0002](0002-vendor-gateway-credential-architecture.md) | Build a credential-scoped gateway per run through a factory |
| [0003](0003-github-connector-lifecycle-events.md) | Emit one immutable record per pull request and issue lifecycle event |
| [0004](0004-bitbucket-rate-limits-and-error-classification.md) | Bitbucket rate limits, per-organization buckets and error classification |

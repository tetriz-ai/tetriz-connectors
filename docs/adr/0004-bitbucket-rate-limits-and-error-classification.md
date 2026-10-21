# ADR-0004: Bitbucket rate limits and error classification

## Status

Accepted. Section numbers below (§1 to §10) are the ones the code cites.

## Context

A Bitbucket-only organization faces three independent problems.

**Identity.** Bitbucket's member API hides email addresses, so a workspace
member cannot be matched to a person by email. A downstream consumer that
resolves authors then drops the member's pull requests.

**Rate limits.** Bitbucket caps each user at 1,000 requests an hour on the
standard tier (up to 10,000 with enough paid users). One limiter bucket
shared by every organization lets one burst starve the rest. Reading each
pull request's diffstat and earliest commit costs two extra calls per pull
request, about 4,000 calls for 20 repositories of 100 pull requests, more
than an hour's budget.

**Error classification.** When a task's deadline has passed and a sink write
then fails with `context deadline exceeded`, labelling the run a sink
failure hides the real cause. A successful run's record count has to come
from the orchestrator.

**Headers.** Bitbucket's documentation says it sends none of
`X-RateLimit-Remaining`, `X-RateLimit-Reset` or `Retry-After` for OAuth
traffic (only `X-RateLimit-Limit`, `X-RateLimit-Resource` and
`X-RateLimit-NearLimit`, and only for access-token and Forge auth). So the
header-driven parts of §4 are inert for OAuth connections, and pacing comes
from waiting on the local limiter (`pacing.ExecuteWaitingForLocalCapacity`).
The live header shape has not been captured to confirm this. See [Bitbucket
Cloud API rate
limits](https://support.atlassian.com/bitbucket-cloud/docs/api-request-limits/).

## Decision

### Identity

- **§1 Identity from commit authors.** Commit author strings carry an
  email (`Name <email>`), so an identity keyed on that email can be merged
  with the email-less member identity downstream. The simulator's Bitbucket
  seed produces that shape. Rejected: a placeholder person keyed on display
  name and account id, which never reconciles with the real person.

### Limits

- **§2 A configured rate at the published cap** for `bitbucket-rest`. The
  shipped default is 0.5556 requests a second (about 33 a minute, 2,000 an
  hour) with a burst of 1; a host overrides it in the gateway configuration.
- **§3 Per-organization limiter buckets.** The Bitbucket and GitHub
  gateways acquire from `group + ":" + orgID`. The orchestrator puts the
  organization id on the context for this.
- **§4 Honour the vendor's retry hint.** The scheduler's retry delay is
  the larger of the vendor's `Retry-After` hint (carried as
  `sdk.SyncError.RetryAfterSeconds`) and the default exponential backoff.
  Rate-limit headers are observed and exported as metrics. The hint is not
  capped (a pinned limitation).
- **§5 Circuit breaker per group and organization.** Three 429 responses
  within five minutes open the breaker for two minutes; while it is open the
  gateway fails fast with a `rate_limit` error so the scheduler backs off.
  The GitLab gateway uses the same breaker (`gateway.CircuitBreaker`) with
  the same values, under its own keys and its own trip counter,
  `gitlab_circuit_breaker_opened_total`. None of the values is
  Bitbucket-specific: GitLab.com's per-minute rate-limit windows and its
  `Retry-After` of up to a minute fall inside the five-minute failure window
  and the two-minute cooldown.
- **§6 Metrics.** `bitbucket_ratelimit_budget_remaining` (gauge),
  `bitbucket_ratelimit_429_total` and
  `bitbucket_circuit_breaker_opened_total` (counters), each by group and
  organization. The host's registration prefix is prepended.

### Caching

- **§7 Cache `first_commit_at` per pull request** across runs, so the
  earliest-commit call is made once per pull request. The cache lives in the
  connector fetch cache.
- **§8 Cache the diffstat of finished pull requests** (merged, declined or
  superseded) in the same fetch cache. Open pull requests are still read
  live, because commits can still arrive.

### Error classification

- **§9 `ctx_exhausted`.** When a sink write fails with a deadline or
  cancellation error and the run's own context is already past its deadline,
  the run is classified `ctx_exhausted`, not `sink_failure`.
- **§10 `records_emitted`.** The success path records the orchestrator's
  count. (The count still misses records published by a batch that later
  failed; this is a pinned limitation.)

### Not decided here

- Splitting enrichment into its own operation with its own budget.
- Pausing an organization whose budget is exhausted.
- Webhook-first ingestion.
- Changing the limiter primitive (it stays a Redis sliding window).

## Consequences

- Bitbucket syncs complete, slowly: at the default rate a large organization
  needs hours for a first `fetch_prs`. The caches of §7 and §8 remove
  most repeat calls after that.
- The rate slows long runs; asynq paces them. No data is lost.
- A member whose commit email differs from the email known downstream ends up
  as two people. GitLab has the same edge case.
- §9 affects labels only: an exhausted run is not a `sink_failure`.

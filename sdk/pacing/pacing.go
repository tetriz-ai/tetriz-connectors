// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
)

// Pacing lets a sequential operation wait out a rejection from OUR OWN rate
// limiter (sdk.ErrLocalRateLimit) and retry, instead of failing the run — which
// freezes its cursor and lets asynq re-run it against a widening window, the
// self-reinforcing backlog that follows. A vendor 429 or an open circuit is
// deliberately NOT waited on here: that is the vendor pacing us, the gateway's own
// retry hint owns it, and sleeping on it would stack two waits for one rejection.
//
// Scope of the bound: the per-run cap bounds ONE run's cumulative wait, not the
// aggregate wait across ops that run concurrently while sharing a worker pool and
// a per-org rate-limit bucket. The Bitbucket PR-pull ops are time-separated by the
// per-org cron stagger, so they rarely sleep against the same bucket at
// once; a budget shared across tasks (Redis-backed) so the fleet as a whole paces
// against one org's real ceiling is a separate, fleet-scale piece of work.
const (
	// pacingSafetyMargin is held back from the run deadline so a wait never
	// consumes the time the caller still needs to finish the page it resumes, emit
	// its records and checkpoint. A wait that fits "exactly" returns to a dead
	// context and loses the work it just paid for.
	pacingSafetyMargin = 90 * time.Second

	// maxPacingWaitPerRun bounds how long ONE run may sleep waiting for its own
	// rate-limit window. A paced run holds a slot in the worker pool every
	// connector and org shares, so an unbounded sleep starves everyone else once
	// several onboard. Reaching the cap is not a failure — work already done is
	// checkpointed and the next run resumes, so a large backlog drains over
	// several runs instead of one long one.
	maxPacingWaitPerRun = 5 * time.Minute

	// localLimiterWaitFloor is a conservative fallback wait used ONLY when the
	// limiter sized no hint. It is a safe floor, not a claim about the window's
	// length: a real sized RetryAfterSeconds always supersedes it (see
	// localLimiterWait).
	localLimiterWaitFloor = time.Minute
)

// giveup reason label values for LocalRateLimitGiveupTotal, returned by canAfford
// so the caller records why pacing stopped. budget_cap: the per-run cap is spent;
// no_deadline: the ctx has no deadline, so nothing would bound the sleep;
// deadline_too_near: not enough time left to wait AND finish useful work after.
const (
	giveupReasonBudgetCap       = "budget_cap"
	giveupReasonNoDeadline      = "no_deadline"
	giveupReasonDeadlineTooNear = "deadline_too_near"
)

// LocalRateLimitWaitSeconds records how long an operation slept waiting out our
// OWN rate limiter, per (connector, operation, org). A rising distribution is the
// early signal that an org is nearing its real vendor cap: pacing absorbs the
// pressure as slower syncs rather than errors, so without this metric the approach
// to the ceiling is invisible. Buckets span 1s to ~8.5min, bounding the run
// budget. The org_id label is collapsed for individual tenants via
// tenantlabel.OrgID at the emit site.
var LocalRateLimitWaitSeconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "connector_local_rate_limit_wait_seconds",
		Help:    "Seconds an operation slept waiting out our OWN local rate limiter (not a vendor 429). Sustained growth for an (org, connector) means it is nearing its real vendor cap and the local budget should rise.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	},
	[]string{"connector_id", "operation_id", "org_id"},
)

// LocalRateLimitGiveupTotal counts the times pacing gave up on a local-limit
// rejection and returned it unchanged instead of waiting, per (connector,
// operation, org, reason). This is the "pacing exhausted, the org is at its real
// ceiling" signal: a sustained non-zero rate for an
// (org, connector) means the per-run budget can no longer absorb the pressure and
// the sync degrades to whatever its caller's error handling does. The reason label
// distinguishes hitting the per-run cap from a deadline that left no room. The
// org_id label is collapsed for individual tenants via tenantlabel.OrgID at the
// emit site.
var LocalRateLimitGiveupTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_local_rate_limit_giveup_total",
		Help: "Count of times local-limit pacing gave up (returned the rejection unchanged) instead of waiting, by reason (budget_cap|no_deadline|deadline_too_near). Sustained non-zero for an (org, connector) means the org is near its real vendor cap and the per-run budget can no longer absorb it.",
	},
	[]string{"connector_id", "operation_id", "org_id", "reason"},
)

// PacingBudget bounds one run's cumulative local-limit wait so a single run cannot
// hold a shared worker slot indefinitely. The zero value is ready to use; it is
// NOT safe for concurrent use — construct one per sequential run and thread it
// through that run's calls.
type PacingBudget struct {
	spent time.Duration
}

// ExecuteWaitingForLocalCapacity runs gw.Execute(ctx, req), waiting out a
// rejection from OUR OWN rate limiter (sdk.ErrLocalRateLimit) and retrying the same
// call rather than failing the run. Every other error — a vendor 429, an open
// circuit — is returned immediately, unchanged: the vendor is pacing us and
// waiting here would stack two waits for one rejection. Waiting is bounded by the
// run deadline (less pacingSafetyMargin) and by maxPacingWaitPerRun across the
// shared budget; a ctx with no deadline never waits (fail-closed), since nothing
// would bound the sleep.
//
// On give-up it returns the last sdk.ErrLocalRateLimit unchanged; the caller's
// existing error handling applies — for fetch_prs a per-repo hold/skip that
// resumes next tick, for fetch_reviews/fetch_pr_comments a run failure that asynq
// reschedules, after the budget is spent rather than on the first denial.
//
// The same wait-out-local-limit-vs-abort policy also lives in
// bitbucket/fetch_repo_tree.go getJSONWaitingForCapacity and
// cursor/operations.go fetchPagePaced; the three are deliberately not merged and
// must stay in sync.
func ExecuteWaitingForLocalCapacity(
	ctx context.Context,
	gw sdk.VendorGateway,
	req proto.Message,
	budget *PacingBudget,
	connectorID sdk.ConnectorID,
	operationID sdk.OperationID,
	orgID string,
) (proto.Message, error) {
	metricOrgID := tenantlabel.OrgID(orgID)
	for {
		resp, err := gw.Execute(ctx, req)
		if err == nil {
			return resp, nil
		}
		if !errors.Is(err, sdk.ErrLocalRateLimit) {
			// Not our own limiter (vendor 429, open circuit, or any other error):
			// return it unchanged for the caller to classify. Wrapped with %w so
			// errors.Is/errors.As still traverse to the underlying sdk.SyncError.
			return nil, fmt.Errorf("vendor request: %w", err)
		}
		wait := localLimiterWait(err)
		if ok, reason := budget.canAfford(ctx, wait); !ok {
			LocalRateLimitGiveupTotal.
				WithLabelValues(string(connectorID), string(operationID), metricOrgID, reason).
				Inc()
			logger.WarnWithCtx(ctx, "local rate-limit pacing exhausted; returning the rejection unchanged",
				zap.String(logger.ConnectorID, string(connectorID)),
				zap.String(logger.OperationID, string(operationID)),
				zap.String(logger.OrgID, orgID),
				zap.String("reason", reason),
				zap.Duration("wait", wait),
				zap.Duration("spent", budget.spent))
			// Budget spent: return the local-limit rejection so the caller's
			// existing handling applies. Wrapped with %w so errors.Is(…,
			// sdk.ErrLocalRateLimit) and errors.As(…, *sdk.SyncError) still traverse.
			return nil, fmt.Errorf("local rate-limit pacing exhausted: %w", err)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("waiting for local rate-limit capacity: %w", ctx.Err())
		case <-timer.C:
			// Charge the budget and record the sleep only once it COMPLETES. A wait
			// cut short by ctx cancellation above is neither spent against the run
			// budget nor recorded as a completed sleep. The loop still bounds total
			// sleep: spent accumulates here on each completed wait, before the next
			// canAfford check.
			budget.spent += wait
			LocalRateLimitWaitSeconds.
				WithLabelValues(string(connectorID), string(operationID), metricOrgID).
				Observe(wait.Seconds())
		}
	}
}

// canAfford reports whether the run may sleep for wait, given what it has already
// spent, and — when it may not — the reason label for LocalRateLimitGiveupTotal.
// Either limit refusing ends the wait: the per-run cap says whether the shared
// worker slot has been held long enough, and the deadline says whether there is
// time left to do useful work afterwards. A run with no deadline may never wait —
// the pacing spends a KNOWN budget, and without a deadline nothing bounds the
// sleep.
func (b *PacingBudget) canAfford(ctx context.Context, wait time.Duration) (bool, string) {
	if b.spent+wait > maxPacingWaitPerRun {
		return false, giveupReasonBudgetCap
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return false, giveupReasonNoDeadline
	}
	if time.Until(deadline) > wait+pacingSafetyMargin {
		return true, ""
	}
	return false, giveupReasonDeadlineTooNear
}

// localLimiterWait reads the wait the gateway sized for the bucket that rejected
// us — already jittered, which keeps orgs rejected by the same window from
// returning in lockstep. The in-run wait (LocalWait) wins over the scheduler's
// retry delay (RetryAfterSeconds), which can be floored far above the time the
// bucket needs to admit again. It falls back to localLimiterWaitFloor when the
// limiter sized neither.
func localLimiterWait(err error) time.Duration {
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return localLimiterWaitFloor
	}
	if syncErr.LocalWait > 0 {
		return syncErr.LocalWait
	}
	if syncErr.RetryAfterSeconds > 0 {
		return sdk.SecondsToDuration(syncErr.RetryAfterSeconds)
	}
	return localLimiterWaitFloor
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		LocalRateLimitWaitSeconds,
		LocalRateLimitGiveupTotal,
	}
}

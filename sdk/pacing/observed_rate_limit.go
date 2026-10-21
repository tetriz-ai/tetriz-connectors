// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// VendorRateLimitObservation records one rate-limit group's most-recently
// observed hourly cap (X-RateLimit-Limit, or vendor equivalent). The
// scheduler gateway factory consumes a `map[group]Observation` from
// sdk.ConnectorConfig.Settings to scale WithRateLimits per-group instead of
// fanning a single cap across groups with different vendor budgets.
//
// Beyond a 7-day staleness window the gateway factory ignores the entry
// and falls back to the yml floor for that group.
type VendorRateLimitObservation struct {
	PerHour    int    `json:"per_hour"`
	ObservedAt string `json:"observed_at"`
}

// MergeVendorRateLimits walks bucketToGroup, queries BudgetLimiter for each
// bucket, and merges fresh observations into existing. Returns the merged
// map and a bool indicating whether any entry changed (callers gate the
// surrounding DB write on this).
//
// Idempotent: an entry is skipped when the value matches AND the prior
// observation is < 24h old. Past 24h the entry is re-stamped even with the
// same value so a stable workspace doesn't drift past the staleness window.
//
// Shared by every connector's end-of-run persist helper to avoid duplicate
// read-modify-write logic — see Bitbucket fetch_prs and GitHub fetch_prs.
// freshObservation pairs a limit with its Redis-side wall-clock stamp so
// the merge keeps the actual observation time instead of overwriting it
// with persist time.
type freshObservation struct {
	limit      int
	observedAt time.Time
}

func MergeVendorRateLimits(
	ctx context.Context,
	bl sdk.BudgetObserver,
	orgID string,
	bucketToGroup map[sdk.RateBucket]string,
	existing map[string]VendorRateLimitObservation,
) (map[string]VendorRateLimitObservation, bool) {
	if bl == nil || len(bucketToGroup) == 0 {
		return existing, false
	}

	fresh := make(map[string]freshObservation, len(bucketToGroup))
	for bucket, group := range bucketToGroup {
		limit, observedAt, ok, err := bl.GetLimit(ctx, orgID, bucket)
		if err != nil {
			logger.WarnWithCtx(ctx, "read observed rate-limit cap",
				zap.String("group", group),
				zap.Error(err))
			continue
		}
		if ok && limit > 0 {
			// Fall back to now() when the snapshot carries no readable
			// observed_at. Treat it as "as fresh as the persist itself",
			// since there is no better signal.
			if observedAt.IsZero() {
				observedAt = time.Now().UTC()
			}
			fresh[group] = freshObservation{limit: limit, observedAt: observedAt}
		}
	}
	if len(fresh) == 0 {
		return existing, false
	}

	if existing == nil {
		existing = make(map[string]VendorRateLimitObservation, len(fresh))
	}
	changed := false
	for group, obs := range fresh {
		stamp := obs.observedAt.UTC().Format(time.RFC3339)
		prior, had := existing[group]
		if had && prior.PerHour == obs.limit && prior.ObservedAt == stamp {
			// Exact match on both limit and snapshot time — nothing new
			// observed since the last persist. Skip the rewrite.
			continue
		}
		existing[group] = VendorRateLimitObservation{PerHour: obs.limit, ObservedAt: stamp}
		changed = true
	}
	return existing, changed
}

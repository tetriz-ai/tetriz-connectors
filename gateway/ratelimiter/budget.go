// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Default rate-limit floors for VCS vendors. Floors are absolute request
// counts against the vendor-side budget; callers pass the floor for their
// operation category and the limiter does not hardcode the choice. Each
// vendor's budget split follows its published rate limits.
//
// These are defaults — per-connector overrides flow through the VCSSettings
// proto (proto/tetriz/connectors/integrations/v1/integrations.proto)
// which is persisted into the connector_configs.settings JSONB column.
const (
	// GitHubCoreFloor reserves 3000/hr of the 5000/hr budget for core
	// ingestion (PR lists, reviews, comments). Diff fetch passes this same
	// floor — diff jobs skip when the core bucket drops below 3000 so the
	// primary ingestion path keeps working.
	GitHubCoreFloor = 3000
	// GitHubOtherFloor reserves 500/hr for infrequent operations.
	GitHubOtherFloor = 500

	// GitLabCoreFloor reserves headroom for core GitLab ingestion. GitLab
	// rate limits are admin-configurable; 1000 is a safe
	// floor against a 2000/min self-hosted default and leaves room for diff
	// fetching to back off cleanly.
	GitLabCoreFloor = 1000

	// BitbucketCoreFloor reserves headroom for core Bitbucket Cloud ingestion.
	// Bitbucket Cloud limits are ~1000/hr per user.
	BitbucketCoreFloor = 600
)

// BudgetLimiter is a header-aware rate-limit floor enforcer. With a Redis
// BudgetStore, state is shared across asynq workers and connector pull runs so floor checks stay consistent regardless of
// which surface issues the outbound call; with the in-memory one it is per process.
//
// Floors are absolute request counts. Each operation category passes its own
// floor when calling Allow — e.g. diff fetching passes the core floor (3000)
// to guarantee core operations retain headroom.
type BudgetLimiter struct {
	store   BudgetStore
	adapter RateLimitAdapter
}

// NewBudgetLimiter constructs a BudgetLimiter using the supplied Redis
// client and vendor adapter.
func NewBudgetLimiter(rdb redis.Cmdable, adapter RateLimitAdapter) *BudgetLimiter {
	return NewBudgetLimiterWithStore(NewRedisBudgetStore(rdb), adapter)
}

// NewBudgetLimiterWithStore constructs a BudgetLimiter on any BudgetStore,
// such as NewInMemoryBudgetStore for a process with no Redis. One store can
// back the limiters of every adapter: keys carry the adapter's name.
func NewBudgetLimiterWithStore(store BudgetStore, adapter RateLimitAdapter) *BudgetLimiter {
	return &BudgetLimiter{store: store, adapter: adapter}
}

// Bucket names the rate-limit bucket a caller intends to charge against.
// GitHub exposes independent buckets (core, graphql, search, code_search)
// each with its own remaining/limit/reset triple — a single shared snapshot
// would be wrong because a graphql response overwrites the core counter.
// Callers pass the bucket they intend to hit; UpdateFromResponse infers the
// bucket from response headers (X-RateLimit-Resource on GitHub).
//
// Vendors without a bucket split (GitLab, Bitbucket) use BucketDefault.
type Bucket = sdk.RateBucket

const (
	BucketDefault       Bucket = ""
	BucketGitHubCore    Bucket = "core"
	BucketGitHubGraphQL Bucket = "graphql"
	BucketGitHubSearch  Bucket = "search"
)

// Allow returns (true, 0, nil) when the remaining budget for the given
// bucket is strictly above the caller-supplied floor (or when no snapshot
// exists / the reset time has passed). Otherwise it returns
// (false, timeUntilReset, nil) so the caller can schedule a retry.
//
// Semantics:
//   - No snapshot yet (pre-first-request): allow. UpdateFromResponse will
//     populate the snapshot after the initial call.
//   - Reset time has passed: allow. The budget has regenerated and the stale
//     snapshot will be overwritten on the next response.
//   - remaining > floor: allow.
//   - Otherwise: deny with the duration until reset.
func (b *BudgetLimiter) Allow(ctx context.Context, orgID string, bucket Bucket, floor int) (bool, time.Duration, error) {
	scope := b.adapter.ScopeKey(orgID)
	vals, err := b.store.HashGet(ctx, b.key(scope, bucket), "remaining", "limit", "reset")
	if err != nil {
		return false, 0, fmt.Errorf("reading rate budget: %w", err)
	}
	// No snapshot yet — allow through; first response will seed the budget.
	if vals[0] == nil || vals[2] == nil {
		return true, 0, nil
	}
	remStr, rstStr := *vals[0], *vals[2]
	rem, err := strconv.Atoi(remStr)
	if err != nil {
		return false, 0, fmt.Errorf("parsing remaining budget %q: %w", remStr, err)
	}
	rst, err := strconv.ParseInt(rstStr, 10, 64)
	if err != nil {
		return false, 0, fmt.Errorf("parsing reset timestamp %q: %w", rstStr, err)
	}
	resetAt := time.Unix(rst, 0)
	if time.Now().After(resetAt) {
		return true, 0, nil
	}
	if rem > floor {
		return true, 0, nil
	}
	return false, time.Until(resetAt), nil
}

// UpdateFromResponse parses vendor rate-limit headers via the adapter and
// persists the snapshot under the bucket key inferred from the response.
// No-op if the adapter returns nil (headers absent or unparseable). The store
// writes the fields and the TTL atomically.
//
// TTL is time-until-reset plus a 10-minute grace window so a replay just
// after reset still observes the stale snapshot and treats it as expired
// rather than missing. A 1-minute floor guards against negative TTLs when
// the reset timestamp is already in the past.
func (b *BudgetLimiter) UpdateFromResponse(ctx context.Context, orgID string, h http.Header) error {
	snap := b.adapter.ParseBudget(h)
	if snap == nil {
		return nil
	}
	scope := b.adapter.ScopeKey(orgID)
	bucket := b.adapter.BucketFromHeaders(h)
	key := b.key(scope, bucket)

	ttl := time.Until(snap.ResetAt) + 10*time.Minute
	if ttl < time.Minute {
		ttl = time.Minute
	}

	fields := map[string]string{
		"remaining":   strconv.Itoa(snap.Remaining),
		"limit":       strconv.Itoa(snap.Limit),
		"reset":       strconv.FormatInt(snap.ResetAt.Unix(), 10),
		"observed_at": strconv.FormatInt(time.Now().Unix(), 10),
	}
	if err := b.store.HashSet(ctx, key, fields, ttl); err != nil {
		return fmt.Errorf("writing rate budget: %w", err)
	}
	return nil
}

// GetLimit returns the most-recently-observed vendor budget cap and the
// wall-clock time the snapshot was written to the store. Callers use this to
// lift an observation off the store into durable per-config storage (e.g.
// ConnectorConfig.Settings.VendorRateLimits) and stamp the durable record
// with the actual observation time, not the persist time — otherwise a
// stale snapshot would silently mask its age every time end-of-run
// re-stamps it as "now".
//
// observedAt is zero when the snapshot lacks the field (older entries
// or external HSET writers); callers must treat zero as "use fallback".
//
// ok=false means no snapshot exists (pre-first-request) or the TTL expired.
// Callers fall back to the static yml floor in that case.
func (b *BudgetLimiter) GetLimit(ctx context.Context, orgID string, bucket Bucket) (int, time.Time, bool, error) {
	scope := b.adapter.ScopeKey(orgID)
	vals, err := b.store.HashGet(ctx, b.key(scope, bucket), "limit", "observed_at")
	if err != nil {
		return 0, time.Time{}, false, fmt.Errorf("reading rate limit: %w", err)
	}
	if vals[0] == nil {
		return 0, time.Time{}, false, nil
	}
	limStr := *vals[0]
	lim, err := strconv.Atoi(limStr)
	if err != nil {
		return 0, time.Time{}, false, fmt.Errorf("parsing limit %q: %w", limStr, err)
	}
	var observedAt time.Time
	if vals[1] != nil {
		obsStr := *vals[1]
		if obs, parseErr := strconv.ParseInt(obsStr, 10, 64); parseErr == nil {
			observedAt = time.Unix(obs, 0).UTC()
		} else {
			// Malformed observed_at: limit read fine, so continue with
			// zero observedAt (callers treat zero as "use fallback").
			logger.WarnWithCtx(ctx, "parsing rate limit observed_at", zap.String("observed_at", obsStr), zap.Error(parseErr))
		}
	}
	return lim, observedAt, true, nil
}

// key returns the store's hash key for this vendor+scope+bucket.
// BucketDefault collapses to vendor-scope form so single-bucket vendors
// don't carry a trailing separator.
func (b *BudgetLimiter) key(scope string, bucket Bucket) string {
	if bucket == BucketDefault {
		return fmt.Sprintf("rate_budget:%s:%s", scope, b.adapter.Name())
	}
	return fmt.Sprintf("rate_budget:%s:%s:%s", scope, b.adapter.Name(), bucket)
}

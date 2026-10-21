// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package anthropicgateway is the vendor gateway for the Claude Enterprise
// Analytics API: five endpoints sharing one rate-limit group, because Anthropic
// states its budget per organisation across the family rather than per endpoint.
// The reasoning lives at constants.GroupAnthropicAnalytics.
package anthropicgateway

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const rateLimitNamespace = "anthropic-gateway"

// rateLimitSharedBucketTotal counts acquisitions that fell back to the shared
// (org-less) bucket because no org_id was on the context. This is the one path where
// our bucket stops matching Anthropic's per-organisation metering, and the Debug log
// beside it is not emitted in real use — so without this counter the collapse is
// invisible exactly where it costs most. Unlabeled to keep cardinality bounded; the
// failing org is in the logs.
// Follows the linear gateway, which counts the same branch for the same reason.
var rateLimitSharedBucketTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "connector_anthropic_rate_limit_shared_bucket_total",
		Help: "Count of Anthropic rate-limit acquisitions that fell back to the shared bucket (missing org_id on context).",
	},
)

// localLimiterRetryJitter decorrelates callers rejected by the same window, which
// would otherwise wake together and re-exhaust it on the next tick.
const localLimiterRetryJitter = 15 * time.Second

// maxWindowSeconds is the widest window a time.Duration can hold, in seconds.
const maxWindowSeconds = float64(math.MaxInt64 / int64(time.Second))

// usableRate reports whether a configured rate can be enforced at all. The bare
// `<= 0` test is not enough: NaN fails it, and so does
// +Inf, yet NaN scales to a zero-width window and +Inf to a count of MaxInt64 —
// unlimited. Both arrive the same way a bad positive value would, through a
// per-organisation override carrying an arbitrary float64.
func usableRate(reqsPerSec float64) bool {
	return reqsPerSec > 0 && !math.IsInf(reqsPerSec, 0)
}

// AnthropicVendorGateway implements sdk.VendorGateway for the Claude
// Enterprise Analytics API. It embeds vendorapi.BaseVendorGateway, which provides
// Do, DoJSON and Execute. Rate limiting is applied by wrapping the HTTPDoer in
// ForGroup.
type AnthropicVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits  []sdk.RateLimitConfig
	rateLimiter ratelimiter.RateLimiter
	rlStore     ratelimiter.Store
	group       string
	baseDoer    vendorapi.HTTPDoer
}

// rateLimitedDoer enforces the Redis-backed limiter before each request.
type rateLimitedDoer struct {
	inner       vendorapi.HTTPDoer
	rateLimiter ratelimiter.RateLimiter
	group       string
	// window is how long the rejecting bucket needs to free a slot. A retry sooner
	// than this is spent against the same exhausted window, so the scheduler's
	// default backoff has to be told the size rather than guess it.
	window time.Duration
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Per-org bucket, so one organisation's sync cannot starve another's. Anthropic
	// meters per organisation and an API key belongs to one organisation, so the
	// bucket and the vendor's own accounting line up rather than approximating each
	// other — except on the orgless fallback below, which is the one path where that
	// stops being true and is why it is counted.
	key := d.group
	if orgID := reqctx.OrgIDFromContext(ctx); orgID != "" {
		key = d.group + ":" + orgID
	} else {
		rateLimitSharedBucketTotal.Inc()
		logger.Debug(ctx, "rate limiter falling back to shared (orgless) bucket",
			zap.String(logger.Group, d.group),
		)
	}
	allowed, err := d.rateLimiter.Acquire(ctx, d.group, key)
	if err != nil {
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindRateLimit,
			Cause:     fmt.Errorf("rate limit acquire failed for group %s: %w", d.group, err),
			Retryable: true,
		}
	}
	if !allowed {
		return nil, &sdk.SyncError{
			Kind: sdk.ErrKindRateLimit,
			// The phrase stays verbatim (operators match it) and the marker is what
			// lets a caller tell our own bucket from a vendor 429 without matching prose.
			Cause: fmt.Errorf("rate limit exceeded for group %s: %w", d.group, sdk.ErrLocalRateLimit),
			// Sized to the window that rejected: the scheduler's default backoff is
			// shorter than a minute, so every retry would land inside the same
			// saturated window and burn the attempt budget without reaching the vendor.
			Retryable:         true,
			RetryAfterSeconds: jitteredRetryAfterSeconds(d.window, localLimiterRetryJitter),
		}
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("anthropic rate-limited doer: %w", err)
	}
	return resp, nil
}

// NewAnthropicVendorGateway builds a gateway from the given HTTP doer, API key and
// rate limiter store. Base URL and rate come from the gateway config.
func NewAnthropicVendorGateway(doer vendorapi.HTTPDoer, apiKey string, rlStore ratelimiter.Store) (*AnthropicVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("anthropicgateway: %w", err)
	}
	endpoints := cfg.AnthropicEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	if err := validateAnthropicEndpoints(endpoints); err != nil {
		return nil, fmt.Errorf("anthropicgateway: %w", err)
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("anthropicgateway: %w", err)
	}

	return &AnthropicVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewAnthropicVendorRequest(apiKey, baseURLs),
		},
		rateLimits:  rateLimits,
		rateLimiter: ratelimiter.NewRateLimiter(rlStore, rlCfg),
		rlStore:     rlStore,
		baseDoer:    doer,
	}, nil
}

// validateAnthropicEndpoints refuses a config the gateway cannot enforce, rather than
// substituting a default — each default is worse than the failure. An absent group
// leaves its base URL empty, which builds a request against a bare path and dies inside
// the HTTP client as "no Host in request URL", a transport-looking error for a config
// problem. A non-positive rate falls through the scaler's floor to the published cap
// with none of the margin the configured value deliberately keeps.
//
// Separated from the constructor so both refusals are reachable from a test without a
// config file or a limiter store.
func validateAnthropicEndpoints(endpoints map[string]gateway.APIEndpointConfig) error {
	ep, ok := endpoints[constants.GroupAnthropicAnalytics]
	if !ok || ep.BaseURL == "" {
		return fmt.Errorf("no base URL configured for group %q", constants.GroupAnthropicAnalytics)
	}
	if !usableRate(ep.RequestsPerSecond) {
		return fmt.Errorf("group %q has an unusable rate of %v",
			constants.GroupAnthropicAnalytics, ep.RequestsPerSecond)
	}
	return nil
}

// scaleRateToIntegerPerPeriod converts a requests-per-second float into the
// (integer rate, period) pair the limiter accepts. Two invariants: the enforced
// rate never exceeds reqsPerSec, so it never rounds up; and the window is never
// wider than the window the vendor states its cap in, because the limiter only
// bounds a count over a trailing window — an hour-wide bucket for a per-minute cap
// permits the whole hour's budget inside the first minute. Anthropic publishes a
// per-minute cap, so a rate lands on a 1-minute window (0.9 rps -> 54/min against
// a published 60/min) and only a rate too small to express there widens to an hour.
//
// Deliberately no special case for a whole rate. Treating one as N-per-second
// would be a narrower window than the unit Anthropic states its cap in, which the
// second invariant forbids for the mirror-image reason the first does: a window
// narrower than the cap's unit rejects a caller that is inside the cap but issues
// its requests together. Every caller here batches, so the narrow window is never
// the safe reading.
//
// A sixth copy of a helper kept per package — extracting it needs the
// copies to be removed, not kept, so read it before adding a seventh. It began as
// cursor's copy verbatim and has since diverged: the sub-hourly branch below is
// this one only, so the two now differ in behaviour for any rate under 1/3600 rps.
// The surrounding plumbing (ForGroup, WithRateLimits, windowForGroup,
// jitteredRetryAfterSeconds) is still cursor's line for line.
//
// Extraction is a behaviour change for every sibling with a fractional rate — they
// disagree on which widening they apply and whether they keep a whole-rate shortcut
// — which is why it is that issue's scope and not this slice's.
func scaleRateToIntegerPerPeriod(reqsPerSec float64) (int64, time.Duration) {
	if reqsPerSec <= 0 {
		// Unreachable by contract — both callers reject a non-positive rate before
		// getting here — and deliberately the tightest window rather than a safe-looking
		// default, so a caller that ever skips that check is throttled, not trusted.
		return 1, time.Second
	}
	if perMinute := int64(math.Floor(reqsPerSec * 60)); perMinute >= 1 {
		return perMinute, time.Minute
	}
	if perHour := int64(math.Floor(reqsPerSec * 3600)); perHour >= 1 {
		return perHour, time.Hour
	}
	// Below one request an hour, flooring would grant one anyway — 0.0001 rps asks
	// for 0.36 an hour and would be handed 1, which is the round-up the first
	// invariant forbids. Widen the window until a single request fits instead. This
	// is the one case where the window exceeds the vendor's stated unit, and it is
	// safe precisely because the count is 1: the second invariant exists to stop a
	// wide window releasing a burst, and one request is not a burst.
	//
	// Clamped because a time.Duration is int64 nanoseconds: a rate below roughly
	// 1.1e-10 rps overflows it, and the wrap produces a NEGATIVE window — no bound at
	// all, which is worse than the round-up this branch exists to avoid. At the clamp
	// the enforced window is about 292 years, so anything hitting it is a
	// misconfiguration rather than a rate anyone meant.
	secs := math.Ceil(1 / reqsPerSec)
	if secs > maxWindowSeconds {
		secs = maxWindowSeconds
	}
	return 1, time.Duration(secs) * time.Second
}

func buildRateLimitConfig(endpoints map[string]gateway.APIEndpointConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(endpoints))
	for _, ep := range endpoints {
		// The constructor validates the one group it knows by name; this catches any
		// other group the yml grows, which would otherwise floor to 1/s — 60/min, the
		// published cap with none of the margin the configured value keeps.
		if !usableRate(ep.RequestsPerSecond) {
			return nil, fmt.Errorf("group %q has an unusable rate of %v", ep.Group, ep.RequestsPerSecond)
		}
		rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
		resourceMap[ep.Group] = &ratelimiter.RateLimit{Rate: rate, Period: period}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

func buildRateLimitConfigFromFramework(configs []sdk.RateLimitConfig) (*ratelimiter.RateLimitConfig, error) {
	// An empty list passes every per-value check below and yields a limiter that
	// configures no group — which ForGroup then declines to wrap, leaving the gateway
	// unlimited with no error and no log. Refuse it so WithRateLimits keeps the
	// limiter it already had, the same way it does for a bad value.
	if len(configs) == 0 {
		return nil, fmt.Errorf("no rate limit configs supplied")
	}
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(configs))
	for _, c := range configs {
		// These arrive as a per-org override, so a bad value has the same consequence
		// as a bad value in the yml: the scaling floor would turn it into 1/s, which
		// is the published cap with no margin. Reject it so WithRateLimits keeps the
		// limiter it already had rather than loosening it.
		if !usableRate(c.RequestsPerSecond) {
			return nil, fmt.Errorf("group %q has an unusable rate of %v", c.Group, c.RequestsPerSecond)
		}
		rate, period := scaleRateToIntegerPerPeriod(c.RequestsPerSecond)
		resourceMap[c.Group] = &ratelimiter.RateLimit{Rate: rate, Period: period}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

// WithRateLimits returns a copy of this gateway with rate limits configured.
func (g *AnthropicVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.ErrorNoCtx("anthropicgateway: keeping existing rate limiter, failed to build config", zap.Error(err))
		return &cpy
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group,
// its HTTPDoer wrapped with that group's limiter.
func (g *AnthropicVendorGateway) ForGroup(group string) sdk.VendorGateway {
	cpy := *g
	cpy.group = group
	if g.rateLimiter != nil && g.rateLimiter.IsResourceConfigured(group) {
		cpy.Doer = &rateLimitedDoer{
			inner:       g.baseDoer,
			rateLimiter: g.rateLimiter,
			group:       group,
			window:      g.windowForGroup(group),
		}
	}
	return &cpy
}

// windowForGroup returns the enforced window for a group, which is what a caller
// rejected by it must wait out. An unconfigured group falls back to a minute — the
// unit Anthropic states its cap in, and the widest window this scaler produces for
// any rate at or above 1/60 rps.
func (g *AnthropicVendorGateway) windowForGroup(group string) time.Duration {
	for _, rl := range g.rateLimits {
		if rl.Group == group && rl.RequestsPerSecond > 0 {
			_, period := scaleRateToIntegerPerPeriod(rl.RequestsPerSecond)
			return period
		}
	}
	return time.Minute
}

// jitteredRetryAfterSeconds spreads callers the same window rejected, and never
// returns less than a second — a zero would read as "no hint" and drop the caller
// back onto the default backoff this exists to override.
func jitteredRetryAfterSeconds(wait, jitter time.Duration) int {
	total := wait
	if jitter > 0 {
		//nolint:gosec // backoff spreader, not a security primitive
		total += time.Duration(rand.Int63n(int64(jitter)))
	}
	if secs := int(total.Round(time.Second) / time.Second); secs > 1 {
		return secs
	}
	return 1
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		rateLimitSharedBucketTotal,
	}
}

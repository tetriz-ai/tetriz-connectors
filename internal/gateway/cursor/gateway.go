// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursorgateway

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const rateLimitNamespace = "cursor-gateway"

// localLimiterRetryJitter decorrelates callers rejected by the same window, which
// would otherwise wake together and re-exhaust it on the next tick.
const localLimiterRetryJitter = 15 * time.Second

// CursorVendorGateway implements sdk.VendorGateway for the Cursor Admin API.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
type CursorVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits  []sdk.RateLimitConfig
	rateLimiter ratelimiter.RateLimiter
	rlStore     ratelimiter.Store
	group       string
	baseDoer    vendorapi.HTTPDoer
}

// rateLimitedDoer wraps an HTTPDoer to enforce rate limiting via the
// Redis-backed ratelimiter before each request.
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
	// Per-org scope: use org ID from context as the rate limit bucket key so one
	// organization's sync cannot starve another's. Cursor's budget is per API key
	// and a key is per organization, so the bucket and the vendor's own accounting
	// line up rather than approximating each other.
	key := d.group
	if orgID := reqctx.OrgIDFromContext(ctx); orgID != "" {
		key = d.group + ":" + orgID
	} else {
		logger.Debug(ctx, "rate limiter falling back to shared (orgless) bucket",
			zap.String("group", d.group),
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
			// Sized to the window that rejected, because the scheduler's default
			// backoff is shorter than a minute: every retry would land inside the
			// same saturated window and burn the attempt budget without ever
			// reaching the vendor.
			Retryable:         true,
			RetryAfterSeconds: jitteredRetryAfterSeconds(d.window, localLimiterRetryJitter),
		}
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cursor rate-limited doer: %w", err)
	}
	return resp, nil
}

// NewCursorVendorGateway creates a new CursorVendorGateway with the given HTTP doer,
// API key, and rate limiter store. Rate limits and base URLs are read from the
// gateway configuration (gateway.LoadConfig).
func NewCursorVendorGateway(doer vendorapi.HTTPDoer, apiKey string, rlStore ratelimiter.Store) (*CursorVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("cursorgateway: %w", err)
	}
	endpoints := cfg.CursorEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	// Both checks refuse construction rather than substituting a default, because
	// each default is worse than the failure. An absent group leaves its base URL
	// empty, which builds a request against a bare path and dies inside the HTTP
	// client as "no Host in request URL" — a transport-looking error for a config
	// problem. A non-positive rate falls through scaleRateToIntegerPerPeriod's
	// floor to 1/s, which for cursor-admin is 60/min against a published 20/min:
	// a config mistake would quietly *raise* the enforced rate into vendor
	// throttling.
	for _, group := range []string{constants.GroupCursorAdmin, constants.GroupCursorUsage} {
		ep, ok := endpoints[group]
		if !ok || ep.BaseURL == "" {
			return nil, fmt.Errorf("cursorgateway: no base URL configured for group %q", group)
		}
		if ep.RequestsPerSecond <= 0 {
			return nil, fmt.Errorf("cursorgateway: group %q has a non-positive rate of %v", group, ep.RequestsPerSecond)
		}
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("cursorgateway: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	return &CursorVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewCursorVendorRequest(apiKey, baseURLs),
		},
		rateLimits:  rateLimits,
		rateLimiter: rl,
		rlStore:     rlStore,
		baseDoer:    doer,
	}, nil
}

// scaleRateToIntegerPerPeriod converts a requests-per-second float into an
// (integer rate, period) pair the underlying limiter accepts (it only takes
// int64 rate values). Two invariants: the enforced rate must never exceed
// reqsPerSec, so it never rounds up; and the window must be no wider than the
// window the vendor states its cap in, because the limiter only bounds the
// count over a trailing window — an hour-wide bucket for a per-minute cap
// permits the whole hour's budget inside the first minute. Cursor publishes
// per-minute caps, so a rate lands on a 1-minute window (0.33 rps -> 19/min
// against a published 20/min) and only a rate too small to express there (below
// 1/min, i.e. under 1/60 rps) widens to an hour.
//
// Deliberately no special case for a whole rate. Treating one as N-per-second
// would be a narrower window than the unit Cursor states its cap in, which the
// second invariant above forbids for the mirror-image reason the first does: a
// window narrower than the cap's unit rejects a caller that is inside the cap
// but issues its requests together. Every caller here batches, so the narrow
// window is never the safe reading.
//
// Only bitbucket/ also widens a fractional rate to a minute before an hour;
// slack/, linear/ and github/ widen straight to an hour. All four keep the
// whole-rate shortcut this one deliberately omits, which is why this stays a
// copy rather than a shared helper.
func scaleRateToIntegerPerPeriod(reqsPerSec float64) (int64, time.Duration) {
	if reqsPerSec <= 0 {
		return 1, time.Second
	}
	if perMinute := int64(math.Floor(reqsPerSec * 60)); perMinute >= 1 {
		return perMinute, time.Minute
	}
	perHour := int64(math.Floor(reqsPerSec * 3600))
	if perHour < 1 {
		perHour = 1
	}
	return perHour, time.Hour
}

func buildRateLimitConfig(endpoints map[string]gateway.APIEndpointConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(endpoints))
	for _, ep := range endpoints {
		rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
		resourceMap[ep.Group] = &ratelimiter.RateLimit{
			Rate:   rate,
			Period: period,
		}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

func buildRateLimitConfigFromFramework(configs []sdk.RateLimitConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(configs))
	for _, c := range configs {
		// These arrive as a per-org override, so a bad value here has the same
		// consequence as a bad value in the yml: the scaling floor would turn it
		// into 1/s, above Cursor's cap. Reject it so WithRateLimits keeps the
		// limiter it already had rather than loosening it.
		if c.RequestsPerSecond <= 0 {
			return nil, fmt.Errorf("group %q has a non-positive rate of %v", c.Group, c.RequestsPerSecond)
		}
		rate, period := scaleRateToIntegerPerPeriod(c.RequestsPerSecond)
		resourceMap[c.Group] = &ratelimiter.RateLimit{
			Rate:   rate,
			Period: period,
		}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

// WithRateLimits returns a copy of this gateway with rate limits configured.
func (g *CursorVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.ErrorNoCtx("cursorgateway: keeping existing rate limiter, failed to build config", zap.Error(err))
		return &cpy
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *CursorVendorGateway) ForGroup(group string) sdk.VendorGateway {
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
// rejected by it must wait out. An unconfigured group falls back to a minute —
// the unit Cursor states its caps in, and the widest window this scaler produces
// for any rate at or above 1/60 rps.
func (g *CursorVendorGateway) windowForGroup(group string) time.Duration {
	for _, rl := range g.rateLimits {
		if rl.Group == group && rl.RequestsPerSecond > 0 {
			_, period := scaleRateToIntegerPerPeriod(rl.RequestsPerSecond)
			return period
		}
	}
	return time.Minute
}

// jitteredRetryAfterSeconds spreads callers that the same window rejected, and
// never returns less than a second — a zero would read as "no hint" and drop the
// caller back onto the default backoff this exists to override.
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

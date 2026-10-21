// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackgateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const rateLimitNamespace = "slack-gateway"

// SlackVendorGateway implements sdk.VendorGateway for Slack Web APIs.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
type SlackVendorGateway struct {
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
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Per-org scope: use org ID from context as the rate limit bucket key to
	// prevent one organization's heavy sync from starving other tenants.
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
			Kind:      sdk.ErrKindRateLimit,
			Cause:     fmt.Errorf("rate limit exceeded for group %s", d.group),
			Retryable: true,
		}
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("slack rate-limited doer: %w", err)
	}
	return resp, nil
}

// NewSlackVendorGateway creates a new SlackVendorGateway with the given HTTP doer,
// credentials, and rate limiter store. Rate limits and base URLs are read from
// the gateway configuration (gateway.LoadConfig).
func NewSlackVendorGateway(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store) (*SlackVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("slackgateway: %w", err)
	}
	endpoints := cfg.SlackEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("slackgateway: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	return &SlackVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewSlackVendorRequest(token, baseURLs),
		},
		rateLimits:  rateLimits,
		rateLimiter: rl,
		rlStore:     rlStore,
		baseDoer:    doer,
	}, nil
}

// scaleRateToIntegerPerPeriod converts a requests-per-second float into an
// (integer rate, period) pair the underlying limiter accepts (it only takes
// int64 rate values). The enforced rate MUST NOT exceed reqsPerSec, else the
// limiter overshoots Slack's Tier 2 cap and re-introduces the 429s this config
// exists to prevent — so we never round up. Whole-RPS values keep a 1s window;
// fractional values widen to a 1h window with Floor(rps*3600), e.g. 0.33 rps ->
// (1188, 1h) = ~19.8/min. Identical private copies exist in bitbucket/,
// linear/, and github/ gateway.go — fix the algorithm in all four if it
// changes here.
func scaleRateToIntegerPerPeriod(reqsPerSec float64) (int64, time.Duration) {
	if reqsPerSec <= 0 {
		return 1, time.Second
	}
	if reqsPerSec == math.Floor(reqsPerSec) {
		return int64(reqsPerSec), time.Second
	}
	floored := int64(math.Floor(reqsPerSec * 3600))
	if floored < 1 {
		floored = 1
	}
	return floored, time.Hour
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
func (g *SlackVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.ErrorNoCtx("slackgateway: keeping existing rate limiter, failed to build config", zap.Error(err))
		return &cpy
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *SlackVendorGateway) ForGroup(group string) sdk.VendorGateway {
	cpy := *g
	cpy.group = group
	if g.rateLimiter != nil && g.rateLimiter.IsResourceConfigured(group) {
		cpy.Doer = &rateLimitedDoer{
			inner:       g.baseDoer,
			rateLimiter: g.rateLimiter,
			group:       group,
		}
	}
	return &cpy
}

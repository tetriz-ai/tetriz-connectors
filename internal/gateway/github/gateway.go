// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ghgateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/simroute"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const rateLimitNamespace = "gh-gateway"

// GitHubVendorGateway implements sdk.VendorGateway for GitHub APIs.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
type GitHubVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits    []sdk.RateLimitConfig
	rateLimiter   ratelimiter.RateLimiter
	rlStore       ratelimiter.Store
	group         string
	baseDoer      vendorapi.HTTPDoer
	budgetLimiter *ratelimiter.BudgetLimiter
}

// rateLimitedDoer wraps an HTTPDoer to enforce rate limiting via the
// Redis-backed ratelimiter before each request.
type rateLimitedDoer struct {
	inner         vendorapi.HTTPDoer
	rateLimiter   ratelimiter.RateLimiter
	group         string
	budgetLimiter *ratelimiter.BudgetLimiter
}

// tokenDoer wraps an HTTPDoer to inject the GitHub installation token on the
// raw Do() path. The Execute() factory path applies auth via authSetter on the
// VendorRequest, but operations using VendorGateway.Do directly (e.g.
// fetch_pr_diffs which fetches text/plain unified diffs) bypass
// that flow. This wrapper closes the gap so every outbound GitHub request —
// proto-Execute or raw Do — carries credentials.
type tokenDoer struct {
	inner vendorapi.HTTPDoer
	token string
}

func (d *tokenDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if d.token != "" && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+d.token)
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing authenticated GitHub request: %w", err)
	}
	return resp, nil
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Per-org scope (ADR-0004 §3). GitHub App installations are per-org, so
	// per-org bucketing matches the vendor's actual accounting and prevents a
	// busy org from starving others. Debug-log the fallback so a future
	// refactor dropping the orchestrator-side reqctx.WithOrgID doesn't
	// silently collapse all orgs into one bucket.
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
	resp, doErr := d.inner.Do(ctx, req)
	if doErr == nil && resp != nil && d.budgetLimiter != nil {
		// Persist X-RateLimit-* snapshot for per-org cap detection.
		// Failure is non-fatal — cap scaling falls back to yml floor.
		orgID := reqctx.OrgIDFromContext(ctx)
		if orgID != "" {
			if err := d.budgetLimiter.UpdateFromResponse(ctx, orgID, resp.Header); err != nil {
				logger.WarnWithCtx(ctx, "github budget snapshot persist failed",
					zap.String("group", d.group),
					zap.String(logger.OrgID, orgID),
					zap.Error(err))
			}
		}
	}
	if doErr != nil {
		return resp, fmt.Errorf("executing rate-limited GitHub request: %w", doErr)
	}
	return resp, nil
}

// NewGitHubVendorGateway creates a new GitHubVendorGateway with the given HTTP doer,
// credentials, and rate limiter store. Rate limits and base URLs are read from
// the gateway configuration (gateway.LoadConfig).
//
// budgetLimiter is optional. When non-nil, X-RateLimit-* response headers are
// persisted to Redis per-org so end-of-run operations can lift the observed
// cap into ConnectorConfig.Settings.VendorRateLimits — a map keyed by
// gateway group (github-rest / github-graphql / github-rest-search)
// holding pacing.VendorRateLimitObservation entries.
func NewGitHubVendorGateway(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store, budgetLimiter *ratelimiter.BudgetLimiter) (*GitHubVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("ghgateway: %w", err)
	}
	endpoints := cfg.GitHubEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("ghgateway: building rate limit config: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	// Wrap the inbound doer so raw Do() calls also get credentials.
	// Ordering: tokenDoer is innermost — rateLimitedDoer (added per-group via
	// ForGroup) wraps it so the rate-limit check happens before the HTTP call,
	// not before token injection.
	authedDoer := &tokenDoer{inner: doer, token: token}

	return &GitHubVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    authedDoer,
			Factory: NewGitHubVendorRequest(token, baseURLs),
		},
		rateLimits:    rateLimits,
		rateLimiter:   rl,
		rlStore:       rlStore,
		baseDoer:      authedDoer,
		budgetLimiter: budgetLimiter,
	}, nil
}

// NewGitHubVendorGatewayForSimulator creates a gateway whose BaseURLs all point
// to simulatorBaseURL. Rate limits are kept so the gateway can still be used
// as a drop-in replacement, but the upstream host is the local simulator.
//
// simulatorBaseURL is the simulator service root (e.g. "http://simulator:8080");
// this constructor appends the "/github" vendor prefix, so a second vendor can
// reuse the same root under its own prefix. The group routing keys are the same as
// the real GitHub groups; the simulator router maps them by path prefix.
func NewGitHubVendorGatewayForSimulator(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store, budgetLimiter *ratelimiter.BudgetLimiter, simulatorBaseURL string) (*GitHubVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("ghgateway simulator: %w", err)
	}
	endpoints := cfg.GitHubEndpoints()

	// Override all group BaseURLs with the simulator endpoint, keeping each
	// group's path. This ensures the GraphQL group (whose real URL is
	// api.github.com/graphql) maps to simulator/graphql rather than simulator/
	// (which would 404).
	baseURLs := make(map[string]string, len(endpoints))
	for group, ep := range endpoints {
		u, err := simroute.URL(simulatorBaseURL, "/github", ep.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("ghgateway simulator: %w", err)
		}
		baseURLs[group] = u
	}

	// Keep rate limits for the group names so the rest of the gateway machinery
	// (ForGroup, rateLimitedDoer) can attach per-group rate limiters if desired.
	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("ghgateway simulator: building rate limit config: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	authedDoer := &tokenDoer{inner: doer, token: token}

	return &GitHubVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    authedDoer,
			Factory: NewGitHubVendorRequest(token, baseURLs),
		},
		rateLimits:    rateLimits,
		rateLimiter:   rl,
		rlStore:       rlStore,
		baseDoer:      authedDoer,
		budgetLimiter: budgetLimiter,
	}, nil
}

func buildRateLimitConfig(endpoints map[string]gateway.APIEndpointConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(endpoints))
	for _, ep := range endpoints {
		rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
		resourceMap[ep.Group] = &ratelimiter.RateLimit{Rate: rate, Period: period}
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
		resourceMap[c.Group] = &ratelimiter.RateLimit{Rate: rate, Period: period}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

// scaleRateToIntegerPerPeriod mirrors the Bitbucket scaler: never round up so the enforced rate stays under the caller's
// intent. Whole RPS stays on a 1s window; fractional RPS widens to a 1h
// window with Floor(rps*3600). Before that, GitHub used Ceil + 1s which
// overshoots fractional caps — e.g. observed 12500/hr -> 3.47 rps ->
// Ceil(3.47)=4 -> 14400/hr enforced -> back to 429s.
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

// WithRateLimits returns a copy of this gateway with rate limits configured.
func (g *GitHubVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.Error(context.Background(), "building github rate limit config from framework", zap.Error(err))
		return &cpy
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *GitHubVendorGateway) ForGroup(group string) sdk.VendorGateway {
	cpy := *g
	cpy.group = group
	if g.rateLimiter != nil && g.rateLimiter.IsResourceConfigured(group) {
		cpy.Doer = &rateLimitedDoer{
			inner:         g.baseDoer,
			rateLimiter:   g.rateLimiter,
			group:         group,
			budgetLimiter: g.budgetLimiter,
		}
	}
	return &cpy
}

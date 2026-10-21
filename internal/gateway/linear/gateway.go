// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineargateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const rateLimitNamespace = "linear-gateway"

// rateLimitSharedBucketTotal counts Linear rate-limit acquisitions that fell back
// to the shared (org-less) bucket because no org_id was on the context. A non-zero
// rate means a caller is missing org context and competes against every org for one
// bucket. Unlabeled to keep cardinality bounded; the failing org is in the logs.
var rateLimitSharedBucketTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "connector_linear_rate_limit_shared_bucket_total",
		Help: "Count of Linear rate-limit acquisitions that fell back to the shared bucket (missing org_id on context).",
	},
)

// LinearVendorGateway implements sdk.VendorGateway for the Linear GraphQL API.
// Authentication is owned by the injected doer (the perOrgTokenDoer), which sets
// the Bearer header from the org's stored user-actor token; the gateway itself
// does not handle auth.
type LinearVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits  []sdk.RateLimitConfig
	rateLimiter ratelimiter.RateLimiter
	rlStore     ratelimiter.Store
	group       string
	baseDoer    vendorapi.HTTPDoer
}

// rateLimitedDoer wraps an HTTPDoer to enforce rate limiting.
type rateLimitedDoer struct {
	inner       vendorapi.HTTPDoer
	rateLimiter ratelimiter.RateLimiter
	group       string
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	key := d.group
	if orgID := reqctx.OrgIDFromContext(ctx); orgID != "" {
		key = d.group + ":" + orgID
	} else {
		rateLimitSharedBucketTotal.Inc()
		logger.Debug(ctx, "linear rate limiter falling back to shared bucket", zap.String("group", d.group))
	}
	allowed, err := d.rateLimiter.Acquire(ctx, d.group, key)
	if err != nil {
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindRateLimit,
			Cause:     fmt.Errorf("rate limit acquire for group %s: %w", d.group, err),
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
		return nil, fmt.Errorf("executing rate-limited linear request: %w", err)
	}
	return resp, nil
}

// NewLinearVendorGateway creates a LinearVendorGateway over the supplied doer,
// which is responsible for authenticating each request (the perOrgTokenDoer
// injects the org's stored user-actor Bearer token).
//
// Rate limits are set via WithRateLimits; ForGroup applies per-request limiting.
// Default static rate limit is 1.38 req/s (5000 req/hr ÷ 3600s) per the Linear API fair-use tier.
func NewLinearVendorGateway(doer vendorapi.HTTPDoer, rlStore ratelimiter.Store) (*LinearVendorGateway, error) {
	defaultRateLimits := []sdk.RateLimitConfig{
		{
			Group:             constants.GroupLinearGraphQL,
			RequestsPerSecond: 1.38, // ~5000 req/hr ÷ 3600 s
			BurstSize:         5,
		},
	}

	rlCfg, err := buildRateLimitConfig(defaultRateLimits)
	if err != nil {
		return nil, fmt.Errorf("build rate limit config: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	return &LinearVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer: doer,
		},
		rateLimits:  defaultRateLimits,
		rateLimiter: rl,
		rlStore:     rlStore,
		baseDoer:    doer,
	}, nil
}

// WithRateLimits returns a copy of this gateway with updated rate limit configs.
func (g *LinearVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfig(configs)
	if err != nil {
		logger.Error(context.Background(), "lineargateway: build rate limit config", zap.Error(err))
		return g
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
func (g *LinearVendorGateway) ForGroup(group string) sdk.VendorGateway {
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

func buildRateLimitConfig(configs []sdk.RateLimitConfig) (*ratelimiter.RateLimitConfig, error) {
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

// scaleRateToIntegerPerPeriod converts a requests-per-second float into an
// (integer rate, period) pair. Whole-RPS values use a 1s window; fractional
// values widen to a 1h window with Floor(rps*3600) so the enforced rate never
// exceeds the vendor cap. For example, Linear's 1.38 req/s → 4968 req/hr
// rather than Ceil→2 req/s = 7200 req/hr.
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

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		rateLimitSharedBucketTotal,
	}
}

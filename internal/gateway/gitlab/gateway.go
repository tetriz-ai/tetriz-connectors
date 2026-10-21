// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/simroute"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/httputil"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ConnectorConfigStore is the part of the runner store a gateway needs, so a
// gateway does not import runner/store.
type ConnectorConfigStore interface {
	Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error)
	Save(ctx context.Context, cfg *sdk.ConnectorConfig) error
	CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time, credentials json.RawMessage) (swapped bool, err error)
}

const (
	rateLimitNamespace = "gl-gateway"

	// accessTokenTTL is the TTL for cached access tokens in Redis.
	// GitLab access tokens expire in 2 hours; we cache for 115 minutes
	// to refresh before expiry.
	accessTokenTTL = 115 * time.Minute

	// refreshExchangeTimeout bounds the token exchange.
	refreshExchangeTimeout = 10 * time.Second
	// refreshLockTTL is the TTL for the distributed refresh lock: it outlives
	// the exchange and the writes that follow it under the lock.
	refreshLockTTL = refreshExchangeTimeout + auth.RefreshLockSlack

	// defaultTokenEndpoint is the GitLab OAuth token endpoint.
	defaultTokenEndpoint = "https://gitlab.com/oauth/token"

	connectorID = "gitlab"
)

// nextPageSetter is implemented by GitLab response proto types that carry
// the X-Next-Page header value as next_page_url.
type nextPageSetter interface {
	proto.Message
	setNextPageURL(url string)
}

// responseUnwrapper is implemented by gateway-internal wrapper types that
// embed a proto.Message and need to return the inner proto before being
// returned to callers (connector operations type-assert to the proto type).
type responseUnwrapper interface {
	unwrap() proto.Message
}

// GitLabVendorGateway implements sdk.VendorGateway for GitLab APIs.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
//
// GitLab uses X-Next-Page header (not a JSON "next" field) for pagination.
// Execute is overridden to intercept the raw HTTP response, read the
// X-Next-Page header, and inject a full next-page URL into the proto response.
type GitLabVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits  []sdk.RateLimitConfig
	rateLimiter ratelimiter.RateLimiter
	rlStore     ratelimiter.Store
	group       string
	baseDoer    vendorapi.HTTPDoer
	// budgetLimiter, when set, records GitLab's RateLimit-* response headers
	// per organization, the budget fetch_mr_diffs and fetch_file_content
	// check (Allow) before their requests.
	budgetLimiter *ratelimiter.BudgetLimiter
	// circuitBreaker, when set, fails fast during a 429 storm (ADR-0004 §5),
	// as the Bitbucket gateway's does. Nil lets every request through.
	circuitBreaker *gateway.CircuitBreaker
}

// rateLimitedDoer wraps an HTTPDoer to enforce rate limiting via the
// Redis-backed ratelimiter before each request, behind the 429 circuit
// breaker when one is set.
type rateLimitedDoer struct {
	inner          vendorapi.HTTPDoer
	rateLimiter    ratelimiter.RateLimiter
	group          string
	budgetLimiter  *ratelimiter.BudgetLimiter
	circuitBreaker *gateway.CircuitBreaker
}

// circuitKeyPrefix keys the GitLab breaker's markers apart from any other
// vendor's in a shared store.
const circuitKeyPrefix = "gl-cb"

// newCircuitBreaker returns the GitLab breaker over store, counting each trip
// in gitlab_circuit_breaker_opened_total. A nil store returns nil.
func newCircuitBreaker(store gateway.CircuitBreakerStore) *gateway.CircuitBreaker {
	return gateway.NewCircuitBreaker(store, circuitKeyPrefix, func(group, orgID string) {
		gitlabCircuitBreakerOpenedTotal.WithLabelValues(group, orgID).Inc()
	})
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Per-org scope: use org ID from context as the rate limit bucket key to
	// prevent one organization's heavy sync from starving other tenants.
	orgID := reqctx.OrgIDFromContext(ctx)
	key := d.group
	if orgID != "" {
		key = d.group + ":" + orgID
	} else {
		logger.Debug(ctx, "rate limiter falling back to shared (orgless) bucket",
			zap.String("group", d.group),
		)
	}
	// Circuit breaker first (ADR-0004 §5): an open or probe-gated circuit
	// fails fast before a limiter slot is spent.
	probing, gateErr := d.circuitBreaker.Gate(ctx, d.group, orgID)
	if gateErr != nil {
		return nil, gateErr
	}
	allowed, err := d.rateLimiter.Acquire(ctx, d.group, key)
	if err != nil {
		if probing {
			d.circuitBreaker.ReleaseProbe(ctx, d.group, orgID)
		}
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindRateLimit,
			Cause:     fmt.Errorf("rate limit acquire failed for group %s: %w", d.group, err),
			Retryable: true,
		}
	}
	if !allowed {
		if probing {
			d.circuitBreaker.ReleaseProbe(ctx, d.group, orgID)
		}
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindRateLimit,
			Cause:     fmt.Errorf("rate limit exceeded for group %s", d.group),
			Retryable: true,
		}
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing rate-limited request: %w", err)
	}
	if resp != nil {
		// A 429 counts toward tripping the breaker; a half-open probe answered
		// below 500 closes it for every caller.
		if n := d.circuitBreaker.Observe(ctx, d.group, orgID, resp.StatusCode, probing); n > 0 {
			logger.Info(ctx, "gitlab 429 recorded",
				zap.String("group", d.group),
				zap.String(logger.OrgID, orgID),
				zap.Int64("failures_in_window", n))
		}
	}
	if resp != nil && d.budgetLimiter != nil {
		// Persist the RateLimit-* snapshot for the per-org budget. Failure
		// is non-fatal: the budget then allows, as before a first response.
		if orgID != "" {
			if err := d.budgetLimiter.UpdateFromResponse(ctx, orgID, resp.Header); err != nil {
				logger.WarnWithCtx(ctx, "gitlab budget snapshot persist failed",
					zap.String("group", d.group),
					zap.String(logger.OrgID, orgID),
					zap.Error(err))
			}
		}
	}
	return resp, nil
}

// NewGitLabVendorGateway creates a new GitLabVendorGateway with the given
// HTTP doer (typically an autoRefreshDoer) and rate limiter store.
// Rate limits and base URLs are read from the gateway config.
//
// budgetLimiter is optional. When non-nil, RateLimit-* response headers are
// persisted per organization, as the GitHub and Bitbucket gateways do, so the
// operations that check the budget before a request see GitLab's.
//
// cbStore is optional. When non-nil it holds the 429 circuit breaker's state
// (ADR-0004 §5), as for the Bitbucket gateway; nil disables the breaker.
func NewGitLabVendorGateway(doer vendorapi.HTTPDoer, rlStore ratelimiter.Store, cbStore gateway.CircuitBreakerStore, budgetLimiter *ratelimiter.BudgetLimiter) (*GitLabVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("glgateway: %w", err)
	}
	endpoints := cfg.GitLabEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("glgateway: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	return &GitLabVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewGitLabVendorRequest(baseURLs),
		},
		rateLimits:     rateLimits,
		rateLimiter:    rl,
		rlStore:        rlStore,
		baseDoer:       doer,
		budgetLimiter:  budgetLimiter,
		circuitBreaker: newCircuitBreaker(cbStore),
	}, nil
}

type simulatorBearerDoer struct {
	inner vendorapi.HTTPDoer
	token string
}

func (d *simulatorBearerDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if d.token != "" && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+d.token)
	}
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("simulator bearer doer: %w", err)
	}
	return resp, nil
}

// simulatorGitLabBaseURLs rewrites each GitLab group's BaseURL onto the simulator
// root, keeping the path, so gitlab-rest (https://gitlab.com/api/v4) becomes
// "<root>/gitlab/api/v4".
func simulatorGitLabBaseURLs(endpoints map[string]gateway.APIEndpointConfig, simulatorBaseURL string) (map[string]string, error) {
	baseURLs := make(map[string]string, len(endpoints))
	for _, ep := range endpoints {
		u, err := simroute.URL(simulatorBaseURL, "/gitlab", ep.BaseURL)
		if err != nil {
			return nil, err
		}
		baseURLs[ep.Group] = u
	}
	return baseURLs, nil
}

// NewGitLabVendorGatewayForSimulator creates a gateway whose gitlab-rest BaseURL
// points at the connector simulator instead of gitlab.com. simulatorBaseURL is
// the simulator service root (e.g. "http://localhost:8080"); the REST base
// becomes "<root>/gitlab/api/v4".
//
// Auth bypasses the OAuth refresh doer entirely — the simulator has no token
// endpoint, so a static bearer token is set directly (never gitlab.com/oauth/token).
func NewGitLabVendorGatewayForSimulator(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store, cbStore gateway.CircuitBreakerStore, budgetLimiter *ratelimiter.BudgetLimiter, simulatorBaseURL string) (*GitLabVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("glgateway simulator: %w", err)
	}
	endpoints := cfg.GitLabEndpoints()
	baseURLs, err := simulatorGitLabBaseURLs(endpoints, simulatorBaseURL)
	if err != nil {
		return nil, fmt.Errorf("glgateway simulator: %w", err)
	}

	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("glgateway simulator: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	authedDoer := &simulatorBearerDoer{inner: doer, token: token}

	return &GitLabVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    authedDoer,
			Factory: NewGitLabVendorRequest(baseURLs),
		},
		rateLimits:     rateLimits,
		rateLimiter:    rl,
		rlStore:        rlStore,
		baseDoer:       authedDoer,
		budgetLimiter:  budgetLimiter,
		circuitBreaker: newCircuitBreaker(cbStore),
	}, nil
}

func buildRateLimitConfig(endpoints map[string]gateway.APIEndpointConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(endpoints))
	for _, ep := range endpoints {
		rate, period := gateway.ScaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
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
		rate, period := gateway.ScaleRateToIntegerPerPeriod(c.RequestsPerSecond)
		resourceMap[c.Group] = &ratelimiter.RateLimit{Rate: rate, Period: period}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

// WithRateLimits returns a copy of this gateway with rate limits configured.
func (g *GitLabVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.ErrorNoCtx("glgateway: build rate limit config, keeping existing limiter", zap.Error(err))
		return &cpy
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *GitLabVendorGateway) ForGroup(group string) sdk.VendorGateway {
	cpy := *g
	cpy.group = group
	if g.rateLimiter != nil && g.rateLimiter.IsResourceConfigured(group) {
		cpy.Doer = &rateLimitedDoer{
			inner:          g.baseDoer,
			rateLimiter:    g.rateLimiter,
			group:          group,
			budgetLimiter:  g.budgetLimiter,
			circuitBreaker: g.circuitBreaker,
		}
	}
	return &cpy
}

// Execute overrides BaseVendorGateway.Execute to handle GitLab's header-based
// pagination. GitLab returns the next page number in the X-Next-Page response
// header rather than a JSON "next" URL field. This method replicates the
// vendorapi.Handle pipeline, captures the X-Next-Page header from the raw HTTP
// response, and injects a full next-page URL into the proto response before
// returning. For response types that do not implement nextPageSetter (i.e. no
// pagination), it falls back to BaseVendorGateway.Execute.
func (g *GitLabVendorGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	reqType := fmt.Sprintf("%T", req)

	vendorReq, err := g.Factory(req)
	if err != nil {
		logger.Error(ctx, "failed to create vendor request from proto", zap.String("request_type", reqType), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: err,
		}
	}

	// Replicate vendorapi.Handle manually so we can intercept the HTTP response.
	body, err := vendorReq.Marshal()
	if err != nil {
		logger.Error(ctx, "failed to marshal vendor request", zap.String("url", httputil.RedactURL(vendorReq.URL())), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to marshal request: %w", err),
		}
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, vendorReq.HTTPMethod(), vendorReq.URL(), bodyReader)
	if err != nil {
		err = httputil.RedactURLError(err)
		logger.Error(ctx, "failed to create HTTP request", zap.String("method", vendorReq.HTTPMethod()), zap.String("url", httputil.RedactURL(vendorReq.URL())), zap.Error(err))
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to create request: %w", err),
		}
	}

	if auth, ok := vendorReq.(vendorapi.VendorRequestWithAuth); ok {
		httpReq = auth.SetAuth(httpReq)
	}

	logger.Debug(ctx, "executing gitlab vendor API request", zap.String("method", vendorReq.HTTPMethod()), zap.String("url", httputil.RedactURL(vendorReq.URL())))

	httpResp, err := g.Doer.Do(ctx, httpReq)
	if err != nil {
		err = httputil.RedactURLError(err)
		logger.Error(ctx, "gitlab vendor API request failed", zap.String("method", vendorReq.HTTPMethod()), zap.String("url", httputil.RedactURL(vendorReq.URL())), zap.Error(err))
		// Preserve a SyncError produced upstream (the auth-refresh doer's
		// dead-credential/invalid_grant error, or the rate limiter) so its
		// Kind/Terminal survive. Re-wrapping as ErrKindVendorError would shadow a
		// Terminal flag — errors.As returns the OUTER SyncError first — and the
		// scheduler would burn MaxRetry on a dead credential. Mirrors vendorapi.Handle.
		var existing *sdk.SyncError
		if errors.As(err, &existing) {
			return nil, existing
		}
		return nil, &sdk.SyncError{
			Kind:      sdk.ErrKindVendorError,
			Cause:     fmt.Errorf("API request failed: %w", err),
			Retryable: true,
		}
	}
	defer func() {
		if cerr := httpResp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "glgateway: close response body", zap.Error(cerr))
		}
	}()

	// Capture X-Next-Page before reading the body.
	nextPage := httpResp.Header.Get("X-Next-Page")

	respBody, err := vendorapi.ReadResponseBody(httpResp.Body)
	if err != nil {
		logger.Error(ctx, "failed to read response body", zap.String("url", httputil.RedactURL(vendorReq.URL())), zap.Error(err))
		var capErr *sdk.SyncError
		if errors.As(err, &capErr) {
			return nil, capErr
		}
		return nil, &sdk.SyncError{
			Kind:  sdk.ErrKindInternal,
			Cause: fmt.Errorf("failed to read response body: %w", err),
		}
	}

	if httpResp.StatusCode != http.StatusOK {
		logger.Error(ctx, "gitlab vendor API returned non-OK status",
			zap.String("method", vendorReq.HTTPMethod()),
			zap.String("url", httputil.RedactURL(vendorReq.URL())),
			zap.Int("status", httpResp.StatusCode),
		)
		if errHandler, ok := vendorReq.GetResponse().(vendorapi.VendorResponseWithErrorHandling); ok {
			return nil, fmt.Errorf("handling gitlab http error: %w", errHandler.HandleHTTPError(ctx, httpResp.StatusCode, respBody))
		}
		return nil, vendorapi.ClassifyHTTPErrorWithHeaders(httpResp.StatusCode, respBody, httpResp.Header)
	}

	logger.Debug(ctx, "gitlab vendor API request succeeded", zap.String("url", httputil.RedactURL(vendorReq.URL())), zap.Int("status", httpResp.StatusCode))

	protoResp, err := vendorReq.GetResponse().Unmarshal(respBody)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling gitlab response: %w", err)
	}

	// Inject next-page URL from X-Next-Page header into the response.
	// nextPage is a page number (e.g. "2"); construct the full URL by
	// replacing or adding the "page" query parameter.
	if nextPage != "" {
		if setter, ok := protoResp.(nextPageSetter); ok {
			nextURL := buildNextPageURL(vendorReq.URL(), nextPage)
			setter.setNextPageURL(nextURL)
		}
	}

	// Unwrap gateway-internal wrapper types to the underlying proto message
	// so callers (connector operations) can type-assert to the proto type.
	if uw, ok := protoResp.(responseUnwrapper); ok {
		return uw.unwrap(), nil
	}

	return protoResp, nil
}

// buildNextPageURL constructs the next-page URL by replacing (or adding) the
// "page" query parameter in baseURL with the value from X-Next-Page header.
func buildNextPageURL(baseURL, nextPage string) string {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		zap.L().Error("glgateway: failed to parse base URL for pagination, returning as-is", zap.String("url", httputil.RedactURL(baseURL)), zap.Error(err))
		return baseURL
	}
	qv := parsed.Query()
	qv.Set("page", nextPage)
	parsed.RawQuery = qv.Encode()
	return parsed.String()
}

// autoRefreshDoer wraps an HTTPDoer to transparently manage GitLab OAuth
// access tokens. Before each request, it ensures a valid access token is
// available (from Redis cache or by refreshing). On 401, it invalidates the
// cache and retries once.
//
// GitLab always rotates refresh tokens on every refresh grant, so the new
// refresh token is ALWAYS persisted regardless of whether it changed.
type autoRefreshDoer struct {
	inner         vendorapi.HTTPDoer
	cache         auth.TokenCache
	locker        auth.Locker
	tx            store.TxRunner
	configDAO     ConnectorConfigStore
	clientID      string
	clientSecret  string
	orgID         string
	tokenEndpoint string // GitLab OAuth token endpoint URL
}

// NewAutoRefreshDoer creates an HTTPDoer that transparently manages GitLab
// OAuth access token lifecycle. It caches access tokens in Redis, acquires
// distributed locks for refresh, and retries on 401.
func NewAutoRefreshDoer(
	inner vendorapi.HTTPDoer,
	deps auth.RefreshDeps,
	clientID, clientSecret, orgID string,
) vendorapi.HTTPDoer {
	return &autoRefreshDoer{
		inner:         inner,
		cache:         auth.NewEncryptedTokenCache(deps.Cache, deps.Encryptor),
		locker:        deps.Locker,
		tx:            deps.Tx,
		configDAO:     deps.Creds,
		clientID:      clientID,
		clientSecret:  clientSecret,
		orgID:         orgID,
		tokenEndpoint: defaultTokenEndpoint,
	}
}

func (d *autoRefreshDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Get a valid access token (from cache or by refreshing).
	token, err := d.getAccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	// Set auth header.
	req.Header.Set("Authorization", "Bearer "+token)

	// Execute request.
	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("executing gitlab request: %w", err)
	}

	// On 401, invalidate cache and retry once.
	if resp.StatusCode == http.StatusUnauthorized {
		if cerr := resp.Body.Close(); cerr != nil {
			logger.WarnWithCtx(ctx, "glgateway: close response body", zap.Error(cerr))
		}

		_ = d.cache.Delete(ctx, glconfig.AccessTokenKey(d.orgID))

		token, err = d.refreshToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("token refresh after 401: %w", err)
		}

		retryReq := req.Clone(ctx)
		// req.Clone keeps the body the first attempt drained; rewind it from
		// GetBody so a retried POST carries its payload.
		if req.GetBody != nil {
			freshBody, gbErr := req.GetBody()
			if gbErr != nil {
				return nil, fmt.Errorf("gitlab retry: rewind request body: %w", gbErr)
			}
			retryReq.Body = freshBody
		}
		retryReq.Header.Set("Authorization", "Bearer "+token)
		retryResp, err := d.inner.Do(ctx, retryReq)
		if err != nil {
			return nil, fmt.Errorf("retrying gitlab request after 401: %w", err)
		}
		return retryResp, nil
	}

	return resp, nil
}

// getAccessToken returns a valid access token. It checks Redis cache first,
// then refreshes if needed.
func (d *autoRefreshDoer) getAccessToken(ctx context.Context) (string, error) {
	// Check Redis cache.
	token, err := d.cache.Get(ctx, glconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	// redis.Nil is an expected cache miss; any other error means the cache read
	// failed and we fall back to a refresh having lost the cause — surface it.
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		logger.WarnWithCtx(ctx, "glgateway: access token cache read failed, falling back to refresh", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	}

	token, err = d.refreshToken(ctx)
	if err != nil {
		return "", fmt.Errorf("refreshing access token: %w", err)
	}
	return token, nil
}

// refreshToken acquires a distributed lock, refreshes the access token via
// GitLab's OAuth endpoint, caches the new access token, and persists the
// new refresh token to the database.
//
// GitLab ALWAYS rotates refresh tokens on every token exchange. A missing
// refresh token in the response is treated as a fatal error.
func (d *autoRefreshDoer) refreshToken(ctx context.Context) (string, error) {
	lockKey := glconfig.RefreshLockKey(d.orgID)

	// Try to acquire the distributed lock.
	lock, acquired, err := d.locker.TryLock(ctx, lockKey, refreshLockTTL)
	if err != nil {
		return "", fmt.Errorf("acquire refresh lock: %w", err)
	}

	if !acquired {
		// Another instance is refreshing. Wait briefly and check cache.
		time.Sleep(2 * time.Second)
		token, err := d.cache.Get(ctx, glconfig.AccessTokenKey(d.orgID))
		if err == nil && token != "" {
			return token, nil
		}
		if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
			return "", fmt.Errorf("waiting for token refresh by another instance: %w", err)
		}
		return "", fmt.Errorf("timeout waiting for token refresh by another instance")
	}
	defer func() { _ = lock.Unlock(context.WithoutCancel(ctx)) }() // a cancelled run still frees the lock

	// Double-check cache after acquiring lock.
	token, err := d.cache.Get(ctx, glconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		logger.WarnWithCtx(ctx, "glgateway: post-lock access token cache read failed, refreshing", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	}

	// Load refresh token from DB — scoped transaction for RLS.
	var cfg *sdk.ConnectorConfig
	if err := d.tx.RunInScope(ctx, d.orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		var getErr error
		cfg, getErr = d.configDAO.Get(sCtx, d.orgID, connectorID)
		if getErr != nil {
			return fmt.Errorf("getting connector config: %w", getErr)
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("load connector config: %w", err)
	}

	var creds glconfig.GitLabCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if creds.OAuthRefreshToken == "" {
		// No refresh token (disconnected/wiped): fail closed — the org must
		// reconnect. Terminal so the scheduler SkipRetries instead of looping
		// MaxRetry; no attempt can mint a token the org has not reconnected.
		return "", &sdk.SyncError{
			Kind:      sdk.ErrKindAuth,
			Cause:     fmt.Errorf("gitlab: no refresh token for org %s (reconnect required)", d.orgID),
			Retryable: false,
			Terminal:  true,
		}
	}

	// Exchange refresh token for new access token.
	tokenResp, err := d.exchangeRefreshToken(ctx, creds.OAuthRefreshToken)
	if err != nil {
		return "", fmt.Errorf("refresh token exchange: %w", err)
	}

	// GitLab always rotates refresh tokens — a missing refresh token is fatal.
	if tokenResp.RefreshToken == "" {
		return "", fmt.Errorf("gitlab: empty refresh token in response")
	}

	// Always persist the new refresh token (GitLab rotates on every exchange).
	creds.OAuthRefreshToken = tokenResp.RefreshToken
	updatedCreds, err := json.Marshal(creds)
	if err != nil {
		return "", fmt.Errorf("marshal updated credentials: %w", err)
	}
	// Credentials only, and only if no other credential write (a reconnect,
	// a disconnect) landed since the read above: that write stands.
	swapped, swapFence, err := auth.SwapRefreshedCredentials(ctx, d.tx, d.configDAO, d.orgID, connectorID, cfg.CredentialsUpdatedAt, updatedCreds)
	if err != nil {
		return "", fmt.Errorf("persist refresh token: %w", err)
	}
	fence := swapFence
	if !swapped {
		logger.Info(ctx, "glgateway: another credential write landed during the refresh; dropping the rotated token",
			zap.String(logger.OrgID, d.orgID))
		_ = d.cache.Delete(ctx, glconfig.AccessTokenKey(d.orgID))
		return tokenResp.AccessToken, nil
	}

	// Cache the token only while the stored credentials are still the ones it
	// was minted from: a reconnect or disconnect during the refresh wins.
	if kept, err := auth.CacheIfCredentialsUnchanged(ctx, d.tx, d.configDAO, d.cache, d.orgID, connectorID, fence, glconfig.AccessTokenKey(d.orgID), tokenResp.AccessToken, accessTokenTTL); err != nil {
		logger.Error(ctx, "glgateway: failed to cache access token", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	} else if !kept {
		logger.Info(ctx, "glgateway: credentials changed during the refresh; not caching the access token", zap.String(logger.OrgID, d.orgID))
	}

	return tokenResp.AccessToken, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// exchangeRefreshToken calls GitLab's OAuth token endpoint to exchange a
// refresh token for a new access token. Credentials are sent as POST body
// params (not Basic auth) as required by GitLab's OAuth implementation. The
// HTTP exchange, outcome classification, metric emission, and SyncError
// wrapping all live in vendorapi.OAuthRefreshClassifier.Fetch — this function
// only owns the vendor-specific request shape and decode.
//
// 200 OK with malformed JSON is intentionally surfaced as a plain error so a
// vendor-side regression doesn't look like a routine OAuth failure;
// classifier records the outcome as `unknown`.
func (d *autoRefreshDoer) exchangeRefreshToken(ctx context.Context, refreshToken string) (*tokenResponse, error) {
	// 10s matches the pre-classifier http.Client.Timeout for this vendor —
	// gitlab's token endpoint historically responds in well under a second
	// and the tight bound caps worst-case stall on a half-open socket.
	classifier := vendorapi.NewOAuthRefreshClassifier(connectorID, vendorapi.WithTimeout(refreshExchangeTimeout))
	body, start, err := classifier.Fetch(ctx, d.orgID, func(ctx context.Context) (*http.Request, error) {
		data := url.Values{
			"grant_type":    {"refresh_token"},
			"refresh_token": {refreshToken},
			"client_id":     {d.clientID},
			"client_secret": {d.clientSecret},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenEndpoint, strings.NewReader(data.Encode())) //nolint:gosec // tokenEndpoint is vendor-config, not user input
		if err != nil {
			return nil, fmt.Errorf("building token refresh request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetching refreshed token: %w", err)
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		classifier.RecordDecodeFailure(start)
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		// A 200 that minted no access token is not a usable refresh; sending
		// "Bearer " would only earn a 401.
		classifier.RecordDecodeFailure(start)
		return nil, fmt.Errorf("token response carries no access_token")
	}
	classifier.RecordSuccess(start)
	return &tokenResp, nil
}

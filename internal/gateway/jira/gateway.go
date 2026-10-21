// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/simroute"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
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
	rateLimitNamespace = "jira-gateway"

	// accessTokenTTL is the TTL for cached access tokens in Redis.
	// Atlassian access tokens are valid for ~1 hour; we cache for 55 minutes
	// to refresh before expiry.
	accessTokenTTL = 55 * time.Minute

	// refreshExchangeTimeout bounds the token exchange.
	refreshExchangeTimeout = vendorapi.DefaultOAuthRefreshTimeout
	// refreshLockTTL is the TTL for the distributed refresh lock: it outlives
	// the exchange and the writes that follow it under the lock.
	refreshLockTTL = refreshExchangeTimeout + auth.RefreshLockSlack

	// defaultTokenEndpoint is the Atlassian OAuth token endpoint.
	defaultTokenEndpoint = "https://auth.atlassian.com/oauth/token"

	connectorID = "jira"
)

// JiraVendorGateway implements sdk.VendorGateway for Jira Cloud APIs.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
type JiraVendorGateway struct {
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
		return nil, fmt.Errorf("jira request after rate-limit check: %w", err)
	}
	return resp, nil
}

// NewJiraVendorGateway creates a new JiraVendorGateway with the given
// HTTP doer (typically an autoRefreshDoer) and rate limiter store.
// Rate limits and base URLs are read from the gateway config.
func NewJiraVendorGateway(doer vendorapi.HTTPDoer, rlStore ratelimiter.Store) (*JiraVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("jiragateway: %w", err)
	}
	endpoints := cfg.JiraEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("jiragateway: build rate limit config: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	return &JiraVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewJiraVendorRequest(baseURLs),
		},
		rateLimits:  rateLimits,
		rateLimiter: rl,
		rlStore:     rlStore,
		baseDoer:    doer,
	}, nil
}

// simulatorBearerDoer sets a static Bearer token on outgoing requests when one is
// not already present. The real Jira gateway gets its auth from the OAuth
// auto-refresh doer; the simulator has no token endpoint, so simulator traffic
// carries a meaningless static token (the simulator ignores it) and never touches
// the OAuth refresh path.
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

// NewJiraVendorGatewayForSimulator creates a gateway whose jira-rest group BaseURL
// points at the connector simulator instead of api.atlassian.com. simulatorBaseURL
// is the simulator service root (e.g. "http://localhost:8080"); the group's path
// suffix relative to the real Jira base is preserved under a /jira prefix, so the
// REST base becomes "<root>/jira" and every request lands at
// "<root>/jira/ex/jira/{cloudId}/rest/...".
//
// Auth bypasses the OAuth refresh doer entirely — the simulator has no token
// endpoint, so a static bearer token is set directly (never auth.atlassian.com's
// hardcoded token URL).
func NewJiraVendorGatewayForSimulator(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store, simulatorBaseURL string) (*JiraVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("jiragateway simulator: %w", err)
	}
	endpoints := cfg.JiraEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		u, err := simroute.URL(simulatorBaseURL, "/jira", ep.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("jiragateway simulator: %w", err)
		}
		baseURLs[ep.Group] = u
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("jiragateway simulator: build rate limit config: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	authedDoer := &simulatorBearerDoer{inner: doer, token: token}

	return &JiraVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    authedDoer,
			Factory: NewJiraVendorRequest(baseURLs),
		},
		rateLimits:  rateLimits,
		rateLimiter: rl,
		rlStore:     rlStore,
		baseDoer:    authedDoer,
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
func (g *JiraVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.Error(context.Background(), "jiragateway: build rate limit config from framework", zap.Error(err))
		return g
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *JiraVendorGateway) ForGroup(group string) sdk.VendorGateway {
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

// autoRefreshDoer wraps an HTTPDoer to transparently manage Jira OAuth
// access tokens. Before each request, it ensures a valid access token is
// available (from Redis cache or by refreshing). On 401, it invalidates the
// cache and retries once.
//
// Jira uses rotating refresh tokens: each refresh returns a NEW refresh token
// that must be persisted. The old refresh token is invalidated (with a 10-minute
// grace window for concurrent requests).
type autoRefreshDoer struct {
	inner         vendorapi.HTTPDoer
	cache         auth.TokenCache
	locker        auth.Locker
	tx            store.TxRunner
	configDAO     ConnectorConfigStore
	clientID      string
	clientSecret  string
	orgID         string
	tokenEndpoint string
}

// NewAutoRefreshDoer creates an HTTPDoer that transparently manages Jira
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
	token, err := d.getAccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("jira request: %w", err)
	}

	// On 401, invalidate cache and retry once.
	if resp.StatusCode == http.StatusUnauthorized {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.WarnWithCtx(ctx, "jiragateway: closing 401 response body failed", zap.String(logger.OrgID, d.orgID), zap.Error(closeErr))
		}

		_ = d.cache.Delete(ctx, jiraconfig.AccessTokenKey(d.orgID))

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
				return nil, fmt.Errorf("jira retry: rewind request body: %w", gbErr)
			}
			retryReq.Body = freshBody
		}
		retryReq.Header.Set("Authorization", "Bearer "+token)
		retryResp, retryErr := d.inner.Do(ctx, retryReq)
		if retryErr != nil {
			return nil, fmt.Errorf("jira request retry after token refresh: %w", retryErr)
		}
		return retryResp, nil
	}

	return resp, nil
}

func (d *autoRefreshDoer) getAccessToken(ctx context.Context) (string, error) {
	token, err := d.cache.Get(ctx, jiraconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	// redis.Nil is an expected cache miss; any other error means the cache read
	// degraded (we still fall through to a refresh, just without the cache hit).
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		logger.WarnWithCtx(ctx, "jiragateway: access token cache read failed, falling back to refresh", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	}

	token, err = d.refreshToken(ctx)
	if err != nil {
		return "", fmt.Errorf("refreshing access token: %w", err)
	}
	return token, nil
}

func (d *autoRefreshDoer) refreshToken(ctx context.Context) (string, error) {
	lockKey := jiraconfig.RefreshLockKey(d.orgID)

	lock, acquired, err := d.locker.TryLock(ctx, lockKey, refreshLockTTL)
	if err != nil {
		return "", fmt.Errorf("acquire refresh lock: %w", err)
	}

	if !acquired {
		// Another instance is refreshing. Wait briefly for it to complete, respecting
		// context cancellation so we don't block indefinitely if the caller times out.
		// A fixed 2-second wait is acceptable here: the refresh exchange is fast and
		// we only land here when multiple goroutines race to refresh the same token.
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for concurrent token refresh: %w", ctx.Err())
		}
		token, err := d.cache.Get(ctx, jiraconfig.AccessTokenKey(d.orgID))
		if err == nil && token != "" {
			return token, nil
		}
		// redis.Nil means the other instance hasn't written the token yet; any
		// other error is a degraded cache read on the wait path — surface it.
		if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
			logger.WarnWithCtx(ctx, "jiragateway: cache read failed while waiting for refresh by another instance", zap.String(logger.OrgID, d.orgID), zap.Error(err))
		}
		return "", fmt.Errorf("timeout waiting for token refresh by another instance")
	}
	// Released under a context that outlives the request's: a refresh whose
	// caller cancels mid-exchange still frees the lock, rather than holding
	// every other refresh for the org until the lock's TTL.
	defer func() { _ = lock.Unlock(context.WithoutCancel(ctx)) }()

	// Double-check cache after acquiring lock.
	token, err := d.cache.Get(ctx, jiraconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	// redis.Nil is the expected miss that justifies the DB-backed refresh below;
	// any other error is a degraded cache read worth surfacing.
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		logger.WarnWithCtx(ctx, "jiragateway: post-lock cache read failed, proceeding to DB refresh", zap.String(logger.OrgID, d.orgID), zap.Error(err))
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

	var creds jiraconfig.JiraCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if creds.OAuthRefreshToken == "" {
		// No refresh token (disconnected/wiped): fail closed — the org must
		// reconnect. Terminal so the scheduler SkipRetries instead of looping
		// MaxRetry; no attempt can mint a token the org has not reconnected.
		return "", &sdk.SyncError{
			Kind:      sdk.ErrKindAuth,
			Cause:     fmt.Errorf("jira: no refresh token for org %s (reconnect required)", d.orgID),
			Retryable: false,
			Terminal:  true,
		}
	}

	// Exchange refresh token for new access token.
	tokenResp, err := d.exchangeRefreshToken(ctx, creds.OAuthRefreshToken)
	if err != nil {
		return "", fmt.Errorf("refresh token exchange: %w", err)
	}

	// The fence the cached token is checked against: the read above, or the
	// swap's own write when a rotated token is stored.
	fence := cfg.CredentialsUpdatedAt
	// Jira uses rotating refresh tokens — always persist the new one.
	if tokenResp.RefreshToken != "" {
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
		if !swapped {
			logger.Info(ctx, "jiragateway: another credential write landed during the refresh; dropping the rotated token",
				zap.String(logger.OrgID, d.orgID))
			_ = d.cache.Delete(ctx, jiraconfig.AccessTokenKey(d.orgID))
			return tokenResp.AccessToken, nil
		}
		fence = swapFence
	}

	// Cache the token only while the stored credentials are still the ones it
	// was minted from: a reconnect or disconnect during the refresh wins.
	if kept, err := auth.CacheIfCredentialsUnchanged(ctx, d.tx, d.configDAO, d.cache, d.orgID, connectorID, fence, jiraconfig.AccessTokenKey(d.orgID), tokenResp.AccessToken, accessTokenTTL); err != nil {
		logger.Error(ctx, "jiragateway: failed to cache access token", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	} else if !kept {
		logger.Info(ctx, "jiragateway: credentials changed during the refresh; not caching the access token", zap.String(logger.OrgID, d.orgID))
	}

	return tokenResp.AccessToken, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// exchangeRefreshToken calls Jira's OAuth token endpoint to exchange a
// refresh token for a new access token. The HTTP exchange, outcome
// classification, metric emission, and SyncError wrapping all live in
// vendorapi.OAuthRefreshClassifier.Fetch — this function only owns the
// vendor-specific request shape (client credentials in body) and the
// vendor-specific decode of the response.
//
// 200 OK with malformed JSON is intentionally surfaced as a plain error so a
// vendor-side regression doesn't look like a routine OAuth failure;
// classifier records the outcome as `unknown`.
func (d *autoRefreshDoer) exchangeRefreshToken(ctx context.Context, refreshToken string) (*tokenResponse, error) {
	classifier := vendorapi.NewOAuthRefreshClassifier(connectorID, vendorapi.WithTimeout(refreshExchangeTimeout))
	body, start, err := classifier.Fetch(ctx, d.orgID, func(ctx context.Context) (*http.Request, error) {
		data := url.Values{
			"grant_type":    {"refresh_token"},
			"client_id":     {d.clientID},
			"client_secret": {d.clientSecret},
			"refresh_token": {refreshToken},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenEndpoint, strings.NewReader(data.Encode())) //nolint:gosec // tokenEndpoint is vendor-config, not user input
		if err != nil {
			return nil, fmt.Errorf("building token-exchange request: %w", err)
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

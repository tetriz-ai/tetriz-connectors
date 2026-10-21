// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineargateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	// defaultTokenEndpoint is Linear's OAuth token endpoint.
	defaultTokenEndpoint = "https://api.linear.app/oauth/token"

	// accessTokenTTL caps how long a refreshed access token is served from Redis.
	// Linear user-actor access tokens are valid ~24h; caching for 23h refreshes
	// before expiry. The 401-retry path is the backstop for early invalidation.
	accessTokenTTL = 23 * time.Hour

	// refreshExchangeTimeout bounds the token exchange.
	refreshExchangeTimeout = vendorapi.DefaultOAuthRefreshTimeout
	// refreshLockTTL bounds the distributed refresh lock so concurrent ops for one
	// org don't race the refresh exchange (Linear rotates the refresh token, so a
	// racing refresh would invalidate the other's token). It outlives the
	// exchange and the writes that follow it under the lock.
	refreshLockTTL = refreshExchangeTimeout + auth.RefreshLockSlack

	connectorID = "linear"
)

// ConnectorConfigStore is the part of the runner store a gateway needs, so a
// gateway does not import runner/store.
type ConnectorConfigStore interface {
	Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error)
	Save(ctx context.Context, cfg *sdk.ConnectorConfig) error
	CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time, credentials json.RawMessage) (bool, error)
}

// autoRefreshDoer authenticates Linear sync requests with the per-org user-actor
// OAuth token and transparently refreshes it.
//
// Tenant isolation: the token is workspace-scoped to the org that authorized it,
// so each org's sync reads only its own Linear workspace — unlike the app-actor
// client-credentials grant it replaces, whose access was fixed by the OAuth app
// and identical across every org (a cross-tenant leak).
//
// Lifecycle: Linear user-actor access tokens expire ~24h and are refreshed via
// the rotating refresh token stored at connect (connector_configs.credentials).
// The doer serves the access token from Redis (encrypted at rest), refreshes on a cache miss, and re-refreshes + retries once on a 401. The
// refresh exchange holds a distributed lock so concurrent ops don't race the
// refresh-token rotation. If the refresh token itself is rejected (user revoked
// the authorization), the auth error propagates and the org must reconnect.
type autoRefreshDoer struct {
	inner         vendorapi.HTTPDoer
	cache         auth.TokenCache
	locker        auth.Locker
	tx            store.TxRunner
	enc           cryptutil.Encryptor
	configDAO     ConnectorConfigStore
	clientID      string
	clientSecret  string
	orgID         string
	tokenEndpoint string
}

// NewAutoRefreshDoer builds the Linear token-managing doer. clientID/clientSecret
// are the Linear OAuth app credentials (used for the refresh exchange); orgID
// scopes the per-org token cache and the refresh-token read/write; enc encrypts
// the cached access token at rest in Redis.
func NewAutoRefreshDoer(
	inner vendorapi.HTTPDoer,
	deps auth.RefreshDeps,
	enc cryptutil.Encryptor,
	clientID, clientSecret, orgID string,
) vendorapi.HTTPDoer {
	return &autoRefreshDoer{
		inner:         inner,
		cache:         deps.Cache,
		locker:        deps.Locker,
		tx:            deps.Tx,
		enc:           enc,
		configDAO:     deps.Creds,
		clientID:      clientID,
		clientSecret:  clientSecret,
		orgID:         orgID,
		tokenEndpoint: defaultTokenEndpoint,
	}
}

func (d *autoRefreshDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	token, err := d.ensureToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("ensure linear access token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := d.inner.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("linear request: %w", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	// 401: the cached token is stale/invalid. Discard the body, refresh, retry once.
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.WarnWithCtx(ctx, "lineargateway: closing 401 body failed", zap.String(logger.OrgID, d.orgID), zap.Error(closeErr))
	}
	_ = d.cache.Delete(ctx, linearconfig.AccessTokenKey(d.orgID))

	newToken, refreshErr := d.refreshToken(ctx)
	if refreshErr != nil {
		return nil, fmt.Errorf("linear token refresh after 401: %w", refreshErr)
	}
	logger.Info(ctx, "linear: token rejected (401), refreshed", zap.String(logger.OrgID, d.orgID))
	retryReq := req.Clone(ctx)
	// req.Clone copies the (now-drained) Body reference; the first Do already
	// consumed it. Rewind from GetBody so the retried POST carries the original
	// payload, not an empty body.
	if req.GetBody != nil {
		freshBody, gbErr := req.GetBody()
		if gbErr != nil {
			return nil, fmt.Errorf("linear retry: rewind request body: %w", gbErr)
		}
		retryReq.Body = freshBody
	}
	retryReq.Header.Set("Authorization", "Bearer "+newToken)
	retryResp, retryErr := d.inner.Do(ctx, retryReq)
	if retryErr != nil {
		return nil, fmt.Errorf("linear request retry after token refresh: %w", retryErr)
	}
	return retryResp, nil
}

// ensureToken returns a cached token or refreshes one on a cache miss.
func (d *autoRefreshDoer) ensureToken(ctx context.Context) (string, error) {
	if token, ok := d.cachedToken(ctx); ok {
		return token, nil
	}
	token, err := d.refreshToken(ctx)
	if err != nil {
		return "", fmt.Errorf("refreshing linear access token: %w", err)
	}
	return token, nil
}

// cachedToken returns the decrypted access token from Redis, or ("", false) on a
// cache miss, a read error, or an entry that is not an envelope under this
// key — all treated as "no usable token" so the caller refreshes. The read goes
// through auth.NewEncryptedTokenCache, as the other gateways' do, so a
// plaintext entry is never used as the bearer token.
func (d *autoRefreshDoer) cachedToken(ctx context.Context) (string, bool) {
	// Local plaintext mode (DisableCredentialsEncryption, nil encryptor): skip the
	// cache so no plaintext token is ever read/written; sync refreshes each run.
	if d.enc == nil {
		return "", false
	}
	plain, err := auth.NewEncryptedTokenCache(d.cache, d.enc).Get(ctx, linearconfig.AccessTokenKey(d.orgID))
	if err != nil {
		if !errors.Is(err, auth.ErrCacheMiss) {
			logger.WarnWithCtx(ctx, "lineargateway: access token cache read failed", zap.String(logger.OrgID, d.orgID), zap.Error(err))
		}
		return "", false
	}
	if plain == "" {
		return "", false
	}
	return plain, true
}

// refreshToken exchanges the stored refresh token for a fresh access token under a
// distributed lock, persists the rotated tokens, and caches the access token.
// Concurrent callers that lose the lock wait briefly for the winner's cached token
// rather than racing the refresh-token rotation.
func (d *autoRefreshDoer) refreshToken(ctx context.Context) (string, error) {
	lockKey := linearconfig.RefreshLockKey(d.orgID)
	lock, acquired, lockErr := d.locker.TryLock(ctx, lockKey, refreshLockTTL)
	if lockErr != nil {
		// Linear rotates the refresh token, so a lock-less stampede would have
		// concurrent ops consume the same refresh token and invalidate each
		// other. Fail the run (retryable) rather than race the rotation — the
		// orchestrator retries with backoff once Redis recovers.
		return "", fmt.Errorf("acquire refresh lock: %w", lockErr)
	}
	if !acquired {
		// Another instance holds the lock and is refreshing. Wait briefly for its
		// cached token; if it is still not visible, fail the run (retryable)
		// rather than refresh with a refresh token the winner may have just
		// rotated away (which would 401 non-retryably).
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for concurrent token refresh: %w", ctx.Err())
		}
		if token, ok := d.cachedToken(ctx); ok {
			return token, nil
		}
		return "", fmt.Errorf("timeout waiting for token refresh by another instance")
	}
	// Release with a cancel-detached context so cleanup survives a request timeout
	// (a leaked lock would otherwise block other ops until its TTL).
	defer func() { _ = lock.Unlock(context.WithoutCancel(ctx)) }()

	// Double-check the cache now that we hold the lock (a racer may have refreshed
	// between our cache miss and acquiring the lock).
	if token, ok := d.cachedToken(ctx); ok {
		return token, nil
	}

	// Load the refresh token from the per-org credentials (RLS-scoped read).
	var cfg *sdk.ConnectorConfig
	if err := d.tx.RunInScope(ctx, d.orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		got, getErr := d.configDAO.Get(sCtx, d.orgID, connectorID)
		if getErr != nil {
			return fmt.Errorf("getting connector config: %w", getErr)
		}
		cfg = got
		return nil
	}); err != nil {
		return "", fmt.Errorf("load connector config: %w", err)
	}

	var creds linearconfig.LinearCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if creds.RefreshToken == "" {
		// No refresh token (disconnected/wiped, or a pre-refresh-migration token):
		// fail closed — the org must reconnect. Terminal so the scheduler
		// SkipRetries instead of looping MaxRetry: no attempt can mint a token
		// the org has not reconnected.
		return "", &sdk.SyncError{
			Kind:      sdk.ErrKindAuth,
			Cause:     fmt.Errorf("linear: no refresh token for org %s (reconnect required)", d.orgID),
			Retryable: false,
			Terminal:  true,
		}
	}

	tokenResp, err := d.exchangeRefreshToken(ctx, creds.RefreshToken)
	if err != nil {
		return "", fmt.Errorf("refresh token exchange: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("linear token endpoint: empty access_token in refresh response")
	}

	// Persist the rotated tokens so the next refresh uses the current refresh
	// token and disconnect revokes the live ones. Linear rotates the refresh
	// token, so a stale stored one would fail the next refresh.
	creds.AccessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" {
		creds.RefreshToken = tokenResp.RefreshToken
	}
	updated, mErr := json.Marshal(creds) //nolint:gosec // G117: credentials are persisted encrypted at rest by the DAO; marshaling the token is intentional
	if mErr != nil {
		return "", fmt.Errorf("marshal updated credentials: %w", mErr)
	}
	// Persist with a cancel-detached, bounded context: Linear has already rotated
	// the refresh token, so if a request-ctx cancellation (e.g. job timeout) aborted
	// the write here, the DB would keep the now-dead refresh token and every future
	// refresh would fail non-retryably until the org reconnects.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auth.PersistTimeout)
	defer cancel()
	swapped := false
	var fence time.Time // the fence the swap wrote, read back in the same scope
	if err := d.tx.RunInScope(saveCtx, d.orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		// Re-read inside the persist scope: if the org disconnected mid-refresh the
		// row is soft-deleted (ErrRecordNotFound) or its credentials were wiped to
		// "{}" — skip the write so it can't resurrect a disconnected
		// connection with fresh tokens (the orphaned rotation is irrelevant for a
		// connection that no longer exists).
		current, getErr := d.configDAO.Get(sCtx, d.orgID, connectorID)
		if errors.Is(getErr, gorm.ErrRecordNotFound) {
			logger.Info(sCtx, "linear: skipping token persist — connection disconnected mid-refresh", zap.String(logger.OrgID, d.orgID))
			return nil
		}
		if getErr != nil {
			return fmt.Errorf("re-reading connector config: %w", getErr)
		}
		var curCreds linearconfig.LinearCredentials
		if uErr := json.Unmarshal(current.Credentials, &curCreds); uErr != nil {
			return fmt.Errorf("parse current credentials before persist: %w", uErr)
		}
		if curCreds.AccessToken == "" {
			logger.Info(sCtx, "linear: skipping token persist — credentials wiped (disconnect in progress)", zap.String(logger.OrgID, d.orgID))
			return nil
		}
		// Credentials only, and only if no other credential write (a reconnect)
		// landed since the read before the exchange: that write stands.
		var casErr error
		swapped, casErr = d.configDAO.CompareAndSwapCredentials(sCtx, d.orgID, connectorID, cfg.CredentialsUpdatedAt, updated)
		if casErr != nil {
			return fmt.Errorf("swapping rotated credentials: %w", casErr)
		}
		if !swapped {
			logger.Info(sCtx, "lineargateway: another credential write landed during the refresh; dropping the rotated token", zap.String(logger.OrgID, d.orgID))
			return nil
		}
		written, getErr := d.configDAO.Get(sCtx, d.orgID, connectorID)
		if getErr != nil {
			return fmt.Errorf("re-reading swapped credentials: %w", getErr)
		}
		fence = written.CredentialsUpdatedAt
		return nil
	}); err != nil {
		return "", fmt.Errorf("persist rotated tokens: %w", err)
	}
	if !swapped {
		// The stored grant is another writer's (or the connection is gone): do
		// not cache the exchanged token under it. It serves this run only.
		_ = d.cache.Delete(ctx, linearconfig.AccessTokenKey(d.orgID))
		return tokenResp.AccessToken, nil
	}

	// Cache the access token encrypted at rest. On an encrypt failure, skip the
	// cache rather than store plaintext — the token is still returned
	// so sync proceeds. A nil encryptor is local plaintext mode: skip caching.
	if d.enc != nil {
		if ciphertext, encErr := d.enc.Encrypt([]byte(tokenResp.AccessToken)); encErr != nil {
			logger.Error(ctx, "lineargateway: failed to encrypt access token for cache; skipping cache", zap.String(logger.OrgID, d.orgID), zap.Error(encErr))
		} else if kept, setErr := auth.CacheIfCredentialsUnchanged(ctx, d.tx, d.configDAO, d.cache, d.orgID, connectorID, fence, linearconfig.AccessTokenKey(d.orgID), string(ciphertext), accessTokenTTL); setErr != nil {
			logger.Error(ctx, "lineargateway: failed to cache refreshed access token", zap.String(logger.OrgID, d.orgID), zap.Error(setErr))
		} else if !kept {
			// Cached only while the stored credentials are the ones it was minted from.
			logger.Info(ctx, "lineargateway: credentials changed during the refresh; not caching the access token", zap.String(logger.OrgID, d.orgID))
		}
	}
	return tokenResp.AccessToken, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// exchangeRefreshToken calls Linear's OAuth token endpoint to exchange the
// refresh token for a fresh access token. The HTTP exchange, outcome
// classification (invalid_grant → ErrKindAuth), metric emission, and SyncError
// wrapping live in vendorapi.OAuthRefreshClassifier.Fetch; this function owns the
// vendor-specific request shape and response decode.
func (d *autoRefreshDoer) exchangeRefreshToken(ctx context.Context, refreshToken string) (*tokenResponse, error) {
	classifier := vendorapi.NewOAuthRefreshClassifier(connectorID, vendorapi.WithTimeout(refreshExchangeTimeout))
	body, start, err := classifier.Fetch(ctx, d.orgID, func(ctx context.Context) (*http.Request, error) {
		data := url.Values{
			"grant_type":    {"refresh_token"},
			"client_id":     {d.clientID},
			"client_secret": {d.clientSecret},
			"refresh_token": {refreshToken},
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenEndpoint, strings.NewReader(data.Encode())) //nolint:gosec // tokenEndpoint is vendor config, not user input
		if reqErr != nil {
			return nil, fmt.Errorf("building token-exchange request: %w", reqErr)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("fetching refreshed token: %w", err)
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		classifier.RecordDecodeFailure(start)
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	classifier.RecordSuccess(start)
	return &tr, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/simroute"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
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
	// localLimiterRetryAfter is the cooldown surfaced to asynq when the local
	// Redis limiter rejects an acquire (separate from a vendor 429 — the
	// vendor never sees the request). 30s comfortably outlasts the sub-minute
	// window the limiter enforces, and is short enough that a healthy bucket
	// recovers within one asynq retry cycle. Without it, asynq retries the
	// same (org, group) within milliseconds and starves the bucket further.
	localLimiterRetryAfter = 30 * time.Second

	// Jitter window for the local limiter's fixed retry cooldown, sized to
	// the wait it spreads (the circuit breaker's are in gateway).
	localLimiterRetryJitter = 15 * time.Second

	// localLimiterPacingJitter spreads in-run pacing waits (SyncError.LocalWait).
	// It is small because that wait is sized to one token of the bucket, not to
	// an asynq retry cycle: a wide spread would only hold a run asleep after
	// the bucket can admit it again.
	localLimiterPacingJitter = time.Second

	rateLimitNamespace = "bb-gateway"

	// accessTokenTTL is the TTL for cached access tokens in Redis.
	// Bitbucket access tokens are valid for 2 hours; we cache for 115 minutes
	// to refresh before expiry.
	accessTokenTTL = 115 * time.Minute

	// refreshExchangeTimeout bounds the token exchange.
	refreshExchangeTimeout = vendorapi.DefaultOAuthRefreshTimeout
	// refreshLockTTL is the TTL for the distributed refresh lock: it outlives
	// the exchange and the writes that follow it under the lock.
	refreshLockTTL = refreshExchangeTimeout + auth.RefreshLockSlack

	// defaultTokenEndpoint is the Bitbucket OAuth token endpoint.
	defaultTokenEndpoint = "https://bitbucket.org/site/oauth2/access_token"

	connectorID = "bitbucket"
)

// BitbucketVendorGateway implements sdk.VendorGateway for Bitbucket APIs.
// It embeds vendorapi.BaseVendorGateway which provides Do, DoJSON, and Execute.
// Rate limiting is applied transparently by wrapping the HTTPDoer in ForGroup.
type BitbucketVendorGateway struct {
	vendorapi.BaseVendorGateway
	rateLimits     []sdk.RateLimitConfig
	rateLimiter    ratelimiter.RateLimiter
	rlCfg          *ratelimiter.RateLimitConfig
	rlStore        ratelimiter.Store
	group          string
	baseDoer       vendorapi.HTTPDoer
	circuitBreaker *circuitBreaker
	budgetLimiter  *ratelimiter.BudgetLimiter
}

// rateLimitedDoer wraps an HTTPDoer to enforce rate limiting via the
// Redis-backed ratelimiter before each request. Optionally tripped by a
// circuit breaker (ADR-0004 §5) so that orgs already inside a vendor
// punishment window fail-fast instead of stacking more 429s on top.
type rateLimitedDoer struct {
	inner       vendorapi.HTTPDoer
	rateLimiter ratelimiter.RateLimiter
	// rlCfg is read only to size the retry delay to the bucket that actually
	// rejected — see retryAfterForResource. Nil is tolerated (the fixed floor).
	rlCfg          *ratelimiter.RateLimitConfig
	group          string
	circuitBreaker *circuitBreaker
	budgetLimiter  *ratelimiter.BudgetLimiter
}

// acquireResource picks the limiter resource this call draws from: the operation's
// own reserved slice if one is configured, otherwise the remainder bucket shared by
// unnamed operations, otherwise the whole group.
//
// Resolved per call rather than cached on the doer because the doer is built once
// per group by ForGroup and reused across operations — binding a resource at
// construction would pin every call to whichever operation happened to be first.
//
// The three-step fallback is what keeps this safe to deploy dark: with no shares
// configured neither lookup matches and every call lands on d.group, exactly as
// before.
func (d *rateLimitedDoer) acquireResource(operation string) string {
	if operation != "" {
		if r := operationResource(d.group, operation); d.rateLimiter.IsResourceConfigured(r) {
			return r
		}
	}
	if r := operationResource(d.group, operationShareRemainder); d.rateLimiter.IsResourceConfigured(r) {
		return r
	}
	return d.group
}

// retryAfterForResource sizes the asynq backoff to the bucket that actually
// rejected, not to the group.
//
// The fixed floor was calibrated against the whole group's rate — at Bitbucket's
// observed cap a token arrives every ~1.8s, so 30s comfortably outlasts a refill. A
// per-operation slice can be far smaller: the smallest representable share is one
// token per window, and on a per-minute window that is 60s — twice the floor.
// Returning the floor there would have asynq retry the same exhausted slice before
// it could possibly succeed, burning the retry budget and re-rejecting each time.
//
// So: wait at least long enough for one token of the SELECTED bucket, and never
// less than the existing floor. With no shares configured the resource is the group
// and this returns the floor, so the behaviour is unchanged.
func (d *rateLimitedDoer) retryAfterForResource(resource string) time.Duration {
	perToken, ok := d.perTokenForResource(resource)
	if !ok || perToken < localLimiterRetryAfter {
		return localLimiterRetryAfter
	}
	return perToken
}

// pacingWaitForResource is the in-run wait (SyncError.LocalWait) after a local
// rejection: one token of the selected bucket plus a little jitter, so a run
// that paces itself retries about when the bucket can admit it, not after the
// asynq floor. Zero when the bucket cannot be sized, which leaves pacing on
// RetryAfterSeconds.
func (d *rateLimitedDoer) pacingWaitForResource(resource string) time.Duration {
	perToken, ok := d.perTokenForResource(resource)
	if !ok {
		return 0
	}
	//nolint:gosec // backoff spreader, not a security primitive
	return perToken + time.Duration(rand.Int63n(int64(localLimiterPacingJitter)))
}

// perTokenForResource is how often the selected bucket earns one token, rounded
// up to whole seconds. ok is false when it cannot be sized.
func (d *rateLimitedDoer) perTokenForResource(resource string) (time.Duration, bool) {
	if d.rlCfg == nil {
		return 0, false
	}
	// Presence is checked FIRST because the getters below do not tolerate a missing
	// resource — they dereference the map entry and panic on nil rather than
	// returning a zero value. Guarding on the returned rate alone is too late, and
	// this runs on the rejection path, where a panic would turn a throttle into a
	// crash.
	if !d.rlCfg.IsResourcePresent(resource) {
		return 0, false
	}
	rate := d.rlCfg.GetMaxRequestCount(resource)
	period := d.rlCfg.GetPeriod(resource)
	if rate <= 0 || period <= 0 {
		// Configured but degenerate — do not derive a delay from a zero rate,
		// which would divide by zero or produce an absurd wait.
		return 0, false
	}
	return time.Duration(math.Ceil(period.Seconds()/float64(rate))) * time.Second, true
}

func (d *rateLimitedDoer) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// Per-org scope so one workspace's burst doesn't drain other orgs' budget.
	// Bitbucket's hourly cap is per user-token (≈per-connector-config), and our
	// connector-config is org-scoped, so per-org bucketing matches the vendor's
	// actual accounting (ADR-0004 §3).
	//
	// limiterKey is the Redis bucket identifier (composite). orgID is the bare
	// org identifier — passed separately to the circuit breaker so its
	// Prometheus label receives a clean value, not "group:org-uuid".
	//
	// Fallback to bare group when ctx has no org_id (test paths, scheduler
	// bootstrap). The Debug log makes the fallback observable so a future
	// refactor that drops the orchestrator-side reqctx.WithOrgID won't
	// silently collapse all orgs into one bucket.
	orgID := reqctx.OrgIDFromContext(ctx)
	limiterKey := d.group
	if orgID != "" {
		limiterKey = d.group + ":" + orgID
	} else {
		logger.Debug(ctx, "rate limiter falling back to shared (orgless) bucket",
			zap.String("group", d.group),
		)
	}

	// Attributed to the operation that caused the call, not just the org: the budget
	// is one bucket shared by every operation, so a rejection counted only per-org
	// cannot say which operation drained it. The orchestrator stamps the operation
	// into ctx; a caller outside that path is labelled unattributed rather than "".
	//
	// Resolved before the circuit-breaker gate below, not after: a call turned away
	// by an open circuit is still demand, and during a 429 storm — the regime the
	// share sizing most needs to describe — it is where the retries pile up.
	operation := reqctx.OperationIDFromContext(ctx)

	// Circuit breaker fail-fast (ADR-0004 §5). If we've recently been
	// 429'd repeatedly for this (group, org), short-circuit instead of
	// burning another rate-limit token + asynq budget on a call that's almost
	// certain to fail. The remaining cooldown is surfaced as
	// RetryAfterSeconds so asynq backs off the full window instead of hot-
	// retrying inside it; without that an org cycles circuit-open → retry →
	// circuit-open every few seconds.
	//
	// Half-open gating runs in the same step, BEFORE the limiter, so a caller
	// that is only going to fail fast never spends a limiter slot: under a
	// retry herd those non-probes would otherwise burn the whole per-minute
	// allowance without a single vendor request. The winner's slot is handed
	// back below if the limiter then rejects it, so the interval's one probe is
	// never spent on a request that did not leave the process.
	probing, gateErr := d.circuitBreaker.Gate(ctx, d.group, orgID)
	if gateErr != nil {
		if errors.Is(gateErr, gateway.ErrCircuitOpen) {
			// Counted, though no token is consumed: the call was still attempted, and
			// omitting it would make the demand series go quiet exactly when the org
			// is most starved — the breaker is where retries accumulate in a storm.
			recordAcquire(d.group, orgID, operation, acquireOutcomeCircuitOpen)
		}
		return nil, gateErr
	}

	// Draw from this operation's reserved slice of the group budget when one is
	// configured, so a heavy operation cannot consume the whole org's allowance and
	// starve the rest. Falls back to the shared group bucket when no shares exist,
	// which is every group's behaviour until an operator configures them.
	resource := d.acquireResource(operation)

	allowed, err := d.rateLimiter.Acquire(ctx, resource, limiterKey)
	if err != nil {
		if probing {
			d.circuitBreaker.ReleaseProbe(ctx, d.group, orgID)
		}
		// An acquire failure is the limiter breaking, not the budget binding — kept
		// distinct so a Redis outage cannot masquerade as ordinary throttling.
		recordAcquire(d.group, orgID, operation, acquireOutcomeError)
		return nil, &sdk.SyncError{
			Kind: sdk.ErrKindRateLimit,
			// "rate limit acquire" is the other phrase operators' log searches use
			// to tell our limiter from the vendor; keep it verbatim.
			Cause:     fmt.Errorf("rate limit acquire failed for group %s (slice %s): %w", d.group, resource, err),
			Retryable: true,
		}
	}
	if !allowed {
		if probing {
			d.circuitBreaker.ReleaseProbe(ctx, d.group, orgID)
		}
		recordAcquire(d.group, orgID, operation, acquireOutcomeLimited)
		// Local Redis limiter is exhausted for this window. Surface a non-zero
		// RetryAfterSeconds so asynq backs off — without it, asynq
		// fast-retries the same (org, group) tuple and keeps the window
		// exhausted. Jittered because
		// every caller rejected by the same window would otherwise return
		// together and re-exhaust it on the next tick.
		return nil, &sdk.SyncError{
			Kind: sdk.ErrKindRateLimit,
			// Keeps the "rate limit exceeded for group X" phrase VERBATIM and appends
			// the exhausted slice. Operators' log searches match this phrase to
			// tell the local bucket from a vendor 429, so keep it verbatim.
			//
			// ErrLocalRateLimit stays wrapped so a caller can discriminate without
			// matching prose: a tree walk waits out OUR bucket but must still abort on
			// a vendor 429 or an open circuit, and errors.Is is the only check that
			// survives a reword of the message above.
			Cause:     fmt.Errorf("rate limit exceeded for group %s (slice %s): %w", d.group, resource, sdk.ErrLocalRateLimit),
			Retryable: true,
			// Magnitude and phase are separate concerns and both are needed here.
			// retryAfterForResource sizes the wait to the bucket that actually rejected —
			// a small per-operation share refills far slower than the group, so the group
			// floor would burn several doomed retries. The jitter then decorrelates
			// callers rejected by the SAME window, which would otherwise wake together and
			// re-exhaust it. Taking either alone reintroduces the other's failure.
			RetryAfterSeconds: jitteredRetryAfterSeconds(
				d.retryAfterForResource(resource),
				localLimiterRetryJitter,
			),
			// The floor above is for asynq's retry of a failed task. A run pacing
			// itself in place waits only until the bucket can admit again.
			LocalWait: d.pacingWaitForResource(resource),
		}
	}
	// A token was consumed. Recorded before the call rather than after it, because
	// the budget is spent whether or not the vendor request then succeeds — sizing a
	// share against only successful calls would under-count real demand.
	recordAcquire(d.group, orgID, operation, acquireOutcomeGranted)

	resp, doErr := d.inner.Do(ctx, req)
	// Update the budget snapshot + classify 429s using Bitbucket's
	// X-RateLimit-* / Retry-After headers (ADR-0004 §4). When the response
	// carries a 429 with Retry-After, surface that to the orchestrator via
	// SyncError.RetryAfterSeconds so the asynq retry honors the vendor's
	// signal instead of fixed backoff.
	if doErr == nil && resp != nil {
		d.observeResponse(ctx, resp)
		// A 429 counts toward tripping the breaker; a half-open probe answered
		// below 500 closes it for every caller (gateway.CircuitBreaker.Observe).
		if n := d.circuitBreaker.Observe(ctx, d.group, orgID, resp.StatusCode, probing); n > 0 {
			logger.Info(ctx, "bitbucket 429 recorded",
				zap.String("group", d.group),
				zap.String(logger.OrgID, orgID),
				zap.Int64("failures_in_window", n),
			)
		}
		// No reset on success. The fail counter has a 5-minute TTL — the
		// "window" semantic comes from the TTL, not from interleaved 2xxs.
		// Resetting on 2xx is wrong for Bitbucket's burst pattern, where a
		// single fetch_prs run mixes 200s (list/commits) with 429s
		// (diffstat). Each successful interleaved call would clear the
		// counter and the breaker would never trip against a sustained 429
		// storm. Defer to TTL — if the org goes a full failureWindow
		// without any 429, the key expires and the next 429 starts fresh.
	}
	if doErr != nil {
		return resp, fmt.Errorf("executing bitbucket request: %w", doErr)
	}
	return resp, nil
}

// jitteredRetryAfterSeconds is gateway.JitteredRetryAfterSeconds: every
// caller this doer defers derives its wait from state shared across callers,
// so the spread keeps them from waking as one herd.
func jitteredRetryAfterSeconds(wait, jitter time.Duration) int {
	return gateway.JitteredRetryAfterSeconds(wait, jitter)
}

// observeResponse pulls Bitbucket rate-limit hints off the response and (a)
// updates the per-org budget snapshot in Redis, (b) reshapes the response into
// a SyncError with RetryAfterSeconds populated when the response was 429.
//
// (a) parses the headers and logs them; the budget store lives behind
// BudgetLimiter, which runner/app/gateways.go wires for diff-fetch only. This
// is the seam where it would attach for all bitbucket-rest traffic without
// touching every operation. (b) applies to every caller.
func (d *rateLimitedDoer) observeResponse(ctx context.Context, resp *http.Response) {
	adapter := ratelimiter.BitbucketAdapter{}
	orgID := reqctx.OrgIDFromContext(ctx)
	if snap := adapter.ParseBudget(resp.Header); snap != nil && snap.Remaining >= 0 {
		// Cheap structured log so operators can grep budget headroom per org
		// without scraping Bitbucket directly.
		logger.Debug(ctx, "bitbucket rate-limit budget snapshot",
			zap.String("group", d.group),
			zap.String(logger.OrgID, orgID),
			zap.Int("remaining", snap.Remaining),
			zap.Int("limit", snap.Limit),
			zap.Time("reset_at", snap.ResetAt),
		)
		bitbucketRateLimitBudgetRemaining.WithLabelValues(d.group, orgID).Set(float64(snap.Remaining))
		// Persist the snapshot to Redis so end-of-run operations can lift the
		// observed cap (snap.Limit) into ConnectorConfig.Settings, and so the
		// gateway factory can scale WithRateLimits per-workspace.
		// Failure is non-fatal — limiter scaling falls back to yml floor.
		if d.budgetLimiter != nil && orgID != "" {
			if err := d.budgetLimiter.UpdateFromResponse(ctx, orgID, resp.Header); err != nil {
				logger.WarnWithCtx(ctx, "bitbucket budget snapshot persist failed",
					zap.String("group", d.group),
					zap.String(logger.OrgID, orgID),
					zap.Error(err))
			}
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		bitbucketRateLimit429Total.WithLabelValues(d.group, orgID).Inc()
	}
}

// NewBitbucketVendorGateway creates a new BitbucketVendorGateway with the given
// HTTP doer (typically an autoRefreshDoer) and rate limiter store.
// Rate limits and base URLs are read from the gateway config.
//
// cbStore is optional. When non-nil it powers the per-org 429 circuit breaker
// (ADR-0004 §5) — short-circuits requests for orgs already inside a
// vendor punishment window so the orchestrator surfaces a fast rate_limit
// error instead of stacking more 429s. Tests / ad-hoc callers can pass nil
// to disable the breaker; rate limiting still applies. A redis.Cmdable shares
// the breaker across processes; NewInMemoryCircuitBreakerStore keeps it in
// this process, for a runner with no Redis.
//
// budgetLimiter is optional. When non-nil, every observed response with
// X-RateLimit-* headers is persisted to its BudgetStore under a per-org key; end-of-run
// operations read the snapshot back to lift the observed vendor cap into
// ConnectorConfig.Settings.VendorRateLimits — a map keyed by gateway
// group (bitbucket-rest) holding pacing.VendorRateLimitObservation
// entries.
func NewBitbucketVendorGateway(doer vendorapi.HTTPDoer, rlStore ratelimiter.Store, cbStore gateway.CircuitBreakerStore, budgetLimiter *ratelimiter.BudgetLimiter) (*BitbucketVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("bbgateway: %w", err)
	}
	endpoints := cfg.BitbucketEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		baseURLs[ep.Group] = ep.BaseURL
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("bbgateway: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	var cb *circuitBreaker
	if cbStore != nil {
		cb = newCircuitBreaker(cbStore)
	}

	return &BitbucketVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    doer,
			Factory: NewBitbucketVendorRequest(baseURLs),
		},
		rateLimits:     rateLimits,
		rateLimiter:    rl,
		rlCfg:          rlCfg,
		rlStore:        rlStore,
		baseDoer:       doer,
		circuitBreaker: cb,
		budgetLimiter:  budgetLimiter,
	}, nil
}

// simulatorBearerDoer sets a static Bearer token on outgoing requests when one is
// not already present. The real Bitbucket gateway gets its auth from the OAuth
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

// NewBitbucketVendorGatewayForSimulator creates a gateway whose group BaseURLs point
// at the connector simulator instead of api.bitbucket.org. simulatorBaseURL is the
// simulator service root (e.g. "http://localhost:8080"); each group's path suffix
// relative to the real Bitbucket base is preserved under a /bitbucket prefix, so the
// REST base becomes "<root>/bitbucket/2.0". The "/2.0" is load-bearing: the request
// factory derives the v1.0 groups base from it by replacing "/2.0" with "/1.0".
//
// Auth bypasses the OAuth refresh doer entirely — the simulator has no token
// endpoint, so a static bearer token is set directly (never api.bitbucket.org's
// hardcoded token URL).
func NewBitbucketVendorGatewayForSimulator(doer vendorapi.HTTPDoer, token string, rlStore ratelimiter.Store, cbStore gateway.CircuitBreakerStore, budgetLimiter *ratelimiter.BudgetLimiter, simulatorBaseURL string) (*BitbucketVendorGateway, error) {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("bbgateway simulator: %w", err)
	}
	endpoints := cfg.BitbucketEndpoints()

	baseURLs := make(map[string]string, len(endpoints))
	rateLimits := make([]sdk.RateLimitConfig, 0, len(endpoints))
	for _, ep := range endpoints {
		u, err := simroute.URL(simulatorBaseURL, "/bitbucket", ep.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("bbgateway simulator: %w", err)
		}
		baseURLs[ep.Group] = u
		rateLimits = append(rateLimits, ep.ToRateLimitConfig())
	}

	rlCfg, err := buildRateLimitConfig(endpoints)
	if err != nil {
		return nil, fmt.Errorf("bbgateway simulator: %w", err)
	}
	rl := ratelimiter.NewRateLimiter(rlStore, rlCfg)

	var cb *circuitBreaker
	if cbStore != nil {
		cb = newCircuitBreaker(cbStore)
	}

	authedDoer := &simulatorBearerDoer{inner: doer, token: token}

	return &BitbucketVendorGateway{
		BaseVendorGateway: vendorapi.BaseVendorGateway{
			Doer:    authedDoer,
			Factory: NewBitbucketVendorRequest(baseURLs),
		},
		rateLimits:     rateLimits,
		rateLimiter:    rl,
		rlCfg:          rlCfg,
		rlStore:        rlStore,
		baseDoer:       authedDoer,
		circuitBreaker: cb,
		budgetLimiter:  budgetLimiter,
	}, nil
}

// operationShareRemainder is the bucket for operations with no named share. Its
// name cannot collide with a real operation id: ids come from the connector's
// registered set and none carries this prefix.
const operationShareRemainder = "_remainder"

// operationResource is the limiter resource for one operation's slice of a group's
// budget. The limiter keys on namespace:resource:requestIdentifier, so pairing this
// with the unchanged per-org requestIdentifier yields a bucket per (org, operation)
// while the RATE comes from the resource — which is what lets shares differ.
func operationResource(group, operation string) string {
	return group + ":" + operation
}

// operationShareResources partitions a group's budget into per-operation buckets.
// Returns nil when no shares are configured, which leaves the single shared bucket
// every group had before.
//
// The sub-rates are derived from the group's ALREADY-SCALED (rate, period) pair
// rather than by re-scaling each share's requests-per-second. Re-scaling would let
// a share land in a different window than its own group: scaleRateToIntegerPerPeriod
// maps whole RPS to a 1-second window and fractional RPS to a 1-hour one, so 30/s
// with a 0.35 share would silently become 37800/hour — a bucket that is neither the
// intended fraction nor the intended window. Deriving from the scaled pair keeps
// every bucket in one window, which is also what makes them summable.
//
// Rounding is always down, preserving scaleRateToIntegerPerPeriod's never-exceed
// invariant per bucket and therefore in total.
func operationShareResources(group string, rps float64, shares map[string]float64) (map[string]*ratelimiter.RateLimit, error) {
	if len(shares) == 0 {
		// Empty rather than nil: callers range over the result, and an empty
		// partition is a legitimate answer meaning "leave the single shared bucket".
		return map[string]*ratelimiter.RateLimit{}, nil
	}
	total, period := scaleRateToIntegerPerPeriod(rps)

	out := make(map[string]*ratelimiter.RateLimit, len(shares)+1)
	var allocated int64
	for op, share := range shares {
		if share <= 0 {
			return nil, fmt.Errorf("operation share for %q must be > 0, got %v", op, share)
		}
		rate := floorShare(total, share)
		if rate < 1 {
			// The share is real but unrepresentable in this group's window, and both
			// ways of forcing it are wrong: a rate of 0 blocks the operation outright,
			// and rounding up to 1 silently hands it MORE than it asked for. The
			// distortion is largest exactly where it is least visible — a 1% share of
			// a 33/window group would become 1, i.e. 3% — so this fails the config
			// instead of quietly re-writing the operator's number.
			return nil, fmt.Errorf("operation share %v for %q rounds to zero of the group's %d per %v; the smallest representable share at this window is %v", share, op, total, period, 1.0/float64(total))
		}
		allocated += rate
		out[operationResource(group, op)] = &ratelimiter.RateLimit{
			Rate:   rate,
			Period: period,
		}
	}
	// Validated on the ALLOCATED integer total, not on the sum of the float shares.
	// The float sum cannot be trusted for this: ten operations at 0.1 accumulate to
	// 0.9999999999999999, so a `sum >= 1` guard admits a partition that consumes the
	// entire budget — and the remainder clamp below would then hand out one more
	// token, putting the buckets OVER the vendor cap. Integers are what the buckets
	// are actually built from, so checking them checks the real invariant.
	//
	// Strictly less than the whole, because the remainder bucket must be able to
	// exist: operations without a named share have nowhere else to draw from, and a
	// config that leaves them nothing reads as a complete partition while silently
	// starving them.
	if allocated >= total {
		return nil, fmt.Errorf("operation shares for group %q allocate %d of the group's %d per window, leaving nothing for operations without a share; they must sum to less than 1", group, allocated, total)
	}
	// The remainder is whatever the named buckets did not take, computed by
	// SUBTRACTION rather than as floor(total * (1-claimed)). The two differ: shares
	// of 0.35+0.20+0.15 sum to 0.7000000000000001 in float64, so the multiplied form
	// floors to 599 of 2000 and quietly loses a request to IEEE 754. Subtracting
	// makes the partition exact by construction — the buckets sum to `total`, which
	// is the property this whole function exists to guarantee.
	//
	// No clamp is needed: the guard above already proved allocated < total, so this
	// is at least 1. A clamp here would be the very thing that pushes the partition
	// over the cap.
	remainder := total - allocated
	out[operationResource(group, operationShareRemainder)] = &ratelimiter.RateLimit{
		Rate:   remainder,
		Period: period,
	}
	return out, nil
}

// floorShare apportions rate by share, always rounding DOWN so a bucket can never
// exceed the fraction it was granted. A result of zero is returned as zero, not
// nudged to one: it means the share cannot be expressed in this window at all, and
// only the caller has the context to reject that config.
func floorShare(total int64, share float64) int64 {
	return int64(math.Floor(float64(total) * share))
}

func buildRateLimitConfig(endpoints map[string]gateway.APIEndpointConfig) (*ratelimiter.RateLimitConfig, error) {
	resourceMap := make(map[string]*ratelimiter.RateLimit, len(endpoints))
	for _, ep := range endpoints {
		rate, period := scaleRateToIntegerPerPeriod(ep.RequestsPerSecond)
		// The whole-group resource stays registered even when shares are set: it is
		// what ForGroup gates on, and the fallback for any call the share lookup
		// cannot place.
		resourceMap[ep.Group] = &ratelimiter.RateLimit{
			Rate:   rate,
			Period: period,
		}
		shares, err := operationShareResources(ep.Group, ep.RequestsPerSecond, ep.OperationShares)
		if err != nil {
			return nil, fmt.Errorf("building operation shares for group %s: %w", ep.Group, err)
		}
		for name, rl := range shares {
			resourceMap[name] = rl
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
		// Per-org overrides get shares too. Without this an org that raised its own
		// budget would silently lose the partition and fall back to one shared
		// bucket — the starvation this exists to prevent, reappearing on exactly the
		// orgs busy enough to need a custom limit.
		shares, err := operationShareResources(c.Group, c.RequestsPerSecond, c.OperationShares)
		if err != nil {
			return nil, fmt.Errorf("building operation shares for group %s: %w", c.Group, err)
		}
		for name, rl := range shares {
			resourceMap[name] = rl
		}
	}
	return &ratelimiter.RateLimitConfig{
		Namespace:   rateLimitNamespace,
		ResourceMap: resourceMap,
	}, nil
}

// scaleRateToIntegerPerPeriod converts a requests-per-second float into an
// (integer rate, period) pair compatible with the underlying limiter, which
// only accepts int64 rate values.
//
// The limiter behind this config is a sliding-window counter, not a pacer: it
// admits every request until the window's count is reached, back to back, with
// no spacing. The window width is therefore the burst allowance — a 1-hour
// window hands out the whole hourly budget in a single second. Fractional
// rates scale into a 1-MINUTE window so a minute can never carry more than
// Floor(rps*60) requests and an hour's work stays spread across the hour.
//
// Invariant: the enforced rate MUST NOT exceed reqsPerSec, otherwise the
// limiter overshoots the vendor cap and re-introduces the 429s it exists to
// prevent. To preserve this we never round up — whole-RPS values use a 1s
// window; fractional values use 1m with Floor(rps*60). Examples:
//
//	30     rps -> (30, 1s)   // whole integer, exact
//	0.2667 rps -> (16, 1m)   // 960/hr, under Bitbucket's 1000/hr OAuth cap
//	2.7777 rps -> (166, 1m)  // observed 10000/hr cap, Floor stays under
//
// Minute-granularity Floor costs a little headroom against the hourly intent
// (0.2777 rps is 999/hr but 16/min = 960/hr); that loss buys the burst bound
// and is the point of the trade.
//
// Floats: 0.28 in float64 is actually 0.28000000000000003 (IEEE 754); the
// Floor handles that cleanly. Callers that need exact whole-RPS should
// pass an integer value.
func scaleRateToIntegerPerPeriod(reqsPerSec float64) (int64, time.Duration) {
	if reqsPerSec <= 0 {
		// Zero or negative is a misconfiguration; preserve the previous
		// behaviour (1 req/sec floor) instead of dividing by zero or
		// silently disabling the limiter.
		return 1, time.Second
	}
	// Whole-RPS shortcut keeps the 1-second window for static yml configs
	// (e.g. 30/s, 20/s) where the operator already chose an integral rate.
	if reqsPerSec == math.Floor(reqsPerSec) {
		return int64(reqsPerSec), time.Second
	}
	// Fractional RPS — a 1-minute window bounds the per-minute burst. This is
	// the path observed-cap scaling flows through.
	if perMinute := int64(math.Floor(reqsPerSec * 60)); perMinute >= 1 {
		return perMinute, time.Minute
	}
	// Below 1 req/min the minute window would floor to zero, which would
	// disable the limiter. Widen to an hour — the only window that can still
	// represent such a rate without rounding up past reqsPerSec.
	perHour := int64(math.Floor(reqsPerSec * 3600))
	if perHour < 1 {
		perHour = 1 // floor would be 0 for rps < 1/3600; keep limiter usable.
	}
	return perHour, time.Hour
}

// WithRateLimits returns a copy of this gateway with rate limits configured.
func (g *BitbucketVendorGateway) WithRateLimits(configs []sdk.RateLimitConfig) sdk.VendorGateway {
	cpy := *g
	cpy.rateLimits = configs
	rlCfg, err := buildRateLimitConfigFromFramework(configs)
	if err != nil {
		logger.ErrorNoCtx("bbgateway: build rate limit config for WithRateLimits", zap.Error(err))
		return g
	}
	cpy.rateLimiter = ratelimiter.NewRateLimiter(g.rlStore, rlCfg)
	// Kept in step with the limiter it describes: retryAfterForResource reads this to
	// size the backoff, and a stale pointer would size it from the pre-override rates.
	cpy.rlCfg = rlCfg
	cpy.Doer = g.baseDoer
	return &cpy
}

// ForGroup returns a copy of this gateway scoped to the named rate limit group.
// The returned gateway's HTTPDoer is wrapped with the group's rate limiter.
func (g *BitbucketVendorGateway) ForGroup(group string) sdk.VendorGateway {
	cpy := *g
	cpy.group = group
	if g.rateLimiter != nil && g.rateLimiter.IsResourceConfigured(group) {
		cpy.Doer = &rateLimitedDoer{
			inner:          g.baseDoer,
			rateLimiter:    g.rateLimiter,
			rlCfg:          g.rlCfg,
			group:          group,
			circuitBreaker: g.circuitBreaker,
			budgetLimiter:  g.budgetLimiter,
		}
	}
	return &cpy
}

// autoRefreshDoer wraps an HTTPDoer to transparently manage Bitbucket OAuth
// access tokens. Before each request, it ensures a valid access token is
// available (from Redis cache or by refreshing). On 401, it invalidates the
// cache and retries once.
//
// This is the Bitbucket equivalent of ghinstallation's automatic token
// management for GitHub App credentials.
type autoRefreshDoer struct {
	inner         vendorapi.HTTPDoer
	cache         auth.TokenCache
	locker        auth.Locker
	tx            store.TxRunner
	configDAO     ConnectorConfigStore
	clientID      string
	clientSecret  string
	orgID         string
	tokenEndpoint string // Bitbucket OAuth token endpoint URL
}

// NewAutoRefreshDoer creates an HTTPDoer that transparently manages Bitbucket
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
		return nil, fmt.Errorf("executing request: %w", err)
	}

	// On 401, invalidate cache and retry once.
	if resp.StatusCode == http.StatusUnauthorized {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(ctx, "bbgateway: closing 401 response body before retry", zap.Error(closeErr))
		}

		_ = d.cache.Delete(ctx, bbconfig.AccessTokenKey(d.orgID))

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
				return nil, fmt.Errorf("bitbucket retry: rewind request body: %w", gbErr)
			}
			retryReq.Body = freshBody
		}
		retryReq.Header.Set("Authorization", "Bearer "+token)
		retryResp, retryErr := d.inner.Do(ctx, retryReq)
		if retryErr != nil {
			return nil, fmt.Errorf("executing retry request: %w", retryErr)
		}
		return retryResp, nil
	}

	return resp, nil
}

// getAccessToken returns a valid access token. It checks Redis cache first,
// then refreshes if needed.
func (d *autoRefreshDoer) getAccessToken(ctx context.Context) (string, error) {
	// Check Redis cache.
	token, err := d.cache.Get(ctx, bbconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		// redis.Nil is the expected cache-miss; any other error means Redis is
		// degraded — log it but still fall through to a refresh so token
		// acquisition keeps working off the slower OAuth path.
		logger.WarnWithCtx(ctx, "bbgateway: access token cache read failed, falling back to refresh",
			zap.String(logger.OrgID, d.orgID), zap.Error(err))
	}

	token, err = d.refreshToken(ctx)
	if err != nil {
		return "", fmt.Errorf("refreshing token: %w", err)
	}
	return token, nil
}

// refreshToken acquires a distributed lock, refreshes the access token via
// Bitbucket's OAuth endpoint, caches the new access token, and persists the
// new refresh token to the database.
func (d *autoRefreshDoer) refreshToken(ctx context.Context) (string, error) {
	lockKey := bbconfig.RefreshLockKey(d.orgID)

	// Try to acquire the distributed lock.
	lock, acquired, err := d.locker.TryLock(ctx, lockKey, refreshLockTTL)
	if err != nil {
		return "", fmt.Errorf("acquire refresh lock: %w", err)
	}

	if !acquired {
		// Another instance is refreshing. Wait briefly and check cache.
		time.Sleep(2 * time.Second)
		token, err := d.cache.Get(ctx, bbconfig.AccessTokenKey(d.orgID))
		if err == nil && token != "" {
			return token, nil
		}
		if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
			return "", fmt.Errorf("read token cached by another instance: %w", err)
		}
		return "", fmt.Errorf("timeout waiting for token refresh by another instance")
	}
	defer func() { _ = lock.Unlock(context.WithoutCancel(ctx)) }() // a cancelled run still frees the lock

	// Double-check cache after acquiring lock.
	token, err := d.cache.Get(ctx, bbconfig.AccessTokenKey(d.orgID))
	if err == nil && token != "" {
		return token, nil
	}
	if err != nil && !errors.Is(err, auth.ErrCacheMiss) {
		// redis.Nil is the expected cache-miss; any other error means Redis is
		// degraded. We hold the lock, so fall through to the OAuth refresh
		// below rather than failing the whole acquire.
		logger.WarnWithCtx(ctx, "bbgateway: post-lock token cache read failed, proceeding to refresh",
			zap.String(logger.OrgID, d.orgID), zap.Error(err))
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

	var creds bbconfig.BitbucketCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if creds.OAuthRefreshToken == "" {
		// No refresh token (disconnected/wiped): fail closed — the org must
		// reconnect. Terminal so the scheduler SkipRetries instead of looping
		// MaxRetry; no attempt can mint a token the org has not reconnected.
		return "", &sdk.SyncError{
			Kind:      sdk.ErrKindAuth,
			Cause:     fmt.Errorf("bitbucket: no refresh token for org %s (reconnect required)", d.orgID),
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
	// Persist new refresh token to DB (Bitbucket rotates refresh tokens).
	if tokenResp.RefreshToken != "" && tokenResp.RefreshToken != creds.OAuthRefreshToken {
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
			logger.Info(ctx, "bbgateway: another credential write landed during the refresh; dropping the rotated token",
				zap.String(logger.OrgID, d.orgID))
			_ = d.cache.Delete(ctx, bbconfig.AccessTokenKey(d.orgID))
			return tokenResp.AccessToken, nil
		}
		fence = swapFence
	}

	// Cache the token only while the stored credentials are still the ones it
	// was minted from: a reconnect or disconnect during the refresh wins.
	if kept, err := auth.CacheIfCredentialsUnchanged(ctx, d.tx, d.configDAO, d.cache, d.orgID, connectorID, fence, bbconfig.AccessTokenKey(d.orgID), tokenResp.AccessToken, accessTokenTTL); err != nil {
		logger.Error(ctx, "bbgateway: failed to cache access token", zap.String(logger.OrgID, d.orgID), zap.Error(err))
	} else if !kept {
		logger.Info(ctx, "bbgateway: credentials changed during the refresh; not caching the access token", zap.String(logger.OrgID, d.orgID))
	}

	return tokenResp.AccessToken, nil
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// exchangeRefreshToken calls Bitbucket's OAuth token endpoint to exchange a
// refresh token for a new access token. The HTTP exchange, outcome
// classification, metric emission, and SyncError wrapping all live in
// vendorapi.OAuthRefreshClassifier.Fetch — this function only owns the
// vendor-specific request shape (BasicAuth + grant_type body) and the
// vendor-specific decode of the response.
//
// 200 OK with malformed JSON is intentionally surfaced as a plain error (not
// a *sdk.SyncError) so a vendor-side regression doesn't look like a
// routine OAuth failure — orchestrator default routing handles it as
// ErrKindInternal. The classifier records the outcome as `unknown` so the
// success rate is not inflated.
func (d *autoRefreshDoer) exchangeRefreshToken(ctx context.Context, refreshToken string) (*tokenResponse, error) {
	classifier := vendorapi.NewOAuthRefreshClassifier(connectorID, vendorapi.WithTimeout(refreshExchangeTimeout))
	body, start, err := classifier.Fetch(ctx, d.orgID, func(ctx context.Context) (*http.Request, error) {
		data := url.Values{
			"grant_type":    {"refresh_token"},
			"refresh_token": {refreshToken},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.tokenEndpoint, strings.NewReader(data.Encode())) //nolint:gosec // tokenEndpoint is vendor-config, not user input
		if err != nil {
			return nil, fmt.Errorf("building token refresh request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.SetBasicAuth(d.clientID, d.clientSecret)
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

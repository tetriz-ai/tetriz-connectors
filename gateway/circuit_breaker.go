// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// CircuitBreaker trips per (group, organization) on vendor 429 storms
// (ADR-0004 §5). The Bitbucket and GitLab gateways each hold one, with their
// own key prefix.
//
// When a vendor group repeatedly returns 429 for the same organization, the
// runner is already inside the vendor's punishment window: every further
// request burns budget without progress and prolongs the limit. The breaker
// opens after CircuitFailureThreshold 429s within CircuitFailureWindow and
// stays open for CircuitCooldown, during which the gateway fails fast with a
// rate_limit error so the scheduler backs off.
//
// Recovery is staged, not instantaneous: an expiring open marker would make
// every deferred request eligible at the same instant, which is the herd the
// jittered retry hints exist to break. So an expiring circuit lands in
// HALF-OPEN, where a SETNX token paces probes to roughly one per
// CircuitProbeInterval while everything else keeps failing fast, and a probe
// that comes back below 500 closes the circuit for everyone.
//
// The state lives in a CircuitBreakerStore: Redis, so all pods share the trip
// count, or NewInMemoryCircuitBreakerStore for one process. Four keys per
// (group, organization), under the breaker's prefix:
//
//	{prefix}:fail:{group}:{org}      INCR-able counter, TTL = failure window
//	{prefix}:open:{group}:{org}      fail-fast marker, TTL = cooldown
//	{prefix}:halfopen:{group}:{org}  probe-gated marker, TTL = cooldown + half-open window
//	{prefix}:probe:{group}:{org}     probe token, TTL = probe interval
type CircuitBreaker struct {
	store            CircuitBreakerStore
	prefix           string
	onOpen           func(group, orgID string)
	failureWindow    time.Duration
	cooldown         time.Duration
	failureThreshold int
	halfOpenWindow   time.Duration
	probeInterval    time.Duration
}

const (
	// CircuitFailureWindow is how long consecutive 429s accumulate before the
	// count resets: short enough that an isolated 429 does not trip the
	// breaker, long enough that a real burst does. Bitbucket's hourly cap and
	// GitLab.com's per-minute windows both fit it.
	CircuitFailureWindow = 5 * time.Minute

	// CircuitCooldown is how long the breaker stays open once tripped: longer
	// than the 60-second Retry-After both vendors typically send, shorter than
	// Bitbucket's hourly reset, so a healthy organization does not lose a sync
	// cycle.
	CircuitCooldown = 2 * time.Minute

	// CircuitFailureThreshold is the number of 429s within the failure window
	// that opens the circuit: it ignores isolated 429s and still trips fast
	// against a hostile vendor.
	CircuitFailureThreshold = 3

	// CircuitHalfOpenWindow is how long past the cooldown the circuit stays
	// probe-gated. It bounds how long a vendor that never recovers costs one
	// probe per probe interval instead of a full re-burst.
	CircuitHalfOpenWindow = 5 * time.Minute

	// CircuitProbeInterval is the spacing between half-open probes: a
	// rounding error against a per-minute budget, and recovery is seen well
	// inside a sync cycle.
	CircuitProbeInterval = 10 * time.Second

	// circuitOpenRetryJitter and circuitHalfOpenRetryJitter spread the retry
	// hints of deferred requests, which all derive from shared breaker state.
	circuitOpenRetryJitter     = 60 * time.Second
	circuitHalfOpenRetryJitter = 10 * time.Second
)

// ErrCircuitOpen and ErrCircuitHalfOpen are wrapped in the SyncError a
// gateway returns when the breaker turns a request away.
var (
	ErrCircuitOpen     = errors.New("circuit open (consecutive 429 storm)")
	ErrCircuitHalfOpen = errors.New("circuit half-open (probe in flight)")
)

// NewCircuitBreaker returns a breaker over store, its keys under prefix (such
// as "bb-cb" or "gl-cb"), with the ADR-0004 §5 thresholds. onOpen, when not
// nil, is called once per closed-to-open transition (a metric). A nil store
// returns nil, and every method of a nil breaker lets traffic through.
func NewCircuitBreaker(store CircuitBreakerStore, prefix string, onOpen func(group, orgID string)) *CircuitBreaker {
	if store == nil {
		return nil
	}
	return &CircuitBreaker{
		store:            store,
		prefix:           prefix,
		onOpen:           onOpen,
		failureWindow:    CircuitFailureWindow,
		cooldown:         CircuitCooldown,
		failureThreshold: CircuitFailureThreshold,
		halfOpenWindow:   CircuitHalfOpenWindow,
		probeInterval:    CircuitProbeInterval,
	}
}

// ProbeInterval is the spacing between half-open probes.
func (cb *CircuitBreaker) ProbeInterval() time.Duration {
	if cb == nil {
		return 0
	}
	return cb.probeInterval
}

// Gate decides whether a request for (group, orgID) may go to the vendor. It
// returns a retryable rate_limit SyncError wrapping ErrCircuitOpen (with the
// remaining cooldown as the retry hint) or ErrCircuitHalfOpen when the request
// is turned away. probing is true when the request is the half-open probe: the
// caller passes it to Observe, or calls ReleaseProbe if the request does not
// reach the vendor after all.
func (cb *CircuitBreaker) Gate(ctx context.Context, group, orgID string) (probing bool, err error) {
	if open, cooldown := cb.OpenState(ctx, group, orgID); open {
		return false, &sdk.SyncError{
			Kind:              sdk.ErrKindRateLimit,
			Cause:             fmt.Errorf("%s: %w", group, ErrCircuitOpen),
			Retryable:         true,
			RetryAfterSeconds: JitteredRetryAfterSeconds(cooldown, circuitOpenRetryJitter),
		}
	}
	gated, won := cb.HalfOpenState(ctx, group, orgID)
	if gated && !won {
		return false, &sdk.SyncError{
			Kind:              sdk.ErrKindRateLimit,
			Cause:             fmt.Errorf("%s: %w", group, ErrCircuitHalfOpen),
			Retryable:         true,
			RetryAfterSeconds: JitteredRetryAfterSeconds(cb.ProbeInterval(), circuitHalfOpenRetryJitter),
		}
	}
	return gated, nil
}

// Observe records a vendor response for (group, orgID): a 429 counts toward
// tripping the breaker, and a probe answered below 500 closes the circuit for
// every caller. A 4xx counts as recovered, since the vendor is serving again;
// a 5xx does not, as re-admitting the herd against a failing vendor would
// rebuild the burst under another status. It returns the failure count after
// a 429, and 0 otherwise.
func (cb *CircuitBreaker) Observe(ctx context.Context, group, orgID string, status int, probing bool) int64 {
	switch {
	case status == http.StatusTooManyRequests:
		return cb.RecordFailure(ctx, group, orgID)
	case probing && status < http.StatusInternalServerError:
		cb.RecordProbeSuccess(ctx, group, orgID)
	}
	return 0
}

// OpenState returns whether the breaker is open and, if so, the remaining
// cooldown, so callers can set SyncError.RetryAfterSeconds and let the
// scheduler back off for the full window.
func (cb *CircuitBreaker) OpenState(ctx context.Context, group, orgID string) (bool, time.Duration) {
	if cb == nil || cb.store == nil {
		return false, 0
	}
	ttl, err := cb.store.PTTL(ctx, cb.key("open", group, orgID)).Result()
	if err != nil {
		// Fail open: an unreadable breaker must not block all traffic.
		logger.WarnWithCtx(ctx, "circuit breaker: open-state read failed, failing open",
			zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
		return false, 0
	}
	if ttl <= 0 {
		return false, 0
	}
	return true, ttl
}

// RecordFailure bumps the 429 counter for (group, orgID) and trips the
// breaker when the threshold is crossed. It returns the post-bump count.
// There is no reset on success: the window comes from the counter's TTL, so a
// run that mixes 200s with a sustained 429 stream still trips.
func (cb *CircuitBreaker) RecordFailure(ctx context.Context, group, orgID string) int64 {
	if cb == nil || cb.store == nil {
		return 0
	}
	key := cb.key("fail", group, orgID)
	n, err := cb.store.Incr(ctx, key).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "circuit breaker: failure-count increment failed",
			zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
		return 0
	}
	// The window TTL is set on the first increment only, so later failures in
	// the window keep its deadline.
	if n == 1 {
		if err := cb.store.Expire(ctx, key, cb.failureWindow).Err(); err != nil {
			logger.Debug(ctx, "circuit breaker: set TTL failed", zap.String("key", key), zap.Error(err))
		}
	}
	if n >= int64(cb.failureThreshold) {
		// SetNX, so exactly one caller of a concurrent burst counts the trip.
		opened, err := cb.store.SetNX(ctx, cb.key("open", group, orgID), "1", cb.cooldown).Result()
		if err != nil {
			logger.WarnWithCtx(ctx, "circuit breaker: set open key failed",
				zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
			return n
		}
		if !opened {
			return n
		}
		// Outlives the open key, so the circuit lands in half-open.
		if err := cb.store.Set(ctx, cb.key("halfopen", group, orgID), "1", cb.cooldown+cb.halfOpenWindow).Err(); err != nil {
			logger.WarnWithCtx(ctx, "circuit breaker: set half-open key failed",
				zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
		}
		if err := cb.store.Del(ctx, key).Err(); err != nil {
			logger.Debug(ctx, "circuit breaker: del fail counter failed", zap.String("key", key), zap.Error(err))
		}
		if cb.onOpen != nil {
			cb.onOpen(group, orgID)
		}
	}
	return n
}

// HalfOpenState reports whether (group, orgID) is in the probe-gated recovery
// window and, if so, whether this caller won the probe slot for the current
// probe interval. The token's TTL paces probes; it is not a strict mutex.
// It fails open on store errors, as OpenState does.
func (cb *CircuitBreaker) HalfOpenState(ctx context.Context, group, orgID string) (gated, wonProbe bool) {
	if cb == nil || cb.store == nil {
		return false, false
	}
	exists, err := cb.store.Exists(ctx, cb.key("halfopen", group, orgID)).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "circuit breaker: half-open read failed, failing open",
			zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
		return false, false
	}
	if exists == 0 {
		return false, false
	}
	won, err := cb.store.SetNX(ctx, cb.key("probe", group, orgID), "1", cb.probeInterval).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "circuit breaker: probe token acquire failed, failing open",
			zap.String("group", group), zap.String(logger.OrgID, orgID), zap.Error(err))
		return false, false
	}
	return true, won
}

// ReleaseProbe hands the current interval's probe slot back, for a winner that
// did not reach the vendor after all.
func (cb *CircuitBreaker) ReleaseProbe(ctx context.Context, group, orgID string) {
	if cb == nil || cb.store == nil {
		return
	}
	if err := cb.store.Del(ctx, cb.key("probe", group, orgID)).Err(); err != nil {
		logger.Debug(ctx, "circuit breaker: release probe token failed",
			zap.String("group", group), zap.Error(err))
	}
}

// RecordProbeSuccess closes a half-open circuit after a probe came back
// without a 429, lifting the probe gate for every caller at once.
func (cb *CircuitBreaker) RecordProbeSuccess(ctx context.Context, group, orgID string) {
	if cb == nil || cb.store == nil {
		return
	}
	if err := cb.store.Del(ctx, cb.key("halfopen", group, orgID), cb.key("probe", group, orgID)).Err(); err != nil {
		logger.Debug(ctx, "circuit breaker: clear half-open keys failed",
			zap.String("group", group), zap.Error(err))
	}
}

// CircuitKey returns the store key of one breaker marker: kind is fail, open,
// halfopen or probe.
func CircuitKey(prefix, kind, group, orgID string) string {
	return fmt.Sprintf("%s:%s:%s:%s", prefix, kind, group, orgID)
}

func (cb *CircuitBreaker) key(kind, group, orgID string) string {
	return CircuitKey(cb.prefix, kind, group, orgID)
}

// JitteredRetryAfterSeconds converts a wait into a whole-second Retry-After,
// spread by a random offset of up to jitter, and never below 1 (zero means no
// hint). Callers deferred by shared state (a breaker's remaining cooldown, a
// probe interval, a limiter window) would otherwise wake together as one herd.
func JitteredRetryAfterSeconds(wait, jitter time.Duration) int {
	total := wait
	if jitter > 0 {
		//nolint:gosec // backoff spreader, not a security primitive
		total += time.Duration(rand.Int63n(int64(jitter)))
	}
	secs := int(total.Round(time.Second) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}

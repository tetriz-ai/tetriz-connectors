// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway"

	"github.com/alicebob/miniredis/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cbBackend is a CircuitBreakerStore under test and a clock that expires its
// TTLs. Every breaker test runs on Redis (miniredis) and on the in-memory
// store with the same expectations, so the two cannot drift.
type cbBackend struct {
	store   gateway.CircuitBreakerStore
	advance func(time.Duration)
}

var cbBackends = []struct {
	name string
	make func(t *testing.T) cbBackend
}{
	{"redis", func(t *testing.T) cbBackend {
		mr := miniredis.RunT(t)
		rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rc.Close() })
		return cbBackend{store: rc, advance: mr.FastForward}
	}},
	{"memory", func(*testing.T) cbBackend {
		clk := clockwork.NewFakeClock()
		return cbBackend{store: gateway.NewInMemoryCircuitBreakerStore(clk), advance: clk.Advance}
	}},
}

// forEachCBBackend runs fn once per backend, each with a fresh store.
func forEachCBBackend(t *testing.T, fn func(t *testing.T, b cbBackend)) {
	t.Helper()
	for _, be := range cbBackends {
		t.Run(be.name, func(t *testing.T) { fn(t, be.make(t)) })
	}
}

// TestCircuitBreaker_OpenState exercises the new openState path that
// surfaces the remaining cooldown to the caller so it can populate
// SyncError.RetryAfterSeconds. Without it an org cycles circuit-open →
// fast-retry → circuit-open every few seconds and the cooldown never
// expires.
func TestCircuitBreaker_OpenState(t *testing.T) {
	const group = "bitbucket-rest"
	const org = "org-test"
	ctx := context.Background()

	tests := []struct {
		name     string
		setup    func(s gateway.CircuitBreakerStore)
		wantOpen bool
		wantMin  time.Duration
		wantMax  time.Duration
	}{
		{
			name:     "no key present → closed",
			setup:    func(gateway.CircuitBreakerStore) {},
			wantOpen: false,
		},
		{
			name: "open key with TTL → open + cooldown returned",
			setup: func(s gateway.CircuitBreakerStore) {
				s.Set(ctx, cbKey("open", group, org), "1", 90*time.Second)
			},
			wantOpen: true,
			wantMin:  85 * time.Second,
			wantMax:  90 * time.Second,
		},
		{
			name: "open key with no TTL → closed",
			setup: func(s gateway.CircuitBreakerStore) {
				s.Set(ctx, cbKey("open", group, org), "1", 0)
			},
			wantOpen: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachCBBackend(t, func(t *testing.T, b cbBackend) {
				cb := newCircuitBreaker(b.store)
				tt.setup(b.store)

				open, cooldown := cb.OpenState(ctx, group, org)
				assert.Equal(t, tt.wantOpen, open)
				if tt.wantOpen {
					assert.GreaterOrEqual(t, cooldown, tt.wantMin)
					assert.LessOrEqual(t, cooldown, tt.wantMax)
				}
			})
		})
	}
}

// TestCircuitBreaker_TripAndCooldown verifies that recordFailure trips
// the breaker after the threshold is crossed and that openState returns
// a cooldown matching gateway.CircuitCooldown.
func TestCircuitBreaker_TripAndCooldown(t *testing.T) {
	forEachCBBackend(t, func(t *testing.T, b cbBackend) {
		cb := newCircuitBreaker(b.store)
		const group = "bitbucket-rest"
		const org = "org-trip-test"
		ctx := context.Background()

		for i := 0; i < gateway.CircuitFailureThreshold; i++ {
			cb.RecordFailure(ctx, group, org)
		}

		open, cooldown := cb.OpenState(ctx, group, org)
		require.True(t, open, "breaker should be open after %d failures", gateway.CircuitFailureThreshold)
		assert.Greater(t, cooldown, time.Duration(0))
		assert.LessOrEqual(t, cooldown, gateway.CircuitCooldown)

		// The cooldown lapses into half-open, and half-open lapses into closed.
		b.advance(gateway.CircuitCooldown)
		open, _ = cb.OpenState(ctx, group, org)
		assert.False(t, open, "the open marker expired")
		gated, _ := cb.HalfOpenState(ctx, group, org)
		assert.True(t, gated, "still probe-gated after the cooldown")
		b.advance(gateway.CircuitHalfOpenWindow)
		gated, _ = cb.HalfOpenState(ctx, group, org)
		assert.False(t, gated, "the half-open marker expired")
	})
}

// The failure count resets when its window lapses, so isolated 429s spread
// over time never trip the breaker.
func TestCircuitBreaker_FailureWindowExpires(t *testing.T) {
	forEachCBBackend(t, func(t *testing.T, b cbBackend) {
		cb := newCircuitBreaker(b.store)
		ctx := context.Background()
		for i := 0; i < gateway.CircuitFailureThreshold-1; i++ {
			assert.Equal(t, int64(i+1), cb.RecordFailure(ctx, "g", "o"))
		}
		b.advance(gateway.CircuitFailureWindow)
		assert.Equal(t, int64(1), cb.RecordFailure(ctx, "g", "o"), "the window lapsed")
		open, _ := cb.OpenState(ctx, "g", "o")
		assert.False(t, open)
	})
}

// TestCircuitBreaker_HalfOpenAdmitsOneProbe pins the
// re-trip loop: a lapsed cooldown must not make every deferred request
// eligible at once. Recovery admits one probe per interval, not the herd.
func TestCircuitBreaker_HalfOpenAdmitsOneProbe(t *testing.T) {
	forEachCBBackend(t, func(t *testing.T, b cbBackend) {
		cb := newCircuitBreaker(b.store)
		const group = "bitbucket-rest"
		const org = "org-halfopen"
		ctx := context.Background()

		for i := 0; i < gateway.CircuitFailureThreshold; i++ {
			cb.RecordFailure(ctx, group, org)
		}

		// Expire the fail-fast marker but not the half-open one: the breaker is
		// half-open, not closed, until the half-open marker expires too.
		b.store.Del(ctx, cbKey("open", group, org))
		open, _ := cb.OpenState(ctx, group, org)
		require.False(t, open, "open marker cleared")

		// Concurrent callers arrive; exactly one may reach the vendor.
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				gated, won := cb.HalfOpenState(ctx, group, org)
				assert.True(t, gated, "still probe-gated after cooldown lapses")
				if won {
					admitted.Add(1)
				}
			}()
		}
		wg.Wait()
		assert.Equal(t, int64(1), admitted.Load(), "half-open must admit exactly one probe per interval")

		b.advance(gateway.CircuitProbeInterval)
		_, won := cb.HalfOpenState(ctx, group, org)
		assert.True(t, won, "the next interval admits the next probe")
	})
}

// TestCircuitBreaker_ProbeSuccessClosesForEveryone verifies a healthy probe
// lifts the gate for all callers at once rather than leaving them to drain by
// TTL — otherwise a recovered vendor stays throttled for the whole half-open
// window.
func TestCircuitBreaker_ProbeSuccessClosesForEveryone(t *testing.T) {
	forEachCBBackend(t, func(t *testing.T, b cbBackend) {
		cb := newCircuitBreaker(b.store)
		const group = "bitbucket-rest"
		const org = "org-probe-ok"
		ctx := context.Background()

		for i := 0; i < gateway.CircuitFailureThreshold; i++ {
			cb.RecordFailure(ctx, group, org)
		}
		b.store.Del(ctx, cbKey("open", group, org))

		gated, won := cb.HalfOpenState(ctx, group, org)
		require.True(t, gated)
		require.True(t, won, "first caller wins the probe slot")

		cb.ReleaseProbe(ctx, group, org)
		_, won = cb.HalfOpenState(ctx, group, org)
		require.True(t, won, "a released probe slot can be won again")

		cb.RecordProbeSuccess(ctx, group, org)

		gated, _ = cb.HalfOpenState(ctx, group, org)
		assert.False(t, gated, "a successful probe closes the circuit for every caller")
	})
}

// TestCircuitBreaker_NilSafe pins the documented fallback: a breaker with no
// Redis must never block traffic.
func TestCircuitBreaker_NilSafe(t *testing.T) {
	ctx := context.Background()
	var cb *circuitBreaker

	gated, won := cb.HalfOpenState(ctx, "bitbucket-rest", "org")
	assert.False(t, gated)
	assert.False(t, won)
	assert.NotPanics(t, func() { cb.RecordProbeSuccess(ctx, "bitbucket-rest", "org") })
}

// cbKey is the Bitbucket breaker's store key of one marker.
func cbKey(kind, group, orgID string) string {
	return gateway.CircuitKey(circuitKeyPrefix, kind, group, orgID)
}

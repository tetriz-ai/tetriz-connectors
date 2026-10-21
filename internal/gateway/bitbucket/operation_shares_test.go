// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fakeResourceLimiter answers only the question acquireResource asks — which
// resources exist. Acquire is never reached by these tests; a real limiter would
// drag Redis into what is a pure resource-selection decision.
type fakeResourceLimiter map[string]bool

func (f fakeResourceLimiter) IsResourceConfigured(resource string) bool { return f[resource] }

func (f fakeResourceLimiter) Acquire(context.Context, string, string) (bool, error) {
	return true, nil
}

func (f fakeResourceLimiter) AcquireUptoN(context.Context, string, string, int64, bool) (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (f fakeResourceLimiter) ReleaseUptoN(context.Context, string, string, int64, time.Time) (int64, error) {
	return 0, nil
}

// The whole point of shares is that they PARTITION a group's budget. If the buckets
// summed to more than the configured rate they would multiply the vendor cap — the
// failure this arithmetic exists to prevent, and one that would look like working
// config right up until the vendor started 429ing.
func TestOperationShareResources_partitionsRatherThanMultiplies(t *testing.T) {
	const group = "bitbucket-rest"
	// 0.5556 rps is the real Bitbucket setting: fractional, so it scales to a
	// 1-minute window. Flooring to a whole token per minute costs 20 requests an
	// hour against the 2000/hour cap (33/min = 1980/hour) — deliberate, because the
	// scaler's invariant is never to exceed the vendor's rate, and bounding the
	// per-minute burst is the point of the narrower window.
	const rps = 0.5556

	total, period := scaleRateToIntegerPerPeriod(rps)
	require.Equal(t, time.Minute, period)
	require.Equal(t, int64(33), total)

	shares := map[string]float64{
		"fetch_prs":         0.35,
		"fetch_commits":     0.20,
		"fetch_pr_comments": 0.15,
	}

	got, err := operationShareResources(group, rps, shares)
	require.NoError(t, err)

	// Every named share, plus the remainder for everything unnamed.
	require.Len(t, got, len(shares)+1)

	var sum int64
	for name, rl := range got {
		assert.Equal(t, period, rl.Period, "bucket %s must share the group's window", name)
		assert.Positive(t, rl.Rate, "bucket %s must be usable", name)
		sum += rl.Rate
	}
	// Exactly the group rate, not merely under it: the remainder is computed by
	// subtraction, so no request is lost to float rounding and none is invented.
	assert.Equal(t, total, sum,
		"buckets must sum to exactly the group rate; above it they multiply the vendor cap, below it they silently shrink it")

	assert.Equal(t, int64(11), got[operationResource(group, "fetch_prs")].Rate)
	assert.Equal(t, int64(6), got[operationResource(group, "fetch_commits")].Rate)
	assert.Equal(t, int64(4), got[operationResource(group, "fetch_pr_comments")].Rate)
	// 33 - 21, by subtraction: floor(33 * 0.30) would be 9 and lose three tokens.
	assert.Equal(t, int64(12), got[operationResource(group, operationShareRemainder)].Rate)
}

// scaleRateToIntegerPerPeriod maps whole RPS to a 1-second window and fractional RPS
// to a 1-hour one. Re-scaling each share's own requests-per-second would therefore
// drop a share into a different window than its group — 30/s at a 0.35 share becomes
// 10.5/s, which scales to 37800/hour: neither the intended fraction nor the intended
// window. Deriving from the group's already-scaled pair is what avoids that.
func TestOperationShareResources_sharesKeepTheGroupWindow(t *testing.T) {
	const group = "whole-rps-group"
	const rps = 30 // whole → 1-second window

	total, period := scaleRateToIntegerPerPeriod(rps)
	require.Equal(t, time.Second, period)
	require.Equal(t, int64(30), total)

	got, err := operationShareResources(group, rps, map[string]float64{"op_a": 0.35})
	require.NoError(t, err)

	for name, rl := range got {
		assert.Equal(t, time.Second, rl.Period,
			"bucket %s escaped the group's 1s window", name)
	}
	assert.Equal(t, int64(10), got[operationResource(group, "op_a")].Rate, "floor(30*0.35)")
	// The remainder is total minus what was allocated, not floor(30*0.65)=19, so the
	// partition is exact: 10 + 20 = 30 rather than 29 with a request lost to rounding.
	assert.Equal(t, int64(20), got[operationResource(group, operationShareRemainder)].Rate)
	assert.Equal(t, total,
		got[operationResource(group, "op_a")].Rate+got[operationResource(group, operationShareRemainder)].Rate,
		"buckets must sum to exactly the group rate")
}

func TestOperationShareResources_validation(t *testing.T) {
	const group = "g"
	// A whole-RPS group deliberately: it scales to an exact per-second total, so the
	// cases below exercise the allocation guard itself rather than the flooring loss
	// of whichever window the observed-cap scaler happens to pick. Under a fractional
	// rate the same shares floor well under the total and would never reach the
	// guard, making these assertions pass for the wrong reason.
	const rps = 30

	tests := []struct {
		name    string
		shares  map[string]float64
		wantNil bool
		wantErr string
	}{
		{
			name:    "no shares keeps the single shared bucket",
			shares:  nil,
			wantNil: true,
		},
		{
			name:    "empty map keeps the single shared bucket",
			shares:  map[string]float64{},
			wantNil: true,
		},
		{
			// Consuming the whole budget reads as a complete partition while leaving
			// unnamed operations with nothing to draw from.
			name:    "shares summing to exactly 1 are rejected",
			shares:  map[string]float64{"a": 0.5, "b": 0.5},
			wantErr: "leaving nothing for operations without a share",
		},
		{
			name:    "shares summing above 1 are rejected",
			shares:  map[string]float64{"a": 0.8, "b": 0.4},
			wantErr: "leaving nothing for operations without a share",
		},
		{
			// Codex's counterexample: ten decimal tenths accumulate to
			// 0.9999999999999999 in float64, so a `sum >= 1` guard would ACCEPT this
			// and the remainder clamp would then push the partition over the cap.
			// Validating the allocated integer total is what catches it.
			name: "ten shares of 0.1 are rejected despite float accumulation",
			shares: map[string]float64{
				"a": 0.1, "b": 0.1, "c": 0.1, "d": 0.1, "e": 0.1,
				"f": 0.1, "g": 0.1, "h": 0.1, "i": 0.1, "j": 0.1,
			},
			wantErr: "leaving nothing for operations without a share",
		},
		{
			name:    "a zero share is rejected rather than silently blocking its op",
			shares:  map[string]float64{"a": 0},
			wantErr: "must be > 0",
		},
		{
			name:    "a negative share is rejected",
			shares:  map[string]float64{"a": -0.1},
			wantErr: "must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := operationShareResources(group, rps, tt.shares)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, got, "no partial partition on error")
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				assert.Empty(t, got, "unset shares must leave the group's single bucket untouched")
			}
		})
	}
}

// A share small enough to floor to zero must still yield a usable bucket: a rate of
// 0 would block that operation outright rather than throttle it, turning a tuning
// knob into an outage.
// A share too small for the group's window is rejected, never rounded up. Rounding
// up is the dangerous direction: it hands the operation more budget than the
// operator wrote, and the narrower the window the more often it fires — on a
// per-minute group of 33 every share below ~3% would silently become 3%.
func TestOperationShareResources_rejectsShareTooSmallForTheWindow(t *testing.T) {
	// 0.5556 rps scales to 33 per minute; 0.0001 of 33 floors to zero.
	_, err := operationShareResources("g", 0.5556, map[string]float64{"tiny": 0.0001})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rounds to zero")

	// The smallest share the same window CAN express is accepted exactly, with no
	// rounding in either direction.
	got, err := operationShareResources("g", 0.5556, map[string]float64{"tiny": 1.0 / 33.0})
	require.NoError(t, err)
	assert.Equal(t, int64(1), got[operationResource("g", "tiny")].Rate)
}

// With nothing configured, every call lands on the plain group resource, the same
// resource an unshared operation uses.
func TestAcquireResource_fallbackChain(t *testing.T) {
	const group = "bitbucket-rest"

	tests := []struct {
		name       string
		configured map[string]bool
		operation  string
		want       string
	}{
		{
			name:       "no shares configured falls back to the group",
			configured: map[string]bool{group: true},
			operation:  "fetch_prs",
			want:       group,
		},
		{
			name: "a named operation draws from its own slice",
			configured: map[string]bool{
				group:                                 true,
				operationResource(group, "fetch_prs"): true,
				operationResource(group, operationShareRemainder): true,
			},
			operation: "fetch_prs",
			want:      operationResource(group, "fetch_prs"),
		},
		{
			name: "an unnamed operation draws from the remainder",
			configured: map[string]bool{
				group:                                 true,
				operationResource(group, "fetch_prs"): true,
				operationResource(group, operationShareRemainder): true,
			},
			operation: "fetch_repo_tree",
			want:      operationResource(group, operationShareRemainder),
		},
		{
			// An unattributed call must not be able to claim a named slice.
			name: "an empty operation draws from the remainder",
			configured: map[string]bool{
				group: true,
				operationResource(group, operationShareRemainder): true,
			},
			operation: "",
			want:      operationResource(group, operationShareRemainder),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &rateLimitedDoer{group: group, rateLimiter: fakeResourceLimiter(tt.configured)}
			assert.Equal(t, tt.want, d.acquireResource(tt.operation))
		})
	}
}

// The fixed 30s floor was calibrated against the whole group's rate. A per-operation
// slice can refill far slower, and returning the floor for it makes asynq retry an
// exhausted slice several times before a token could possibly have arrived.
func TestRetryAfterForResource_sizedToTheSelectedBucket(t *testing.T) {
	const group = "bitbucket-rest"

	cfg, err := buildRateLimitConfig(map[string]gateway.APIEndpointConfig{
		group: {
			Group:             group,
			RequestsPerSecond: 0.5556, // 2000/hour
			// The smallest share this group's per-minute window can express: 1 token
			// per minute. That is the case a fixed 30s delay gets wrong.
			OperationShares: map[string]float64{"tiny_op": 0.04},
		},
	})
	require.NoError(t, err)

	d := &rateLimitedDoer{group: group, rlCfg: cfg}

	// The group itself refills every ~1.8s, so the floor still governs.
	assert.Equal(t, localLimiterRetryAfter, d.retryAfterForResource(group))

	// 33/min * 0.04 → 1 token per minute → 60s per token, twice the floor.
	assert.Equal(t, 60*time.Second, d.retryAfterForResource(operationResource(group, "tiny_op")))

	// An unknown resource must not derive a delay from a zero rate.
	assert.Equal(t, localLimiterRetryAfter, d.retryAfterForResource("no-such-resource"))

	// A doer built before any config is still safe.
	assert.Equal(t, localLimiterRetryAfter,
		(&rateLimitedDoer{group: group}).retryAfterForResource(group))
}

// ToRateLimitConfig feeds the observed-cap rebuild. Dropping the shares there would
// collapse the partition back to one shared bucket the moment an org gets a live cap
// observation — silently, and only for the busiest orgs.
func TestToRateLimitConfig_carriesOperationShares(t *testing.T) {
	ep := gateway.APIEndpointConfig{
		Group:             "bitbucket-rest",
		RequestsPerSecond: 0.5556,
		OperationShares:   map[string]float64{"fetch_prs": 0.35},
	}
	assert.Equal(t, ep.OperationShares, ep.ToRateLimitConfig().OperationShares)

	// And the rebuild path actually produces the slices.
	cfg, err := buildRateLimitConfigFromFramework([]sdk.RateLimitConfig{ep.ToRateLimitConfig()})
	require.NoError(t, err)
	assert.Positive(t, cfg.GetMaxRequestCount(operationResource(ep.Group, "fetch_prs")),
		"observed-cap rebuild must keep the per-operation slice")
}

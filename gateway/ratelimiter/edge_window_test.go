// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package ratelimiter_test

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
)

func edgeConf(ns string, rate int64, period time.Duration, pd *ratelimiter.PriorityDistribution) *ratelimiter.RateLimitConfig {
	return &ratelimiter.RateLimitConfig{
		Namespace:   ns,
		ResourceMap: map[string]*ratelimiter.RateLimit{"r": {Rate: rate, Period: period, PriorityDistribution: pd}},
	}
}

// Exactly Rate calls fit in one window, the next is refused, and once the
// window has slid past the first call a slot frees again.
func TestEdge_ExactlyRateCallsPerWindow(t *testing.T) {
	clk := clockwork.NewFakeClockAt(time.Unix(1_800_000_000, 0))
	ns := "edge-" + uuid.NewString()
	r := ratelimiter.NewRateLimiter(store.NewSlidingWindowLogWithRedis(redisClient), edgeConf(ns, 3, time.Second, nil), ratelimiter.WithClock(clk))
	ctx := context.Background()
	for i := range 3 {
		ok, err := r.Acquire(ctx, "r", "org")
		require.NoError(t, err)
		require.True(t, ok, "call %d of 3", i+1)
		clk.Advance(time.Millisecond)
	}
	ok, err := r.Acquire(ctx, "r", "org")
	require.NoError(t, err)
	assert.False(t, ok, "the 4th call in a 3/s window was admitted")

	clk.Advance(time.Second) // the first call is now older than the window
	ok, err = r.Acquire(ctx, "r", "org")
	require.NoError(t, err)
	assert.True(t, ok)
}

// Slots booked in bulk can be handed back by their timestamp, but only as
// many as were booked at it: releasing more than were taken, or at a
// timestamp nothing was booked at, frees nothing extra.
func TestEdge_ReleaseUptoNReturnsOnlyWhatWasBooked(t *testing.T) {
	ns := "edge-" + uuid.NewString()
	r := ratelimiter.NewRateLimiter(store.NewSlidingWindowLogWithRedis(redisClient), edgeConf(ns, 5, time.Minute, nil))
	ctx := context.Background()

	got, ts, err := r.AcquireUptoN(ctx, "r", "org", 3, true)
	require.NoError(t, err)
	require.Equal(t, int64(3), got)

	removed, err := r.ReleaseUptoN(ctx, "r", "org", 1, ts.Add(time.Nanosecond*7919))
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed, "released slots at a timestamp nothing was booked at")

	removed, err = r.ReleaseUptoN(ctx, "r", "org", 10, ts)
	require.NoError(t, err)
	assert.Equal(t, int64(3), removed)

	got, _, err = r.AcquireUptoN(ctx, "r", "org", 10, true)
	require.NoError(t, err)
	assert.Equal(t, int64(5), got, "after releasing every slot the whole budget is free again")

	got, _, err = r.AcquireUptoN(ctx, "r", "org", 1, true)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

// Every sorted set the priority limiter writes gets a TTL, so a key for an
// org that stops calling expires instead of living forever. A high-priority
// call that spills into the medium set writes that set.
func TestEdge_PrioritySpillKeyGetsATTL(t *testing.T) {
	ns := "edge-" + uuid.NewString()
	pd := &ratelimiter.PriorityDistribution{Enabled: true, HighPercent: 10, MediumPercent: 10, LowPercent: 80}
	conf := edgeConf(ns, 10, time.Minute, pd)
	r := ratelimiter.NewRateLimiter(store.NewSlidingWindowLogWithRedis(redisClient), conf)
	ctx := context.Background()

	for range 2 { // the first fills the high slice, the second spills to medium
		ok, err := r.AcquireWithPriority(ctx, "r", "org", ratelimiter.PriorityHigh)
		require.NoError(t, err)
		require.True(t, ok)
	}
	base := conf.GetKeyWithNamespace("r", "org")
	medium := base + ":" + strconv.Itoa(int(ratelimiter.PriorityMedium))
	require.Eventually(t, func() bool {
		return redisClient.TTL(ctx, base+":0").Val() > 0
	}, 2*time.Second, 10*time.Millisecond, "the high set never got a TTL")
	n, err := redisClient.ZCard(ctx, medium).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "the second high call should have booked the medium set")
	assert.Eventually(t, func() bool {
		return redisClient.TTL(ctx, medium).Val() > 0
	}, time.Second, 10*time.Millisecond, "the medium set a high call spilled into has no TTL")
}

// A GitHub primary limit whose reset is already in the past (clock skew, a
// delayed response) asks for no wait, never a negative one.
func TestEdge_GitHubPrimaryResetInThePastIsNotNegative(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{
		"X-Ratelimit-Remaining": {"0"},
		"X-Ratelimit-Reset":     {strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)},
	}}
	kind, wait := ratelimiter.GitHubAdapter{}.ClassifyError(resp)
	assert.Equal(t, ratelimiter.PrimaryRateLimit, kind)
	assert.GreaterOrEqual(t, wait, time.Duration(0))

	resp.Header = http.Header{"Retry-After": {"-30"}}
	_, wait = ratelimiter.GitHubAdapter{}.ClassifyError(resp)
	assert.GreaterOrEqual(t, wait, time.Duration(0))
}

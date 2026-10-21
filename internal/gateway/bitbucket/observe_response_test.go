// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
)

func init() {
	logger.Init("test", nil)
}

// newObserveResponseDoer wires the package-private rateLimitedDoer directly
// so the test does not need the gateway config to construct the full gateway
// via NewBitbucketVendorGateway.
func newObserveResponseDoer(bl *ratelimiter.BudgetLimiter) *rateLimitedDoer {
	return &rateLimitedDoer{
		inner:         nil,
		rateLimiter:   nil,
		group:         "bitbucket-rest",
		budgetLimiter: bl,
	}
}

func TestObserveResponse_SeedsBudgetSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})

	resetAt := time.Now().Add(30 * time.Minute).Unix()
	resp := &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"X-Ratelimit-Remaining": []string{"8500"},
			"X-Ratelimit-Limit":     []string{"10000"},
			"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
		},
	}

	const orgID = "org-premium-1"
	ctx := reqctx.WithOrgID(context.Background(), orgID)
	d := newObserveResponseDoer(bl)
	d.observeResponse(ctx, resp)

	limit, observedAt, ok, err := bl.GetLimit(ctx, orgID, ratelimiter.BucketDefault)
	require.NoError(t, err)
	require.True(t, ok, "expected snapshot seeded by observeResponse")
	assert.Equal(t, 10000, limit)
	assert.WithinDuration(t, time.Now(), observedAt, 5*time.Second)
}

func TestObserveResponse_NoOpWithoutBudgetLimiter(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	resetAt := time.Now().Add(30 * time.Minute).Unix()
	resp := &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"X-Ratelimit-Remaining": []string{"500"},
			"X-Ratelimit-Limit":     []string{"1000"},
			"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
		},
	}

	ctx := reqctx.WithOrgID(context.Background(), "org-no-bl")
	d := newObserveResponseDoer(nil)
	d.observeResponse(ctx, resp) // must not panic

	keys, err := rc.Keys(ctx, "rate_budget:*").Result()
	require.NoError(t, err)
	assert.Empty(t, keys, "expected no budget keys when BudgetLimiter is nil")
}

func TestObserveResponse_SkipsWhenOrgMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	bl := ratelimiter.NewBudgetLimiter(rc, ratelimiter.BitbucketAdapter{})

	resetAt := time.Now().Add(30 * time.Minute).Unix()
	resp := &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"X-Ratelimit-Remaining": []string{"500"},
			"X-Ratelimit-Limit":     []string{"1000"},
			"X-Ratelimit-Reset":     []string{strconv.FormatInt(resetAt, 10)},
		},
	}

	d := newObserveResponseDoer(bl)
	d.observeResponse(context.Background(), resp) // no org_id on ctx

	keys, err := rc.Keys(context.Background(), "rate_budget:*").Result()
	require.NoError(t, err)
	assert.Empty(t, keys, "expected no budget keys when ctx carries no org_id")
}

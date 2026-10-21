// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// denyingLimiter knows every resource and admits nothing.
type denyingLimiter struct{ fakeResourceLimiter }

func (denyingLimiter) Acquire(context.Context, string, string) (bool, error) { return false, nil }

// A local rejection keeps the 30s floor (plus jitter) for asynq's retry of the
// failed task, but tells a run that paces itself in place to wait only about
// one token of the rejecting bucket. The floor alone made a paced run sleep
// 30-45s for a bucket that refills in a second.
func TestLocalRejectionSizesThePacingWaitToTheBucket(t *testing.T) {
	const fast, slow = "fast-rest", "slow-rest"
	cfg, err := buildRateLimitConfig(map[string]gateway.APIEndpointConfig{
		fast: {Group: fast, RequestsPerSecond: 1000},                                                         // 1000 per 1s
		slow: {Group: slow, RequestsPerSecond: 0.5556, OperationShares: map[string]float64{"tiny_op": 0.04}}, // 33/min; 1/min slice
	})
	require.NoError(t, err)
	lim := denyingLimiter{fakeResourceLimiter{fast: true, slow: true, operationResource(slow, "tiny_op"): true, operationResource(slow, operationShareRemainder): true}}

	for _, tc := range []struct {
		name, group, resource string
		pacingFrom            time.Duration
		retryFrom             time.Duration
	}{
		{"one-second window", fast, fast, time.Second, localLimiterRetryAfter},
		{"per-minute group", slow, operationResource(slow, operationShareRemainder), 2 * time.Second, localLimiterRetryAfter},
		{"one-token-a-minute slice", slow, operationResource(slow, "tiny_op"), 60 * time.Second, 60 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &rateLimitedDoer{rateLimiter: lim, rlCfg: cfg, group: tc.group}
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://unused", nil)
			if tc.resource == operationResource(slow, "tiny_op") {
				req = req.WithContext(reqctx.WithOperationID(req.Context(), "tiny_op"))
			}
			_, err := d.Do(req.Context(), req)
			require.ErrorIs(t, err, sdk.ErrLocalRateLimit)
			var se *sdk.SyncError
			require.True(t, errors.As(err, &se))

			retry := time.Duration(se.RetryAfterSeconds) * time.Second
			assert.GreaterOrEqual(t, retry, tc.retryFrom, "asynq's retry keeps its floor")
			assert.LessOrEqual(t, retry, tc.retryFrom+localLimiterRetryJitter)

			assert.GreaterOrEqual(t, se.LocalWait, tc.pacingFrom, "never sooner than one token")
			assert.Less(t, se.LocalWait, tc.pacingFrom+localLimiterPacingJitter, "not the asynq floor")
		})
	}
}

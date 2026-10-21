// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A local rejection carries two waits: RetryAfterSeconds for the scheduler's
// retry of a failed task (floored at 30s by the Bitbucket gateway) and LocalWait
// for how long the bucket needs before it admits again. Pacing sleeps the
// second. Sleeping the first held a run asleep for 30-45s per rejection of a
// one-second window, so it could afford only a handful per run.
func TestExecuteWaitingForLocalCapacity_SleepsTheInRunWait(t *testing.T) {
	rejection := &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		Cause:             fmt.Errorf("rate limit exceeded for group bitbucket-rest: %w", sdk.ErrLocalRateLimit),
		Retryable:         true,
		RetryAfterSeconds: 30,
		LocalWait:         50 * time.Millisecond,
	}
	gw := &scriptedGateway{results: []gwResult{{err: rejection}, {err: rejection}, {resp: wrapperspb.String("ok")}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	budget := &PacingBudget{}
	start := time.Now()
	resp, err := ExecuteWaitingForLocalCapacity(ctx, gw, wrapperspb.String("req"), budget, "bitbucket", "fetch_commits", "org-test")
	require.NoError(t, err)
	assert.Equal(t, "ok", resp.(*wrapperspb.StringValue).GetValue())
	assert.Equal(t, 3, gw.calls)
	assert.Less(t, time.Since(start), 5*time.Second, "slept the bucket's wait, not the asynq floor")
	assert.Equal(t, 100*time.Millisecond, budget.spent, "the budget is charged the waits actually slept")
}

// Without a LocalWait the gateway's retry hint still sizes the wait.
func TestLocalLimiterWaitFallsBackToTheRetryHint(t *testing.T) {
	err := &sdk.SyncError{Kind: sdk.ErrKindRateLimit, Cause: sdk.ErrLocalRateLimit, RetryAfterSeconds: 7}
	assert.Equal(t, 7*time.Second, localLimiterWait(err))
	assert.Equal(t, localLimiterWaitFloor, localLimiterWait(sdk.ErrLocalRateLimit))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package pacing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The per-run cap is inclusive: a wait that brings the spend to exactly the cap
// is affordable, one nanosecond more is not. The deadline must leave the wait
// plus the safety margin, strictly.
func TestEdge_CanAffordBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	b := &PacingBudget{spent: maxPacingWaitPerRun - time.Minute}
	ok, _ := b.canAfford(ctx, time.Minute)
	assert.True(t, ok, "spending exactly the cap is allowed")
	ok, reason := b.canAfford(ctx, time.Minute+time.Nanosecond)
	assert.False(t, ok)
	assert.Equal(t, giveupReasonBudgetCap, reason)

	near, cancelNear := context.WithTimeout(context.Background(), pacingSafetyMargin+time.Minute)
	defer cancelNear()
	ok, reason = (&PacingBudget{}).canAfford(near, time.Minute)
	assert.False(t, ok, "a wait that leaves exactly the margin (less elapsed time) is refused")
	assert.Equal(t, giveupReasonDeadlineTooNear, reason)

	// The cap is checked before the deadline, so a deadline-less ctx over the cap
	// reports the cap.
	ok, reason = (&PacingBudget{spent: maxPacingWaitPerRun}).canAfford(context.Background(), time.Second)
	assert.False(t, ok)
	assert.Equal(t, giveupReasonBudgetCap, reason)
}

// A zero or negative hint is no hint: the floor applies, never a zero or
// negative sleep that would spin the loop against the limiter.
func TestEdge_LocalLimiterWaitIgnoresNonPositiveHints(t *testing.T) {
	for _, s := range []int{0, -5} {
		err := fmt.Errorf("wrapped: %w", &sdk.SyncError{Kind: sdk.ErrKindRateLimit, Cause: sdk.ErrLocalRateLimit, RetryAfterSeconds: s})
		assert.Equal(t, localLimiterWaitFloor, localLimiterWait(err), "hint %d", s)
	}
	assert.Equal(t, localLimiterWaitFloor, localLimiterWait(sdk.ErrLocalRateLimit), "a bare sentinel carries no hint")
	assert.Equal(t, 3*time.Second, localLimiterWait(&sdk.SyncError{RetryAfterSeconds: 3}))
}

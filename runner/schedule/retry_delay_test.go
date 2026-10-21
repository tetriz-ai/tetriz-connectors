// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestRetryDelayFromVendorHintTakesMaxOfHintAndDefault pins the rule that
// drives ADR-0004 §4: when a vendor's Retry-After hint is larger than the
// exponential default we use the hint; when the default is larger (because
// we've retried many times) we use the default. We never go below the
// vendor's wait window AND we never collapse the exponential schedule.
func TestRetryDelayFromVendorHintTakesMaxOfHintAndDefault(t *testing.T) {
	tests := []struct {
		name           string
		retryAttempt   int
		retryAfterSecs int
		// minDelay / maxDelay bound the expected output. defaultRetryDelay
		// adds 0-10s jitter so we can't pin an exact value — only a range.
		minDelay time.Duration
		maxDelay time.Duration
	}{
		{
			name:           "vendor hint larger than default → use hint, jittered",
			retryAttempt:   0,
			retryAfterSecs: 120, // 2 min hint vs default 1s + jitter
			minDelay:       120 * time.Second,
			maxDelay:       120*time.Second + retryJitterWindow,
		},
		{
			name:           "default larger than vendor hint → use default",
			retryAttempt:   8, // 2^8 = 256s base
			retryAfterSecs: 30,
			minDelay:       256 * time.Second,
			maxDelay:       256*time.Second + retryJitterWindow,
		},
		{
			name:           "no vendor hint → default + jitter only",
			retryAttempt:   0,
			retryAfterSecs: 0,
			minDelay:       time.Second,
			maxDelay:       time.Second + retryJitterWindow,
		},
		{
			name:           "vendor hint equals default → use default range",
			retryAttempt:   2, // 2^2 = 4s
			retryAfterSecs: 4,
			minDelay:       4 * time.Second,
			maxDelay:       4*time.Second + retryJitterWindow,
		},
		{
			name:           "huge attempt count clamps at 2^12 (4096s ≈ 68 min) ceiling",
			retryAttempt:   50,
			retryAfterSecs: 0,
			minDelay:       4096 * time.Second,
			maxDelay:       4096*time.Second + retryJitterWindow,
		},
		{
			name:           "negative attempt count treated as 0",
			retryAttempt:   -3,
			retryAfterSecs: 0,
			minDelay:       time.Second,
			maxDelay:       time.Second + retryJitterWindow,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var err error
			if tt.retryAfterSecs > 0 {
				err = &sdk.SyncError{
					Kind:              sdk.ErrKindRateLimit,
					RetryAfterSeconds: tt.retryAfterSecs,
				}
			} else {
				err = errors.New("plain error, no vendor hint")
			}

			got := retryDelayFromVendorHint(tt.retryAttempt, err, nil)
			assert.GreaterOrEqual(t, got, tt.minDelay, "below floor")
			assert.LessOrEqual(t, got, tt.maxDelay, "above ceiling")
		})
	}
}

// TestRetryDelayFromVendorHintUnwrapsWrappedSyncError verifies that the asynq
// path can recover RetryAfterSeconds even after failRun has wrapped the
// SyncError with fmt.Errorf %w (the case ADR-0004 §4
// covers). If errors.As stops working here, vendor hints stop reaching asynq.
func TestRetryDelayFromVendorHintUnwrapsWrappedSyncError(t *testing.T) {
	inner := &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		RetryAfterSeconds: 90,
	}
	// Simulate failRun's actual wrap shape.
	var wrapped error = wrappedErr{wrapped: inner, msg: "orchestrator: operation failed [rate_limit]"}

	got := retryDelayFromVendorHint(0, wrapped, nil)
	assert.GreaterOrEqual(t, got, 90*time.Second, "vendor hint should win once unwrapped")
	assert.LessOrEqual(t, got, 90*time.Second+retryJitterWindow, "hint should be jittered, not inflated")
}

// TestRetryDelayJittersIdenticalVendorHints pins the invariant: identical
// hints must not produce identical delays. Every task one incident throttled
// receives the same RetryAfterSeconds, so a bare hint would wake them together.
func TestRetryDelayJittersIdenticalVendorHints(t *testing.T) {
	err := &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		RetryAfterSeconds: 120,
	}

	seen := make(map[time.Duration]struct{})
	for range 200 {
		seen[retryDelayFromVendorHint(0, err, nil)] = struct{}{}
	}

	assert.Greater(t, len(seen), 1,
		"200 identical hints produced a single delay — the herd would return together")
}

// wrappedErr is a tiny stand-in for fmt.Errorf("...%w...") that exercises the
// errors.As code path without depending on fmt's exact wrapping shape.
type wrappedErr struct {
	wrapped error
	msg     string
}

func (w wrappedErr) Error() string { return w.msg + ": " + w.wrapped.Error() }
func (w wrappedErr) Unwrap() error { return w.wrapped }

// The backoff doubles up to attempt 12 and stays there: attempts 12 and 13
// wait the same 2^12 seconds plus jitter, and attempt 11 half that.
func TestDefaultRetryDelayCapsAtAttempt12(t *testing.T) {
	for i := 0; i < 50; i++ {
		for _, n := range []int{12, 13, 40} {
			d := defaultRetryDelay(n)
			assert.GreaterOrEqual(t, d, 4096*time.Second, "attempt %d", n)
			assert.Less(t, d, 4096*time.Second+retryJitterWindow, "attempt %d", n)
		}
		assert.Less(t, defaultRetryDelay(11), 4096*time.Second)
	}
}

// A vendor hint larger than any jittered backoff always wins, and the retry
// waits at least that long.
func TestRetryDelayVendorHintAboveBackoffAlwaysWins(t *testing.T) {
	const n = 4 // backoff 16s plus up to 10s of jitter
	hint := 16*time.Second + retryJitterWindow + time.Second
	err := &sdk.SyncError{Kind: sdk.ErrKindRateLimit, RetryAfterSeconds: int(hint / time.Second)}
	for i := 0; i < 200; i++ {
		got := retryDelayFromVendorHint(n, err, nil)
		require.GreaterOrEqual(t, got, hint)
		require.Less(t, got, hint+retryJitterWindow)
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/hibiken/asynq"
)

// retryJitterWindow matches asynq.DefaultRetryDelayFunc's jitter (uniform
// 0..10s added to the exponential base). Without jitter, every worker that
// failed at the same instant retries together — thundering herd against the
// same vendor right after a 429 burst. With jitter, retries spread out
// across the window and the next 429 wave is much smaller.
const retryJitterWindow = 10 * time.Second

// maxRetryAfterHint caps a vendor's Retry-After hint: a hint above it waits
// one hour (plus jitter). It matches the cap the built-in gateways apply when
// they read the header, so a custom gateway's hint is bounded the same way,
// and a hostile or broken hint of days cannot park a task for days.
const maxRetryAfterHint = time.Hour

// defaultRetryDelay mirrors asynq's stock exponential schedule
// (2^n + uniform-jitter seconds, n = retry count). We re-implement the floor
// here rather than calling asynq.DefaultRetryDelayFunc because we want to
// take the *max* of the vendor hint and the default, not return the vendor
// hint blindly — a vendor that always replies "1 second" would defeat the
// exponential schedule and amplify a thundering herd.
func defaultRetryDelay(n int) time.Duration {
	if n < 0 {
		n = 0
	}
	if n > 12 { // cap to keep the math safe; 2^12 = 4096s ≈ 68 min
		n = 12
	}
	base := time.Duration(math.Pow(2, float64(n))) * time.Second
	//nolint:gosec // jitter is a backoff spreader, not a security primitive
	return base + time.Duration(rand.Int63n(int64(retryJitterWindow)))
}

// RetryAfterHinter is an error that carries the vendor's requested wait
// before a retry. sdk.SyncError implements it.
type RetryAfterHinter interface {
	RetryAfter() time.Duration
}

// retryDelayFromVendorHint is asynq's RetryDelayFunc for every queue. A task
// whose error carries a RetryAfterHinter waits at least the hinted duration,
// capped at maxRetryAfterHint, plus jitter; every other task waits the
// jittered exponential default (ADR-0004 §4).
//
// Signature matches asynq.RetryDelayFunc:
//
//	func(n int, e error, t *asynq.Task) time.Duration
//
// n is the retry attempt count starting at 0 for the second attempt.
func retryDelayFromVendorHint(n int, err error, _ *asynq.Task) time.Duration {
	base := defaultRetryDelay(n)
	var hinter RetryAfterHinter
	if errors.As(err, &hinter) && hinter.RetryAfter() > 0 {
		hint := min(hinter.RetryAfter(), maxRetryAfterHint)
		if hint > base {
			// Jittered like the exponential base below it: a hint is identical
			// for every task one incident throttled, so returning it bare
			// re-synchronises the herd. This branch wins while the hint exceeds
			// 2^n seconds — the early retries, where the herd is densest.
			//nolint:gosec // jitter is a backoff spreader, not a security primitive
			return hint + time.Duration(rand.Int63n(int64(retryJitterWindow)))
		}
	}
	return base
}

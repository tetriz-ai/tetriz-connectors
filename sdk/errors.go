// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// ErrLocalRateLimit wraps a rejection by the local rate limiter, before the
// request reached the vendor. Its Kind is ErrKindRateLimit, like a vendor 429
// or an open circuit. Match it with errors.Is. A caller may wait for capacity
// and continue.
var ErrLocalRateLimit = errors.New("local rate limiter: no capacity in window")

// SyncErrorKind categorizes sync errors for routing to the correct error handling policy.
type SyncErrorKind int

const (
	ErrKindAuth          SyncErrorKind = iota // 401/403, token expired/revoked
	ErrKindRateLimit                          // 429, quota exceeded
	ErrKindNotFound                           // 404, resource gone
	ErrKindTimeout                            // A connector reports a vendor call that timed out
	ErrKindVendorError                        // 5xx from vendor
	ErrKindConfigInvalid                      // Bad connector config
	ErrKindSinkFailure                        // Could not emit to queue
	ErrKindInternal                           // Anything else, including a plain error from connector code
	ErrKindInvalidInput                       // Caller-supplied args failed validation (400-class)
	ErrKindCtxExhausted                       // The run's context ended; the error is a context error
	ErrKindAbandoned                          // The run's lease expired: the process running it stopped
)

var syncErrorKindNames = [...]string{
	ErrKindAuth:          "auth",
	ErrKindRateLimit:     "rate_limit",
	ErrKindNotFound:      "not_found",
	ErrKindTimeout:       "timeout",
	ErrKindVendorError:   "vendor_error",
	ErrKindConfigInvalid: "config_invalid",
	ErrKindSinkFailure:   "sink_failure",
	ErrKindInternal:      "internal",
	ErrKindInvalidInput:  "invalid_input",
	ErrKindCtxExhausted:  "ctx_exhausted",
	ErrKindAbandoned:     "abandoned",
}

// String returns a human-readable name for the error kind.
func (k SyncErrorKind) String() string {
	if k >= 0 && int(k) < len(syncErrorKindNames) {
		return syncErrorKindNames[k]
	}
	return fmt.Sprintf("unknown(%d)", int(k))
}

// SyncError wraps errors with connector context for observability.
type SyncError struct {
	ConnectorID    ConnectorID
	OperationID    OperationID
	OrganizationID string
	Kind           SyncErrorKind
	Cause          error
	Retryable      bool
	HTTPStatus     int // populated when error came from vendor API

	// Terminal marks a failure the next identical attempt cannot fix — the fault
	// is in the task input, not a transient state. Distinct from Retryable=false,
	// which a vendor error sets for "this attempt failed" without claiming
	// re-execution is futile; set Terminal only when re-execution is provably
	// pointless. The scheduler maps Terminal to asynq.SkipRetry to fail once
	// instead of exhausting MaxRetry.
	Terminal bool

	// RetryAfterSeconds is the vendor's suggested wait before the caller
	// retries. Populated from the Retry-After header on 429 responses (or
	// from body-level hints like Slack's ok:false / error:ratelimited +
	// its accompanying header). Zero means "no guidance - caller decides".
	// The scheduler waits at least this long before retrying.
	RetryAfterSeconds int

	// LocalWait is set only on a rejection by our own rate limiter
	// (ErrLocalRateLimit): about how long until the rejecting bucket can admit
	// again. A run that paces itself sleeps this long and retries in place.
	// RetryAfterSeconds stays the delay before the scheduler retries a failed
	// task, which can be longer. Zero means unset; pacing then falls back to
	// RetryAfterSeconds.
	LocalWait time.Duration
}

func (e *SyncError) Error() string {
	// Only the names that are set: a SyncError built below the connector (in a
	// gateway) carries none, and empty "connector=" fields read as a bug.
	msg := fmt.Sprintf("sync error [%s]", e.Kind)
	for _, f := range []struct{ k, v string }{
		{"connector", string(e.ConnectorID)}, {"operation", string(e.OperationID)}, {"org", e.OrganizationID},
	} {
		if f.v != "" {
			msg += " " + f.k + "=" + f.v
		}
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	if e.RetryAfterSeconds > 0 {
		msg += fmt.Sprintf(" (retry after %ds)", e.RetryAfterSeconds)
	}
	return msg
}

func (e *SyncError) Unwrap() error {
	return e.Cause
}

// RetryAfter returns RetryAfterSeconds as a duration, so a scheduler can read
// the vendor's wait without importing this package.
// A hint too large for a time.Duration is clamped to the longest one rather
// than overflowing to a negative wait.
func (e *SyncError) RetryAfter() time.Duration {
	return SecondsToDuration(e.RetryAfterSeconds)
}

// SecondsToDuration converts whole seconds to a duration, clamped to
// [0, the longest duration] so a vendor's huge hint cannot overflow.
func SecondsToDuration(seconds int) time.Duration {
	const maxSeconds = int64(math.MaxInt64 / int64(time.Second))
	switch s := int64(seconds); {
	case s <= 0:
		return 0
	case s > maxSeconds:
		return time.Duration(math.MaxInt64)
	default:
		return time.Duration(s) * time.Second
	}
}

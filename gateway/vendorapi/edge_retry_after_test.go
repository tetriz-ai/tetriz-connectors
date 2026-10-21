// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A Retry-After too large for an int is still a vendor asking for a long
// wait. It must clamp to the one-hour cap like any other oversized value,
// not collapse to 0 ("no hint"), which would hot-retry into the throttle.
func TestEdge_RetryAfterOverflowClampsToCap(t *testing.T) {
	for _, v := range []string{"99999999999999999999", "9223372036854775808"} {
		err := ClassifyHTTPErrorWithHeaders(http.StatusTooManyRequests, nil, http.Header{"Retry-After": {v}})
		require.NotNil(t, err)
		assert.Equal(t, sdk.ErrKindRateLimit, err.Kind)
		assert.Equal(t, maxRetryAfterSeconds, err.RetryAfterSeconds, "Retry-After %q", v)
	}
}

// An HTTP-date Retry-After in the past, or a fractional/whitespace-padded
// value, never yields a negative or bogus wait.
func TestEdge_RetryAfterPastDateAndJunkAreZero(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	for _, v := range []string{past, "1.5", "-0", "0x10", "Wed, 99 Foo 2026 00:00:00 GMT"} {
		got := parseRetryAfter(http.Header{"Retry-After": {v}})
		assert.GreaterOrEqual(t, got, 0, v)
		assert.LessOrEqual(t, got, maxRetryAfterSeconds, v)
	}
	assert.Equal(t, 0, parseRetryAfter(http.Header{"Retry-After": {past}}))
	// A far-future HTTP-date is clamped, not overflowed.
	far := time.Now().Add(100 * 365 * 24 * time.Hour).UTC().Format(http.TimeFormat)
	assert.Equal(t, maxRetryAfterSeconds, parseRetryAfter(http.Header{"Retry-After": {far}}))
}

// Linear's reset header in the past (clock skew) is 0, and an absurd future
// epoch is clamped rather than overflowing.
func TestEdge_LinearResetHeaderPastAndHuge(t *testing.T) {
	body := []byte(`{"errors":[{"extensions":{"code":"RATELIMITED"}}]}`)
	past := http.Header{"X-Ratelimit-Requests-Reset": {"1000"}}
	err := ClassifyHTTPErrorWithHeaders(http.StatusBadRequest, body, past)
	assert.Equal(t, sdk.ErrKindRateLimit, err.Kind)
	assert.Equal(t, 0, err.RetryAfterSeconds)

	huge := http.Header{"X-Ratelimit-Requests-Reset": {"9223372036854775807"}}
	err = ClassifyHTTPErrorWithHeaders(http.StatusBadRequest, body, huge)
	assert.Equal(t, maxRetryAfterSeconds, err.RetryAfterSeconds)
}

// A 401 is never a throttle, even with a Retry-After header and a
// rate-limit-looking body: it stays terminal auth.
func TestEdge_401WithRetryAfterStaysTerminalAuth(t *testing.T) {
	err := ClassifyHTTPErrorWithHeaders(http.StatusUnauthorized, []byte("API rate limit exceeded"), http.Header{"Retry-After": {"30"}})
	assert.Equal(t, sdk.ErrKindAuth, err.Kind)
	assert.True(t, err.Terminal)
	assert.False(t, err.Retryable)
	assert.Equal(t, 0, err.RetryAfterSeconds)
}

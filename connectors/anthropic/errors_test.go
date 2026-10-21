// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestLatestAvailableDay is the connector's sharpest decision, and both answers are
// dangerous in opposite directions. Read a genuine not-ready 400 as a failure and the
// operation fails every day the vendor is late; read any other 400 as not-ready and the walk
// checkpoints an organisation as caught up on a day it never read, quietly, forever.
func TestLatestAvailableDay(t *testing.T) {
	t.Parallel()

	requested := time.Date(2026, time.August, 26, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		err      error
		cursor   bool
		wantDay  time.Time
		wantHeld bool
	}{
		{
			name:     "a day earlier than the request is the aggregation lag",
			err:      vendorErr(http.StatusBadRequest, `{"error":{"message":"data available through 2026-08-25"}}`),
			wantDay:  time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC),
			wantHeld: true,
		},
		{
			// The message names the REQUESTED day first. Taking the first date found would
			// resolve to the request itself and take the loud path on a normal condition.
			name:     "the available day is chosen even when the requested day is named first",
			err:      vendorErr(http.StatusBadRequest, `{"error":{"message":"2026-08-26 is not yet aggregated; data available through 2026-08-25"}}`),
			wantDay:  time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC),
			wantHeld: true,
		},
		{
			// Two dates, both earlier than the request. Taking the FIRST would resolve to the
			// span's start and hold the walk a further five days back than the vendor said,
			// re-reading days it has actually finished.
			name:     "the latest of several earlier days is the available one",
			err:      vendorErr(http.StatusBadRequest, `{"error":{"message":"aggregated 2026-08-20 through 2026-08-25"}}`),
			wantDay:  time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC),
			wantHeld: true,
		},
		{
			// A range or account-age rejection carries a date too. An aggregation lag is
			// days, not months, so a far-behind day must not buy a quiet hold.
			name: "a date far behind the request is some other rejection, not a lag",
			err:  vendorErr(http.StatusBadRequest, `{"error":{"message":"starting_at must not precede 2026-01-01"}}`),
		},
		{
			name: "a day at or after the request cannot be a lag",
			err:  vendorErr(http.StatusBadRequest, `{"error":{"message":"data available through 2026-08-27"}}`),
		},
		{
			// Mid-pagination the vendor is rejecting the cursor or a changed parameter. Both
			// cases can carry a date, so the date alone cannot tell them apart.
			name:   "a 400 while paginating is a query defect, whatever date it names",
			err:    vendorErr(http.StatusBadRequest, `{"error":{"message":"data available through 2026-08-25"}}`),
			cursor: true,
		},
		{
			name: "a 400 naming no date is not interpretable and must not buy a quiet hold",
			err:  vendorErr(http.StatusBadRequest, `{"error":{"message":"invalid request"}}`),
		},
		{
			name: "a date-shaped value that is not a real date",
			err:  vendorErr(http.StatusBadRequest, `{"error":{"message":"through 2026-13-45"}}`),
		},
		{name: "another status is not this signal", err: vendorErr(http.StatusForbidden, `{"error":"nope"}`)},
		{name: "a 500 is not this signal", err: vendorErr(http.StatusInternalServerError, `{"error":"boom"}`)},
		{name: "an error that is not a SyncError at all", err: errors.New("dial tcp: timeout")},
		{name: "no error", err: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			day, held := latestAvailableDay(context.Background(), tt.err, requested, tt.cursor)
			assert.Equal(t, tt.wantHeld, held)
			if tt.wantHeld {
				assert.Equal(t, tt.wantDay, day)
			}
		})
	}
}

// TestIsExpiredCursor pins the one status that means "restart this unit" rather than "fail".
// Widening it would turn a real failure into a silent restart loop; narrowing it would fail a
// run the vendor told us how to recover from.
func TestIsExpiredCursor(t *testing.T) {
	t.Parallel()

	assert.True(t, isExpiredCursor(vendorErr(http.StatusGone, `{"error":"cursor expired"}`)))

	for _, other := range []int{
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusTooManyRequests, http.StatusInternalServerError,
	} {
		assert.Falsef(t, isExpiredCursor(vendorErr(other, "x")),
			"HTTP %d must not be read as an expired cursor", other)
	}
	assert.False(t, isExpiredCursor(errors.New("dial tcp: timeout")))
	assert.False(t, isExpiredCursor(nil))
}

// TestVendorErrFixtureMatchesProduction guards the fixture itself. These tests are only
// evidence about production if the error they feed in is the error production builds — the
// classifier decides Kind and Terminal and formats the Cause, and the 400 parser reads that
// Cause string.
func TestVendorErrFixtureMatchesProduction(t *testing.T) {
	t.Parallel()

	var syncErr *sdk.SyncError
	require.ErrorAs(t, vendorErr(http.StatusBadRequest, `{"type":"error","error":{"type":"invalid_request_error","message":"data available through 2026-08-25"}}`), &syncErr)
	assert.Equal(t, http.StatusBadRequest, syncErr.HTTPStatus)
	require.Error(t, syncErr.Cause)
	assert.Contains(t, syncErr.Cause.Error(), "HTTP 400",
		"the status prefix is part of the message the 400 parser reads")
	assert.Contains(t, syncErr.Cause.Error(), "2026-08-25", "the vendor's error message survives into the message")

	require.ErrorAs(t, vendorErr(http.StatusUnauthorized, "dead key"), &syncErr)
	assert.True(t, syncErr.Terminal, "a 401 is terminal, and a walk must not retry past it")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// datesInMessage finds the calendar dates in a vendor message. The named day is the only
// authoritative statement of the aggregation lag; guessing at it from the clock makes an
// operation either miss a day or hammer a 400 every run.
var datesInMessage = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)

// latestAvailableDay reads the most recent available day out of a vendor 400.
//
// The discrimination matters more than the parse, and the design separates the two cases
// explicitly: a 400 naming the latest available day is not-ready and holds the cursor with
// no failure, while a 400 on a CURSOR is a parameter that changed mid-sequence — a defect in
// our own code, which must fail the run. Both can carry a date in the message, so the date alone
// cannot tell them apart.
//
// requestedDay and cursorInFlight are what separate them. A not-ready answer names a day
// EARLIER than the one asked for and arrives on a fresh request; a 400 while paginating, or
// one naming a day at or after the request, is the defect case and returns false so the
// caller fails loudly.
//
// Returns false for anything else too, including a 400 that names no date: a 400 we cannot
// interpret must not be silently converted into "stop here, all is well", which would
// checkpoint an organisation as caught-up on a day it never read.
func latestAvailableDay(
	ctx context.Context, err error, requestedDay time.Time, cursorInFlight bool,
) (time.Time, bool) {
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) || syncErr.HTTPStatus != http.StatusBadRequest {
		return time.Time{}, false
	}
	if syncErr.Cause == nil {
		return time.Time{}, false
	}
	if cursorInFlight {
		// Mid-pagination: the vendor is rejecting the cursor or a changed parameter, not
		// reporting aggregation lag.
		logger.WarnWithCtx(ctx, "claude-code: vendor 400 while paginating — treating as a query defect, not aggregation lag",
			zap.Time("requested_day", requestedDay))
		return time.Time{}, false
	}
	// Every date in the message is considered, not just the first. A message naming the
	// requested day before the available one ("2026-08-26 is not yet aggregated; data
	// available through 2026-08-25") would otherwise resolve to the requested day and take
	// the loud-failure path on an ordinary not-ready answer. Choosing the LATEST candidate
	// that is strictly earlier than the request also rejects a message whose only earlier
	// date is a floor or account-creation complaint far in the past, because such a day
	// would fail the lag bound below.
	//
	// The message text arrives through the vendor gateway's classifier, which keeps only the
	// vendor's error message fields of a JSON body (each cut to 200 bytes) and none of any
	// other body's text — a 400 whose date is not in those fields therefore takes the loud
	// path, which is the safe direction.
	var best time.Time
	for _, match := range datesInMessage.FindAllString(syncErr.Cause.Error(), -1) {
		day, parseErr := time.ParseInLocation(dateLayout, match, time.UTC)
		switch {
		case parseErr != nil:
			// The pattern matched but the value is not a real date. Logged rather than
			// dropped: it means the vendor's message format moved, and the next reader needs
			// to know that is why the walk started failing.
			logger.WarnWithCtx(ctx, "claude-code: vendor 400 named an unparseable date",
				zap.String("matched", match), zap.Error(parseErr))
		case !day.Before(requestedDay):
			// At or after the request, so it cannot be an aggregation-lag answer.
		case day.After(best):
			best = day
		}
	}
	if best.IsZero() {
		return time.Time{}, false
	}

	// An aggregation lag is days, not months. A named day far behind the request is some
	// other complaint that happens to carry a date — a range or account-age rejection — and
	// reading it as "stop here, all is well" would checkpoint the walk as caught up on a day
	// it never read, quietly, on every run.
	if requestedDay.Sub(best) > maxAggregationLag {
		logger.WarnWithCtx(ctx, "claude-code: vendor 400 named a day too far behind to be an aggregation lag",
			zap.Time("requested_day", requestedDay), zap.Time("named_day", best))
		return time.Time{}, false
	}
	return best, true
}

// maxAggregationLag bounds how far behind the request a named day may be and still be read as
// the vendor catching up rather than as a different rejection.
const maxAggregationLag = 31 * 24 * time.Hour

// isBadRequest reports a vendor 400. It exists so the two meanings a 400 carries can be counted
// apart at the call sites that know which one they are looking at — the discrimination itself is
// latestAvailableDay's job, not this predicate's.
func isBadRequest(err error) bool {
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.HTTPStatus == http.StatusBadRequest
}

// isExpiredCursor reports a 410, which the vendor uses for a pagination cursor that has
// expired. It is an instruction to restart that window from its first page rather than a
// failure: the window's rows are still there, only the cursor is gone.
func isExpiredCursor(err error) bool {
	var syncErr *sdk.SyncError
	return errors.As(err, &syncErr) && syncErr.HTTPStatus == http.StatusGone
}

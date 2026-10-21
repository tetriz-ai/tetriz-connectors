// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A fixed clock. Mid-afternoon deliberately: a run at 15:00 must still read only up to
// the end of the previous day, so an implementation that used "now" as the window end
// fails here rather than passing by accident at midnight.
var testNow = time.Date(2026, 8, 20, 15, 30, 0, 0, time.UTC)

// storedAt builds a resolved position the way production gets one: written as a stored
// position, then read back through parseUsagePosition. A test that hand-built the
// resolved shape could disagree with the parse and never notice.
func storedAt(t *testing.T, pos usagePosition) resolvedPosition {
	t.Helper()
	cur, err := newUsagePositionCursor(pos)
	require.NoError(t, err)
	out, present, err := parseUsagePosition(cur)
	require.NoError(t, err)
	require.True(t, present)
	return out
}

func TestResolveUsageWindow_FirstRunUsesTheLookback(t *testing.T) {
	t.Parallel()

	w := resolveUsageWindow(testNow, resolvedPosition{}, false, 90, DefaultTrailingOverlapDays)

	assert.Equal(t, reasonFirstRun, w.Reason, "an absent position means no sync has run")
	assert.Equal(t, "2026-05-22", w.Start.Format(dateLayout), "ninety days back from today's start")
	// The window ends when yesterday ended, never inside today: the current day is
	// unsettled, and the vendor's two usage endpoints disagree about it.
	assert.Equal(t, "2026-08-19", w.End.Format(dateLayout))
	assert.Equal(t, 23, w.End.UTC().Hour(), "the end is the last instant of the previous day")
}

func TestResolveUsageWindow_SteadyStateIsAFixedWidth(t *testing.T) {
	t.Parallel()

	// A position that has kept up: its floor is already past the trailing start.
	pos := usagePosition{FloorDate: "2026-08-19"}
	w := resolveUsageWindow(testNow, storedAt(t, pos), true, 90, DefaultTrailingOverlapDays)

	assert.Equal(t, reasonTrailing, w.Reason)
	assert.Equal(t, "2026-08-17", w.Start.Format(dateLayout),
		"the start is the trailing overlap back from today, never the stored floor")
	assert.Equal(t, "2026-08-19", w.End.Format(dateLayout))
}

// TestResolveUsageWindow_DormantOrgDoesNotWiden is the property that makes the sliding
// window safe. An organisation that stops using Cursor keeps re-reading a fixed span
// and getting nothing, which costs four small requests a day — where a high-water-mark
// anchor would pin the start at its last event and widen the window forever.
func TestResolveUsageWindow_DormantOrgDoesNotWiden(t *testing.T) {
	t.Parallel()

	// A floor from three months ago, as if the organisation had gone quiet: a stale
	// completed position.
	pos := usagePosition{FloorDate: "2026-05-20"}
	later := testNow.AddDate(0, 3, 0)

	w := resolveUsageWindow(later, storedAt(t, pos), true, 90, DefaultTrailingOverlapDays)
	span := w.End.Sub(w.Start)
	assert.Less(t, span, time.Duration(DefaultTrailingOverlapDays+2)*24*time.Hour,
		"a stale floor must not widen the window; the ends come from the clock")
}

func TestUsageWindow_Chunks(t *testing.T) {
	t.Parallel()

	t.Run("a steady-state window is one request", func(t *testing.T) {
		t.Parallel()
		w := resolveUsageWindow(testNow, storedAt(t, usagePosition{FloorDate: "2026-08-19"}), true, 90, DefaultTrailingOverlapDays)
		assert.Len(t, w.chunks(), 1, "slicing the daily window would multiply requests for nothing")
	})

	t.Run("a first run is sliced, and no slice runs past the window", func(t *testing.T) {
		t.Parallel()
		w := resolveUsageWindow(testNow, resolvedPosition{}, false, 90, DefaultTrailingOverlapDays)
		chunks := w.chunks()

		require.Greater(t, len(chunks), 1, "a ninety-day span is a bet on a limit the vendor never published")
		assert.Equal(t, w.Start, chunks[0].Start, "the first slice starts where the window does")
		last := chunks[len(chunks)-1]
		assert.Equal(t, w.End, last.End,
			"the last slice ends exactly at the window end, so a run never asks for the unsettled current day")

		// Contiguous and forward-only: a gap would silently skip days, and an overlap
		// would re-read them for no reason.
		for i := 1; i < len(chunks); i++ {
			assert.True(t, chunks[i].Start.After(chunks[i-1].Start), "slices must move forward")
			assert.WithinDuration(t, chunks[i-1].End, chunks[i].Start, time.Second,
				"slices must be contiguous, or a day is never requested")
		}
	})

	// The rule is a span comparison, so the exact boundary is where an off-by-one either
	// splits a window that fits in one request or merges one that does not.
	t.Run("the boundary is exactly one request wide", func(t *testing.T) {
		t.Parallel()
		start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

		exact := usageWindow{Start: start, End: start.AddDate(0, 0, chunkDays).Add(-time.Nanosecond), Reason: reasonFirstRun}
		assert.Len(t, exact.chunks(), 1, "a window exactly one request wide stays one request")

		oneMore := usageWindow{Start: start, End: start.AddDate(0, 0, chunkDays+1).Add(-time.Nanosecond), Reason: reasonFirstRun}
		assert.Len(t, oneMore.chunks(), 2, "one day wider must split")
	})
}

func TestUsagePositionRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("absent is not an error", func(t *testing.T) {
		t.Parallel()
		_, present, err := parseUsagePosition(nil)
		require.NoError(t, err, "no sync having run is a state, not a failure")
		assert.False(t, present)
	})

	t.Run("a covered run stores the day after the window", func(t *testing.T) {
		t.Parallel()
		w := resolveUsageWindow(testNow, storedAt(t, usagePosition{FloorDate: "2026-08-19"}), true, 90, DefaultTrailingOverlapDays)
		cur, err := newUsagePositionCursor(coveredPosition(w, w))
		require.NoError(t, err)

		pos, present, err := parseUsagePosition(cur)
		require.NoError(t, err)
		require.True(t, present)
		assert.Equal(t, "2026-08-20", pos.Floor.Format(dateLayout), "everything up to the window end is covered")
	})

	t.Run("a position that does not parse is an error, not a fresh start", func(t *testing.T) {
		t.Parallel()
		_, _, err := parseUsagePosition(&sdk.Cursor{Data: []byte(`{"floor_date":20260811}`)})
		require.Error(t, err)
		_, _, err = parseUsagePosition(&sdk.Cursor{Data: []byte(`{"floor_date":"11/08/2026"}`)})
		require.Error(t, err)
	})
}

// TestStartOfUTCDay_IsExplicitlyUTC guards the trap the consumer sets: it snaps a
// record's business date to start-of-day in whatever location the time carries, so a
// locally-located time yields a local midnight — a different storage partition and a
// different uniqueness key on a run that happened to be located differently.
func TestStartOfUTCDay_IsExplicitlyUTC(t *testing.T) {
	t.Parallel()

	// The same instant, expressed in a location whose calendar day differs from UTC's.
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	instant := time.Date(2026, 8, 20, 22, 0, 0, 0, time.UTC)

	assert.Equal(t, startOfUTCDay(instant), startOfUTCDay(instant.In(tokyo)),
		"the same instant must yield the same business date whatever location it arrives in")
	assert.Equal(t, time.UTC, startOfUTCDay(instant.In(tokyo)).Location(),
		"the result must carry UTC, or the consumer snaps it to a local midnight")
}

// TestUsageWindow_ResumedWindowIsChunkedToo is the case a reason-driven rule gets wrong.
// A resume widens the window without it being a first run. Sending that as one request
// would wedge the recovery: the request that fails is the one that would let the walk
// catch up, so every later run fails identically.
func TestUsageWindow_ResumedWindowIsChunkedToo(t *testing.T) {
	t.Parallel()

	// A backfill interrupted two months back.
	pos := usagePosition{FloorDate: "2026-06-20", BackfillUntil: "2026-08-19"}
	w := resolveUsageWindow(testNow, storedAt(t, pos), true, 90, DefaultTrailingOverlapDays)

	require.Equal(t, reasonResuming, w.Reason, "this is a resumed window, not a first run")
	chunks := w.chunks()
	require.Greater(t, len(chunks), 1,
		"a widened recovery window must be sliced; one request is the failure that wedges the recovery")

	for _, c := range chunks {
		assert.LessOrEqual(t, c.End.Sub(c.Start), time.Duration(chunkDays)*24*time.Hour,
			"no slice may exceed the per-request width, however the window came about")
	}
	assert.Equal(t, w.Start, chunks[0].Start)
	assert.Equal(t, w.End, chunks[len(chunks)-1].End)
}

// TestResolveUsageWindow_InterruptedBackfillResumes is the guard against silent
// permanent loss on the run most likely to fail.
//
// A widened window is chunked and checkpoints as it goes, so any interruption — a vendor
// outage, the execution deadline, a duplicate-identity failure — leaves a floor partway
// through. Reading that as steady state takes a three-day window and abandons every
// remaining day, with no error and nothing in the metrics.
// The recorded target is what separates this from a paused connector.
func TestResolveUsageWindow_InterruptedBackfillResumes(t *testing.T) {
	t.Parallel()

	// A first run covered its first two weeks and then stopped; the target says where it
	// was meant to reach.
	pos := storedAt(t, usagePosition{FloorDate: "2026-06-05", BackfillUntil: "2026-08-19"})
	w := resolveUsageWindow(testNow, pos, true, 90, DefaultTrailingOverlapDays)

	assert.Equal(t, reasonResuming, w.Reason)
	assert.Equal(t, "2026-06-05", w.Start.Format(dateLayout),
		"the resume must continue from the floor, not jump to the trailing window")
	require.Greater(t, len(w.chunks()), 1, "and it is still chunked")
}

// TestResolveUsageWindow_FloorEqualToTheTargetStillResumes pins the comparison. The
// floor is the first day NOT covered and the target the last day that WAS to be covered,
// so equality still leaves that day outstanding — a strict comparison dropped it, once
// per interrupted recovery whose remainder happened to land on a chunk boundary.
func TestResolveUsageWindow_FloorEqualToTheTargetStillResumes(t *testing.T) {
	t.Parallel()

	pos := storedAt(t, usagePosition{FloorDate: "2026-08-10", BackfillUntil: "2026-08-10"})
	w := resolveUsageWindow(testNow, pos, true, 90, DefaultTrailingOverlapDays)

	assert.Equal(t, reasonResuming, w.Reason)
	assert.Equal(t, "2026-08-10", w.Start.Format(dateLayout),
		"the day left over is the first one requested")
	assert.Equal(t, "2026-08-19", w.End.Format(dateLayout),
		"and the run carries on to the clock rather than stopping at a satisfied target")
}

// TestResolveUsageWindow_ResumeRunsToTheClock pins that a resume runs to the clock.
//
// Ending a resume at its recorded target looks tidier and loses days. The target does not
// advance across resumes while the clock does, so a backfill that finished a few days
// after it began left the floor short of the trailing window with its target already
// satisfied — which is exactly the shape the stale-floor branch reads as a pause. The days
// in between were then requested by no run, with nothing to show they were missing.
//
// Everything from the floor forward is genuinely uncovered, so there is no redundant work
// to avoid by stopping earlier.
func TestResolveUsageWindow_ResumeRunsToTheClock(t *testing.T) {
	t.Parallel()

	// Interrupted in May, and nothing ran for three months afterwards. Those three months
	// were never covered either, so they belong in this window.
	pos := storedAt(t, usagePosition{FloorDate: "2026-05-01", BackfillUntil: "2026-05-20"})
	w := resolveUsageWindow(testNow, pos, true, 90, DefaultTrailingOverlapDays)

	require.Equal(t, reasonResuming, w.Reason)
	assert.Equal(t, "2026-05-01", w.Start.Format(dateLayout))
	assert.Equal(t, "2026-08-19", w.End.Format(dateLayout),
		"a resume runs to the clock")

	// And the target it records is its own end, so it advances rather than staying frozen
	// at a date the floor will pass while the clock moves on.
	assert.Equal(t, "2026-08-19", coveredPosition(w, w).BackfillUntil)
}

// TestResolveUsageWindow_NoDayIsEverSkipped is the test shape that caught the gap above,
// and the only one that could: every single-window assertion passed while consecutive runs
// lost a day between them.
//
// It replays a first sync that covers only part of its window per run — the ordinary case
// against a per-minute rate budget and an execution deadline — one run per day, and checks
// that the union of every window requested leaves no hole between the first day asked for
// and the last.
func TestResolveUsageWindow_NoDayIsEverSkipped(t *testing.T) {
	t.Parallel()

	for _, overlap := range []int{1, 3, 7} {
		t.Run("overlap of "+string(rune('0'+overlap))+" days", func(t *testing.T) {
			t.Parallel()

			const chunksPerRun = 4
			covered := map[string]bool{}
			var pos resolvedPosition
			present := false
			day := testNow

			for range 40 {
				w := resolveUsageWindow(day, pos, present, 90, overlap)
				chunks := w.chunks()
				if len(chunks) > chunksPerRun {
					chunks = chunks[:chunksPerRun]
				}
				for _, c := range chunks {
					for d := startOfUTCDay(c.Start); !d.After(c.End); d = d.AddDate(0, 0, 1) {
						covered[d.Format(dateLayout)] = true
					}
				}
				pos = storedAt(t, coveredPosition(chunks[len(chunks)-1], w))
				present = true
				day = day.AddDate(0, 0, 1)
			}

			// No hole anywhere in the range actually requested.
			var days []string
			for d := range covered {
				days = append(days, d)
			}
			sort.Strings(days)
			first, err := time.ParseInLocation(dateLayout, days[0], time.UTC)
			require.NoError(t, err)
			last, err := time.ParseInLocation(dateLayout, days[len(days)-1], time.UTC)
			require.NoError(t, err)

			for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
				assert.True(t, covered[d.Format(dateLayout)],
					"%s falls inside the covered range but was never requested by any run", d.Format(dateLayout))
			}
		})
	}
}

// TestResolveUsageWindow_FinishedBackfillDoesNotResume is the other side. Once the
// recovery reached its target, a floor that has since fallen behind means the connector
// was paused — and re-reading a pause is the unbounded growth the sliding window exists
// to prevent.
func TestResolveUsageWindow_FinishedBackfillDoesNotResume(t *testing.T) {
	t.Parallel()

	pos := storedAt(t, usagePosition{FloorDate: "2026-05-21", BackfillUntil: "2026-05-20"})
	w := resolveUsageWindow(testNow, pos, true, 90, DefaultTrailingOverlapDays)

	assert.Equal(t, reasonStaleFloor, w.Reason)
	assert.Equal(t, "2026-08-17", w.Start.Format(dateLayout),
		"a finished recovery whose floor went stale is a pause, and the window stays narrow")
}

// TestResolveUsageWindow_PositionWithoutATargetStaysNarrow pins the migration case: a
// position written before the target existed carries none, and must read as finished
// rather than resuming — the safe direction, since the alternative re-reads a quarter
// for every organisation on the first tick after deploy.
func TestResolveUsageWindow_PositionWithoutATargetStaysNarrow(t *testing.T) {
	t.Parallel()

	pos := storedAt(t, usagePosition{FloorDate: "2026-05-20"})
	require.True(t, pos.BackfillUntil.IsZero(), "the premise: no target was stored")

	w := resolveUsageWindow(testNow, pos, true, 90, DefaultTrailingOverlapDays)
	assert.Equal(t, reasonStaleFloor, w.Reason)
	assert.Equal(t, "2026-08-17", w.Start.Format(dateLayout))
}

// TestBackfillTarget_RecordedOnEveryWindow pins the half that makes resuming possible at
// all. Every window records where it meant to reach, with no test for which kind of window
// it is — deciding that per window would let only a first run record a target, and an
// interrupted recovery would be left unrecognisable.
func TestBackfillTarget_RecordedOnEveryWindow(t *testing.T) {
	t.Parallel()

	t.Run("a first run records where it was meant to reach", func(t *testing.T) {
		t.Parallel()
		whole := resolveUsageWindow(testNow, resolvedPosition{}, false, 90, DefaultTrailingOverlapDays)
		first := whole.chunks()[0]

		got := coveredPosition(first, whole)
		assert.Equal(t, "2026-08-19", got.BackfillUntil, "the target is the window's own end")
		assert.Equal(t, first.End.AddDate(0, 0, 1).Format(dateLayout), got.FloorDate)
	})

	t.Run("a resume records its own end", func(t *testing.T) {
		t.Parallel()
		prior := storedAt(t, usagePosition{FloorDate: "2026-06-05", BackfillUntil: "2026-08-19"})
		whole := resolveUsageWindow(testNow, prior, true, 90, DefaultTrailingOverlapDays)

		got := coveredPosition(whole.chunks()[0], whole)
		assert.Equal(t, "2026-08-19", got.BackfillUntil,
			"losing the target here would strand the rest of the recovery on the next run")
	})

	// This is the property that makes recording a target unconditionally safe, and it is
	// why no window has to be classified first: a narrow window satisfies its own target
	// the moment it writes one, because the floor lands the day after the end the target
	// names. A later run therefore finds it finished — which it is — and a floor that has
	// since gone stale still reads as the pause it is, not as work to resume.
	t.Run("a trailing window satisfies its own target immediately", func(t *testing.T) {
		t.Parallel()
		whole := resolveUsageWindow(testNow, storedAt(t, usagePosition{FloorDate: "2026-08-19"}), true, 90, DefaultTrailingOverlapDays)
		require.Equal(t, reasonTrailing, whole.Reason)

		written := storedAt(t, coveredPosition(whole, whole))
		assert.True(t, written.Floor.After(written.BackfillUntil),
			"the floor must already be past the target, or every trailing run would look like an unfinished one")

		// And months later, that same position is still a pause rather than a resume.
		muchLater := resolveUsageWindow(testNow.AddDate(0, 3, 0), written, true, 90, DefaultTrailingOverlapDays)
		assert.Equal(t, reasonStaleFloor, muchLater.Reason)
	})

	// The overlap is an operator setting with no upper bound, so a TRAILING window can
	// itself be wider than one chunk — and then a per-chunk checkpoint can store a floor
	// short of that window's own end. This is the one case where recording a target
	// unconditionally is not merely inert: the run after it resumes, where the older
	// predicate would have read the same floor as a pause.
	//
	// That is the correct direction — those days were never covered — but it is a
	// behaviour change rather than a no-op, so it is asserted rather than left to the
	// default overlap, which is a single chunk and cannot reach this state.
	t.Run("a wide overlap makes even a trailing window resumable", func(t *testing.T) {
		t.Parallel()
		const wideOverlap = 14

		caughtUp := storedAt(t, usagePosition{FloorDate: "2026-08-19"})
		whole := resolveUsageWindow(testNow, caughtUp, true, 90, wideOverlap)
		require.Equal(t, reasonTrailing, whole.Reason)

		chunks := whole.chunks()
		require.Greater(t, len(chunks), 1, "the premise: an overlap past the chunk width slices")

		// The run covers its first chunk, checkpoints, and dies.
		interrupted := storedAt(t, coveredPosition(chunks[0], whole))
		require.True(t, interrupted.Floor.Before(startOfUTCDay(whole.End)),
			"the premise: the floor is short of the window it was covering")

		// Days later, the remainder is still owed and the run resumes for it.
		next := resolveUsageWindow(testNow.AddDate(0, 0, 11), interrupted, true, 90, wideOverlap)
		assert.Equal(t, reasonResuming, next.Reason,
			"the uncovered tail of an interrupted trailing window must not read as a pause")
		assert.Equal(t, interrupted.Floor.Format(dateLayout), next.Start.Format(dateLayout))
	})
}

// TestParseUsagePosition_RejectsAnUnreadableDate keeps the parse honest. A silently
// unparsed floor would read as the zero time — before every trailing start — so it would
// look like an enormous resume, or like a caught-up position depending on the branch. An
// error is the only reading the caller can act on, and it falls back to the lookback.
func TestParseUsagePosition_RejectsAnUnreadableDate(t *testing.T) {
	t.Parallel()

	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "not-a-date"})
	require.NoError(t, err)

	_, present, err := parseUsagePosition(cur)
	require.Error(t, err)
	assert.False(t, present)
	assert.Contains(t, err.Error(), "parsing stored floor")
}

// TestResolveUsageWindow_OverlapIsOperatorAdjustable pins the lever the design promises:
// the overlap IS the warehouse write multiplier — a day's events are rewritten once per
// day for as long as they stay in the window — so narrowing it is the response to capture
// volume, and it must not need a deploy.
func TestResolveUsageWindow_OverlapIsOperatorAdjustable(t *testing.T) {
	t.Parallel()

	caughtUp := storedAt(t, usagePosition{FloorDate: "2026-08-19"})

	narrowed := resolveUsageWindow(testNow, caughtUp, true, 90, 1)
	assert.Equal(t, "2026-08-19", narrowed.Start.Format(dateLayout),
		"one day of overlap re-reads yesterday and nothing else")

	wide := resolveUsageWindow(testNow, caughtUp, true, 90, 7)
	assert.Equal(t, "2026-08-13", wide.Start.Format(dateLayout))

	// The floor is judged against the SAME overlap, so changing it cannot turn a
	// caught-up position into a resume or a stale floor.
	assert.Equal(t, reasonTrailing, narrowed.Reason)
	assert.Equal(t, reasonTrailing, wide.Reason)
}

// TestCursorSettings_OverlapDefaults keeps the zero value meaning "the default" rather
// than "no overlap at all", which would silently stop every corrected charge being picked
// up on any organisation that never set the field.
func TestCursorSettings_OverlapDefaults(t *testing.T) {
	t.Parallel()

	assert.Equal(t, DefaultTrailingOverlapDays, CursorSettings{}.GetTrailingOverlapDays())
	assert.Equal(t, 1, CursorSettings{TrailingOverlapDays: 1}.GetTrailingOverlapDays())
}

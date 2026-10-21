// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// mustDayA parses a fixture's covered date directly. Tests whose subject is the position
// PARSING go through the real parser instead — see resolved().

// chunksOf drops the unreachable-span returns for the tests whose subject is the plan itself.
// The tests that care about a skipped span read all three.
func chunksOf(chunks []reportChunk, _, _ time.Time) []reportChunk { return chunks }

// steadyStored is a well-formed stored watermark for fixtures whose subject is the window
// arithmetic rather than the watermark comparison.
const steadyStored = "2026-06-01T10:00:00Z"

// resolved builds the position the planner receives, through the real parser, so a fixture
// cannot present a shape production never produces.
func resolved(t *testing.T, p reportPosition) resolvedReportPosition {
	t.Helper()
	if p.CoveredThrough == "" {
		return resolvedReportPosition{}
	}
	cur, err := encodeCursor(p)
	require.NoError(t, err)
	out, err := parseReportPosition(cur)
	require.NoError(t, err)
	return out
}

func mustDayA(p activityPosition) time.Time {
	if p.CoveredThrough == "" {
		return time.Time{}
	}
	t, _ := time.ParseInLocation(dateLayout, p.CoveredThrough, time.UTC) //nolint:errcheck // fixture input
	return t
}

// TestResolveReportChunks_NoChunkExceedsTheVendorMaximum is an acceptance check,
// and it is a hard bound rather than a courtesy: a wider range returns 400 before
// ingesting anything, so a ninety-day first sync that is not chunked ingests nothing at
// all.
func TestResolveReportChunks_NoChunkExceedsTheVendorMaximum(t *testing.T) {
	t.Parallel()

	// A ninety-day first sync — the default lookback, and the case that matters most.
	chunks := chunksOf(resolveReportChunks(day(2026, time.June, 1), resolved(t, reportPosition{}), 90, 15, defaultReportChunkDays, maxForwardChunksPerRun, ""))
	require.NotEmpty(t, chunks)

	// The literal, not the constant: asserting width <= maxReportRangeDays is a tautology
	// for any value of maxReportRangeDays, so it pinned nothing. Thirty-one is the vendor's
	// bound, and a change to the constant has to fail here.
	assert.Equal(t, 31, maxReportRangeDays,
		"the vendor caps one report request at a 31-day range; a wider one returns 400")

	for i, c := range chunks {
		width := c.EndingAt.Sub(c.StartingAt).Hours() / 24
		assert.LessOrEqualf(t, width, 31.0,
			"chunk %d spans %.0f days, past the vendor's 31-day maximum for one request", i, width)
		assert.Truef(t, c.StartingAt.Before(c.EndingAt), "chunk %d is empty or inverted", i)
	}

	// Contiguous and gapless: a gap is history that is never read and never noticed,
	// because the position advances past it either way.
	for i := 1; i < len(chunks); i++ {
		assert.Truef(t, chunks[i].StartingAt.Equal(chunks[i-1].EndingAt),
			"chunk %d starts at %v but %d ended at %v — a gap here is silently unread history",
			i, chunks[i].StartingAt, i-1, chunks[i-1].EndingAt)
	}

	// And the whole window is covered, end to end.
	assert.Equal(t, day(2026, time.March, 3), chunks[0].StartingAt)
	assert.Equal(t, day(2026, time.June, 1), chunks[len(chunks)-1].EndingAt)
}

// TestResolveReportChunks_ClampsToTheVendorFloor keeps a first sync from spending its
// budget on pages that cannot contain anything.
func TestResolveReportChunks_ClampsToTheVendorFloor(t *testing.T) {
	t.Parallel()

	chunks := chunksOf(resolveReportChunks(day(2026, time.February, 1), resolved(t, reportPosition{}), 365, 15, defaultReportChunkDays, maxForwardChunksPerRun, ""))
	require.NotEmpty(t, chunks)
	assert.Equal(t, vendorFloorDate, chunks[0].StartingAt,
		"a lookback deeper than the vendor's floor must clamp to it")
}

// TestResolveReportChunks_RereadSkippedWhenTheVendorHasNotRefreshed is the watermark. The
// re-read exists because the vendor re-prices after the fact and the stored record is
// verbatim as of the last read — but re-reading a window the vendor has not touched rewrites
// the same rows for nothing, and this operation runs daily.
func TestResolveReportChunks_RereadSkippedWhenTheVendorHasNotRefreshed(t *testing.T) {
	t.Parallel()

	now := day(2026, time.June, 2)
	pos := reportPosition{
		CoveredThrough:  "2026-06-01",
		DataRefreshedAt: "2026-06-01T10:00:00Z",
	}

	// Observed == stored: nothing has been restated, so no trailing re-read.
	unchanged := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, 15, defaultReportChunkDays, maxForwardChunksPerRun, "2026-06-01T10:00:00Z"))
	require.Len(t, unchanged, 1)
	assert.Equal(t, day(2026, time.June, 1), unchanged[0].StartingAt,
		"an unrefreshed window must not be re-read — the same rows would be rewritten for nothing")

	// Observed moved: the trailing window is pulled back in.
	moved := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, 15, defaultReportChunkDays, maxForwardChunksPerRun, "2026-06-02T10:00:00Z"))
	require.NotEmpty(t, moved)
	assert.Equal(t, day(2026, time.May, 18), moved[0].StartingAt,
		"a refreshed window must re-read the trailing span, or a re-priced amount is never collected")
}

// TestResolveReportChunks_RereadsWhenNoWatermarkWasStored pins the direction of the
// unknown case: never having recorded a watermark must mean re-read, not assume nothing
// changed. Assuming would silently drop every correction for an organisation that
// connected before the field existed.
func TestResolveReportChunks_RereadsWhenNoWatermarkWasStored(t *testing.T) {
	t.Parallel()

	pos := reportPosition{CoveredThrough: "2026-05-31"}
	chunks := chunksOf(resolveReportChunks(day(2026, time.June, 2), resolved(t, pos), 90, 15, defaultReportChunkDays, maxForwardChunksPerRun, "2026-06-01T10:00:00Z"))
	require.NotEmpty(t, chunks)
	assert.Equal(t, day(2026, time.May, 18), chunks[0].StartingAt)
}

// TestResolveReportChunks_CaughtUpIsNoWork — nothing to read is not a failure.
func TestResolveReportChunks_CaughtUpIsNoWork(t *testing.T) {
	t.Parallel()

	pos := reportPosition{CoveredThrough: "2026-06-01", DataRefreshedAt: steadyStored}
	assert.Empty(t, chunksOf(resolveReportChunks(day(2026, time.June, 1), resolved(t, pos), 90, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored)))
}

// TestResolveReportChunks_EveryChunkStartsWhereTheLastEnded is what makes a partial run
// safe without any mid-chunk resume: the plan is contiguous from the covered point, so a
// chunk a previous run died inside is planned again whole. Resuming it at a page instead
// would emit each identity from only the remaining pages.
func TestResolveReportChunks_EveryChunkStartsWhereTheLastEnded(t *testing.T) {
	t.Parallel()

	pos := reportPosition{CoveredThrough: "2026-03-03", DataRefreshedAt: steadyStored}
	chunks := chunksOf(resolveReportChunks(day(2026, time.June, 1), resolved(t, pos), 90, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored))
	require.Greater(t, len(chunks), 1)

	assert.Equal(t, day(2026, time.March, 3), chunks[0].StartingAt,
		"the walk resumes at the covered point, re-reading its chunk whole")
	for i := 1; i < len(chunks); i++ {
		assert.Equalf(t, chunks[i-1].EndingAt, chunks[i].StartingAt,
			"chunk %d must start where %d ended, or the gap is never read", i, i-1)
	}
}

// TestResolveActivityWindow covers the day walk, including the two cases that are
// otherwise indistinguishable from a healthy sync.
func TestResolveActivityWindow(t *testing.T) {
	t.Parallel()

	latest := day(2026, time.June, 1)

	t.Run("a first run reads the lookback, clamped to the floor", func(t *testing.T) {
		t.Parallel()
		w := resolveActivityWindow(time.Time{}, false, latest, 90, defaultActivityOverlapDays)
		assert.True(t, w.FirstRun)
		assert.Equal(t, day(2026, time.March, 4), w.From)
		assert.Equal(t, latest, w.Through)
		assert.Len(t, w.Days(), 90)
	})

	t.Run("a resume re-enters the overlap rather than starting strictly after", func(t *testing.T) {
		t.Parallel()
		pos := activityPosition{CoveredThrough: "2026-05-30"}
		w := resolveActivityWindow(mustDayA(pos), true, latest, 90, defaultActivityOverlapDays)
		assert.False(t, w.FirstRun)
		// These endpoints are not revised, but a row can arrive LATE for a covered day. With
		// no overlap that row is never fetched, because the position has moved past its day
		// and nothing brings the walk back. Re-reading is free: the identity is actor-and-day,
		// so a re-emitted row updates itself in place.
		assert.Equal(t, day(2026, time.May, 29), w.From,
			"a covered day must be re-entered within the overlap")
	})

	t.Run("the overlap is tunable per organisation", func(t *testing.T) {
		t.Parallel()
		pos := activityPosition{CoveredThrough: "2026-05-30"}
		w := resolveActivityWindow(mustDayA(pos), true, latest, 90, 5)
		assert.Equal(t, day(2026, time.May, 26), w.From)
	})

	t.Run("caught up is no work, not an error", func(t *testing.T) {
		t.Parallel()
		// Caught up, and the overlap must not manufacture work past the latest available
		// day — only re-enter days at or before it.
		pos := activityPosition{CoveredThrough: "2026-06-01"}
		w := resolveActivityWindow(mustDayA(pos), true, latest, 90, defaultActivityOverlapDays)
		assert.True(t, w.HasWork(), "the overlap re-enters the covered day")
		assert.Equal(t, day(2026, time.May, 31), w.From)
		assert.Equal(t, latest, w.Through)
	})

	t.Run("days come oldest first, so an interrupted run leaves a contiguous span", func(t *testing.T) {
		t.Parallel()
		pos := activityPosition{CoveredThrough: "2026-05-28"}
		w := resolveActivityWindow(mustDayA(pos), true, latest, 90, defaultActivityOverlapDays)
		days := w.Days()
		require.Len(t, days, 6, "four unread days plus the two-day overlap")
		for i := 1; i < len(days); i++ {
			assert.True(t, days[i].After(days[i-1]), "days must ascend")
		}
	})
}

// TestParsePositions covers the round-trip and the unreadable case, which must be
// reported rather than silently treated as caught-up — that reading loses history.
func TestParsePositions(t *testing.T) {
	t.Parallel()

	t.Run("activity round-trips", func(t *testing.T) {
		t.Parallel()
		c, err := encodeCursor(activityPosition{CoveredThrough: "2026-05-30"})
		require.NoError(t, err)
		got, covered, present, err := parseActivityPosition(c)
		require.NoError(t, err)
		require.True(t, present)
		assert.Equal(t, "2026-05-30", got.CoveredThrough)
		assert.Equal(t, day(2026, time.May, 30), covered)
	})

	t.Run("report round-trips", func(t *testing.T) {
		t.Parallel()
		c, err := encodeCursor(reportPosition{
			CoveredThrough: "2026-05-01", DataRefreshedAt: "2026-05-01T10:00:00Z",
		})
		require.NoError(t, err)
		got, err := parseReportPosition(c)
		require.NoError(t, err)
		require.True(t, got.Present)
		assert.Equal(t, "2026-05-01", got.Stored.CoveredThrough)
		assert.Equal(t, "2026-05-01T10:00:00Z", got.Stored.DataRefreshedAt)
		assert.Equal(t, day(2026, time.May, 1), got.RefreshedDay)
	})

	t.Run("a watermark that will not parse makes the position unreadable, not unclamped", func(t *testing.T) {
		t.Parallel()
		// We wrote that value, so one that will not parse is our own defect — and zeroing it
		// would silently unclamp the request window against an incomplete tail.
		c, err := encodeCursor(reportPosition{CoveredThrough: "2026-05-01", DataRefreshedAt: "not-a-time"})
		require.NoError(t, err)
		_, err = parseReportPosition(c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data_refreshed_at")
	})

	t.Run("absent and empty are not present, and not errors", func(t *testing.T) {
		t.Parallel()
		for _, c := range []*sdk.Cursor{nil, {}, {Data: json.RawMessage(`{}`)}} {
			_, _, present, err := parseActivityPosition(c)
			require.NoError(t, err)
			assert.False(t, present)
		}
	})

	t.Run("an unreadable position is an error, never a silent caught-up", func(t *testing.T) {
		t.Parallel()
		_, _, _, err := parseActivityPosition(&sdk.Cursor{Data: json.RawMessage(`{`)})
		require.Error(t, err)
		_, _, _, err = parseActivityPosition(&sdk.Cursor{Data: json.RawMessage(`{"covered_through":"not-a-date"}`)})
		require.Error(t, err)
	})
}

// TestResolveReportChunks_TheVendorsYearBoundIsHonoured covers the bound the floor date does
// not. An organisation whose connector was paused for over a year resumes from a position
// that is past the 2026-01-01 floor yet older than a year — and the vendor refuses a start
// more than 365 days back, so every chunk would 400 on every run, forever.
func TestResolveReportChunks_TheVendorsYearBoundIsHonoured(t *testing.T) {
	t.Parallel()

	now := day(2027, time.June, 1)
	oldest := now.AddDate(0, 0, -maxReportLookbackDays)

	// A resumed walk whose stored position is over a year old. Past the 2026-01-01 floor, so
	// that clamp does not fire.
	pos := reportPosition{CoveredThrough: "2026-03-01", DataRefreshedAt: steadyStored}
	resumed := chunksOf(resolveReportChunks(now, resolved(t, pos), 400, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored))
	require.NotEmpty(t, resumed)
	assert.Falsef(t, resumed[0].StartingAt.Before(oldest),
		"a start of %v is more than a year back and the vendor refuses it", resumed[0].StartingAt)

	// And an operator who sets a lookback wider than the vendor allows: the request must be
	// clamped rather than sent and refused on every run.
	first := chunksOf(resolveReportChunks(now, resolved(t, reportPosition{}), 400, 0, defaultReportChunkDays, maxForwardChunksPerRun, ""))
	require.NotEmpty(t, first)
	assert.Falsef(t, first[0].StartingAt.Before(oldest),
		"an over-wide lookback must be clamped; got %v", first[0].StartingAt)
}

// TestResolveReportChunks_AResumedWalkReadsTheWholeGap is the data-loss guard on the bound.
//
// Clamping a resumed start to the lookback is the tempting way to bound request volume, and
// the span between the stored position and that clamp is then never requested while the
// position advances past it — unrecoverably, since the next run starts from the new position,
// so even raising lookback_days cannot bring those days back. Volume is bounded by the number
// of chunks a run plans instead, so a wide gap closes over runs rather than disappearing.
func TestResolveReportChunks_AResumedWalkReadsTheWholeGap(t *testing.T) {
	t.Parallel()

	now := day(2026, time.June, 1)
	// Covered long ago, and the operator asked for a 30-day window — far narrower than the gap.
	pos := reportPosition{CoveredThrough: "2026-01-05", DataRefreshedAt: steadyStored}
	chunks := chunksOf(resolveReportChunks(now, resolved(t, pos), 30, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored))
	require.NotEmpty(t, chunks)

	assert.Equal(t, day(2026, time.January, 5), chunks[0].StartingAt,
		"a resumed walk must start at the covered point; anything later is a span silently skipped")
	// Contiguous from there, so the plan opens no gap of its own.
	for i := 1; i < len(chunks); i++ {
		assert.Equal(t, chunks[i-1].EndingAt, chunks[i].StartingAt)
	}
}

// TestResolveReportChunks_OneRunPlansOnlyWhatItCanFinish is the other half of that trade. A
// chunk is all-or-nothing against a request budget the connector does not own, so an unbounded
// plan would attempt more than one run can finish and land nothing at all.
func TestResolveReportChunks_OneRunPlansOnlyWhatItCanFinish(t *testing.T) {
	t.Parallel()

	now := day(2027, time.January, 1)
	// A year-wide gap: far more than one run's worth.
	pos := reportPosition{CoveredThrough: "2026-01-05", DataRefreshedAt: steadyStored}
	chunks := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored))

	require.NotEmpty(t, chunks)
	assert.LessOrEqual(t, len(chunks), maxForwardChunksPerRun, "one run must plan only what it can finish")
	assert.Equal(t, day(2026, time.January, 5), chunks[0].StartingAt,
		"and it must still start at the covered point, so the next run continues rather than skipping")

	// A ninety-day first sync still fits one run at the default width, or every first sync
	// would need several scheduled runs before any data landed.
	first := chunksOf(resolveReportChunks(now, resolved(t, reportPosition{}), 90, 0, defaultReportChunkDays, maxForwardChunksPerRun, ""))
	require.NotEmpty(t, first)
	assert.Equal(t, day(2026, time.October, 3), first[0].StartingAt)
	assert.Equal(t, now, first[len(first)-1].EndingAt, "a first sync must reach the present in one run")
}

// TestResolveReportChunks_ChunkWidthIsTunableDownward is the lever an operator has when one
// chunk cannot be read inside a single run's vendor-request budget. A chunk is all-or-nothing
// — a partially-read chunk emits nothing — so without narrowing, such an organisation makes
// no progress at all: every attempt restarts the same chunk and is refused at the same page.
func TestResolveReportChunks_ChunkWidthIsTunableDownward(t *testing.T) {
	t.Parallel()

	now := day(2026, time.June, 1)
	pos := reportPosition{CoveredThrough: "2026-04-01", DataRefreshedAt: steadyStored}

	wide := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, 0, defaultReportChunkDays, maxForwardChunksPerRun, steadyStored))
	narrow := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, 0, 7, maxForwardChunksPerRun, "x"))

	require.NotEmpty(t, wide)
	require.NotEmpty(t, narrow)
	assert.Greater(t, len(narrow), len(wide), "a narrower width must yield more, smaller chunks")

	for i, c := range narrow {
		assert.LessOrEqualf(t, c.EndingAt.Sub(c.StartingAt), 7*24*time.Hour,
			"chunk %d is wider than the configured width", i)
	}
	// Still contiguous, so narrowing cannot open a gap that is never read.
	for i := 1; i < len(narrow); i++ {
		assert.Equal(t, narrow[i-1].EndingAt, narrow[i].StartingAt)
	}
}

// TestOperationOptionsChunkDaysClamps — a width above the vendor maximum would be refused by
// the vendor rather than honoured, so accepting it turns a tuning mistake into a dead
// operation.
func TestOperationOptionsChunkDaysClamps(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		set  int
		want int
	}{
		{"unset falls back to the vendor maximum", 0, defaultReportChunkDays},
		{"negative falls back", -3, defaultReportChunkDays},
		{"above the vendor maximum is clamped, not honoured", maxReportRangeDays + 1, defaultReportChunkDays},
		{"a narrower width is honoured", 7, 7},
		{"exactly the maximum is honoured", maxReportRangeDays, maxReportRangeDays},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, operationOptions{ChunkDays: tt.set}.chunkDays())
		})
	}
}

// TestOperationOptionsRereadDaysClamps bounds the other knob. The trailing pass is
// deliberately uncapped in CHUNKS — truncating it would drop the newest, most-revised days —
// so the WINDOW is the only thing bounding its cost. Left unclamped, one per-organisation
// override makes every run re-read a year of report data against the request pool all five
// operations share.
func TestOperationOptionsRereadDaysClamps(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		set  int
		want int
	}{
		{"unset falls back", 0, defaultReportRereadDays},
		{"negative falls back", -5, defaultReportRereadDays},
		{"a wider window is honoured up to the cap", 25, 25},
		{"exactly the cap is honoured", maxReportRereadDays, maxReportRereadDays},
		{"beyond the revision horizon is clamped, not honoured", 400, maxReportRereadDays},
		{"an absurd value is clamped too", 10000, maxReportRereadDays},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, operationOptions{RereadDays: tt.set}.rereadDays())
		})
	}
}

// TestResolveReportChunks_TheTrailingPassIsNotChunkCapped is the asymmetry between the two
// passes, and it is deliberate.
//
// The forward pass is capped in chunks so a wide gap closes over runs. Applying that cap to
// the trailing re-read truncates it from the NEWEST end — so with a narrow chunk_days the days
// most likely to have been restated are the ones dropped, and because the window re-anchors to
// the clock every run they are never re-read at all.
func TestResolveReportChunks_TheTrailingPassIsNotChunkCapped(t *testing.T) {
	t.Parallel()

	now := day(2026, time.June, 20)
	pos := reportPosition{CoveredThrough: "2026-06-19", DataRefreshedAt: steadyStored}

	// A narrow width and the default re-read window: more chunks than the forward cap allows.
	trailing := chunksOf(resolveReportChunks(now, resolved(t, pos), 90, defaultReportRereadDays, 1, 0, "2026-06-20T10:00:00Z"))
	require.Greater(t, len(trailing), maxForwardChunksPerRun,
		"the trailing pass must not be bounded by the forward cap")

	// The window covers the full re-read span, ending at the clock — the newest days included.
	assert.Equal(t, now.AddDate(0, 0, -defaultReportRereadDays), trailing[0].StartingAt)
	assert.Equal(t, now, trailing[len(trailing)-1].EndingAt,
		"the re-read must reach the present, or the most recently revised days are never re-read")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// dateLayout is the vendor's calendar-day format, used by the activity endpoints and by
// every stored position. Stored as text rather than a timestamp so a position is legible
// in the database and cannot acquire a spurious time-of-day.
const dateLayout = "2006-01-02"

// vendorFloorDate is the earliest date any analytics endpoint carries data for. A deeper
// request is answered with empty pages rather than an error, so clamping to it makes the
// request honest rather than preventing a failure — and keeps a first sync from spending
// its budget on pages that cannot contain anything.
var vendorFloorDate = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// maxReportRangeDays is the widest range the vendor accepts in ONE report request. It is a
// hard bound, not a courtesy: a wider window returns 400 before ingesting anything, so a
// ninety-day first sync has to be a sequence of chunks rather than a single call.
const maxReportRangeDays = 31

// finalityCeiling is the furthest day a report walk may record as COVERED, given what the
// vendor says about its own currency.
//
// Claiming coverage of a day the vendor is still revising means the next run starts after it
// and never asks again, so a restatement to that day is lost. The ceiling prevents that
// without touching the request window.
//
// maxUnfinalizedLag is what keeps the ceiling from becoming its own stall. A watermark weeks
// behind the day just read is not a usable finality signal — honouring it would park the
// position there and re-read the same span every run, forever, for a vendor whose watermark
// has simply stopped moving. Past that bound the read stands and the caller says so loudly.
func finalityCeiling(observedRefreshedDay, readThrough time.Time) (time.Time, bool) {
	if observedRefreshedDay.IsZero() {
		return time.Time{}, false
	}
	if readThrough.Sub(observedRefreshedDay) > maxUnfinalizedLag {
		return time.Time{}, false
	}
	return observedRefreshedDay, true
}

// maxUnfinalizedLag bounds how far behind a day just read the vendor's watermark may sit and
// still bound coverage. Reuses the aggregation-lag bound: a vendor claiming nothing in the
// last month is final is not reporting a lag, it has stopped reporting.
const maxUnfinalizedLag = maxAggregationLag

// maxForwardChunksPerRun bounds how many ranges the FORWARD pass will request.
//
// It is what makes reading the whole gap affordable. A run may only issue so many vendor
// requests before the shared per-organisation limiter refuses, and a chunk is all-or-nothing,
// so an unbounded plan would attempt more than it can finish and land nothing. Bounding the
// plan instead means each run finishes what it planned and checkpoints it, and an organisation
// resumed after a long pause closes its gap a few chunks at a time.
//
// Sized so a ninety-day first sync still completes in one run at the default chunk width.
//
// THE INVARIANT THIS CREATES: a run advances at most maxForwardChunksPerRun × chunk_days days,
// so that product must exceed the interval between runs, or the position falls behind the clock
// by the difference on every run — and once it falls past the vendor's year limit the excess is
// dropped for good. The two knobs an operator reaches for under rate-limit pressure (narrow
// chunk_days, run it less often) push the same way, so the walk reports a run that cannot reach
// the present rather than leaving it to be found years later.
//
// It applies to the forward pass ALONE. Capping the trailing re-read as well would truncate the
// correction window from its NEWEST end, dropping the days most likely to have been restated —
// and because that window re-anchors to the clock each run, they would never be re-read at all.
// Its cost is bounded by maxReportRereadDays instead.
const maxForwardChunksPerRun = 4

// maxCursorRestarts bounds how many times one unit may restart on an expired page cursor.
// More than a couple means the unit takes longer to read than a cursor lives, which narrowing
// the unit fixes and retrying does not.
const maxCursorRestarts = 3

// maxReportLookbackDays is the vendor's other bound on a report range: a start more than a
// year ago is refused. It bites where the floor date does not — an organisation whose
// connector was paused for over a year resumes from a stored position that is past the floor
// yet older than a year, and every chunk would then be refused on every run.
const maxReportLookbackDays = 365

// activityPosition is the stored cursor for the two activity operations. They walk whole
// calendar days, so the position is the last day fully covered.
// There is deliberately no mid-day page cursor. Storing one would not just be extra
// machinery, it would be wrong: a day is emitted as one record per actor, assembled from
// every page, so resuming at page 41 would emit each actor with only pages 41-onward and
// overwrite the rows the earlier pages contributed. A partial day emits nothing, so
// re-reading it whole is both correct and free.
type activityPosition struct {
	// CoveredThrough is the last calendar day fully covered, except the days FailedDays
	// lists.
	CoveredThrough string `json:"covered_through"`
	// FailedDays are days at or before CoveredThrough whose read failed on its own, as
	// YYYY-MM-DD, oldest first. The walk advances past such a day and every later run reads
	// it again until it succeeds; see maxFailedUnits for the bound. Absent in a position
	// written before the field existed, which decodes as none.
	FailedDays []string `json:"failed_days,omitempty"`
}

// reportPosition is the stored cursor for the two report operations. They walk timestamp
// ranges in chunks, so the position is the end of the last chunk fully covered, plus the
// watermark that decides whether an already-covered chunk is worth re-reading.
type reportPosition struct {
	// CoveredThrough is the exclusive end of the last chunk fully read and emitted.
	CoveredThrough string `json:"covered_through"`
	// DataRefreshedAt is the vendor's own statement of currency at the last read. It
	// bounds a re-read: there is nothing to gain from re-reading a window the vendor has
	// not refreshed since it was stored, so an unchanged value lets a run skip the
	// trailing re-read entirely.
	DataRefreshedAt string `json:"data_refreshed_at,omitempty"`
	// FailedChunks are chunks the position has passed whose read failed on its own, each as
	// "YYYY-MM-DD..YYYY-MM-DD" (start inclusive, end exclusive), oldest first. Every later
	// run reads them again until each succeeds; see maxFailedUnits for the bound. Absent in
	// a position written before the field existed, which decodes as none.
	FailedChunks []string `json:"failed_chunks,omitempty"`
}

// chunkUnit is a chunk's entry in FailedChunks.
func chunkUnit(c reportChunk) string {
	return c.StartingAt.Format(dateLayout) + ".." + c.EndingAt.Format(dateLayout)
}

// parseChunkUnit reads a FailedChunks entry back.
func parseChunkUnit(unit string) (reportChunk, error) {
	from, to, ok := strings.Cut(unit, "..")
	if !ok {
		return reportChunk{}, fmt.Errorf("failed chunk %q is not start..end", unit)
	}
	start, err := time.ParseInLocation(dateLayout, from, time.UTC)
	if err != nil {
		return reportChunk{}, fmt.Errorf("parsing failed chunk %q: %w", unit, err)
	}
	end, err := time.ParseInLocation(dateLayout, to, time.UTC)
	if err != nil {
		return reportChunk{}, fmt.Errorf("parsing failed chunk %q: %w", unit, err)
	}
	if !start.Before(end) {
		return reportChunk{}, fmt.Errorf("failed chunk %q ends before it starts", unit)
	}
	return reportChunk{StartingAt: start, EndingAt: end}, nil
}

// activityWindow is the span one activity run will cover.
type activityWindow struct {
	// From and Through are inclusive calendar days, oldest first.
	From, Through time.Time
	// FirstRun distinguishes a first sync from a resume. Both advance the position and are
	// otherwise indistinguishable from each other.
	FirstRun bool
}

// reportChunk is one request's worth of a report walk.
type reportChunk struct {
	// StartingAt is inclusive, EndingAt exclusive — the vendor's own convention.
	StartingAt, EndingAt time.Time
}

// parseActivityPosition resolves a stored activity cursor, returning the covered day
// already parsed.
//
// The parse happens here rather than in the window arithmetic because this is where a
// failure can be reported: arithmetic has nowhere to send one, and a silently unparsed
// position looks exactly like a caught-up one — the reading that loses history.
//
// An unreadable position is reported rather than swallowed, so the caller decides what an
// unusable cursor means.
func parseActivityPosition(c *sdk.Cursor) (pos activityPosition, covered time.Time, present bool, err error) {
	if c == nil || len(c.Data) == 0 {
		return activityPosition{}, time.Time{}, false, nil
	}
	var stored activityPosition
	if err := json.Unmarshal(c.Data, &stored); err != nil {
		return activityPosition{}, time.Time{}, false, fmt.Errorf("unmarshalling activity position: %w", err)
	}
	if stored.CoveredThrough == "" {
		return activityPosition{}, time.Time{}, false, nil
	}
	for _, d := range stored.FailedDays {
		if _, err := time.ParseInLocation(dateLayout, d, time.UTC); err != nil {
			return activityPosition{}, time.Time{}, false, fmt.Errorf("parsing failed day %q: %w", d, err)
		}
	}
	parsed, err := time.ParseInLocation(dateLayout, stored.CoveredThrough, time.UTC)
	if err != nil {
		return activityPosition{}, time.Time{}, false, fmt.Errorf("parsing covered_through %q: %w", stored.CoveredThrough, err)
	}
	return stored, parsed, true, nil
}

// resolveActivityWindow decides which calendar days one run covers.
//
// latestAvailable is the newest day worth asking for. The caller derives it from the
// clock, which is a guess at the vendor's aggregation lag: the authoritative answer only
// arrives as the 400 the walk gets for a day not yet aggregated, and that stops the walk
// where it stands rather than re-planning the window.
func resolveActivityWindow(
	covered time.Time, present bool,
	latestAvailable time.Time, lookbackDays, overlapDays int,
) activityWindow {
	through := latestAvailable.UTC().Truncate(24 * time.Hour)

	if !present {
		from := through.AddDate(0, 0, -(lookbackDays - 1))
		if from.Before(vendorFloorDate) {
			from = vendorFloorDate
		}
		return activityWindow{From: from, Through: through, FirstRun: true}
	}

	// Re-enter the overlap rather than starting strictly after the covered day, so a row
	// that arrived late for an already-covered day is still picked up.
	from := covered.AddDate(0, 0, -(overlapDays - 1))
	if from.Before(vendorFloorDate) {
		from = vendorFloorDate
	}
	return activityWindow{From: from, Through: through}
}

// HasWork reports whether the window contains any day to read. A caught-up operation
// resolves to an empty window rather than an error: there is nothing to do, which is not
// a failure.
func (w activityWindow) HasWork() bool { return !w.From.After(w.Through) }

// Days yields the window's calendar days oldest first, which is the order the walk must
// use: a run that stops partway must leave a contiguous covered span behind it, and only
// oldest-first does that.
func (w activityWindow) Days() []time.Time {
	if !w.HasWork() {
		return nil
	}
	var out []time.Time
	for d := w.From; !d.After(w.Through); d = d.AddDate(0, 0, 1) {
		out = append(out, d)
	}
	return out
}

// resolvedReportPosition is a stored report position with its dates already parsed. The
// planning arithmetic works on times, so it never re-parses a string it cannot report a
// failure from — every parse happens here, where there is an error to return.
type resolvedReportPosition struct {
	Stored reportPosition
	// Covered is CoveredThrough as a time. RefreshedAt is the stored watermark at full
	// precision and RefreshedDay is the same value at day grain — both zero when no watermark
	// has been stored. Full precision is kept because two watermarks on one calendar day are
	// ordered only by their times, and that is the ordering the no-regression guard needs.
	Covered      time.Time
	RefreshedAt  time.Time
	RefreshedDay time.Time
	Present      bool
	// FailedChunks is Stored.FailedChunks parsed, oldest first.
	FailedChunks []reportChunk
}

// parseReportPosition resolves a stored report cursor, with the same
// unreadable-is-reported contract as its activity sibling.
//
// An unparseable watermark makes the whole position unreadable, which the caller answers by
// restarting from the lookback. That is a deliberate trade rather than an oversight: we wrote
// that value, so one that will not parse is our own defect, and re-reading the lookback is
// cheap and self-correcting where carrying on with a zeroed watermark would silently unclamp
// coverage against an unfinished tail.
func parseReportPosition(c *sdk.Cursor) (resolvedReportPosition, error) {
	if c == nil || len(c.Data) == 0 {
		return resolvedReportPosition{}, nil
	}
	var stored reportPosition
	if err := json.Unmarshal(c.Data, &stored); err != nil {
		return resolvedReportPosition{}, fmt.Errorf("unmarshalling report position: %w", err)
	}
	if stored.CoveredThrough == "" {
		return resolvedReportPosition{}, nil
	}
	covered, err := time.ParseInLocation(dateLayout, stored.CoveredThrough, time.UTC)
	if err != nil {
		return resolvedReportPosition{}, fmt.Errorf("parsing covered_through %q: %w", stored.CoveredThrough, err)
	}

	out := resolvedReportPosition{Stored: stored, Covered: covered, Present: true}
	for _, unit := range stored.FailedChunks {
		c, err := parseChunkUnit(unit)
		if err != nil {
			return resolvedReportPosition{}, err
		}
		out.FailedChunks = append(out.FailedChunks, c)
	}
	if stored.DataRefreshedAt != "" {
		at, refreshErr := time.Parse(time.RFC3339, stored.DataRefreshedAt)
		if refreshErr != nil {
			return resolvedReportPosition{}, fmt.Errorf("parsing data_refreshed_at %q: %w", stored.DataRefreshedAt, refreshErr)
		}
		out.RefreshedAt = at.UTC()
		out.RefreshedDay = out.RefreshedAt.Truncate(24 * time.Hour)
	}
	return out, nil
}

// resolveReportChunks decides which ranges one report run will request.
//
// Two spans can need reading and they are different in kind. The first is unread history,
// which advances forward from the stored position. The second is the trailing re-read: a
// window already covered whose amounts the vendor may have re-priced since, which is the
// only way a corrected figure is ever collected.
//
// The trailing re-read is skipped when the vendor's data_refreshed_at has not moved since
// it was stored — re-reading a window the vendor has not refreshed writes the same rows
// again for nothing, and this is the check that makes the re-read cheap enough to run
// daily.
// The window ends at the clock; the vendor's currency bounds what the position CLAIMS rather
// than what is requested — see finalityCeiling.
//
// Bounding the request by the stored watermark is the intuitive reading, and it deadlocks.
// data_refreshed_at only ever arrives IN a response, so a walk that is caught up to the
// stored watermark would plan no chunk, issue no request, learn no newer watermark, and never
// advance again. Reading the unfinished tail costs nothing that matters — those rows are
// stored verbatim and re-read — while never asking is unrecoverable.
//
// unreadableFrom/unreadableTo name a span the vendor will not serve, from either bound below.
// Both are zero when neither fired. Whether that span is data LOSS is the caller's judgement:
// it is, for a resumed position that has fallen behind the year bound; it is not for a first
// sync, where no position ever covered those days.
func resolveReportChunks(
	now time.Time, pos resolvedReportPosition,
	lookbackDays, rereadDays, chunkDays, maxChunks int, observedRefreshedAt string,
) (chunks []reportChunk, unreadableFrom, unreadableTo time.Time) {
	covered, present := pos.Covered, pos.Present

	end := now.UTC().Truncate(24 * time.Hour)

	// The lookback bounds a FIRST sync only. A resumed walk starts at the covered point and
	// reads the whole gap, however wide, because narrowing that span would skip the days in
	// between while the position still advanced past them — unrecoverably, since the next run
	// starts from the new position. Volume is bounded by how many chunks one run plans instead
	// (maxForwardChunksPerRun).
	start := end.AddDate(0, 0, -lookbackDays)
	if present {
		start = covered

		// The trailing re-read, when the vendor has refreshed since the last read.
		//
		// observedRefreshedAt must be a value read from the vendor THIS run. Passing the
		// stored value makes the comparison self-referential, and the re-read then never
		// fires. An empty stored value means we have never recorded one, so re-read rather
		// than assume nothing changed; an empty observed value means we have not learned it
		// yet, so re-read for the same reason.
		if rereadDays > 0 && (pos.Stored.DataRefreshedAt == "" || observedRefreshedAt == "" || observedRefreshedAt != pos.Stored.DataRefreshedAt) {
			rereadFrom := end.AddDate(0, 0, -rereadDays)
			if rereadFrom.Before(start) {
				start = rereadFrom
			}
		}
	}
	// The vendor's hard bounds. Reaching either means the span before it cannot be requested
	// at all, so the caller is told which days are unreachable instead of the walk advancing
	// past them in silence.
	if oldest := end.AddDate(0, 0, -maxReportLookbackDays); start.Before(oldest) {
		unreadableFrom, unreadableTo = start, oldest
		start = oldest
	}
	if start.Before(vendorFloorDate) {
		if unreadableFrom.IsZero() {
			unreadableFrom = start
		}
		unreadableTo = vendorFloorDate
		start = vendorFloorDate
	}
	if !start.Before(end) {
		return nil, unreadableFrom, unreadableTo
	}

	for from := start; from.Before(end) && (maxChunks <= 0 || len(chunks) < maxChunks); from = from.AddDate(0, 0, chunkDays) {
		to := from.AddDate(0, 0, chunkDays)
		if to.After(end) {
			to = end
		}
		chunks = append(chunks, reportChunk{StartingAt: from, EndingAt: to})
	}
	return chunks, unreadableFrom, unreadableTo
}

// encodeCursor marshals a position back into a framework cursor.
func encodeCursor(pos any) (*sdk.Cursor, error) {
	data, err := json.Marshal(pos)
	if err != nil {
		return nil, fmt.Errorf("marshalling position: %w", err)
	}
	return &sdk.Cursor{Data: data}, nil
}

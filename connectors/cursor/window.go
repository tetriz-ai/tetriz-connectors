// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// chunkDays slices a widened window into per-request spans. The vendor documents no
// maximum request range, and the prior design document's claim of a ninety-day cap is
// unverified, so the width is chosen to keep any single request small rather than to
// respect a limit we cannot confirm.
//
// Three rather than seven, because the width sets a ceiling. A chunk is only
// checkpointed once it completes, so an organisation whose events in ONE chunk need
// more requests than a run can pace through never finishes it — narrowing the span
// narrows what has to fit. Against the per-run pacing allowance that is roughly a
// hundred thousand events a day at three, against forty-five thousand at seven.
//
// The cost is one extra terminating request per added chunk — a chunk always spends a
// short page to learn it is done — so a ninety-day first sync issues about seventeen
// more. That is under three per cent for a busy organisation and irrelevant in
// absolute terms for a quiet one, which is why the width moves down and not up.
// Narrower still would keep paying that fixed cost for shrinking returns.
const chunkDays = 3

// dateLayout is the one format a stored date is written and read in.
const dateLayout = "2006-01-02"

// usagePosition is the stored sync position. It is a floor — the earliest day not
// yet safely covered — and deliberately not a high-water mark.
//
// Storing the newest event seen was considered and rejected: as a window anchor it is
// actively wrong, because an organisation that stops using Cursor would pin its start
// at its last event while the end advanced daily, widening the window without bound.
// And as a record of how far the data reached, it only duplicates what the stored
// records already say.
type usagePosition struct {
	// FloorDate is the earliest day not yet safely covered, as YYYY-MM-DD in
	// co-ordinated universal time. A run that covers its whole window sets this past the
	// window; a run interrupted partway leaves it at the first day it did not cover.
	FloorDate string `json:"floor_date"`
	// BackfillUntil is the last day the run that wrote this floor set out to cover. Every
	// run records it, and it is what makes a window safe to interrupt.
	//
	// Without it, "the recovery stopped a third of the way through" and "the connector
	// was paused for a quarter" are the same stored state — a floor well behind the
	// trailing window — and treating them alike has to be wrong in one
	// direction or the other. With it, a floor at or before the target is unfinished
	// work to resume, and a floor past it is a pause to leave alone.
	//
	// Every run writes it. An absent value parses to the zero time, which reads as a
	// satisfied target, the safe direction: it keeps the window narrow rather than
	// re-reading a quarter on the next tick.
	BackfillUntil string `json:"backfill_until,omitempty"`
	// FailedDays are days before FloorDate whose chunk failed on its own, as YYYY-MM-DD,
	// oldest first. The walk moves the floor past such a chunk and lists its days, and every
	// later run reads them again until they succeed; see maxFailedDays for the bound. Absent
	// in a position written before the field existed, which decodes as none.
	FailedDays []string `json:"failed_days,omitempty"`
}

// maxFailedDays bounds FailedDays. At the bound the walk stops advancing, as it did before
// failed days were tracked: a further chunk that fails ends the run there, so the floor
// stays on its first day and no listed day is ever dropped.
const maxFailedDays = 31

// windowReason names why a run's window is the shape it is. It is a label for whoever
// reads the logs and nothing else — no arithmetic branches on it.
//
// It is recorded because two of the shapes are otherwise indistinguishable from a
// healthy sync: a resume looks exactly like a first run, and a floor left behind by a
// pause looks exactly like an ordinary trailing read. Both advance the floor, so
// neither shows up as a stalled cursor either.
type windowReason string

const (
	// reasonFirstRun is the lookback, taken when no sync has run.
	reasonFirstRun windowReason = "first_run"
	// reasonResuming continues a widened window an earlier run left unfinished.
	reasonResuming windowReason = "resuming"
	// reasonTrailing is the steady state: the overlap and nothing more.
	reasonTrailing windowReason = "trailing"
	// reasonStaleFloor is a floor behind the trailing window that is deliberately not
	// resumed, because it records a pause rather than unfinished work.
	reasonStaleFloor windowReason = "stale_floor_not_resumed"
	// reasonRetryingFailed is a chunk of listed failed days before the window.
	reasonRetryingFailed windowReason = "retrying_failed"
)

// usageWindow is the span one run covers, in whole co-ordinated-universal-time days.
type usageWindow struct {
	Start  time.Time
	End    time.Time
	Reason windowReason
}

// newUsagePositionCursor serialises a floor into the framework's cursor envelope.
func newUsagePositionCursor(pos usagePosition) (*sdk.Cursor, error) {
	data, err := json.Marshal(pos)
	if err != nil {
		return nil, fmt.Errorf("marshalling cursor position: %w", err)
	}
	return &sdk.Cursor{Data: data, UpdatedAt: time.Now().UTC()}, nil
}

// parseUsagePosition reads a stored floor. A nil or empty cursor is not an error: it
// means no sync has run, which is what selects the first-run window.
//
// It resolves the stored dates here rather than leaving them as text for the window
// arithmetic to interpret. That keeps the parse where a failure can be reported —
// arithmetic has nowhere to send one, and a silently unparsed floor would look exactly
// like a caught-up one, which is the reading that loses history.
func parseUsagePosition(c *sdk.Cursor) (pos resolvedPosition, present bool, err error) {
	if c == nil || len(c.Data) == 0 {
		return resolvedPosition{}, false, nil
	}
	var stored usagePosition
	if err := json.Unmarshal(c.Data, &stored); err != nil {
		return resolvedPosition{}, false, fmt.Errorf("unmarshalling cursor position: %w", err)
	}
	if stored.FloorDate == "" {
		return resolvedPosition{}, false, nil
	}

	floor, err := time.ParseInLocation(dateLayout, stored.FloorDate, time.UTC)
	if err != nil {
		return resolvedPosition{}, false, fmt.Errorf("parsing stored floor %q: %w", stored.FloorDate, err)
	}
	out := resolvedPosition{Floor: floor}
	for _, d := range stored.FailedDays {
		if _, err := time.ParseInLocation(dateLayout, d, time.UTC); err != nil {
			return resolvedPosition{}, false, fmt.Errorf("parsing stored failed day %q: %w", d, err)
		}
	}
	out.FailedDays = stored.FailedDays
	if stored.BackfillUntil != "" {
		target, err := time.ParseInLocation(dateLayout, stored.BackfillUntil, time.UTC)
		if err != nil {
			return resolvedPosition{}, false, fmt.Errorf("parsing stored backfill target %q: %w", stored.BackfillUntil, err)
		}
		out.BackfillUntil = target
	}
	return out, true, nil
}

// resolvedPosition is a stored position with its dates already parsed, so the window
// arithmetic works in time.Time and cannot silently misread text.
type resolvedPosition struct {
	Floor time.Time
	// BackfillUntil is the zero time when none was stored. That needs no special case in
	// the arithmetic: no real floor precedes the zero time, so an absent target reads as
	// a satisfied one, which is the safe direction.
	BackfillUntil time.Time
	// FailedDays is the stored list, each entry already validated.
	FailedDays []string
}

// stored is the position as it was read, for a run that has covered nothing yet.
func (p resolvedPosition) stored() usagePosition {
	out := usagePosition{FloorDate: p.Floor.Format(dateLayout), FailedDays: p.FailedDays}
	if !p.BackfillUntil.IsZero() {
		out.BackfillUntil = p.BackfillUntil.Format(dateLayout)
	}
	return out
}

// chunkDayList is every day a chunk spans, as YYYY-MM-DD.
func chunkDayList(c usageWindow) []string {
	var out []string
	for d := startOfUTCDay(c.Start); !d.After(c.End); d = d.AddDate(0, 0, 1) {
		out = append(out, d.Format(dateLayout))
	}
	return out
}

// retryChunks groups the listed days before the window into chunks of at most chunkDays
// consecutive days, oldest first. A listed day inside the window is read by the window.
func retryChunks(failed []string, window usageWindow) []usageWindow {
	var out []usageWindow
	for _, f := range failed {
		d, err := time.ParseInLocation(dateLayout, f, time.UTC)
		if err != nil || !d.Before(window.Start) {
			continue
		}
		end := d.AddDate(0, 0, 1).Add(-time.Nanosecond)
		if n := len(out); n > 0 && out[n-1].End.Add(time.Nanosecond).Equal(d) &&
			out[n-1].End.Sub(out[n-1].Start) < time.Duration(chunkDays-1)*24*time.Hour {
			out[n-1].End = end
			continue
		}
		out = append(out, usageWindow{Start: d, End: end, Reason: reasonRetryingFailed})
	}
	return out
}

// resolveUsageWindow decides the span a run covers.
//
// Both ends come from the clock, not from the stored position: the end is the end of
// the previous whole day, and the start is that end less the trailing overlap. So the
// window is a fixed width and slides forward daily whether or not the organisation
// was active — which is what stops a dormant organisation from asking for an
// ever-wider range, and it needs no stored state at all.
//
// The stored position exists to widen that window in the case where the clock alone
// would lose data: a widened window an earlier run did not finish. Beyond that, the
// position's only job is to exist, which is what separates a first sync from a later
// one.
//
// The current day is deliberately excluded. It is unsettled — the vendor's two usage
// endpoints disagree about it — so reading it would store figures that change
// underneath us.
func resolveUsageWindow(now time.Time, pos resolvedPosition, present bool, lookbackDays, overlapDays int) usageWindow {
	// End of the previous whole day: the last instant of yesterday, in UTC.
	endOfYesterday := startOfUTCDay(now).Add(-time.Nanosecond)

	if !present {
		return usageWindow{
			Start:  startOfUTCDay(now).AddDate(0, 0, -lookbackDays),
			End:    endOfYesterday,
			Reason: reasonFirstRun,
		}
	}

	trailingStart := startOfUTCDay(now).AddDate(0, 0, -overlapDays)
	if !pos.Floor.Before(trailingStart) {
		// The floor has already caught up with the trailing window, so there is nothing
		// to widen for.
		return usageWindow{Start: trailingStart, End: endOfYesterday, Reason: reasonTrailing}
	}

	// A floor short of its target is unfinished work. A widened window is chunked and
	// checkpoints as it goes, so any interruption leaves the floor partway through, and
	// reading that as steady state abandons every remaining day silently — no error and
	// nothing in the metrics.
	//
	// The comparison is inclusive because the two dates count differently: the floor is
	// the first day NOT covered, the target the last day that WAS to be covered. A floor
	// equal to the target still has that day outstanding, and a strict comparison
	// dropped it.
	if pos.Floor.After(pos.BackfillUntil) {
		// A satisfied target whose floor has since gone stale is a paused connector, and
		// re-reading the pause would reintroduce the unbounded growth the clock-derived
		// window exists to prevent. Recovering a pause is a deliberate backfill, not
		// something a daily window should quietly attempt.
		return usageWindow{Start: trailingStart, End: endOfYesterday, Reason: reasonStaleFloor}
	}

	// A resume runs to the clock: everything from the floor forward is genuinely uncovered, so there is no redundant
	// work to avoid by stopping earlier. Stopping at the target instead looks tidier and
	// silently loses data — the target does not advance across resumes while the clock
	// does, so a backfill finishing a few days after it started left the floor short of
	// the trailing window with the target already satisfied, which the branch above
	// correctly reads as a pause. The days in between were then requested by no run.
	//
	// This is not the unbounded growth that branch refuses. That case is a FINISHED
	// target whose floor merely went stale, where the intervening days were covered and
	// re-reading them buys nothing. Here they were never covered at all.
	return usageWindow{Start: pos.Floor, End: endOfYesterday, Reason: reasonResuming}
}

// chunks splits a window into per-request spans, so no single request covers more than
// chunkDays. A window narrower than that yields exactly one span.
//
// Every window goes through the same slicing, and never a branch on why the window was
// widened. A first run or a resume can be months wide, and sending that as one request is
// the case chunking exists to prevent: the request that fails is the one that would let
// the walk catch up, so every later run would fail identically.
//
// The last chunk ends exactly at the window end rather than overshooting, so a run
// never asks for the unsettled current day.
func (w usageWindow) chunks() []usageWindow {
	var out []usageWindow
	for start := w.Start; !start.After(w.End); start = start.AddDate(0, 0, chunkDays) {
		end := start.AddDate(0, 0, chunkDays).Add(-time.Nanosecond)
		if end.After(w.End) {
			end = w.End
		}
		out = append(out, usageWindow{Start: start, End: end, Reason: w.Reason})
	}
	return out
}

// startOfUTCDay truncates to midnight in co-ordinated universal time.
//
// Explicitly UTC rather than the process location, and that is load-bearing rather
// than tidy: the consumer snaps a record's business date to start-of-day in whatever
// location the time it receives carries, so a locally-located time yields a local
// midnight — a different storage partition and a different uniqueness key on a run
// that happened to be located differently.
func startOfUTCDay(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// coveredPosition is the floor after a chunk or window was covered: the day after its
// end, since everything up to that end is now stored.
func coveredPosition(covered usageWindow, whole usageWindow) usagePosition {
	return usagePosition{
		FloorDate:     startOfUTCDay(covered.End).AddDate(0, 0, 1).Format(dateLayout),
		BackfillUntil: backfillTarget(whole),
	}
}

// backfillTarget records how far the window set out to reach, so a run interrupted
// partway through stays distinguishable from a connector that was paused.
//
// Every window records it, with no test for which kind of window this is, and that is
// deliberate. Deciding per window would record a target only for some windows, which leaves an
// interrupted recovery unrecognisable as unfinished, and any such test would re-arm that for
// whatever window shape is added next.
//
// Recording it always is safe because a narrow window satisfies its own target
// immediately: its floor lands the day after its end, so a later run finds the floor
// already past the target and reads it as finished, which is what it is.
func backfillTarget(whole usageWindow) string {
	return startOfUTCDay(whole.End).Format(dateLayout)
}

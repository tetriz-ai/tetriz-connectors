// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// This file is the connector's read side for an operator, and it exists so the stored
// position has exactly ONE parser.
//
// The three position shapes are unexported on purpose: cursor_shape_contract_test.go
// freezes their JSON tags, and that test can only see the shapes declared in this
// package. A caller outside it that re-declared them would be guarded by neither the
// compiler nor that test, so a tag rename would leave the caller reporting "never
// synced" for every organisation while the connector kept working. Exporting a decode
// FUNCTION instead keeps the shapes and their contract test on the same side of the
// package boundary.

// PositionShape names which of the three stored shapes a row decoded as. An operator
// reading a report needs it: the shapes answer different questions, and a report that
// silently rendered one as another would invite the wrong repair.
type PositionShape string

const (
	// PositionShapeActivity is the per-day walk's position: a covered calendar day.
	PositionShapeActivity PositionShape = "activity"
	// PositionShapeReport is the chunked report walk's position: a covered point plus
	// the vendor's own statement of how current its data is.
	PositionShapeReport PositionShape = "report"
	// PositionShapeRoster is the roster snapshot's record. It is not a resume point —
	// see PositionReport.Resumable.
	PositionShapeRoster PositionShape = "roster"
)

// PositionReport is what a stored position can actually answer. It deliberately carries
// no run outcome: nothing in connector_sync_states records whether a run reached the
// present, so a field for it could only ever be a guess. The covered and yielded
// counters are where that lives.
type PositionReport struct {
	Shape PositionShape

	// Present is false when a row exists but holds no covered point, which the walks
	// read as "no sync has ever run" and answer by starting from the lookback.
	Present bool

	// Covered is the point the walk has read through. Its meaning differs by shape and
	// the difference is load-bearing for anyone reasoning about a gap: for an activity
	// walk it is the last day fully covered, for a report walk the exclusive end of the
	// last covered chunk, and for the roster the date of the last snapshot.
	Covered time.Time

	// Watermark is the vendor's own statement of how current its data is, present only
	// on a report position. Empty is not an error: the vendor omits it, and the walk
	// treats an absent watermark as "unbounded" rather than as stale.
	//
	// Kept as the raw string because it is compared against the vendor's next response
	// verbatim; RefreshedDay is the same instant at the grain a date comparison needs.
	Watermark    string
	RefreshedDay time.Time

	// Age is how far behind the caller's clock Covered sits. Zero when no position is
	// present, because "the epoch" is not an age.
	Age time.Duration

	// MemberCount is the roster's denominator, meaningful only for that shape.
	MemberCount int

	// Resumable reports whether rewriting this position changes what the next run
	// fetches. It is false for the roster, which starts from the first page every run
	// and never reads its cursor, so a rewrite there corrupts the operator-facing
	// record and changes nothing else.
	Resumable bool
}

// DescribeStoredPosition decodes one stored cursor for one operation.
//
// now is passed rather than read from the clock so a caller can render a report against
// a fixed instant and a test can assert an age.
//
// The three failure states are kept distinct because they call for three different
// repairs: no cursor at all (nothing has run), a cursor carrying no covered point (a
// run started and never completed a unit), and a cursor that will not parse (the shape
// moved, or the row is corrupt — the connector answers this one by restarting from the
// lookback, silently). Collapsing them into "not synced" is how a confident false stall
// gets reported.
func DescribeStoredPosition(
	opID sdk.OperationID, cur *sdk.Cursor, now time.Time,
) (PositionReport, error) {
	shape, ok := positionShapeFor(opID)
	if !ok {
		return PositionReport{}, fmt.Errorf("%s is not a %s operation", opID, connectorID)
	}
	report := PositionReport{Shape: shape, Resumable: shape != PositionShapeRoster}

	switch shape {
	case PositionShapeActivity:
		_, covered, present, err := parseActivityPosition(cur)
		if err != nil {
			return PositionReport{}, fmt.Errorf("reading the stored %s position: %w", opID, err)
		}
		report.Present, report.Covered = present, covered

	case PositionShapeReport:
		pos, err := parseReportPosition(cur)
		if err != nil {
			return PositionReport{}, fmt.Errorf("reading the stored %s position: %w", opID, err)
		}
		report.Present, report.Covered = pos.Present, pos.Covered
		report.Watermark, report.RefreshedDay = pos.Stored.DataRefreshedAt, pos.RefreshedDay

	case PositionShapeRoster:
		// No parser exists for the roster because the walk never reads it back, so the
		// decode lives here rather than being added to the walk's side unused.
		if cur == nil || len(cur.Data) == 0 {
			return report, nil
		}
		var stored rosterPosition
		if err := json.Unmarshal(cur.Data, &stored); err != nil {
			return PositionReport{}, fmt.Errorf("unmarshalling the stored %s position: %w", opID, err)
		}
		if stored.SnapshotDate == "" {
			return report, nil
		}
		day, err := time.ParseInLocation(dateLayout, stored.SnapshotDate, time.UTC)
		if err != nil {
			return PositionReport{}, fmt.Errorf("parsing snapshot_date %q: %w", stored.SnapshotDate, err)
		}
		report.Present, report.Covered, report.MemberCount = true, day, stored.MemberCount
	}

	if report.Present {
		report.Age = now.UTC().Sub(report.Covered)
	}
	return report, nil
}

// RewindPosition re-encodes a position so the next scheduled run re-reads from `from`.
//
// Only the lower bound is expressible, and callers must say so rather than accepting an
// upper one: both planners end their window at the clock, never at a stored value, so a
// rewind re-reads from `from` forward to the present.
//
// It rewinds, and only rewinds. Four refusals bound it, and each exists because the state it
// would otherwise write is one the walk itself can never produce — so nothing downstream is
// built to survive it:
//
//   - a day after now parks the operation. Both planners resolve an empty forward window
//     until the clock catches up, and every health signal reads green while it does: the
//     activity walk returns before touching an outcome counter, the report walk counts a
//     COVER, and the far-behind counter sees a negative age so it cannot fire.
//   - a day at or after the stored covered point is a fast-forward, not a rewind. The
//     intervening days are then skipped permanently — the activity walk has no other path
//     back to them, and a report jump deeper than the trailing re-read is equally lost.
//   - for a report position, a day after the stored watermark freezes the walk. The claim is
//     clamped to the watermark, so the chunk is fetched, paged and emitted on every run while
//     the position never advances — and the run still counts a cover. Up to a month of paying
//     full vendor cost for no progress.
//   - the vendor bounds: no data exists before the floor, and a report start beyond the
//     year limit cannot be requested at all.
//
// The watermark is round-tripped as the OPAQUE STRING it was stored as, never re-rendered.
// Its stored form is compared against the vendor's next response verbatim, so a re-rendered
// value would read as a moved watermark on every run and re-read the trailing window
// unconditionally.
//
// now is a parameter because two of the bounds are relative to it. Reading the clock here
// instead would leave them untestable, and one of them decides whether a deep repair is
// possible at all.
func RewindPosition(
	opID sdk.OperationID, cur *sdk.Cursor, from, now time.Time,
) (*sdk.Cursor, error) {
	shape, ok := positionShapeFor(opID)
	if !ok {
		return nil, fmt.Errorf("%s is not a %s operation", opID, connectorID)
	}
	if shape == PositionShapeRoster {
		return nil, fmt.Errorf("%s does not resume from a position, so there is nothing to rewind", opID)
	}

	day := from.UTC().Truncate(24 * time.Hour)
	today := now.UTC().Truncate(24 * time.Hour)

	if day.After(today) {
		return nil, fmt.Errorf("%s is in the future; a position there parks %s until the clock reaches it, and no health signal reports the stall",
			day.Format(dateLayout), opID)
	}
	if day.Before(vendorFloorDate) {
		return nil, fmt.Errorf("%s precedes the vendor's %s floor, before which no data exists",
			day.Format(dateLayout), vendorFloorDate.Format(dateLayout))
	}

	current, err := DescribeStoredPosition(opID, cur, now)
	if err != nil {
		return nil, fmt.Errorf("reading the stored %s position before rewinding it: %w", opID, err)
	}
	if !current.Present {
		// A row with no covered point is a run that began and completed no unit — a reachable
		// state the read side reports as its own. Writing a day into it is not a rewind, it is
		// an initialisation from nothing: the next run would treat everything before that day
		// as already covered, having read none of it. An uninitialised operation catches up
		// through its first-sync lookback, which is a setting, not a repair.
		return nil, fmt.Errorf("%s has no covered point to rewind from; it has completed no unit, so it is not behind — it has not started. Leave it to the first-sync lookback rather than writing a position into it", opID)
	}
	if !day.Before(current.Covered) {
		return nil, fmt.Errorf("%s is at or after the covered point %s, so this would advance the position rather than rewind it and the days between would never be read again",
			day.Format(dateLayout), current.Covered.Format(dateLayout))
	}

	switch shape {
	case PositionShapeActivity:
		// The failed days are carried: a rewind re-reads from `from` forward, and a failed day
		// before it would otherwise never be read again.
		stored, _, _, err := parseActivityPosition(cur)
		if err != nil {
			return nil, fmt.Errorf("reading the stored %s position before rewinding it: %w", opID, err)
		}
		cur, err := encodeCursor(activityPosition{CoveredThrough: day.Format(dateLayout), FailedDays: stored.FailedDays})
		if err != nil {
			return nil, fmt.Errorf("encoding the rewound %s position: %w", opID, err)
		}
		return cur, nil

	default:
		// Compared at day grain on both sides. now carries a time of day, so subtracting the
		// limit from it directly would refuse the earliest day the planner will happily read.
		if lookback := today.AddDate(0, 0, -maxReportLookbackDays); day.Before(lookback) {
			return nil, fmt.Errorf("%s is beyond the vendor's %d-day limit for %s, past which a start cannot be requested at all",
				day.Format(dateLayout), maxReportLookbackDays, opID)
		}
		// The failed chunks are carried, for the reason the activity case gives.
		stored, err := parseReportPosition(cur)
		if err != nil {
			return nil, fmt.Errorf("reading the stored %s position before rewinding it: %w", opID, err)
		}
		pos := reportPosition{CoveredThrough: day.Format(dateLayout), FailedChunks: stored.Stored.FailedChunks}
		if current.Watermark != "" {
			if !current.RefreshedDay.IsZero() && day.After(current.RefreshedDay) {
				return nil, fmt.Errorf("%s is after the vendor's own watermark of %s, which would freeze %s: every run would re-fetch the window without advancing. Rewind to %s or earlier",
					day.Format(dateLayout), current.RefreshedDay.Format(dateLayout), opID,
					current.RefreshedDay.Format(dateLayout))
			}
			pos.DataRefreshedAt = current.Watermark
		}
		cur, err := encodeCursor(pos)
		if err != nil {
			return nil, fmt.Errorf("encoding the rewound %s position: %w", opID, err)
		}
		return cur, nil
	}
}

// ActivityRewindWidening is how many days earlier than the requested day an activity walk
// actually begins.
//
// A rewind to X does not start the next run at X: the walk re-enters a trailing overlap so
// a day that arrived late is still picked up, which means it begins overlapDays-1 days
// earlier. The number is per-organisation, so a caller that wants to state it must pass the
// organisation's real config — passing nil yields the default and would understate an
// organisation that overrides the overlap.
func ActivityRewindWidening(cfg *sdk.ConnectorConfig, opID sdk.OperationID) (int, error) {
	opts, err := parseOperationOptions(cfg, opID)
	if err != nil {
		return 0, fmt.Errorf("reading the operation overrides for %s: %w", opID, err)
	}
	return opts.overlapDays() - 1, nil
}

func positionShapeFor(opID sdk.OperationID) (PositionShape, bool) {
	switch opID {
	case opFetchAnalyticsUsers, opFetchAnalyticsSkills:
		return PositionShapeActivity, true
	case opFetchAnalyticsUserCost, opFetchAnalyticsUserUsage:
		return PositionShapeReport, true
	case opFetchOrganizationMembers:
		return PositionShapeRoster, true
	default:
		return "", false
	}
}

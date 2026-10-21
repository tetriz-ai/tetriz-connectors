// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// dateLayout is the day format used in three places that must agree: the record key's
// trailing component, the stored position, and an operator-facing report. One constant so a
// format change cannot move one of them and leave the others behind.
const dateLayout = "2006-01-02"

// position is what the walk stores between runs: the last calendar day it read in full.
//
// Unexported, with the JSON tags frozen by a contract test, because that test can only see
// shapes declared in this package. A caller outside it that re-declared this struct would be
// guarded by neither the compiler nor that test, so a tag rename would leave the caller
// reporting "never synced" for every organisation while the connector kept working. The
// exported decoder below is what crosses the package boundary instead.
type position struct {
	// CoveredThrough is the last day read to completion, as YYYY-MM-DD. A day appears here
	// only once every page of it has been read and emitted, so it is a statement about
	// coverage rather than progress.
	CoveredThrough string `json:"covered_through"`
	// WorkspaceID is the workspace the coverage above was read FROM, and it is what makes
	// that coverage meaningful.
	//
	// Neither sibling needs this, because their credential implies the tenant and a
	// reconnect cannot re-point them. A Codex reconnect can: the administrator types a
	// different workspace and the sync-state row survives it. Without the workspace here, the
	// next run reads a covered day belonging to the OLD workspace, treats the new one as
	// already backfilled, and reads only the trailing overlap — permanently omitting the rest
	// of its lookback, with no error anywhere. Every run stamps it, so a position without
	// one belongs to no workspace and is discarded like one for another workspace.
	WorkspaceID string `json:"workspace_id,omitempty"`
	// FailedDays are days at or before CoveredThrough whose read failed, as YYYY-MM-DD,
	// oldest first. The walk advances past a day that fails on its own (a vendor error for
	// that day) and lists it here, and every later run reads the listed days again until
	// each succeeds. So CoveredThrough means "read in full, except the days listed", and a
	// failed day is never skipped. Absent in a position written before this field existed,
	// which decodes as no failed days.
	FailedDays []string `json:"failed_days,omitempty"`
}

// maxFailedDays bounds FailedDays. At the bound the walk stops advancing, as it did
// before failed days were tracked: a further day that fails ends the run there, so the
// position stays on the day before it and no failed day is ever dropped from the list.
const maxFailedDays = 31

// PositionReport is what a stored position can answer for an operator.
type PositionReport struct {
	// Readable is false when a row exists but its bytes do not decode. Distinct from Present
	// because the two call for different repairs: an absent position is a connector that has
	// not run, an unreadable one is a row that needs rewriting.
	Readable bool
	// Unreadable is why the decode failed, and it is the whole value of the report in that
	// case. "Not readable" alone tells an operator to rewrite a row without telling them what
	// is wrong with it — a truncated JSON body and a covered_through holding a malformed date
	// need different repairs, and this is the only place that distinction survives. Empty
	// whenever Readable is true.
	Unreadable string
	// Present is false when a row exists but holds no covered day, which the walk reads as
	// "no sync has ever run" and answers by starting from the lookback.
	Present bool
	// Covered is the last day read in full.
	Covered time.Time
}

// DecodePosition reads a stored cursor into an operator-facing report.
//
// An unreadable position comes back as absent with Readable false, never as an error, and
// that is deliberate: this is what an operator reaches for when a position is SUSPECT, so
// failing on the one input they are investigating would be the worst possible moment to
// refuse. It matches how the walk treats the same bytes — restart from the lookback rather
// than stall — while still letting a report distinguish "never synced" from "we could not
// read what is stored", which call for different repairs.
func DecodePosition(cur *sdk.Cursor) PositionReport {
	_, covered, present, err := parsePosition(cur)
	if err != nil {
		// Carried rather than dropped. This function deliberately does not return an error —
		// an operator reaches for it precisely when a position is suspect — but "deliberately
		// not fatal" is not the same as "deliberately unexplained".
		return PositionReport{Readable: false, Unreadable: err.Error()}
	}
	return PositionReport{Readable: true, Present: present, Covered: covered}
}

// parsePosition reads the stored cursor. It returns present=false for an absent cursor and
// for one carrying no covered day, so a first run and a run whose position was cleared take
// the same path.
func parsePosition(cur *sdk.Cursor) (position, time.Time, bool, error) {
	if cur == nil || len(cur.Data) == 0 {
		return position{}, time.Time{}, false, nil
	}
	var p position
	if err := json.Unmarshal(cur.Data, &p); err != nil {
		return position{}, time.Time{}, false, fmt.Errorf("unmarshalling position: %w", err)
	}
	for _, d := range p.FailedDays {
		if _, err := time.Parse(dateLayout, d); err != nil {
			return position{}, time.Time{}, false, fmt.Errorf("parsing failed day %q: %w", d, err)
		}
	}
	if p.CoveredThrough == "" {
		return p, time.Time{}, false, nil
	}
	day, err := time.Parse(dateLayout, p.CoveredThrough)
	if err != nil {
		return position{}, time.Time{}, false, fmt.Errorf("parsing covered_through %q: %w", p.CoveredThrough, err)
	}
	return p, day.UTC(), true, nil
}

// positionFor reads the stored cursor and reports whether its coverage may be trusted for
// workspaceID.
//
// A position written against a DIFFERENT workspace is discarded rather than honoured: its
// covered day says nothing about the workspace now configured, and treating it as coverage
// would skip that workspace's backfill entirely. Discarding costs a re-read of the lookback,
// which is the correct price for a connector that was just re-pointed.
func positionFor(cur *sdk.Cursor, workspaceID string) (position, time.Time, bool, error) {
	stored, covered, present, err := parsePosition(cur)
	if err != nil {
		return stored, covered, present, fmt.Errorf("reading the position for workspace %q: %w", workspaceID, err)
	}
	if !present {
		return stored, covered, present, nil
	}
	if stored.WorkspaceID != workspaceID {
		return position{}, time.Time{}, false, nil
	}
	return stored, covered, present, nil
}

// storePosition encodes a covered day back into a cursor.
func storePosition(p position) (*sdk.Cursor, error) {
	encoded, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshalling codex position: %w", err)
	}
	return &sdk.Cursor{Data: encoded}, nil
}

// RewindPosition returns the position a rewind to `from` should store, refusing a rewind that
// would not actually re-open anything.
//
// Exported for the operational job, which is the only caller. It lives here rather than in that
// job because the position's meaning lives here: a rewind is "set covered_through to the day
// BEFORE the one to resume from", and a job that encoded that arithmetic itself would drift from
// this package's own reading of the same field.
//
// The workspace is carried through unchanged. A rewind re-reads days for the workspace the
// connector is pointed at now; dropping it would make the next run treat the position as
// belonging to an unknown workspace and re-read the whole lookback instead of the named span.
func RewindPosition(cur *sdk.Cursor, from time.Time, now time.Time) (*sdk.Cursor, error) {
	stored, covered, present, err := parsePosition(cur)
	if err != nil {
		return nil, fmt.Errorf("the stored position is unreadable, so it cannot be rewound safely: %w", err)
	}
	from = from.UTC().Truncate(24 * time.Hour)
	latest := now.UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	if from.After(latest) {
		return nil, fmt.Errorf("cannot resume from %s: the newest readable day is %s, because a partially aggregated day is never requested",
			from.Format(dateLayout), latest.Format(dateLayout))
	}
	// A FAST-FORWARD is refused; a repeat is not. The distinction is the whole difference
	// between a safe repair tool and a destructive one.
	//
	// Resuming from a day strictly beyond the next uncovered one would move the position
	// FORWARD over days nothing ever reads again — the trailing overlap does not reach that
	// far, so those days' records are never written and no later run notices. That is data
	// loss dressed as a repair, so it is refused.
	//
	// Resuming from exactly the next uncovered day is a no-op, and it is ALLOWED. An operator
	// who is unsure whether the first rewind landed re-runs it; refusing there would make the
	// tool non-idempotent at precisely the moment someone is repeating themselves out of
	// caution, and would read as failure when the state is already correct.
	if present && from.After(covered.AddDate(0, 0, 1)) {
		return nil, fmt.Errorf("cannot resume from %s: the position covers through %s, so this would skip %s..%s forward and those days are never read again",
			from.Format(dateLayout), covered.Format(dateLayout),
			covered.AddDate(0, 0, 1).Format(dateLayout), from.AddDate(0, 0, -1).Format(dateLayout))
	}
	stored.CoveredThrough = from.AddDate(0, 0, -1).Format(dateLayout)
	rewound, err := storePosition(stored)
	if err != nil {
		return nil, fmt.Errorf("encoding the rewound position: %w", err)
	}
	return rewound, nil
}

// DescribeStoredPosition reports what a stored position says, for an operator.
//
// `now` is a parameter rather than read from the clock so the caller's single reading of the
// time is what every line of one report is computed against — two calls to time.Now() inside one
// render can straddle midnight and print a report that disagrees with itself.
func DescribeStoredPosition(cur *sdk.Cursor, now time.Time) (PositionReport, time.Duration) {
	report := DecodePosition(cur)
	if !report.Readable || !report.Present {
		return report, 0
	}
	return report, now.UTC().Sub(report.Covered)
}

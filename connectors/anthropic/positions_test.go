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

// TestDescribeStoredPositionSeparatesTheThreeAbsences is the point of the whole read side.
//
// No row, a row with no covered point, and a row that will not parse are three different
// operator actions — nothing has run, a run started and finished no unit, the shape moved or
// the row is corrupt. The connector answers the third by silently restarting from the
// lookback, so a report that collapsed them into "not synced" would send an operator to fix
// the wrong thing, confidently.
func TestDescribeStoredPositionSeparatesTheThreeAbsences(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)

	t.Run("no cursor at all", func(t *testing.T) {
		t.Parallel()
		got, err := DescribeStoredPosition(opFetchAnalyticsUsers, nil, now)
		require.NoError(t, err)
		assert.False(t, got.Present)
		assert.Zero(t, got.Age, "the epoch is not an age")
	})

	t.Run("a cursor carrying no covered point", func(t *testing.T) {
		t.Parallel()
		got, err := DescribeStoredPosition(opFetchAnalyticsUsers,
			&sdk.Cursor{Data: json.RawMessage(`{}`)}, now)
		require.NoError(t, err)
		assert.False(t, got.Present, "a run began and completed no unit; that is not a parse failure")
	})

	t.Run("a cursor that will not parse", func(t *testing.T) {
		t.Parallel()
		_, err := DescribeStoredPosition(opFetchAnalyticsUsers,
			&sdk.Cursor{Data: json.RawMessage(`{"covered_through":"not-a-date"}`)}, now)
		require.Error(t, err, "an unreadable position must surface, not read as absent")
	})
}

// TestDescribeStoredPositionReadsEachShape walks all five operations, because the report is
// per-operation and a shape rendered as another shape invites the wrong repair.
func TestDescribeStoredPositionReadsEachShape(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	covered := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)

	activity, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
	require.NoError(t, err)
	report, err := encodeCursor(reportPosition{
		CoveredThrough:  covered.Format(dateLayout),
		DataRefreshedAt: "2026-08-19T06:00:00Z",
	})
	require.NoError(t, err)
	roster, err := encodeCursor(rosterPosition{
		SnapshotDate: covered.Format(dateLayout), MemberCount: 42,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		opID          sdk.OperationID
		cur           *sdk.Cursor
		wantShape     PositionShape
		wantWatermark string
		wantMembers   int
		wantResumable bool
	}{
		{opFetchAnalyticsUsers, activity, PositionShapeActivity, "", 0, true},
		{opFetchAnalyticsSkills, activity, PositionShapeActivity, "", 0, true},
		{opFetchAnalyticsUserCost, report, PositionShapeReport, "2026-08-19T06:00:00Z", 0, true},
		{opFetchAnalyticsUserUsage, report, PositionShapeReport, "2026-08-19T06:00:00Z", 0, true},
		{opFetchOrganizationMembers, roster, PositionShapeRoster, "", 42, false},
	} {
		t.Run(string(tt.opID), func(t *testing.T) {
			t.Parallel()
			got, err := DescribeStoredPosition(tt.opID, tt.cur, now)
			require.NoError(t, err)
			assert.Equal(t, tt.wantShape, got.Shape)
			assert.True(t, got.Present)
			assert.Equal(t, covered, got.Covered)
			assert.Equal(t, 7*24*time.Hour, got.Age, "seven days behind the fixed clock")
			assert.Equal(t, tt.wantWatermark, got.Watermark)
			assert.Equal(t, tt.wantMembers, got.MemberCount)
			assert.Equal(t, tt.wantResumable, got.Resumable,
				"only a walk that reads its cursor back can be rewound")
		})
	}
}

// TestRewindPreservesTheWatermarkVerbatim. The stored watermark is compared against the
// vendor's next response as an opaque string, so re-rendering it — even to an equivalent
// instant — reads as a moved watermark on every run and re-reads the trailing window
// unconditionally. Round-tripping the raw string is the contract.
func TestRewindPreservesTheWatermarkVerbatim(t *testing.T) {
	t.Parallel()

	// A form no Go renderer would reproduce: an offset rather than Z, and a fractional second.
	const stored = "2026-08-19T06:00:00.250+02:00"
	cur, err := encodeCursor(reportPosition{CoveredThrough: "2026-08-20", DataRefreshedAt: stored})
	require.NoError(t, err)

	out, err := RewindPosition(opFetchAnalyticsUserCost, cur,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	var got reportPosition
	require.NoError(t, json.Unmarshal(out.Data, &got))
	assert.Equal(t, "2026-08-01", got.CoveredThrough)
	assert.Equal(t, stored, got.DataRefreshedAt, "byte-for-byte, or every run re-reads the window")
}

// TestRewindRefusesWhatCannotBeRead covers the three bounds, each for its own reason: no data
// exists before the vendor floor; a report start beyond the year limit cannot be requested at
// all; and the roster never reads its cursor, so rewinding it changes nothing about what is
// fetched and only falsifies the operator-facing record.
func TestRewindRefusesWhatCannotBeRead(t *testing.T) {
	t.Parallel()

	// A clock far enough past the vendor floor that the year limit is the binding bound.
	// With a present-day clock the floor is the tighter of the two and the year limit is
	// unreachable, so a test using time.Now would assert the wrong branch and pass.
	late := time.Date(2028, 6, 1, 0, 0, 0, 0, time.UTC)

	// Every case supplies a position that EXISTS and is well ahead of the requested day, so each
	// trips the bound under test rather than the uninitialised-position guard that precedes it.
	present, err := encodeCursor(activityPosition{CoveredThrough: late.Format(dateLayout)})
	require.NoError(t, err)
	presentReport, err := encodeCursor(reportPosition{CoveredThrough: late.Format(dateLayout)})
	require.NoError(t, err)
	presentFor := func(op sdk.OperationID) *sdk.Cursor {
		if op == opFetchAnalyticsUserCost || op == opFetchAnalyticsUserUsage {
			return presentReport
		}
		return present
	}

	for _, tt := range []struct {
		name    string
		opID    sdk.OperationID
		from    time.Time
		now     time.Time
		wantErr string
	}{
		{
			name:    "before the vendor floor",
			opID:    opFetchAnalyticsUsers,
			from:    vendorFloorDate.AddDate(0, 0, -1),
			now:     late,
			wantErr: "floor",
		},
		{
			name:    "a report start past the vendor's year limit",
			opID:    opFetchAnalyticsUserCost,
			from:    late.AddDate(0, 0, -(maxReportLookbackDays + 5)),
			now:     late,
			wantErr: "day limit",
		},
		{
			name:    "the roster, which does not resume",
			opID:    opFetchOrganizationMembers,
			from:    late.AddDate(0, 0, -10),
			now:     late,
			wantErr: "does not resume",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := RewindPosition(tt.opID, presentFor(tt.opID), tt.from, tt.now)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestRewindAcceptsTheVendorFloorItself — the floor is the earliest readable day, not the
// first refused one. An off-by-one here refuses the deepest legitimate repair.
func TestRewindAcceptsTheVendorFloorItself(t *testing.T) {
	t.Parallel()

	covered, err := encodeCursor(activityPosition{
		CoveredThrough: vendorFloorDate.AddDate(0, 0, 10).Format(dateLayout),
	})
	require.NoError(t, err)
	out, err := RewindPosition(opFetchAnalyticsUsers, covered, vendorFloorDate,
		vendorFloorDate.AddDate(0, 1, 0))
	require.NoError(t, err)
	var got activityPosition
	require.NoError(t, json.Unmarshal(out.Data, &got))
	assert.Equal(t, vendorFloorDate.Format(dateLayout), got.CoveredThrough)
}

// TestActivityRewindWideningIsPerOrganisation. A rewind to X starts the walk earlier than X,
// and by how much is an organisation's own setting — so an operator told the default while
// their organisation overrides it is told the wrong number about a vendor bill.
func TestActivityRewindWideningIsPerOrganisation(t *testing.T) {
	t.Parallel()

	got, err := ActivityRewindWidening(nil, opFetchAnalyticsUsers)
	require.NoError(t, err)
	assert.Equal(t, defaultActivityOverlapDays-1, got, "no config means the default")

	cfg := &sdk.ConnectorConfig{OperationOverrides: map[sdk.OperationID]json.RawMessage{
		opFetchAnalyticsUsers: json.RawMessage(`{"overlap_days":5}`),
	}}
	got, err = ActivityRewindWidening(cfg, opFetchAnalyticsUsers)
	require.NoError(t, err)
	assert.Equal(t, 4, got, "an override widens the re-read, and the operator is owed that number")
}

// TestDescribeRejectsAForeignOperation — the report is per-operation, so an operation this
// connector does not own must fail rather than decode as one of the three shapes by default.
func TestDescribeRejectsAForeignOperation(t *testing.T) {
	t.Parallel()

	_, err := DescribeStoredPosition(sdk.OperationID("fetch_pull_requests"), nil, time.Now())
	require.Error(t, err)
	_, err = RewindPosition(sdk.OperationID("fetch_pull_requests"), nil, time.Now(), time.Now())
	require.Error(t, err)
}

// TestARewindIsRefusedUnlessItActuallyRewinds pins the four refusals together, because they
// share one property: each would write a position the WALK ITSELF can never produce, so nothing
// downstream is built to survive it.
func TestARewindIsRefusedUnlessItActuallyRewinds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 14, 30, 0, 0, time.UTC)
	covered := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)

	activity, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
	require.NoError(t, err)
	report, err := encodeCursor(reportPosition{
		CoveredThrough:  covered.Format(dateLayout),
		DataRefreshedAt: "2026-08-14T06:00:00Z",
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name    string
		opID    sdk.OperationID
		cur     *sdk.Cursor
		from    time.Time
		wantErr string
	}{
		{
			name:    "a day in the future parks the operation with every signal green",
			opID:    opFetchAnalyticsUsers,
			cur:     activity,
			from:    now.AddDate(0, 0, 30),
			wantErr: "in the future",
		},
		{
			name:    "even one day into the future, which renders as 0d behind",
			opID:    opFetchAnalyticsUsers,
			cur:     activity,
			from:    now.AddDate(0, 0, 1),
			wantErr: "in the future",
		},
		{
			name:    "a day after the covered point is a fast-forward, not a rewind",
			opID:    opFetchAnalyticsUsers,
			cur:     activity,
			from:    covered.AddDate(0, 0, 3),
			wantErr: "would advance the position rather than rewind it",
		},
		{
			name:    "the covered point itself, which moves nothing and re-reads nothing",
			opID:    opFetchAnalyticsUsers,
			cur:     activity,
			from:    covered,
			wantErr: "at or after the covered point",
		},
		{
			name:    "a report day after the vendor's watermark freezes the walk",
			opID:    opFetchAnalyticsUserCost,
			cur:     report,
			from:    time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC),
			wantErr: "after the vendor's own watermark",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := RewindPosition(tt.opID, tt.cur, tt.from, now)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestARewoundPositionIsConsumedAsARewind composes the rewind with the planners that read it.
//
// This is the gap every one of the rewind's defects lived in: the bytes were valid and the
// refusals were absent, so nothing failed until a planner consumed them. Asserting on the
// encoded position alone cannot see a parked window or a frozen claim.
func TestARewoundPositionIsConsumedAsARewind(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	covered := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	target := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

	t.Run("the activity planner reads from the rewound day, widened by the overlap", func(t *testing.T) {
		t.Parallel()

		cur, err := encodeCursor(activityPosition{CoveredThrough: covered.Format(dateLayout)})
		require.NoError(t, err)
		out, err := RewindPosition(opFetchAnalyticsUsers, cur, target, now)
		require.NoError(t, err)

		_, rewound, present, err := parseActivityPosition(out)
		require.NoError(t, err)
		require.True(t, present)

		window := resolveActivityWindow(rewound, present, now.AddDate(0, 0, -1),
			DefaultLookbackDays, defaultActivityOverlapDays)
		require.True(t, window.HasWork(), "a rewound position must give the next run work to do")
		assert.Equal(t, target.AddDate(0, 0, -(defaultActivityOverlapDays-1)), window.From,
			"the walk re-enters the overlap, so it starts earlier than the day asked for")
		assert.True(t, window.Through.After(target), "and reads forward from there")
	})

	t.Run("the report planner plans forward chunks and can claim them", func(t *testing.T) {
		t.Parallel()

		// The watermark sits at the clock, so the finality ceiling cannot clamp the claim —
		// which is the state the watermark refusal exists to preserve.
		cur, err := encodeCursor(reportPosition{
			CoveredThrough:  covered.Format(dateLayout),
			DataRefreshedAt: now.Format(time.RFC3339),
		})
		require.NoError(t, err)
		out, err := RewindPosition(opFetchAnalyticsUserCost, cur, target, now)
		require.NoError(t, err)

		pos, err := parseReportPosition(out)
		require.NoError(t, err)
		require.True(t, pos.Present)
		assert.Equal(t, target, pos.Covered)

		chunks, _, _ := resolveReportChunks(now, pos, DefaultLookbackDays,
			defaultReportRereadDays, maxReportRangeDays, maxForwardChunksPerRun,
			pos.Stored.DataRefreshedAt)
		require.NotEmpty(t, chunks, "a rewound report position must give the next run chunks to read")
		assert.False(t, chunks[0].StartingAt.After(target),
			"and the first chunk must not start after the day asked for")
	})
}

// TestARewindRefusesAnUninitialisedPosition. A row that holds no covered point has not fallen
// behind — it has not started, which the read side reports as its own distinct state. Writing a
// day into it would be an initialisation dressed as a repair, and the next run would treat every
// day before it as covered having read none of them.
func TestARewindRefusesAnUninitialisedPosition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name string
		cur  *sdk.Cursor
	}{
		{"no cursor at all", nil},
		{"a row that completed no unit", &sdk.Cursor{Data: json.RawMessage(`{}`)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := RewindPosition(opFetchAnalyticsUsers, tt.cur,
				time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), now)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "has not started")
		})
	}
}

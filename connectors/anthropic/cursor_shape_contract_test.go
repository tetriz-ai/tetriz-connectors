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

// The three stored positions are pinned against frozen literals, not round-tripped.
//
// There is no compile break to rely on. A position lives as JSON in connector_sync_states,
// and every parser here treats a missing covered_through as "no sync has ever run" — so
// renaming a tag fails nowhere: every stored position for every organisation quietly becomes
// unreadable-as-absent, and the next scheduled run restarts the ninety-day lookback for all
// of them at once, against a request pool the whole operation family shares. A round-trip test
// cannot see this, because a rename moves both sides together.
//
// Read against literals in BOTH directions: the writer's output is compared to a fixed
// document, and a fixed document is parsed back. A one-directional check would still pass if
// the reader and writer were renamed in step.

func TestActivityPositionJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(activityPosition{CoveredThrough: "2026-08-25"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"covered_through":"2026-08-25"}`, string(raw))

	cur, err := encodeCursor(json.RawMessage(`{"covered_through":"2026-08-25"}`))
	require.NoError(t, err)
	got, covered, present, err := parseActivityPosition(cur)
	require.NoError(t, err)
	require.True(t, present, "a stored position must read back as present, or the walk restarts the lookback")
	assert.Equal(t, "2026-08-25", got.CoveredThrough)
	assert.Equal(t, time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC), covered)
}

func TestReportPositionJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(reportPosition{
		CoveredThrough:  "2026-08-25",
		DataRefreshedAt: "2026-08-25T10:00:00Z",
	})
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"covered_through":"2026-08-25","data_refreshed_at":"2026-08-25T10:00:00Z"}`,
		string(raw))

	cur, err := encodeCursor(json.RawMessage(
		`{"covered_through":"2026-08-25","data_refreshed_at":"2026-08-25T10:00:00Z"}`))
	require.NoError(t, err)
	got, err := parseReportPosition(cur)
	require.NoError(t, err)
	require.True(t, got.Present)
	assert.Equal(t, "2026-08-25", got.Stored.CoveredThrough)
	// The watermark is compared as an opaque string against the vendor's next report, so its
	// stored form must survive verbatim — a re-rendered value would read as a moved watermark
	// on every run and re-read the trailing window unconditionally.
	assert.Equal(t, "2026-08-25T10:00:00Z", got.Stored.DataRefreshedAt)
	assert.Equal(t, time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC), got.Covered)
	assert.Equal(t, time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC), got.RefreshedDay,
		"the watermark must parse at day grain, or the request window is not clamped")
}

// The failed-unit lists are part of the stored shape, and a position written before they
// existed still decodes, with none listed.
func TestFailedUnitListsJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(activityPosition{CoveredThrough: "2026-08-25", FailedDays: []string{"2026-08-20"}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"covered_through":"2026-08-25","failed_days":["2026-08-20"]}`, string(raw))
	act, _, _, err := parseActivityPosition(&sdk.Cursor{Data: raw})
	require.NoError(t, err)
	assert.Equal(t, []string{"2026-08-20"}, act.FailedDays)

	raw, err = json.Marshal(reportPosition{CoveredThrough: "2026-08-25", FailedChunks: []string{"2026-08-10..2026-08-17"}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"covered_through":"2026-08-25","failed_chunks":["2026-08-10..2026-08-17"]}`, string(raw))
	rep, err := parseReportPosition(&sdk.Cursor{Data: raw})
	require.NoError(t, err)
	require.Len(t, rep.FailedChunks, 1)
	assert.Equal(t, time.Date(2026, time.August, 17, 0, 0, 0, 0, time.UTC), rep.FailedChunks[0].EndingAt)

	_, _, _, err = parseActivityPosition(&sdk.Cursor{Data: []byte(`{"covered_through":"2026-08-25","failed_days":["yesterday"]}`)})
	assert.Error(t, err, "a failed day that does not parse makes the position unreadable")
	_, err = parseReportPosition(&sdk.Cursor{Data: []byte(`{"covered_through":"2026-08-25","failed_chunks":["2026-08-10"]}`)})
	assert.Error(t, err, "a failed chunk that does not parse makes the position unreadable")
}

func TestRosterPositionJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(rosterPosition{SnapshotDate: "2026-08-25", MemberCount: 42})
	require.NoError(t, err)
	assert.JSONEq(t, `{"snapshot_date":"2026-08-25","member_count":42}`, string(raw))

	var back rosterPosition
	require.NoError(t, json.Unmarshal([]byte(`{"snapshot_date":"2026-08-25","member_count":42}`), &back))
	assert.Equal(t, "2026-08-25", back.SnapshotDate)
	assert.Equal(t, 42, back.MemberCount)
}

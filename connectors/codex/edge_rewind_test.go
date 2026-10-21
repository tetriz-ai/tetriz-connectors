// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func storedAt(t *testing.T, covered, workspace string) *sdk.Cursor {
	t.Helper()
	c, err := storePosition(position{CoveredThrough: covered, WorkspaceID: workspace})
	require.NoError(t, err)
	return c
}

// A rewind to the first day of a year stores the last day of the previous year,
// and keeps the workspace so the next run does not re-read the whole lookback.
func TestEdge_RewindAcrossTheYearKeepsTheWorkspace(t *testing.T) {
	now := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	got, err := RewindPosition(storedAt(t, "2026-01-08", "ws-1"), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), now)
	require.NoError(t, err)
	p, covered, present, err := parsePosition(got)
	require.NoError(t, err)
	assert.True(t, present)
	assert.Equal(t, "2025-12-31", p.CoveredThrough)
	assert.Equal(t, time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), covered)
	assert.Equal(t, "ws-1", p.WorkspaceID)

	// Leap day.
	got, err = RewindPosition(storedAt(t, "2024-03-05", "ws-1"), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 3, 6, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	p, _, _, _ = parsePosition(got)
	assert.Equal(t, "2024-02-29", p.CoveredThrough)
}

// Resuming from exactly the next uncovered day is an idempotent no-op; one day
// further is a fast-forward over unread days and is refused; today (partially
// aggregated) is refused even with no stored position.
func TestEdge_RewindBoundaries(t *testing.T) {
	now := time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC)
	stored := storedAt(t, "2026-05-10", "ws-1")

	same, err := RewindPosition(stored, time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC), now)
	require.NoError(t, err)
	p, _, _, _ := parsePosition(same)
	assert.Equal(t, "2026-05-10", p.CoveredThrough, "a repeat leaves the position where it was")

	_, err = RewindPosition(stored, time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC), now)
	assert.ErrorContains(t, err, "2026-05-11..2026-05-11", "a one-day fast-forward is refused")

	_, err = RewindPosition(nil, time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC), now)
	assert.Error(t, err, "today is never readable")
	ok, err := RewindPosition(nil, time.Date(2026, 5, 19, 23, 59, 0, 0, time.UTC), now)
	require.NoError(t, err, "yesterday is, and the time of day is dropped")
	p, _, _, _ = parsePosition(ok)
	assert.Equal(t, "2026-05-18", p.CoveredThrough)

	_, err = RewindPosition(&sdk.Cursor{Data: []byte(`{"covered_through":"20260510"}`)}, now.AddDate(0, 0, -5), now)
	assert.Error(t, err, "an unreadable stored position is not rewound")
}

// DescribeStoredPosition reports the lag of a readable position and nothing for
// an unreadable one, carrying the reason.
func TestEdge_DescribeStoredPosition(t *testing.T) {
	now := time.Date(2026, 5, 20, 6, 0, 0, 0, time.UTC)
	r, lag := DescribeStoredPosition(storedAt(t, "2026-05-18", ""), now)
	assert.True(t, r.Readable && r.Present)
	assert.Equal(t, 54*time.Hour, lag)

	r, lag = DescribeStoredPosition(&sdk.Cursor{Data: []byte(`{`)}, now)
	assert.False(t, r.Readable)
	assert.NotEmpty(t, r.Unreadable)
	assert.Zero(t, lag)

	r, _ = DescribeStoredPosition(&sdk.Cursor{Data: []byte(`{"covered_through":""}`)}, now)
	assert.True(t, r.Readable)
	assert.False(t, r.Present)
}

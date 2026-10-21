// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Chunks tile a window with no gap and no overlap across a leap day and a year
// boundary, each at most chunkDays wide, and the last ends exactly at the window
// end (never reaching the unsettled current day).
func TestEdge_ChunksTileAcrossLeapDayAndYearEnd(t *testing.T) {
	for name, w := range map[string]usageWindow{
		"leap day": {Start: time.Date(2024, 2, 26, 0, 0, 0, 0, time.UTC), End: time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)},
		"year end": {Start: time.Date(2025, 12, 29, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)},
		"one day":  {Start: time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond)},
	} {
		t.Run(name, func(t *testing.T) {
			cs := w.chunks()
			require.NotEmpty(t, cs)
			assert.Equal(t, w.Start, cs[0].Start)
			assert.Equal(t, w.End, cs[len(cs)-1].End)
			for i, c := range cs {
				assert.LessOrEqual(t, c.End.Sub(c.Start), time.Duration(chunkDays)*24*time.Hour)
				if i > 0 {
					assert.Equal(t, cs[i-1].End.Add(time.Nanosecond), c.Start, "chunk %d does not start where %d ended", i, i-1)
				}
			}
		})
	}
}

// A run just after midnight UTC on Jan 1 covers through Dec 31 and stores a
// floor of Jan 1 with the year's last day as its target; that floor reads back
// as caught up on the next day's run.
func TestEdge_WindowAndFloorAcrossNewYear(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	w := resolveUsageWindow(now, resolvedPosition{}, false, 30, 2)
	assert.Equal(t, time.Date(2025, 12, 31, 23, 59, 59, 999999999, time.UTC), w.End)

	pos := coveredPosition(w, w)
	assert.Equal(t, "2026-01-01", pos.FloorDate)
	assert.Equal(t, "2025-12-31", pos.BackfillUntil)

	c, err := newUsagePositionCursor(pos)
	require.NoError(t, err)
	rp, present, err := parseUsagePosition(c)
	require.NoError(t, err)
	require.True(t, present)
	next := resolveUsageWindow(now.Add(24*time.Hour), rp, true, 30, 2)
	assert.Equal(t, reasonTrailing, next.Reason)
}

// A non-UTC clock is normalised: 00:30 in UTC+2 on Jan 1 is still Dec 31 in
// UTC, so Dec 31 is the unsettled current day and is not requested.
func TestEdge_WindowEndIgnoresTheProcessZone(t *testing.T) {
	zone := time.FixedZone("UTC+2", 2*3600)
	w := resolveUsageWindow(time.Date(2026, 1, 1, 0, 30, 0, 0, zone), resolvedPosition{}, false, 7, 2)
	assert.Equal(t, time.Date(2025, 12, 30, 23, 59, 59, 999999999, time.UTC), w.End)
	assert.Equal(t, time.UTC, w.End.Location())
}

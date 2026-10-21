// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// oldest is the first readable day: a listed day exactly at it is kept, one before it
// is dropped.
func TestKeepRecentDaysKeepsTheOldestReadableDay(t *testing.T) {
	oldest := time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC)
	got := keepRecentDays(context.Background(), "fetch_activity", []string{"2025-03-09", "2025-03-10", "2025-03-11"}, oldest)
	assert.Equal(t, []string{"2025-03-10", "2025-03-11"}, got)
}

// A listed report chunk (end exclusive) is dropped when it ends at or before the oldest
// readable day, since it then holds no readable day, and clipped to start at that day
// when it starts before it, since the vendor refuses a request reaching past its range.
// The stored list follows each drop and clip.
func TestKeepRecentChunksDropsAndClipsAtTheOldestReadableDay(t *testing.T) {
	d := func(day int) time.Time { return time.Date(2025, 3, day, 0, 0, 0, 0, time.UTC) }
	oldest := d(10)
	chunks := []reportChunk{
		{StartingAt: d(1), EndingAt: d(5)},   // wholly before: dropped
		{StartingAt: d(5), EndingAt: d(10)},  // ends exactly at oldest: no readable day, dropped
		{StartingAt: d(8), EndingAt: d(12)},  // straddles: clipped to start at oldest
		{StartingAt: d(12), EndingAt: d(15)}, // readable: kept
	}
	state := &reportWalkState{}
	for _, c := range chunks {
		state.covered.FailedChunks = append(state.covered.FailedChunks, chunkUnit(c))
	}
	got := keepRecentChunks(context.Background(), "fetch_report", state, chunks, oldest)
	assert.Equal(t, []reportChunk{{StartingAt: d(10), EndingAt: d(12)}, {StartingAt: d(12), EndingAt: d(15)}}, got)
	assert.Equal(t, []string{"2025-03-10..2025-03-12", "2025-03-12..2025-03-15"}, state.covered.FailedChunks)
}

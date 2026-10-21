// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cronstagger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A negative offset is a misuse; it degrades to the per-key daily slot
// rather than wrapping through uint32 onto another member's minute.
func TestEdge_GroupEntryForNegativeOffsetFallsBackToDaily(t *testing.T) {
	t.Parallel()
	for _, k := range edgeKeys(50) {
		for _, off := range []int{-1, -15, -61} {
			assert.Equal(t, DailyStaggeredCron(k), GroupEntryFor(k, 15, off), "key %q offset %d", k, off)
		}
		assert.NotEqual(t, GroupEntryFor(k, 15, 0), GroupEntryFor(k, 15, -1), "offset -1 must not collide with offset 0")
	}
}

// A step at or past the hour degrades to the daily slot too.
func TestEdge_GroupEntryForStepAtOrPastTheHourFallsBackToDaily(t *testing.T) {
	t.Parallel()
	for _, step := range []int{60, 61, 120} {
		assert.Equal(t, DailyStaggeredCron("k"), GroupEntryFor("k", step, 0), "step %d", step)
	}
	assert.NotEqual(t, DailyStaggeredCron("k"), GroupEntryFor("k", 59, 0), "59 is still a valid in-hour step")
}

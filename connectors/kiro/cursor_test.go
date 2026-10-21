// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func day(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

// The invariant the whole trailing-window design rests on. A steady-state run re-reads
// days BEHIND the mark, so if a checkpoint could lower it, a run cancelled mid-re-read
// would leave the organisation further back than it started — and repeated cancellations
// would ratchet it backwards without bound.
func TestCoveredThroughOnlyEverRises(t *testing.T) {
	t.Parallel()

	var s syncState
	s.raiseCoveredThrough(day(2026, 8, 30))
	require.Equal(t, "2026-08-30", s.CoveredThrough)

	for _, earlier := range []time.Time{day(2026, 8, 29), day(2026, 7, 1), day(2025, 1, 1)} {
		s.raiseCoveredThrough(earlier)
		assert.Equal(t, "2026-08-30", s.CoveredThrough,
			"re-reading a trailing day must move the mark not at all")
	}

	s.raiseCoveredThrough(day(2026, 8, 31))
	assert.Equal(t, "2026-08-31", s.CoveredThrough, "a later day must still advance it")
}

// An object only becomes terminal after the threshold, and the count has to survive the
// runs in between — the case that silently never terminates if the count is returned with
// the error rather than checkpointed.
func TestFailureCountReachesTerminalAcrossRuns(t *testing.T) {
	t.Parallel()

	var s syncState
	d := day(2026, 8, 30)

	for i := 1; i < maxConsecutiveFailures; i++ {
		assert.False(t, s.recordFailure(d, "obj-a", "unreadable"), "must not terminate early (attempt %d)", i)
		assert.False(t, s.isObjectTerminal(d, "obj-a"))
	}

	assert.True(t, s.recordFailure(d, "obj-a", "unreadable"), "the threshold attempt must settle it")
	assert.True(t, s.isObjectTerminal(d, "obj-a"), "a permanently unreadable object must not be retried forever")
	assert.NotContains(t, s.FailureCounts, "2026-08-30|obj-a", "the count is spent once the object is settled")
}

// Settling one object must not settle the day. A day holds one file per client type, and a
// settled entry is skipped on every later run — so if the verdict were day-wide, the
// healthy sibling's report would never arrive.
func TestSettlingOneObjectLeavesItsSiblingCollectable(t *testing.T) {
	t.Parallel()

	var s syncState
	d := day(2026, 8, 30)

	for i := 0; i < maxConsecutiveFailures; i++ {
		s.recordFailure(d, "KIRO_CHAT_1.csv", "kms key we may not use")
	}

	assert.True(t, s.isObjectTerminal(d, "KIRO_CHAT_1.csv"), "the broken object is settled")
	assert.False(t, s.isObjectTerminal(d, "KIRO_IDE_1.csv"),
		"a sibling that never failed must still be collectable on the same day")
}

// A republished file carries a new publication timestamp, so it presents a different key
// and must be attempted rather than inheriting the verdict on the file it replaces.
func TestARepublishedObjectIsNotCoveredByTheOldVerdict(t *testing.T) {
	t.Parallel()

	var s syncState
	d := day(2026, 8, 30)
	original := "KIRO_IDE_123456789012_user_report_1717171717.csv"
	corrected := "KIRO_IDE_123456789012_user_report_1717999999.csv"

	for i := 0; i < maxConsecutiveFailures; i++ {
		s.recordFailure(d, original, "kms key we may not use")
	}

	assert.True(t, s.isObjectTerminal(d, original))
	assert.False(t, s.isObjectTerminal(d, corrected),
		"a corrected file is a different object and gets its own attempts")
}

// Settling an object must not disturb how far collection has reached. If it did, the
// failure path would double as progress and could lower the mark.
func TestRecordingTerminalLeavesTheMarkAlone(t *testing.T) {
	t.Parallel()

	var s syncState
	s.raiseCoveredThrough(day(2026, 8, 30))
	for i := 0; i < maxConsecutiveFailures; i++ {
		s.recordFailure(day(2026, 8, 28), "obj-a", "unreadable")
	}
	assert.Equal(t, "2026-08-30", s.CoveredThrough)
}

// An intermittent object must not accumulate its way to terminal across unrelated runs.
func TestASuccessfulCollectionForgetsPriorFailures(t *testing.T) {
	t.Parallel()

	var s syncState
	d := day(2026, 8, 30)
	s.recordFailure(d, "obj-a", "transient")
	s.recordFailure(d, "obj-a", "transient")
	s.clearFailures(d)

	for i := 1; i < maxConsecutiveFailures; i++ {
		assert.False(t, s.recordFailure(d, "obj-a", "transient"), "the count must have restarted (attempt %d)", i)
	}
}

func TestCursorRoundTrips(t *testing.T) {
	t.Parallel()

	var s syncState
	s.raiseCoveredThrough(day(2026, 8, 30))
	s.recordFailure(day(2026, 8, 12), "obj-a", "kms")

	c, err := s.toCursor()
	require.NoError(t, err)
	back, err := loadState(c)
	require.NoError(t, err)
	assert.Equal(t, s, back)
}

// A cursor we cannot read must not strand the organisation: the walk re-derives its start
// from the bucket, and both stores converge on a re-read anyway.
func TestAnUnreadableMarkIsTreatedAsUnset(t *testing.T) {
	t.Parallel()

	s := syncState{CoveredThrough: "not-a-day"}
	_, ok := s.coveredThroughDay()
	assert.False(t, ok)
}

// A different object failing each run must not accumulate toward one threshold: five
// unrelated failures are not evidence that any single object is permanently unreadable.
func TestFailuresOnDifferentObjectsDoNotAccumulate(t *testing.T) {
	t.Parallel()

	var s syncState
	d := day(2026, 8, 30)

	for i := 0; i < maxConsecutiveFailures*2; i++ {
		key := fmt.Sprintf("obj-%d", i)
		assert.False(t, s.recordFailure(d, key, "kms"),
			"a distinct object must not settle on another's evidence (attempt %d)", i)
		assert.False(t, s.isObjectTerminal(d, key))
	}
	assert.Empty(t, s.FailedTerminal, "nothing may be settled on unrelated failures")
}

func TestAWriteThatDoesNotOwnTheMarkCannotMoveIt(t *testing.T) {
	t.Parallel()

	// The concurrency this closes: a repair reads the cursor, a scheduled run advances the
	// mark while the repair walks, and the repair's own write lands last. Writing its
	// snapshot back would ratchet the mark BACKWARDS — the one thing raiseCoveredThrough
	// exists to prevent, and an in-function assertion cannot see it because the repair is
	// comparing its own copy against itself.
	repairState := &syncState{
		CoveredThrough: "2026-06-01", // what the repair read when it started
		FailedTerminal: map[string]string{},
		FailureCounts:  map[string]int{"2026-06-02|obj": 2},
	}
	repairCursor, err := repairState.toCursor()
	require.NoError(t, err)

	advanced := &syncState{CoveredThrough: "2026-06-20"} // where a scheduled run got to
	storedCursor, err := advanced.toCursor()
	require.NoError(t, err)

	merged, err := PreserveHighWaterMark(context.Background(), storedCursor, repairCursor)
	require.NoError(t, err)

	got, err := loadState(merged)
	require.NoError(t, err)
	assert.Equal(t, "2026-06-20", got.CoveredThrough,
		"the mark the repair does not own must survive its write")
	assert.Equal(t, 2, got.FailureCounts["2026-06-02|obj"],
		"the bookkeeping the repair does own must still be written")
}

func TestPreservingTheMarkToleratesNoStoredCursor(t *testing.T) {
	t.Parallel()

	// A first repair against an organisation that has never run: there is no stored mark
	// to carry, which is not a fault.
	fresh := &syncState{FailureCounts: map[string]int{"2026-06-02|obj": 1}}
	freshCursor, err := fresh.toCursor()
	require.NoError(t, err)

	merged, err := PreserveHighWaterMark(context.Background(), nil, freshCursor)
	require.NoError(t, err)

	got, err := loadState(merged)
	require.NoError(t, err)
	assert.Empty(t, got.CoveredThrough)
	assert.Equal(t, 1, got.FailureCounts["2026-06-02|obj"])
}

func TestAMarkBuiltForAnotherBucketIsNotAMark(t *testing.T) {
	t.Parallel()

	// The reconnect race this closes. Correcting a bucket commits the new settings and
	// clears the cursor as two separate writes; a scheduled run landing between them
	// would read the NEW settings beside the OLD mark, walk only the trailing window
	// against a bucket whose history it had never seen, and raise the mark past all of it.
	// The mark may only rise and no run looks further back than the window, so that
	// history would be lost permanently and in silence — and the run would write a cursor
	// row back, undoing the clear.
	state := &syncState{
		CoveredThrough: "2026-08-20",
		Path:           "old-bucket/kiro",
		FailureCounts:  map[string]int{"2026-08-19|obj": 3},
	}

	reset := state.adoptPath("new-bucket/kiro")
	assert.True(t, reset, "a different path must reset rather than be trusted")
	assert.Empty(t, state.CoveredThrough, "the mark belonged to the old bucket")
	assert.Empty(t, state.FailureCounts, "so did the failure history")
	assert.Equal(t, "new-bucket/kiro", state.Path)
}

func TestTheSamePathKeepsEverything(t *testing.T) {
	t.Parallel()

	state := &syncState{CoveredThrough: "2026-08-20", Path: "b/kiro"}
	assert.False(t, state.adoptPath("b/kiro"))
	assert.Equal(t, "2026-08-20", state.CoveredThrough, "an unchanged path must not reset")
}

// Every write stores the path, so a cursor with none is reset like one for another location.
func TestACursorWithNoPathResets(t *testing.T) {
	t.Parallel()

	state := &syncState{CoveredThrough: "2026-08-20"}
	assert.True(t, state.adoptPath("b/kiro"), "an absent path is a different path")
	assert.Empty(t, state.CoveredThrough)
	assert.Equal(t, "b/kiro", state.Path, "and the path is recorded, so the next change is caught")
}

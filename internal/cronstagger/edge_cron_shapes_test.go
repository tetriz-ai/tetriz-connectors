// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cronstagger

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// edgeKeys is a wider key set than staggerProbeKeys, so a phase that only
// goes wrong for some hash residues is reached.
func edgeKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("00000000-0000-4000-8000-%012d/github/fetch_prs", i)
	}
	return keys
}

// Whatever the operator configured, every entry EntryFor emits must parse
// with the grammar asynq runs entries with: an entry that does not parse is
// dropped by the config provider, so that key silently never runs.
func TestEdge_EveryEmittedEntryParses(t *testing.T) {
	t.Parallel()
	configured := []string{
		"", "*/15 * * * *", "*/45 * * * *", "*/59 * * * *", "*/2 * * * *",
		"0 */5 * * *", "0 */23 * * *", "0 */2 * * *", "30 */6 * * 1-5",
		"0 3 31 * *", "0 0 29 2 *", "0 9-17/2 * * 1-5", "5 4 * JAN MON",
		"7 */1 * * *", "0 */24 * * *", "@every 30m", "@hourly",
		"CRON_TZ=Europe/Berlin 30 9 * * *", "0 0 * * * *", "not a cron",
	}
	for _, c := range configured {
		slot := Resolve(context.Background(), "edge", c)
		for _, k := range edgeKeys(500) {
			entry := slot.EntryFor(k)
			_, err := staggerProbeParser.Parse(entry)
			require.NoErrorf(t, err, "configured %q, key %q: emitted %q does not parse", c, k, entry)
		}
	}
}

// A day-of-month the operator restricted survives the stagger: a monthly run
// on the 31st still fires only on the 31st, and skips months without one.
func TestEdge_DayOfMonth31IsKept(t *testing.T) {
	t.Parallel()
	slot := Resolve(context.Background(), "edge", "0 3 31 * *")
	require.Equal(t, "minute_only", slot.Mode())
	for _, k := range edgeKeys(20) {
		entry := slot.EntryFor(k)
		fields := strings.Fields(entry)
		require.Len(t, fields, 5)
		assert.Equal(t, []string{"3", "31", "*", "*"}, fields[1:], "only the minute moves, got %q", entry)
		sched, err := staggerProbeParser.Parse(entry)
		require.NoError(t, err)
		next := sched.Next(time.Date(2026, time.February, 1, 0, 0, 0, 0, time.UTC))
		assert.Equal(t, time.March, next.Month(), "February has no 31st, so the next fire is March 31")
		assert.Equal(t, 31, next.Day())
	}
}

// A CRON_TZ-prefixed schedule is honoured as written, zone and all, rather
// than collapsed to a UTC daily slot.
func TestEdge_TimeZonePrefixIsHonouredVerbatim(t *testing.T) {
	t.Parallel()
	const c = "CRON_TZ=Europe/Berlin 30 9 * * *"
	slot := Resolve(context.Background(), "edge", c)
	assert.Equal(t, "verbatim", slot.Mode())
	assert.Equal(t, c, slot.EntryFor("org-a"))
}

// Extra white space and tabs between fields do not change the classification
// or the emitted entry.
func TestEdge_WhiteSpaceVariantsResolveAlike(t *testing.T) {
	t.Parallel()
	a := Resolve(context.Background(), "edge", "*/15 * * * *")
	b := Resolve(context.Background(), "edge", "  */15\t*  * * *\n")
	assert.Equal(t, a.Mode(), b.Mode())
	for _, k := range edgeKeys(10) {
		assert.Equal(t, a.EntryFor(k), b.EntryFor(k))
	}
}

// A seconds-field (6-field) cron is not something asynq runs, so it falls back
// to the per-key daily slot rather than being emitted verbatim.
func TestEdge_SixFieldSecondsCronFallsBackToDaily(t *testing.T) {
	t.Parallel()
	slot := Resolve(context.Background(), "edge", "0 */5 * * * *")
	assert.Equal(t, "daily_fallback", slot.Mode())
	assert.Equal(t, DailyStaggeredCron("k"), slot.EntryFor("k"))
}

// GroupEntryFor guards a non-positive step so a misuse "fails visibly-daily,
// not with a panic". A step longer than the hour is the same kind of misuse,
// and must not emit an entry the scheduler cannot parse (which drops the
// schedule entirely).
func TestEdge_GroupEntryForStepPastTheHourStaysParseable(t *testing.T) {
	t.Parallel()
	for _, step := range []int{61, 90, 120} {
		for _, k := range edgeKeys(200) {
			entry := GroupEntryFor(k, step, 0)
			_, err := staggerProbeParser.Parse(entry)
			require.NoErrorf(t, err, "step %d, key %q: emitted %q does not parse", step, k, entry)
		}
	}
}

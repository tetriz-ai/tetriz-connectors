// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cronstagger

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// staggerProbeStart is a fixed Monday 00:00 UTC. Fixed rather than time.Now() so a day-of-week
// restriction is exercised rather than silently never fired, and so no case passes or fails
// depending on the day the suite runs.
var staggerProbeStart = time.Date(2026, time.January, 5, 0, 0, 0, 0, time.UTC)

// staggerProbeKeys are the stagger keys the fan-out is measured over. Named ids rather than a
// generated range because the offset is a hash of the key, so a fixed set keeps it reproducible.
var staggerProbeKeys = []string{"org-a", "org-b", "org-c", "org-d", "org-e", "org-f"}

// staggerProbeParser is the grammar asynq's own scheduler reads a cron entry with, built here
// independently of the production parser so a wrong flag set there cannot make this pass.
var staggerProbeParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// firings expands one emitted entry into every instant it fires in the probe day.
//
// Judging the entry on its instants rather than on its text is the only way a step, a list or a
// range is checked on what it does instead of on how it is spelled — and a phase-shifted step
// reads nothing like the expression it came from.
func firings(t *testing.T, entry string) map[time.Time]bool {
	t.Helper()
	sched, err := staggerProbeParser.Parse(entry)
	require.NoErrorf(t, err, "emitted entry %q must be parseable by the scheduler that runs it", entry)

	end := staggerProbeStart.Add(24 * time.Hour)
	instants := map[time.Time]bool{}
	// Seed one tick early so an entry firing exactly at the window start is counted rather than
	// dropped, which would silently understate every `*/n` by one.
	for cur := staggerProbeStart.Add(-time.Second); ; {
		next := sched.Next(cur)
		if !next.Before(end) {
			return instants
		}
		instants[next] = true
		cur = next
	}
}

// TestSlot_EntryHonoursTheConfiguredCadence is the durable guard against the defect this
// resolution keeps having to re-fix: an expression the code replaces with something else.
//
// The stagger and the operator's expression pull against each other — keys must not all fire at
// the same instant, and the operator must be able to choose how often work happens. Satisfying
// one and quietly dropping the other is the failure, in both directions, so every case here
// asserts the cadence AND the fan-out.
func TestSlot_EntryHonoursTheConfiguredCadence(t *testing.T) {
	t.Parallel()

	type args struct {
		configured string
	}
	tests := []struct {
		name string
		args args
		// wantFiringsPerDay is what one key's emitted entry must fire in the probe day. Written
		// out rather than derived from the configured expression, so changing either the stagger
		// or the intended cadence has to be stated here to pass.
		wantFiringsPerDay int
		// wantFields are the fields of the emitted entry that must survive verbatim, by index.
		// An index the stagger owns is absent; an index the operator owns is pinned, because
		// widening a window the operator restricted is the same class of defect as ignoring it.
		wantFields map[int]string
		// wantStaggered is whether two keys must get different entries. False is the honest
		// answer for an expression with no room to carry a per-key offset, not a relaxed check.
		wantStaggered bool
	}{
		{
			name:              "a fixed hour survives and only the minute carries the offset",
			args:              args{configured: "0 4 * * *"},
			wantFiringsPerDay: 1,
			wantFields:        map[int]string{1: "4"},
			wantStaggered:     true,
		},
		{
			name:              "a sub-hourly step keeps its cadence and each key gets its own phase",
			args:              args{configured: "*/20 * * * *"},
			wantFiringsPerDay: 72,
			wantFields:        map[int]string{1: "*"},
			wantStaggered:     true,
		},
		{
			name:              "a ten-minute step keeps its twelve-per-hour cadence, offset per key",
			args:              args{configured: "*/10 * * * *"},
			wantFiringsPerDay: 144,
			wantFields:        map[int]string{1: "*"},
			wantStaggered:     true,
		},
		{
			name:              "a step restricted to a window keeps both the cadence and the window",
			args:              args{configured: "*/20 2-4 * * *"},
			wantFiringsPerDay: 9,
			wantFields:        map[int]string{1: "2-4"},
			wantStaggered:     true,
		},
		{
			name:              "a day-of-week restriction passes through untouched",
			args:              args{configured: "*/20 * * * 1"},
			wantFiringsPerDay: 72,
			wantFields:        map[int]string{1: "*", 4: "1"},
			wantStaggered:     true,
		},
		{
			name:              "a multi-hour step keeps its cadence with the offset spread across the period",
			args:              args{configured: "0 */6 * * *"},
			wantFiringsPerDay: 4,
			wantStaggered:     true,
		},
		{
			name:              "an hour list is honoured in full rather than reduced to one of its hours",
			args:              args{configured: "0 2,14 * * *"},
			wantFiringsPerDay: 2,
			wantFields:        map[int]string{1: "2,14"},
			wantStaggered:     true,
		},
		{
			name:              "an every-hour field stays hourly instead of collapsing to daily",
			args:              args{configured: "0 * * * *"},
			wantFiringsPerDay: 24,
			wantFields:        map[int]string{1: "*"},
			wantStaggered:     true,
		},
		{
			name:              "an every-minute expression is honoured verbatim, offset and all",
			args:              args{configured: "* * * * *"},
			wantFiringsPerDay: 1440,
			wantFields:        map[int]string{0: "*", 1: "*"},
			wantStaggered:     false,
		},
		{
			name:              "a minute list is honoured verbatim rather than thinned to one minute",
			args:              args{configured: "0,30 * * * *"},
			wantFiringsPerDay: 48,
			wantFields:        map[int]string{0: "0,30"},
			wantStaggered:     false,
		},
		{
			name:              "a non-cron expression still yields a valid per-key daily entry",
			args:              args{configured: "not a cron"},
			wantFiringsPerDay: 1,
			wantStaggered:     true,
		},
		{
			name:              "an hour outside 0-23 falls back rather than emitting an entry nothing can parse",
			args:              args{configured: "0 99 * * *"},
			wantFiringsPerDay: 1,
			wantStaggered:     true,
		},
		{
			name:              "an unset schedule yields the per-key daily entry",
			args:              args{configured: ""},
			wantFiringsPerDay: 1,
			wantStaggered:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			slot := Resolve(context.Background(), "test", tt.args.configured)

			entries := map[string]bool{}
			for _, key := range staggerProbeKeys {
				entry := slot.EntryFor(key)
				fields := strings.Fields(entry)
				require.Lenf(t, fields, 5, "entry must stay a 5-field cron, got %q", entry)
				for idx, want := range tt.wantFields {
					assert.Equalf(t, want, fields[idx],
						"field %d of %q is the operator's and must survive as configured", idx, entry)
				}
				assert.Lenf(t, firings(t, entry), tt.wantFiringsPerDay,
					"entry %q must fire as often as %q asked", entry, tt.args.configured)
				entries[entry] = true
			}

			if tt.wantStaggered {
				assert.Greaterf(t, len(entries), 1,
					"keys must not share one entry — the stagger is why the fleet is not one burst, got %v", entries)
				return
			}
			assert.Lenf(t, entries, 1,
				"an expression with no room for an offset must be emitted verbatim for every key, got %v", entries)
		})
	}
}

// TestSlot_EntryForIsStableAndDeterministic pins the two properties a scheduler leans on: the
// same key resolves to the identical entry across calls (so an entry does not move between ticks
// or replicas), and the phase is a pure function of the key (no clock, no process state).
func TestSlot_EntryForIsStableAndDeterministic(t *testing.T) {
	t.Parallel()

	for _, configured := range []string{"*/10 * * * *", "0 * * * *", "0 */6 * * *", "0 4 * * *", ""} {
		slotA := Resolve(context.Background(), "test", configured)
		slotB := Resolve(context.Background(), "test", configured)
		for _, key := range staggerProbeKeys {
			first := slotA.EntryFor(key)
			second := slotA.EntryFor(key)
			assert.Equalf(t, first, second, "same key+slot must yield an identical entry across calls (%q)", configured)
			assert.Equalf(t, first, slotB.EntryFor(key),
				"a re-resolved slot must yield the identical entry for a key (%q) — the phase cannot depend on process state", configured)
		}
	}
}

// TestSlot_DescriptorHonouredVerbatim pins that an `@`-descriptor the runtime scheduler accepts
// (`@hourly`, `@every 30m`) is emitted verbatim with its cadence intact, NOT silently collapsed
// to a per-key daily slot. A descriptor has no field to phase-shift, so the fleet fires together
// — but keeping the operator's cadence is the whole point; this test catches
// a collapse of 24 fires/day to 1.
func TestSlot_DescriptorHonouredVerbatim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		configured        string
		wantFiringsPerDay int
	}{
		{"@hourly", 24},
		{"@every 30m", 48},
		{"@daily", 1},
		{"@midnight", 1},
	}
	for _, tt := range tests {
		t.Run(tt.configured, func(t *testing.T) {
			t.Parallel()
			slot := Resolve(context.Background(), "test", tt.configured)
			for _, key := range staggerProbeKeys {
				entry := slot.EntryFor(key)
				assert.Equalf(t, tt.configured, entry,
					"a runtime-parseable descriptor must be honoured verbatim, not collapsed to daily")
				assert.Lenf(t, firings(t, entry), tt.wantFiringsPerDay,
					"descriptor %q must keep its cadence", tt.configured)
			}
		})
	}
}

// TestResolve_SaysWhatItCouldNotHonour pins the loudness, which is the property whose absence
// made this defect invisible: an expression discarded in favour of a fallback, logged nowhere,
// leaves a config surface promising a control that is not connected to anything.
//
// Not parallel: the observer is installed on the process-wide logger.
func TestResolve_SaysWhatItCouldNotHonour(t *testing.T) {
	type args struct {
		configured string
	}
	tests := []struct {
		name string
		args args
		// wantWarning is a substring of the one warning the expression must produce, or empty
		// when it is honoured in full and staggered, and so must produce none.
		wantWarning string
		// wantUsed is the value the warning must report as what ran instead. Empty means the
		// warning names no substitute because the expression itself is what ran.
		wantUsed string
	}{
		{
			name: "a fully honoured and staggered step says nothing",
			args: args{configured: "*/20 * * * *"},
		},
		{
			name: "a fixed hour says nothing",
			args: args{configured: "0 4 * * *"},
		},
		{
			name: "an unset schedule is not a discarded one",
			args: args{configured: ""},
		},
		{
			name:        "a non-cron expression says it was not honoured at all",
			args:        args{configured: "not a cron"},
			wantWarning: "was NOT honoured",
			wantUsed:    "<hash-minute> <hash-hour> * * *",
		},
		{
			name:        "an hour outside 0-23 says it was not honoured at all",
			args:        args{configured: "0 99 * * *"},
			wantWarning: "was NOT honoured",
			wantUsed:    "<hash-minute> <hash-hour> * * *",
		},
		{
			name:        "a minute list says the fleet will fire together",
			args:        args{configured: "0,30 * * * *"},
			wantWarning: "cannot be per-key staggered",
			wantUsed:    "0,30 * * * *",
		},
		{
			name:        "an @-descriptor is honoured verbatim but says the fleet fires together",
			args:        args{configured: "@hourly"},
			wantWarning: "cannot be per-key staggered",
			wantUsed:    "@hourly",
		},
		{
			name:        "an every-minute expression says the fleet will fire together",
			args:        args{configured: "* * * * *"},
			wantWarning: "cannot be per-key staggered",
			wantUsed:    "* * * * *",
		},
		{
			name:        "a minute step that does not divide the hour says a firing may be lost",
			args:        args{configured: "*/7 * * * *"},
			wantWarning: "does not divide its period evenly",
		},
		{
			name:        "an hour step that does not divide the day says a firing may be lost",
			args:        args{configured: "0 */5 * * *"},
			wantWarning: "does not divide its period evenly",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.DebugLevel)
			prev := logger.Log
			logger.Log = zap.New(core)
			t.Cleanup(func() { logger.Log = prev })

			Resolve(context.Background(), "test", tt.args.configured)

			logged := recorded.All()
			if len(logged) == 1 {
				// A verbatim, unstaggerable schedule is valid, so it is noted at DEBUG; a
				// schedule that loses or changes firings is a WARN.
				wantLevel := zapcore.WarnLevel
				if tt.wantWarning == "cannot be per-key staggered" {
					wantLevel = zapcore.DebugLevel
				}
				assert.Equal(t, wantLevel, logged[0].Level, "log level")
			}
			if tt.wantWarning == "" {
				assert.Emptyf(t, logged, "an honoured schedule must not warn, got %v", logged)
				return
			}
			// One per fetch, not one per key: a static misconfiguration must stay visible without
			// becoming log volume proportional to the fleet.
			require.Lenf(t, logged, 1, "expected exactly one warning, got %v", logged)
			assert.Contains(t, logged[0].Message, tt.wantWarning)
			fields := logged[0].ContextMap()
			assert.Equal(t, tt.args.configured, fields["configured"],
				"the operator cannot act on a warning that does not name what they set")
			if tt.wantUsed != "" {
				assert.Equal(t, tt.wantUsed, fields["used"],
					"the operator cannot act on a warning that does not name what ran instead")
			}
		})
	}
}

// TestResolve_WarningNamesTheSource pins that the emitting source is identifiable in the warning,
// since one shared engine now serves several sources and an operator must know which one could
// not honour their expression.
func TestResolve_WarningNamesTheSource(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	prev := logger.Log
	logger.Log = zap.New(core)
	t.Cleanup(func() { logger.Log = prev })

	Resolve(context.Background(), "connector", "* * * * *")

	logged := recorded.All()
	require.Len(t, logged, 1)
	assert.Contains(t, logged[0].Message, "connector:")
	assert.Equal(t, "connector", logged[0].ContextMap()["source"])
}

// TestDailyStaggeredCron pins the daily helper: the entry is deterministic per key, keeps the
// day-of-month/month/day-of-week wildcards (so it stays daily), fires once a day, and distinct
// keys fan out across distinct slots rather than collapsing to one.
func TestDailyStaggeredCron(t *testing.T) {
	t.Parallel()
	slots := map[string]bool{}
	for _, key := range staggerProbeKeys {
		got := DailyStaggeredCron(key)
		require.Equalf(t, got, DailyStaggeredCron(key), "the same key must yield an identical entry across calls")
		fields := strings.Fields(got)
		require.Lenf(t, fields, 5, "must be a 5-field cron, got %q", got)
		assert.Equalf(t, []string{"*", "*", "*"}, fields[2:], "daily entry must keep dom/month/dow wildcards, got %q", got)
		assert.Lenf(t, firings(t, got), 1, "a daily entry must fire once in the probe day, got %q", got)
		slots[got] = true
	}
	assert.Greater(t, len(slots), 1, "distinct keys must fan out across distinct slots, not collapse to one")
}

// minuteOfGroupEntry extracts the phase minute from a `<m>-59/step * * * *` group entry,
// asserting the entry has exactly that shape, keeps the configured step, and runs at that step's
// cadence when handed to the scheduler.
func minuteOfGroupEntry(t *testing.T, entry string, step int) int {
	t.Helper()
	fields := strings.Fields(entry)
	require.Lenf(t, fields, 5, "group entry must be a 5-field cron, got %q", entry)
	assert.Equalf(t, []string{"*", "*", "*", "*"}, fields[1:], "group entry must leave hour/dom/month/dow as *, got %q", entry)
	base, stepStr, hasStep := strings.Cut(fields[0], "/")
	require.Truef(t, hasStep, "minute field %q must be a step form", fields[0])
	assert.Equalf(t, strconv.Itoa(step), stepStr, "group entry must keep the configured step")
	lo, hi, hasRange := strings.Cut(base, "-")
	require.Truef(t, hasRange, "minute field %q must be a p-59 range", fields[0])
	assert.Equal(t, strconv.Itoa(minutesPerHour-1), hi, "group entry range must run to the last minute")
	assert.Lenf(t, firings(t, entry), (minutesPerHour/step)*hoursPerDay, "entry %q must fire at the step cadence", entry)
	m, err := strconv.Atoi(lo)
	require.NoError(t, err)
	return m
}

// TestGroupEntryFor_SpacesMembersAndSpreadsOrgs pins the two properties the group primitive
// exists for: within one org the members hold their fixed spacing (a distinct minute each), and
// different orgs land on different base phases so the group does not fire fleet-wide in lockstep.
// Distinct offsets modulo step is what keeps the spacing for every org's φ. The offsets used are
// the live Bitbucket PR-pull offsets.
func TestGroupEntryFor_SpacesMembersAndSpreadsOrgs(t *testing.T) {
	t.Parallel()
	const step = 15
	offsets := []int{0, 3, 5, 10, 12}

	baseMinutes := map[string]int{}
	for _, org := range staggerProbeKeys {
		groupKey := org + "/bitbucket/bitbucket-pr-pull"
		var phi int
		minutes := map[int]bool{}
		for i, off := range offsets {
			minute := minuteOfGroupEntry(t, GroupEntryFor(groupKey, step, off), step)
			if i == 0 {
				phi = minute
			}
			assert.Equalf(t, (phi+off)%step, minute,
				"org %s: member offset %d must sit at (φ+offset) mod step from the shared phase", org, off)
			assert.Falsef(t, minutes[minute], "org %s: member offset %d collides on minute :%d", org, off, minute)
			minutes[minute] = true
		}
		assert.Lenf(t, minutes, len(offsets), "org %s: every member must own a distinct minute", org)
		baseMinutes[org] = phi
	}

	distinctBases := map[int]bool{}
	for _, m := range baseMinutes {
		distinctBases[m] = true
	}
	assert.Greater(t, len(distinctBases), 1, "different orgs must land on different base phases, not one shared minute")
}

// TestGroupEntryFor_Deterministic pins that a group member's entry is a pure function of its
// (groupKey, step, offset) — the scheduler leans on an entry not moving between ticks or replicas.
func TestGroupEntryFor_Deterministic(t *testing.T) {
	t.Parallel()
	for _, org := range staggerProbeKeys {
		groupKey := org + "/bitbucket/bitbucket-pr-pull"
		for _, off := range []int{0, 3, 5, 10, 12} {
			first := GroupEntryFor(groupKey, 15, off)
			assert.Equalf(t, first, GroupEntryFor(groupKey, 15, off),
				"same (groupKey, step, offset) must yield an identical entry across calls (%s, %d)", org, off)
		}
	}
}

// TestGroupStaggerStep pins which schedules a stagger group can place members inside: a full-hour
// minute step (`*/n` or a phase-shifted `p-59/n`) with every other field left as `*`. A schedule
// that restricts another field, or a range stopping short of :59, is rejected rather than silently
// widened or re-cadenced by the `<phase>-59/n` group emit.
func TestGroupStaggerStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		configured string
		wantStep   int
		wantOK     bool
	}{
		{"a plain */n minute step is eligible", "*/15 * * * *", 15, true},
		{"a phase-shifted p-59/n step is eligible", "3-59/15 * * * *", 15, true},
		{"a high phase within the period is eligible", "12-59/15 * * * *", 15, true},
		{"a different even-divisor step is read off the field", "*/20 * * * *", 20, true},
		{"a restricted hour window would be widened, so is rejected", "*/15 2-4 * * *", 0, false},
		{"a day-of-week restriction is rejected", "*/15 * * * 1", 0, false},
		{"a range stopping short of :59 changes the cadence, so is rejected", "3-40/15 * * * *", 0, false},
		{"a range starting at the step fires fewer times, so is rejected", "15-59/15 * * * *", 0, false},
		{"a range starting past the step fires fewer times, so is rejected", "20-59/15 * * * *", 0, false},
		{"a */n step that does not divide the hour desyncs cadence, so is rejected", "*/7 * * * *", 0, false},
		{"a p-59/n step that does not divide the hour is rejected", "3-59/7 * * * *", 0, false},
		{"an every-minute field has no period to place into", "* * * * *", 0, false},
		{"a single fixed minute is not a step", "5 * * * *", 0, false},
		{"a fixed multi-hour schedule is not a minute step", "7 */6 * * *", 0, false},
		{"a descriptor has no 5 fields to read", "@hourly", 0, false},
		{"empty is not eligible", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			step, ok := GroupStaggerStep(tt.configured)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantStep, step)
		})
	}
}

// TestMonthlyStaggeredCron pins the monthly helper: deterministic per key, pinned to
// day-of-month 1, and fanned out across the day.
func TestMonthlyStaggeredCron(t *testing.T) {
	t.Parallel()
	slots := map[string]bool{}
	for _, key := range staggerProbeKeys {
		got := MonthlyStaggeredCron(key)
		require.Equalf(t, got, MonthlyStaggeredCron(key), "the same key must yield an identical entry across calls")
		fields := strings.Fields(got)
		require.Lenf(t, fields, 5, "must be a 5-field cron, got %q", got)
		assert.Equalf(t, []string{"1", "*", "*"}, fields[2:], "monthly entry must fire on the 1st, got %q", got)
		slots[got] = true
	}
	assert.Greater(t, len(slots), 1, "distinct keys must fan out across distinct slots, not collapse to one")
}

// TestSlot_Mode pins the metric-label name each classification resolves to. The names are a
// stable metric label of the scheduler cron-entry metric, so a mode renamed here
// (or an enum reordered so Resolve lands on a different mode) is caught rather than silently
// re-bucketing the series. One expression per mode, plus the unset/zero-value fallback.
func TestSlot_Mode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configured string
		want       string
	}{
		{name: "minute step is minute_phase", configured: "*/10 * * * *", want: "minute_phase"},
		{name: "hour step is hour_phase", configured: "0 */6 * * *", want: "hour_phase"},
		{name: "single minute in every hour is minute_only", configured: "0 3 * * *", want: "minute_only"},
		{name: "several minutes per hour is verbatim", configured: "0,30 * * * *", want: "verbatim"},
		{name: "unset schedule is daily_fallback", configured: "", want: "daily_fallback"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Resolve(context.Background(), "test", tt.configured).Mode()
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestSlot_Mode_ZeroValue pins the zero-value Slot (a source that resolved nothing) to
// daily_fallback, matching its modeDaily zero value — so the metric never labels an
// unresolved slot with a phase mode it does not have.
func TestSlot_Mode_ZeroValue(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "daily_fallback", Slot{}.Mode())
}

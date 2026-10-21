// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/cronstagger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// scheduleWindow is the span over which firing instants are compared. A full
// week is the shortest window that exercises every field an op here uses,
// including the weekly day-of-week ops — a 24h window would simply never fire
// them and would pass by not looking.
const scheduleWindow = 7 * 24 * time.Hour

// windowStart is a fixed Monday 00:00 UTC. Fixed rather than time.Now() so the
// test cannot pass or fail depending on the day it runs.
var windowStart = time.Date(2026, time.January, 5, 0, 0, 0, 0, time.UTC)

// firingInstants expands a 5-field cron into every instant it fires within
// scheduleWindow. Expanded through the same parser the scheduler feeds rather
// than read off the literal, so an op written as a list, range or step is
// judged on what it actually does.
func firingInstants(t *testing.T, spec string) map[time.Time]bool {
	t.Helper()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(spec)
	require.NoErrorf(t, err, "cron %q must parse", spec)

	end := windowStart.Add(scheduleWindow)
	instants := map[time.Time]bool{}
	// Start one tick early so windowStart itself can be returned by Next —
	// it is a firing instant for any */15-style schedule, and seeding at
	// windowStart would silently drop it from the comparison.
	for cur := windowStart.Add(-time.Second); ; {
		next := sched.Next(cur)
		if !next.Before(end) {
			return instants
		}
		instants[next] = true
		cur = next
	}
}

// TestOperationSchedulesDoNotCollide is the durable guard against the failure this
// connector keeps rediscovering: the several PR-pull ops firing at the same instant.
//
// They contend for a single per-org bitbucket-rest bucket, and that limiter bounds a
// window's COUNT without spacing requests inside it — so ops landing together spend the
// window in one burst instead of across it. The ops are now one stagger group: they share a
// per-org phase φ and each sits at (φ+offset) mod step, so the group shifts together per org
// (removing the fleet-wide spike) while its members keep their spacing.
//
// The guarantee is asserted for EVERY φ in [0,step) rather than off the static config
// strings, because the emitted schedule is per-org and the static string is only the
// verbatim fallback — φ=0 is that fallback layout, and the other φ values are the orgs the
// static test never saw. The offsets and step are read from the descriptors, so a member
// added with a colliding offset or an off-cadence schedule fails here.
func TestOperationSchedulesDoNotCollide(t *testing.T) {
	conn := NewConnector(nil)

	type member struct {
		id     sdk.OperationID
		offset int
	}
	var members []member
	step := 0
	for _, op := range conn.Operations() {
		if op.StaggerGroup != bbPRPullGroup {
			continue
		}
		opStep, ok := cronstagger.GroupStaggerStep(op.DefaultSchedule)
		require.Truef(t, ok, "group member %s has a schedule %q the group primitive cannot place", op.ID, op.DefaultSchedule)
		if step == 0 {
			step = opStep
		}
		assert.Equalf(t, step, opStep, "every group member must share one repeat period; %s is %d", op.ID, opStep)
		// GroupEntryFor derives the minute with unsigned modulo, so a negative offset would wrap
		// to a residue the signed arithmetic below would miss — assert the documented
		// non-negativity so this guard cannot false-pass on a negative-offset typo.
		require.GreaterOrEqualf(t, op.StaggerOffsetMinutes, 0, "%s offset must be non-negative", op.ID)
		members = append(members, member{id: op.ID, offset: op.StaggerOffsetMinutes})
	}
	require.GreaterOrEqualf(t, len(members), 2, "expected the PR-pull group members, got %d", len(members))

	for phi := 0; phi < step; phi++ {
		minuteOwner := map[int]sdk.OperationID{}
		for _, m := range members {
			minute := (phi + m.offset) % step
			if other, taken := minuteOwner[minute]; taken {
				assert.Failf(t, "colliding PR-pull group members",
					"at org phase φ=%d, %s (offset %d) and %s share minute :%d of the %d-minute period — they would burst together on the shared bitbucket-rest bucket",
					phi, m.id, m.offset, other, minute, step)
			}
			minuteOwner[minute] = m.id
		}
	}
}

// TestFixedScheduleOpsDoNotCollide guards the ops whose schedules are fixed (not in a
// per-org stagger group): the six-hourly band and the hourly commit sweep must sit on
// distinct minutes, since they too share the per-org bitbucket-rest bucket.
//
// It deliberately does NOT check a fixed op against a PR-pull group member: those members
// roam per-org (see bbPRPullGroup), so one may share a minute with a fixed op for some orgs.
// That single-op coincidence is the accepted trade-off for removing the sustained five-op
// burst — documented on fetch_workspace_members — not a collision to fail on.
//
// Comparing instants rather than minute-of-hour is deliberate: two ops may legitimately
// share a minute in different hours (03:37 vs 05:37) without ever running together, and
// flagging that would push future ops off good slots for no reason.
func TestFixedScheduleOpsDoNotCollide(t *testing.T) {
	conn := NewConnector(nil)

	type scheduledOp struct {
		id       sdk.OperationID
		spec     string
		instants map[time.Time]bool
	}

	var scheduled []scheduledOp
	for _, op := range conn.Operations() {
		// Webhook-driven and deliberately-unscheduled ops never enqueue on a tick, and
		// stagger-group members are guarded per-φ by TestOperationSchedulesDoNotCollide.
		if op.IngestionType != sdk.IngestionPull || op.DefaultSchedule == "" || op.StaggerGroup != "" {
			continue
		}
		scheduled = append(scheduled, scheduledOp{
			id:       op.ID,
			spec:     op.DefaultSchedule,
			instants: firingInstants(t, op.DefaultSchedule),
		})
	}
	require.NotEmpty(t, scheduled, "expected fixed-schedule pull operations")

	for i := range scheduled {
		for j := i + 1; j < len(scheduled); j++ {
			a, b := scheduled[i], scheduled[j]
			var shared []time.Time
			for instant := range a.instants {
				if b.instants[instant] {
					shared = append(shared, instant)
				}
			}
			if len(shared) == 0 {
				continue
			}
			// Map iteration is unordered and a weekly window can collect dozens of hits;
			// sort and show a couple so the failure names the pair and a reproducible
			// example rather than a wall of instants.
			sort.Slice(shared, func(x, y int) bool { return shared[x].Before(shared[y]) })
			assert.Failf(t, "colliding schedules",
				"%s (%q) and %s (%q) fire together %d times a week, first at %s — they share the per-org bitbucket-rest bucket and would burst as one",
				a.id, a.spec, b.id, b.spec, len(shared), shared[0].Format(time.RFC3339))
		}
	}
}

// TestPRPullGroupBurstsWithAtMostOneFixedOp bounds the trade-off the group stagger accepts.
// Because the group members roam per-org, a member can share a firing minute with a fixed-schedule
// op (fetch_commits at :08, the six-hourly band at :07/:22/:37/:52, fetch_file_content at 05:02) —
// that is the accepted, documented cost. What must never return is the sustained multi-op burst the
// group removes: this asserts that for EVERY org phase φ, at every instant at most two Bitbucket
// pull ops fire together — one group member (they are mutually spaced, per
// TestOperationSchedulesDoNotCollide) plus at most one fixed op (they are mutually spaced, per
// TestFixedScheduleOpsDoNotCollide). Group members are synthesised at (φ+offset) mod step, the same
// minutes GroupEntryFor emits (verified in cronstagger's TestGroupEntryFor_SpacesMembersAndSpreadsOrgs).
func TestPRPullGroupBurstsWithAtMostOneFixedOp(t *testing.T) {
	conn := NewConnector(nil)

	var fixedInstants []map[time.Time]bool
	var groupOffsets []int
	step := 0
	for _, op := range conn.Operations() {
		if op.IngestionType != sdk.IngestionPull || op.DefaultSchedule == "" {
			continue
		}
		if op.StaggerGroup == bbPRPullGroup {
			opStep, ok := cronstagger.GroupStaggerStep(op.DefaultSchedule)
			require.Truef(t, ok, "group member %s schedule %q must be placeable", op.ID, op.DefaultSchedule)
			step = opStep
			groupOffsets = append(groupOffsets, op.StaggerOffsetMinutes)
			continue
		}
		fixedInstants = append(fixedInstants, firingInstants(t, op.DefaultSchedule))
	}
	require.NotZero(t, step, "expected a PR-pull group with a resolved step")

	for phi := 0; phi < step; phi++ {
		opsPerInstant := map[time.Time]int{}
		for _, instants := range fixedInstants {
			for ts := range instants {
				opsPerInstant[ts]++
			}
		}
		for _, off := range groupOffsets {
			spec := fmt.Sprintf("%d-%d/%d * * * *", (phi+off)%step, 59, step)
			for ts := range firingInstants(t, spec) {
				opsPerInstant[ts]++
			}
		}
		for ts, n := range opsPerInstant {
			assert.LessOrEqualf(t, n, 2,
				"at org phase φ=%d, %d Bitbucket pull ops fire together at %s — the group stagger must bound coincidence to one group member plus at most one fixed op, never a multi-op burst",
				phi, n, ts.Format(time.RFC3339))
		}
	}
}

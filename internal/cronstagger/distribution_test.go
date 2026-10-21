// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cronstagger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// censusSchedules is a representative sample of default schedules that all share one
// wall-clock phase: a host's own periodic jobs and the connector-operation DefaultSchedules.
// The point of the test below is to demonstrate, on this sample, the fan-out this package
// removes — every `*/n` and every `0 …` piles onto :00, and staggering spreads them without
// changing any schedule's cadence.
var censusSchedules = []string{
	// A host's periodic jobs.
	"*/10 * * * *", // 10-minute cadence
	"*/5 * * * *",  // 5-minute cadence
	"*/3 * * * *",  // 3-minute cadence
	"*/15 * * * *", // 15-minute cadence
	// Connector-operation DefaultSchedules.
	"*/30 * * * *", // gitlab, linear
	"0 * * * *",    // github hourly
	"0 */6 * * *",  // 6-hourly org fetches
	"0 1 * * *",    // a daily fetch
}

// distributionFleet is the set of orgs the census is expanded over. Named ids (not a generated
// range) so the hash-derived phases are reproducible run to run.
var distributionFleet = []string{
	"org-01", "org-02", "org-03", "org-04", "org-05", "org-06", "org-07", "org-08",
	"org-09", "org-10", "org-11", "org-12", "org-13", "org-14", "org-15", "org-16",
	"org-17", "org-18", "org-19", "org-20",
}

// minuteOfHourTally counts, across a full probe day, how many times an entry fires at each
// minute-of-hour [0,59]. Aggregating minute-of-hour across the day shows the pile at :00 and
// :30.
func minuteOfHourTally(t *testing.T, entry string, tally []int) int {
	t.Helper()
	total := 0
	for instant := range firings(t, entry) {
		tally[instant.Minute()]++
		total++
	}
	return total
}

// firingsPerDay counts an entry's firings across the probe day, for the cadence-preservation
// assertion (staggering moves phase, never period).
func firingsPerDay(t *testing.T, entry string) int {
	t.Helper()
	return len(firings(t, entry))
}

// TestCronMinuteDistribution_StaggerFlattensThePile is acceptance (b): it recomputes the
// per-minute distribution for the census expanded over a fleet, both as it fires today (every
// org identical) and after this package staggers each org, and shows the busiest minute drops
// sharply toward the mean while every schedule keeps its exact cadence.
func TestCronMinuteDistribution_StaggerFlattensThePile(t *testing.T) {
	t.Parallel()

	rawTally := make([]int, minutesPerHour)
	staggeredTally := make([]int, minutesPerHour)

	for _, sched := range censusSchedules {
		slot := Resolve(context.Background(), "distribution_test", sched)
		for _, org := range distributionFleet {
			staggered := slot.EntryFor(org)
			// Cadence is the invariant: a staggered entry fires exactly as many times a day as
			// the schedule it came from. Only the phase moved.
			require.Equalf(t, firingsPerDay(t, sched), firingsPerDay(t, staggered),
				"stagger changed the cadence of %q -> %q", sched, staggered)

			minuteOfHourTally(t, sched, rawTally)
			minuteOfHourTally(t, staggered, staggeredTally)
		}
	}

	rawMax, rawBusiest := maxAndArg(rawTally)
	staggeredMax, _ := maxAndArg(staggeredTally)
	total := sumInts(rawTally)
	mean := float64(total) / float64(minutesPerHour)

	// Visible "post-change run" (acceptance (b) asks the recomputed distribution be shown).
	t.Logf("census=%d schedules x fleet=%d orgs", len(censusSchedules), len(distributionFleet))
	t.Logf("total firings/day=%d, per-minute mean=%.1f", total, mean)
	t.Logf("BEFORE stagger: busiest minute :%02d carries %d firings (max)", rawBusiest, rawMax)
	t.Logf("AFTER  stagger: busiest minute carries %d firings (max)", staggeredMax)

	// The pile is real before: the busiest minute (:00) carries several multiples of the mean.
	assert.Greaterf(t, float64(rawMax), 3*mean,
		"the census must reproduce the fan-out (busiest minute far above the mean), got max=%d mean=%.1f", rawMax, mean)
	// And the stagger flattens it toward the mean: the busiest minute drops to within a small
	// factor of the per-minute mean. It cannot reach the mean exactly — `* * * * *`
	// (unstaggerable) contributes the whole fleet to every minute, and each `*/n`'s phase 0
	// still lands on some minutes — but the outlier pile at :00 is gone.
	assert.Lessf(t, float64(staggeredMax), 1.5*mean,
		"stagger must flatten the busiest minute toward the mean, got after=%d mean=%.1f", staggeredMax, mean)
	// Concretely, the busiest minute is at least halved versus today.
	assert.Lessf(t, staggeredMax*2, rawMax,
		"stagger must at least halve the busiest minute, before=%d after=%d", rawMax, staggeredMax)
}

func maxAndArg(tally []int) (hi, arg int) {
	for i, v := range tally {
		if v > hi {
			hi, arg = v, i
		}
	}
	return hi, arg
}

func sumInts(tally []int) int {
	total := 0
	for _, v := range tally {
		total += v
	}
	return total
}

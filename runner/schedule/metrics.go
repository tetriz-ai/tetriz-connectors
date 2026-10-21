// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Early-warning observability for the scheduler refresh chokepoint.
// ConfigProvider.GetConfigs aggregates every
// ScheduleSource into the asynq periodic-task set on each SyncInterval; these two
// gauges make the cost and size of that aggregation visible so the Model B
// trigger (per-refresh fan-out growth) is observed rather than guessed. Neither
// carries an org_id label — they describe the refresh as a whole, so there is no
// cardinality concern.

// schedulerRefreshDurationSeconds is the wall-clock time for one full GetConfigs
// aggregation (all sources fetched and converted). A histogram so a rising p95
// surfaces a slow source before it stalls the refresh.
var schedulerRefreshDurationSeconds = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "scheduler_refresh_duration_seconds",
		Help:    "Wall-clock duration of one ConfigProvider.GetConfigs aggregation across all schedule sources.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~40s
	},
)

// schedulerPeriodicTasks is the number of distinct periodic tasks produced by
// the last GetConfigs aggregation (post-dedup). Watching this gauge climb is the
// early warning that the individual-tenant fan-out is growing the periodic-task
// set.
var schedulerPeriodicTasks = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "scheduler_periodic_tasks",
		Help: "Number of distinct periodic tasks produced by the last schedule-source aggregation.",
	},
)

// schedulerInvalidDefinitionsTotal counts schedule definitions GetConfigs
// dropped because they could not be registered, by source.
var schedulerInvalidDefinitionsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "scheduler_invalid_definitions_total",
		Help: "Schedule definitions dropped because their ID, job type or cron expression is invalid, by source.",
	},
	[]string{labelSource},
)

const (
	labelSource      = "source"
	labelConnectorID = "connector_id"
	labelStaggerMode = "stagger_mode"
	labelOrigin      = "origin"

	// originStaggeredDefault marks an entry emitted from the operation's descriptor
	// default schedule, phase-shifted per org by cronstagger. originOperatorOverride
	// marks one an operator pinned via a schedule override, emitted verbatim.
	originStaggeredDefault = "staggered_default"
	originOperatorOverride = "operator_override"
)

// schedulerCronEntries counts the resolved schedule entries the sources emit on each
// refresh, bucketed by the source that produced them, the connector they
// belong to, how cronstagger classified the schedule (Slot.Mode), and whether the
// schedule was the descriptor default (staggered per org) or an operator override
// (verbatim).
//
// It carries NO org_id label: the value is the COUNT of (org, connector[, operation])
// entries in a bucket, summed across orgs, so cardinality stays at roughly
// connectors x modes x origins regardless of tenant count. It pairs with
// schedulerPeriodicTasks (the post-dedup total) to answer what is scheduled, how each
// staggers, and which schedules could not be staggered at all.
var schedulerCronEntries = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "scheduler_cron_entries",
		Help: "Count of resolved periodic-schedule entries from the last refresh, by source, connector, stagger mode, and origin.",
	},
	[]string{labelSource, labelConnectorID, labelStaggerMode, labelOrigin},
)

// cronEntryBucket is one distinct label combination a CronEntriesRecorder counts entries
// into. connectorID is the connector id.
type cronEntryBucket struct {
	connectorID string
	staggerMode string
	origin      string
}

// CronEntriesRecorder accumulates one schedule source's per-bucket entry counts across a
// single FetchSchedules pass, then publishes them as a set with Publish.
//
// A source builds one recorder per pass, `defer`s Publish (so it runs on every exit — see
// Publish), and calls Add for every entry it emits. Publish writes the whole set at once, so
// the gauge is a snapshot and drops buckets that vanished. Not safe for concurrent use; each pass owns its own recorder.
type CronEntriesRecorder struct {
	source string
	counts map[cronEntryBucket]int
}

// NewCronEntriesRecorder returns a recorder tagged with the calling source's Name(). The
// name scopes the reset in Publish, so two sources publishing to the shared vec in the
// same process never wipe each other's series.
func NewCronEntriesRecorder(source string) *CronEntriesRecorder {
	return &CronEntriesRecorder{source: source, counts: map[cronEntryBucket]int{}}
}

// Add records one emitted entry. staggerMode is the cronstagger classification of the
// schedule (Slot.Mode); fromDefault is true when the schedule is the descriptor default
// (staggered per org) and false when it is an operator override (emitted verbatim). For
// an override, staggerMode still describes the descriptor default's classification —
// origin is what says the operator replaced it.
func (r *CronEntriesRecorder) Add(connectorID, staggerMode string, fromDefault bool) {
	origin := originStaggeredDefault
	if !fromDefault {
		origin = originOperatorOverride
	}
	r.counts[cronEntryBucket{connectorID: connectorID, staggerMode: staggerMode, origin: origin}]++
}

// Publish resets this source's series and sets each accumulated bucket to its count.
//
// The reset is a DeletePartialMatch scoped to this source's label, not a vec-wide reset,
// so a source publishing an empty or shrunk bucket set drops only its own stale series and
// never the other source's. `defer` it so it runs on EVERY exit. A failed read keeps its
// last good schedules registered (GetConfigs keeps them), so the source Adds those kept
// entries before it returns, and the metric matches what stays scheduled.
func (r *CronEntriesRecorder) Publish() {
	schedulerCronEntries.DeletePartialMatch(prometheus.Labels{labelSource: r.source})
	for b, n := range r.counts {
		schedulerCronEntries.WithLabelValues(r.source, b.connectorID, b.staggerMode, b.origin).Set(float64(n))
	}
}

// resetLeaderCronMetrics clears the gauges only the leader pod publishes, so a stepped-down
// former leader stops exporting a frozen snapshot. GetConfigs (and thus every Publish and the
// schedulerPeriodicTasks Set) runs on the leader only; a Prometheus gauge holds its last value
// until re-set, and a former leader never re-sets one, so without this a failover that shrank
// the schedule set would leave the stale higher count winning a cross-pod max().
// Called at the end of each leader term (runLeaderOnce); harmless to call when never a leader.
func resetLeaderCronMetrics() {
	schedulerCronEntries.Reset()
	schedulerPeriodicTasks.Set(0)
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		schedulerRefreshDurationSeconds,
		schedulerPeriodicTasks,
		schedulerInvalidDefinitionsTotal,
		schedulerCronEntries,
	}
}

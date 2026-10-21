// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"github.com/prometheus/client_golang/prometheus"
)

// usageEventsSkipped counts billed events the operation declined to emit because it
// could not construct a routing identity for them.
//
// How recoverable a skip is depends on one thing: whether its day is still inside the
// trailing overlap. The trailing window starts at a fixed number of days before now
// regardless of the floor, so a skipped event whose day still falls in it is requested
// again on every run — a parser fix lands before the day ages out and nothing is lost.
// Once the day falls outside that overlap the floor has passed it for good, and only a
// deliberate re-read of those days recovers them.
//
// A consequence worth knowing before reading the number: while the day stays inside the
// overlap the same event is re-read and re-counted every run, so this counts skips, not
// distinct events.
//
// It is the only signal that this happened. The whole-response case does not come
// here: when every event in a chunk is unreadable the run fails with vendor_error. A
// failure rate says little about a once-a-day operation; this counter says it.
var usageEventsSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursor_usage_events_skipped_total",
	Help: "Billed Cursor usage events skipped because no routing identity could be constructed.",
}, []string{"org_id"})

// usageRunYielded counts runs that stopped before covering their whole window and
// still reported success. The reason separates the two limits that stop a walk:
// "cap" is this run holding a shared worker slot long enough, "deadline" is the run
// simply running out of time.
//
// Without this a capped run is indistinguishable from a healthy one — same completed
// status, same last-success timestamp — and the framework's per-entry cursor gauges
// do not cover this operation, because its position is a flat lower-case object that
// isPerEntryCursorKey deliberately rejects. An organisation needing many more chunks
// than one run can pace could therefore crawl for months in silence, which is the
// same class of invisible stall this connector already counts for the case where
// NO progress is made at all.
var usageRunYielded = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursor_usage_run_yielded_total",
	Help: "Cursor usage runs that stopped short of covering their window and reported success, by why they stopped.",
}, []string{"org_id", "reason"})

// usageWindowCompleted counts runs that covered their whole window. It is the
// denominator that makes a yield readable: yields alone are ordinary during a large
// backfill, and only become a stall when no run finishes a window at all.
var usageWindowCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "cursor_usage_window_completed_total",
	Help: "Cursor usage runs that covered their whole window before returning.",
}, []string{"org_id"})

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		usageEventsSkipped,
		usageRunYielded,
		usageWindowCompleted,
	}
}

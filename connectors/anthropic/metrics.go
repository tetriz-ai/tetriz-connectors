// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// These counters exist because the framework's cursor-health series cannot see this
// connector at all. `isPerEntryCursorKey` deliberately rejects a lower-case key, and all three
// positions here are flat snake_case objects — so `connector_cursor_oldest_entry_age_seconds`
// and the regression counter yield no samples for any of the five operations, and the
// orchestrator still records every run as completed.
//
// That leaves the stop-short-but-succeed paths with no signal but a log line, which is the
// invisible-stall class this connector otherwise has no way to surface.

// positionUnreadable counts each failure to read a stored position, where the run then restarts
// from the lookback.
//
// It is the top risk this connector's stored shapes carry: a renamed JSON tag makes every
// organisation's position read as absent at once, and five operations then each restart a
// ninety-day sync for every organisation against one shared per-organisation request budget.
// The event is per-organisation but the cause is global, so the shape of the series over the
// fleet is what identifies it — a single organisation is a corrupt row, every organisation at
// once is a code change.
var positionUnreadable = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_position_unreadable_total",
	Help: "Claude analytics runs that could not read their stored position and restarted from the lookback.",
}, []string{logger.OrgID, logger.OperationID})

// runYielded counts runs that stopped before covering their whole window and still reported
// success. The reason separates the causes, because they call for different responses:
//
//   - not_aggregated: the vendor has not finished the day asked for AND the walk had not reached
//     that day either, so it genuinely fell short. A walk that had already reached the vendor's
//     currency counts a cover instead, because it read everything available.
//   - forward_cap: the gap is wider than one run may request, so it closes over several runs.
//     Ordinary during a backfill; a stall if it never stops.
//   - unreadable_span: days a resumed position had fallen behind the vendor's own limits, which
//     cannot be requested at all. Never self-healing — the position advances past them.
var runYielded = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_run_yielded_total",
	Help: "Claude analytics runs that stopped short of covering their window and reported success, by why they stopped.",
}, []string{logger.OrgID, logger.OperationID, "reason"})

// coverageUnbounded counts runs that recorded coverage with NO finality bound, because the
// vendor's watermark was too stale to apply one.
//
// Deliberately not a yield reason: such a run does not stop short, it covers everything — so
// filing it under runYielded broke that counter's own contract and made a single run increment
// both halves of the pair. What it signals is a completeness risk rather than a progress one:
// the span was claimed without the vendor confirming it final, so a restatement inside it will
// not be collected.
var coverageUnbounded = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_coverage_unbounded_total",
	Help: "Claude analytics runs that claimed coverage with no finality bound, the vendor's watermark being too stale to apply.",
}, []string{logger.OrgID, logger.OperationID})

// runCovered counts runs that reached the furthest point they could LEGITIMATELY claim — the
// clock, or the vendor's own currency when that is nearer. That distinction is the whole
// contract: an organisation whose vendor runs a day or two behind covers on every healthy run,
// and measuring against the clock instead would report a permanent stall for the most ordinary
// configuration there is.
//
// The consequence is that a cover is not by itself progress: a vendor frozen on one day lets
// every run cover while the position never moves. So this is the DIAGNOSTIC that splits the two
// causes of a stalled connector, not the progress signal — covers still growing means the runs
// are getting where they can and the vendor has stopped; covers flat means the runs are not
// getting there. periodsCovered is what measures progress.
var runCovered = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_run_covered_total",
	Help: "Claude analytics runs that reached the furthest point they could claim (the clock, or the vendor's currency when nearer).",
}, []string{logger.OrgID, logger.OperationID})

// recordsEmitted counts records the sink ACCEPTED, per operation. It is the volume number a
// reader wants first — a run that covered its window and emitted nothing is a different problem
// from a run that stopped short.
//
// Accepted, not constructed, and every emit site owes that ordering: counted before the sink,
// this series reports healthy volume straight through a queue outage, and because the unit
// retries it re-counts the same unsent record on every attempt — so the number climbs while
// nothing is ingested.
var recordsEmitted = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_records_emitted_total",
	Help: "Claude analytics records accepted by the sink.",
}, []string{logger.OrgID, logger.OperationID})

// pagesRead counts vendor pages read, per operation. Against a request budget shared by the
// whole operation family at organisation level, this is what says which operation is spending
// it — and a page count climbing while records stay flat is a fan-out the identity is absorbing.
var pagesRead = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_pages_read_total",
	Help: "Vendor pages read by the Claude analytics operations.",
}, []string{logger.OrgID, logger.OperationID})

// periodsCovered counts the units the position ADVANCED PAST — a calendar day for the activity
// walks, a date-range chunk for the report pair, a snapshot for the roster.
//
// Advanced past, not completed, for the walks: they deliberately re-read a trailing overlap for
// late arrivals, so a re-read day completes without being progress and counting completions
// would report movement on a run that moved nothing. The roster is the exception and cannot be
// otherwise — its unit is a whole snapshot with no position to advance, so it counts each
// completed snapshot.
//
// What this series does NOT distinguish is the rate of progress. A forward-capped run advances
// several chunks and is still falling behind the clock, so a stall of that shape reads as
// healthy here; compare the stored position with the clock to catch it. What it does catch, and
// runCovered cannot, is a run that fails outright and a vendor parked on one day.
var periodsCovered = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_periods_covered_total",
	Help: "Units (a day, a chunk, a snapshot) the Claude analytics operations advanced their position past.",
}, []string{logger.OrgID, logger.OperationID})

// rowsUnreadable counts vendor rows this connector could not key.
//
// The walks do not skip an unkeyable row: it fails the run, because these units checkpoint on
// completion and a skipped row would be lost for good. So this counts the failure, and the
// signature to watch for is the inverse — a rising count with the run status already failing
// tells you which organisation and which operation, which the status alone does not.
var rowsUnreadable = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_rows_unreadable_total",
	Help: "Vendor rows the Claude analytics operations could not key, each of which fails its run.",
}, []string{logger.OrgID, logger.OperationID})

// vendorBadRequest counts the vendor's 400s by what they MEAN, which is the whole point: a 400
// saying a day is not yet aggregated is expected and holds the position, and a 400 saying a
// parameter changed mid-sequence is a defect in our own query. They are the same status code, so
// counted together the defect hides inside the normal condition and nothing distinguishes a
// vendor running late from a connector asking wrongly.
//
// This is the event-level view. runYielded's not_aggregated reason is the run-level one — a run
// stops at the first not-ready day, so the two answer different questions.
var vendorBadRequest = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_vendor_bad_request_total",
	Help: "Vendor 400 responses to the Claude analytics operations, by what the 400 means.",
}, []string{logger.OrgID, logger.OperationID, "kind"})

// positionFarBehind counts runs whose stored position is further behind the clock than a
// healthy backfill would leave it.
//
// A COUNTER, and the metric type is the whole design here. The obvious shape is a gauge of the
// position's age — but `org_id` is collapsed to one label for individual tenants, and a gauge
// Set is last-writer-wins, so one healthy individual tenant would overwrite a stuck one and the
// signal would be silently lost for the tenants there are most of. A counter aggregates
// correctly under that collapse: ten stuck tenants read as ten.
//
// It exists at all because the framework's cursor-age gauge cannot see this connector:
// isPerEntryCursorKey rejects a lower-case key and all three positions here are flat snake_case
// objects. Without it a position that stops advancing is invisible, since every run still
// reports completed.
var positionFarBehind = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "anthropic_position_far_behind_total",
	Help: "Claude analytics runs whose stored position is further behind the clock than a converging backfill would be.",
}, []string{logger.OrgID, logger.OperationID})

// maxHealthyPositionAge is how far behind the clock a stored position may sit before a run that
// loads it is counted. 120 days: past the 90-day first-sync lookback, so a converging backfill is
// never counted, and well short of the 365 days past which the report pair cannot request a start
// at all.
const maxHealthyPositionAge = 120 * 24 * time.Hour

// The two meanings a vendor 400 carries. Constants so the label set is closed and a query
// matching one cannot be broken by a typo at an emit site.
const (
	badRequestNotAggregated = "not_aggregated"
	badRequestQueryDefect   = "query_defect"
)

// Yield reasons. Kept as constants so the label set is closed and greppable — a query that
// matches a reason string cannot be broken by a typo at one emit site.
const (
	yieldNotAggregated = "not_aggregated"
	yieldForwardCap    = "forward_cap"
	yieldUnreadable    = "unreadable_span"
)

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		positionUnreadable,
		runYielded,
		coverageUnbounded,
		runCovered,
		recordsEmitted,
		pagesRead,
		periodsCovered,
		rowsUnreadable,
		vendorBadRequest,
		positionFarBehind,
	}
}

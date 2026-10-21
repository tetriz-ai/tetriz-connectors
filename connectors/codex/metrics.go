// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Counters, not gauges, throughout. The organisation label collapses tenants onto one series in
// most views, and a gauge under a collapsed label is last-writer-wins — it reports one arbitrary
// tenant as if it spoke for all of them. Counters add under collapse. The price is that a count
// says how MANY organisations are affected rather than which, which is the right trade for
// a fleet-wide count and the wrong one for diagnosis; the org label is still there when you
// need it.

// recordsEmitted counts records the sink accepted.
//
// Counted AFTER the sink accepts, so the series means "reached the queue" rather than "was
// constructed". Counting before would report healthy volume straight through a queue outage, and
// the day's retry would re-count the same record on every attempt.
var recordsEmitted = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_records_emitted_total",
	Help: "Codex usage records accepted by the sink.",
}, []string{logger.OrgID, logger.OperationID})

// pagesRead counts vendor requests. Against the records emitted it tells a dropped page from a
// quiet workspace: a day that read several pages and emitted nothing is a page fetched and
// dropped, which no other series can distinguish.
var pagesReadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_pages_read_total",
	Help: "Codex usage page requests issued to the vendor.",
}, []string{logger.OrgID, logger.OperationID})

// daysCovered counts calendar days that ADVANCED the stored position.
//
// Advanced, not merely completed, and the distinction is what makes it a progress signal. The
// walk deliberately re-reads a trailing overlap, so an ordinary run completes at least three days
// while gaining one — and a run whose budget runs out inside the overlap completes days while
// gaining nothing at all. Counting completions would show healthy movement through a total stall.
//
// Run status cannot answer this either. A run that gains eight of ninety days and is then rate
// limited is recorded as failed, and one that gains nothing because its window was empty is
// recorded as completed. During a backfill this is the only series that shows ground gained.
var daysCovered = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_days_covered_total",
	Help: "Codex usage calendar days read to completion.",
}, []string{logger.OrgID, logger.OperationID})

// rowsUnreadable counts rows that could not be keyed, and its expected value is zero forever.
//
// This is the schema-drift alarm. OpenAI publishes no response schema, so the shape this
// connector reads is modelled on a third-party contract; a non-zero value means the response no
// longer matches it. Incremented on the same path that FAILS the day, so it is never a count of
// rows quietly skipped — the walk refuses to checkpoint past a row it cannot key.
var rowsUnreadable = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_rows_unreadable_total",
	Help: "Codex usage rows that could not be keyed, which means the response shape has drifted.",
}, []string{logger.OrgID, logger.OperationID})

// daysWithoutRecords counts days that read MORE THAN ONE page and stored nothing.
//
// The per-day fact has to be counted here because Prometheus cannot reconstruct it. An org-wide
// page total over a window cannot tell a multi-page day from several single-page days, and the
// walk re-reads a trailing overlap on every run — so an established but quiet workspace
// contributes three page requests and no records on an ordinary day, which is indistinguishable
// from a dropped page at the aggregate level. Counting the condition where it is actually known
// is the only way the count means what it says.
//
// A quiet workspace answers in ONE page, so it never increments this.
var daysWithoutRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_days_without_records_total",
	Help: "Codex usage days that read more than one page and stored no records, which means a page was fetched and dropped.",
}, []string{logger.OrgID, logger.OperationID})

// runCovered counts runs that reached the last day they could claim.
var runCovered = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_run_covered_total",
	Help: "Codex usage runs that reached the newest day available to them.",
}, []string{logger.OrgID, logger.OperationID})

// runYielded counts runs that stopped before covering their window, by why.
//
// It exists because a run that stops short can still be a normal condition, and one that never
// stops being short is a stall. The reason separates them:
//
//   - budget: the run ran out of vendor requests mid-window. Ordinary during a backfill, since a
//     ninety-day first sync spans several runs by design; a stall if the pair never converges,
//     which is what makes the covered/yielded RATIO the thing to watch rather than either alone.
//   - position_ahead: the stored day is past the newest readable one, so no day can be read at
//     all. Never self-healing — it needs the position repaired.
var runYielded = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_run_yielded_total",
	Help: "Codex usage runs that stopped short of covering their window, by why they stopped.",
}, []string{logger.OrgID, logger.OperationID, "reason"})

// The yield reasons. Named rather than inlined because they are a metric label, and a string
// typed differently at two call sites splits one condition into two series that each look quiet.
const (
	yieldBudget        = "budget"
	yieldPositionAhead = "position_ahead"
)

// positionUnreadable counts runs that could not read their stored position and restarted from the
// lookback.
//
// The event is per-organisation but the most serious cause is global: a renamed JSON tag makes
// every organisation's position read as absent at once, and each then restarts a ninety-day sync
// against a per-organisation request budget measured in tens per minute. So the SHAPE of the
// series over the fleet is what identifies the cause — one organisation is a corrupt row, every
// organisation at once is a code change.
var positionUnreadable = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "codex_position_unreadable_total",
	Help: "Codex usage runs that could not read their stored position and restarted from the lookback.",
}, []string{logger.OrgID, logger.OperationID})

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		recordsEmitted,
		pagesReadTotal,
		daysCovered,
		rowsUnreadable,
		daysWithoutRecords,
		runCovered,
		runYielded,
		positionUnreadable,
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Three counters the framework cannot infer, because they are about what this connector
// found rather than whether its run succeeded.
//
// The orchestrator already publishes per-operation success, failure and duration for every
// connector, and this operation inherits all of it. What none of that can say is whether a
// run that succeeded actually collected anything — and for a connector whose whole job is
// copying files that somebody else publishes, "succeeded and found nothing" is the state
// worth watching.
//
// Labels are bounded the way the framework's other series are: organisation identifiers
// are collapsed for individual tenants before they reach these, so cardinality is the
// number of connected organizations rather than the person count.
// orgLabel is the organisation dimension every series here carries. A constant because
// three string literals is where a typo starts producing a second, silently-empty series.
const orgLabel = "org_id"

var (
	// ReportsCopiedTotal counts report files copied into the configured blob store.
	//
	// The rate is the honest liveness signal for this connector. A revoked grant, a
	// customer who switched reporting off, and a vendor that silently stopped publishing
	// all look identical in the run-level series — every run succeeds, having found
	// nothing — and differ only here.
	ReportsCopiedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kiro_reports_copied_total",
			Help: "Count of Kiro report files copied from a customer's bucket into the configured blob store, by organisation and client type. A flat rate for an organisation that was collecting is the signal that collection stopped without any run failing.",
		},
		[]string{orgLabel, "client_type"},
	)

	// EmptyDaysTotal counts days walked that held no reports at all.
	//
	// This is what separates a healthy organisation whose vendor simply did not publish
	// from one whose read grant has been revoked. Without it the two are the same
	// observation from outside: a successful run that copied nothing. Before a customer
	// enables reporting, and on any day the vendor skips, an empty day is correct — so
	// this is a counter to compare against the copied rate rather than one to read
	// on its own.
	EmptyDaysTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kiro_empty_days_total",
			Help: "Count of days walked that held no Kiro reports. Correct before reporting is enabled and on any day the vendor skipped; read alongside reports_copied_total, where a rising empty rate against a flat copied rate is a grant that has gone away.",
		},
		[]string{orgLabel},
	)

	// ReportsUnplaceableTotal counts objects under a day's prefix whose key does not match
	// the documented layout.
	//
	// It is the second way a report can go missing, and until now the only one with no
	// signal. An object we cannot place cannot be filed under a day, so it is neither
	// collected nor settled — and the day completes anyway, because holding it open would
	// pin the organisation at that day the moment the vendor drops any extra file beside
	// its reports. That trade is deliberate; being unable to SEE it was not.
	//
	// It is also the earliest evidence that the layout this connector was built against is
	// wrong, which is the design's largest open assumption — the layout comes from vendor
	// documentation, never from an observed enterprise bucket. A rising rate here is that
	// assumption failing, and it is worth reading before any organisation's data looks odd.
	ReportsUnplaceableTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kiro_reports_unplaceable_total",
			Help: "Count of objects under a Kiro day prefix whose key does not match the documented report layout, by organisation. Neither collected nor settled: the day completes without them. A non-zero rate is either a vendor file this connector does not expect, or the layout assumption being wrong.",
		},
		[]string{orgLabel},
	)

	// ReportsAbandonedTotal counts report files settled as permanently unreadable.
	//
	// It is its own counter because it watches a different thing from the configuration
	// fault, which stops collection entirely and is therefore visible in the run series.
	// This counts the opposite shape: collection deliberately continuing past a file it could not read.
	// The day completes, the mark advances, every run stays green — and a report is gone.
	// This is the only signal that the hole exists, so any increment is actionable.
	ReportsAbandonedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kiro_reports_abandoned_total",
			Help: "Count of Kiro report files settled as permanently unreadable and skipped thereafter. Expected value is zero: collection continues past these, so no run fails and nothing else records that a report will never arrive.",
		},
		[]string{orgLabel},
	)
)

// The client types the vendor documents, and the only values allowed to become a metric
// label. Anything else collapses to metricClientTypeOther.
//
// The label's source is a filename in a bucket the CUSTOMER controls, and the key pattern
// accepts any identifier-shaped value — so an unbounded label here lets one organisation's
// bucket mint a new Prometheus series per distinct filename, up to a thousand per listed day
// and far more across a long first run. Cardinality growth in the monitoring system is not
// something the connector can undo afterwards, so it is refused at the source.
//
// An unrecognised value still reaches the RECORD, which is where the vendor's own spelling
// belongs; only the label is collapsed. A new client type therefore shows up as a rising
// "other" rate rather than as missing data.
var metricClientTypes = map[string]struct{}{
	"KIRO_IDE": {},
	"KIRO_CLI": {},
	"PLUGIN":   {},
}

const metricClientTypeOther = "other"

// metricClientType bounds the client-type label to the documented set.
func metricClientType(clientType string) string {
	if _, ok := metricClientTypes[clientType]; ok {
		return clientType
	}
	return metricClientTypeOther
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		ReportsCopiedTotal,
		EmptyDaysTotal,
		ReportsUnplaceableTotal,
		ReportsAbandonedTotal,
	}
}

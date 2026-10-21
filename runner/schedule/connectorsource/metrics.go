// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// scheduleSuppressed counts schedule entries an organisation-scoped gate withheld, by
// connector and organisation.
//
// It exists because a gate used as a rollback lever is otherwise invisible: an operator
// who closes one has nothing to confirm the emission actually stopped except the absence
// of activity, and an absence takes a full sync interval to become convincing. This
// turns that into a positive signal on the next schedule refresh.
//
// A counter rather than a log: the schedule source rebuilds on a timer, so a log line
// per withheld organisation per refresh would be noise, while a rate that climbs is
// exactly the confirmation wanted.
//
// Every schedule gate reports here — the VCS content pull and each gated
// connector — so a zero for a connector means nothing was withheld rather than that its
// gate was never instrumented. Deliberately not counted: the emission loop also withholds an entry for a
// multi-user operation on an individual tenant, and for a schedule that resolves to no
// cron at all. Those are an intent and a misconfiguration, not levers anyone pulls during
// an incident, and counting routine skips here would bury the signal this exists for.
var scheduleSuppressed = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "connector_schedule_suppressed_total",
	Help: "Connector schedule entries withheld by an organisation-scoped gate.",
}, []string{"connector_id", "org_id"})

// recordSuppressedSchedule increments the counter with the organisation collapsed for
// metric use. The collapse is not optional: an individual tenant is one organisation
// each, so a raw ID here would mint a series per tenant, and a series already ingested
// cannot be recovered by a recording rule.
func recordSuppressedSchedule(individualSet map[string]struct{}, connectorID sdk.ConnectorID, orgID string) {
	scheduleSuppressed.WithLabelValues(
		string(connectorID),
		tenantlabel.MetricTenantID(individualSet, orgID),
	).Inc()
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		scheduleSuppressed,
	}
}

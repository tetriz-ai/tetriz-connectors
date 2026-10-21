// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import "github.com/prometheus/client_golang/prometheus"

var gitlabCircuitBreakerOpenedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitlab_circuit_breaker_opened_total",
		Help: "Count of per-(group, org) circuit-breaker trips. Each trip pauses GitLab traffic for that org until the cooldown expires.",
	},
	[]string{"group", "org_id"},
)

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{gitlabCircuitBreakerOpenedTotal}
}

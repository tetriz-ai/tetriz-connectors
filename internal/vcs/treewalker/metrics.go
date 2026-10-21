// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package treewalker

import (
	"github.com/prometheus/client_golang/prometheus"
)

// subtreeFetchesTotal counts non-recursive per-subtree API calls made during
// the BFS fallback walk when the vendor returns a truncated recursive result.
// Only incremented inside walkSubtrees; Walk's initial recursive attempt is not
// counted here (it succeeds fully or triggers the BFS path).
var subtreeFetchesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_repo_tree_walker_subtree_fetches_total",
		Help: "Total non-recursive per-subtree API fetches performed during BFS fallback tree walk.",
	},
	[]string{"connector"},
)

// RecordSubtreeFetch increments the subtree fetch counter for connector.
// Called by vendor-specific tree walker adapters before each non-recursive
// GetTree call inside the BFS fallback.
func RecordSubtreeFetch(connector string) {
	subtreeFetchesTotal.WithLabelValues(connector).Inc()
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		subtreeFetchesTotal,
		repoTreeFetchErrorsTotal,
		repoTreeFetchedTotal,
	}
}

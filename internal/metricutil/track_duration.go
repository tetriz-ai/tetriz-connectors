// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package metricutil records how long store methods take.
package metricutil

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var daoFunctionRunDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "dao_function_run_duration_seconds",
		Help: "time taken by each dao functions",
		// The top bound sits above a two-minute statement timeout, so a query
		// that exhausts one still lands in a finite bucket rather than +Inf.
		Buckets: []float64{0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 2, 4, 6, 10, 15, 30, 60, 120, 180},
	},
	[]string{"service_name", "pkg_path", "struct_name", "func_name"})

// TrackDuration observes the time since st for one store method. Call it
// deferred at the top of the method.
func TrackDuration(pkgPath, daoImplName, funcName string, st time.Time) {
	runDuration := time.Since(st)
	serviceName := getServiceNameFromPkgPath(pkgPath)
	daoFunctionRunDuration.WithLabelValues(serviceName, pkgPath, daoImplName, funcName).Observe(runDuration.Seconds())
}

func getServiceNameFromPkgPath(pkgPath string) string {
	if i := strings.Index(pkgPath, "/"); i >= 0 {
		return pkgPath[:i]
	}
	return pkgPath
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		daoFunctionRunDuration,
	}
}

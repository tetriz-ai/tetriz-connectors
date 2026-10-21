// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Connector-side observability for the Linear fetch operations. These
// cover the producer half of the pipeline — what the connector pulled from
// Linear and how long the API leg took. Downstream pm_* processing emits its own
// consumer-side metrics.
//
// Label cardinality is bounded: operation_id ~ low single digits for Linear,
// org_id ~ connected organizations (same precedent as VendorPermissionDeniedTotal), and
// status is a closed {success,error} set.

const (
	labelStatusSuccess = "success"
	labelStatusError   = "error"
)

var (
	// recordsFetchedTotal counts raw records emitted to the sink per Linear
	// operation. A sustained zero for an enabled op on an active workspace means
	// the op is silently fetching nothing.
	recordsFetchedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_linear_records_fetched_total",
			Help: "Count of Linear raw records emitted to the sink, by operation and org.",
		},
		[]string{"operation_id", "org_id"},
	)

	// opDurationSeconds measures wall-clock duration of a full Linear operation
	// run (all teams, all pages). Tagged with status so error runs (which may
	// short-circuit) are distinguishable from clean completions.
	opDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "connector_linear_op_duration_seconds",
			Help: "Wall-clock duration of a Linear connector operation run, by operation, org, and status.",
			// A full run pages every team and can take minutes; DefBuckets (max
			// 10s) would dump nearly everything into +Inf. Exponential 1s→~34min
			// keeps latency quantiles meaningful across backfill and incremental.
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		},
		[]string{"operation_id", "org_id", "status"},
	)
)

// recordFetched adds n to the records-fetched counter for (op, org). n == 0 is
// intentionally NOT skipped: Add(0) materializes the series at zero so a
// "ran but emitted nothing" run reads as a flat zero — an absent series reads
// as no run at all, which would defeat this metric's purpose. Only a
// negative n (never expected) is skipped to avoid corrupting a monotonic counter.
func recordFetched(opID, orgID string, n int) {
	if n < 0 {
		return
	}
	recordsFetchedTotal.WithLabelValues(opID, orgID).Add(float64(n))
}

// recordOpDuration observes an operation's run duration, deriving the status
// label from whether the run returned an error.
func recordOpDuration(opID, orgID string, start time.Time, err error) {
	status := labelStatusSuccess
	if err != nil {
		status = labelStatusError
	}
	opDurationSeconds.WithLabelValues(opID, orgID, status).Observe(time.Since(start).Seconds())
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		recordsFetchedTotal,
		opDurationSeconds,
	}
}

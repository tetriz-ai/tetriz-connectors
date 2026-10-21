// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics for the Bitbucket vendor gateway (ADR-0004 §6).
//
// Cardinality: labels are (group, org). Groups are bounded by the
// gateway/defaults.yaml config (~1 today). Orgs grow with the number of
// connected organizations — acceptable for the monitoring that reads these.
// bitbucket_ratelimit_acquire_total adds (operation, outcome): operations
// are bounded by the connector's registered set (~9) and outcomes by the four
// constants below, so it stays a small multiple of the (group, org) series count.

// Acquire outcomes. They are kept distinct because each demands a different
// response: `limited` is the budget doing its job, `error` is the limiter itself
// failing (a Redis problem, not a quota one), and `circuit_open` is a call refused
// upstream of the bucket after a 429 storm. Collapsing them would hide an outage
// behind what looks like ordinary throttling.
//
// `circuit_open` consumes no token, but it is still attempted demand — and it is
// the outcome that dominates precisely when an org is most starved, so leaving it
// out would make the series fall silent in the regime it exists to describe.
const (
	acquireOutcomeGranted     = "granted"
	acquireOutcomeLimited     = "limited"
	acquireOutcomeError       = "error"
	acquireOutcomeCircuitOpen = "circuit_open"
)

// operationUnattributed labels a call whose context carries no operation id. It is
// an explicit value rather than "" so that unattributed traffic is visibly its own
// series — an empty label reads as a legitimate operation named nothing, and would
// silently pool every such caller together with no way to tell from the metric.
const operationUnattributed = "unattributed"

var (
	bitbucketRateLimitBudgetRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bitbucket_ratelimit_budget_remaining",
			Help: "Bitbucket X-RateLimit-Remaining for the most recent response, per (group, org). Negative or zero values mean the org is at the bucket floor and the next call will 429.",
		},
		[]string{"group", "org_id"},
	)

	bitbucketRateLimit429Total = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bitbucket_ratelimit_429_total",
			Help: "Count of HTTP 429 responses from Bitbucket REST per (group, org). Spikes correlate with the circuit breaker tripping.",
		},
		[]string{"group", "org_id"},
	)

	bitbucketCircuitBreakerOpenedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bitbucket_circuit_breaker_opened_total",
			Help: "Count of per-(group, org) circuit-breaker trips. Each trip pauses Bitbucket traffic for that org until the cooldown expires.",
		},
		[]string{"group", "org_id"},
	)

	// bitbucketRateLimitAcquireTotal attributes local-budget consumption to the
	// operation that caused it. The org budget is one bucket shared by every
	// operation, so without this label a rejection says only "the org ran out" and
	// not which operation drained it — the question that decides how the budget
	// should be apportioned.
	//
	// Counted at every point a call is admitted or turned away, so it measures
	// DEMAND (calls attempted) rather than only the ones that spent a token — which
	// is what a share must be sized against. Calls served from a fetch cache never
	// reach the gateway and are correctly absent.
	bitbucketRateLimitAcquireTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bitbucket_ratelimit_acquire_total",
			Help: "Local rate-limit decisions per (group, org, operation, outcome). granted consumed a token; limited was rejected by the local bucket; circuit_open was refused upstream of the bucket during a 429 storm (no token spent); error means the limiter itself failed. Sum over all four outcomes is the operation's attempted demand.",
		},
		[]string{"group", "org_id", "operation", "outcome"},
	)
)

// recordAcquire attributes one acquire decision. operation is normalized so an
// unattributed call is its own visible series rather than an empty label.
func recordAcquire(group, orgID, operation, outcome string) {
	if operation == "" {
		operation = operationUnattributed
	}
	bitbucketRateLimitAcquireTotal.WithLabelValues(group, orgID, operation, outcome).Inc()
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		bitbucketRateLimitBudgetRemaining,
		bitbucketRateLimit429Total,
		bitbucketCircuitBreakerOpenedTotal,
		bitbucketRateLimitAcquireTotal,
	}
}

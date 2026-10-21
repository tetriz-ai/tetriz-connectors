// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"github.com/tetriz-ai/tetriz-connectors/gateway"
)

// circuitBreaker is the shared 429 circuit breaker (gateway.CircuitBreaker,
// ADR-0004 §5), keyed under circuitKeyPrefix so its state never mixes with
// another vendor's in a shared store.
type circuitBreaker = gateway.CircuitBreaker

const circuitKeyPrefix = "bb-cb"

// newCircuitBreaker returns the Bitbucket breaker over store, counting each
// trip in bitbucket_circuit_breaker_opened_total. A nil store returns nil.
func newCircuitBreaker(store gateway.CircuitBreakerStore) *circuitBreaker {
	return gateway.NewCircuitBreaker(store, circuitKeyPrefix, func(group, orgID string) {
		bitbucketCircuitBreakerOpenedTotal.WithLabelValues(group, orgID).Inc()
	})
}

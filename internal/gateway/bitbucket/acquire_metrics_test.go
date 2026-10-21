// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bbgateway

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// allAcquireOutcomes is the complete outcome set the gateway can record. Anything
// summing to "attempted demand" enumerates from here, so the sum has one definition.
// A new outcome added to the consts but not to this list under-reports demand — the
// drift this list exists to localize.
var allAcquireOutcomes = []string{
	acquireOutcomeGranted,
	acquireOutcomeLimited,
	acquireOutcomeError,
	acquireOutcomeCircuitOpen,
}

// recordAcquire is what attributes local-budget consumption to an operation, so the
// label it writes is the whole value of the metric — a wrong or collapsed label makes
// the series unusable for sizing a per-operation share while still looking
// populated. These pin the two properties that matter: the operation label survives
// intact, and an unattributed call is its own visible series rather than "".
func TestRecordAcquire_labels(t *testing.T) {
	const group = "bitbucket-rest"

	tests := []struct {
		name          string
		orgID         string
		operation     string
		outcome       string
		wantOperation string
	}{
		{
			name:          "operation is recorded as given",
			orgID:         "org-a",
			operation:     "fetch_pr_comments",
			outcome:       acquireOutcomeGranted,
			wantOperation: "fetch_pr_comments",
		},
		{
			// An empty label reads as a legitimate operation named nothing and pools
			// every such caller into one anonymous series.
			name:          "an unattributed call gets an explicit label",
			orgID:         "org-b",
			operation:     "",
			outcome:       acquireOutcomeLimited,
			wantOperation: operationUnattributed,
		},
		{
			name:          "limiter failure is its own outcome",
			orgID:         "org-c",
			operation:     "fetch_prs",
			outcome:       acquireOutcomeError,
			wantOperation: "fetch_prs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := testutil.ToFloat64(
				bitbucketRateLimitAcquireTotal.WithLabelValues(group, tt.orgID, tt.wantOperation, tt.outcome))

			recordAcquire(group, tt.orgID, tt.operation, tt.outcome)

			after := testutil.ToFloat64(
				bitbucketRateLimitAcquireTotal.WithLabelValues(group, tt.orgID, tt.wantOperation, tt.outcome))
			assert.Equal(t, before+1, after, "counter for the expected label set")
		})
	}
}

// The three outcomes must stay distinct: `limited` is the budget working as intended,
// `error` is the limiter itself failing. Collapsing them would hide a Redis outage
// behind what reads as ordinary throttling.
func TestRecordAcquire_outcomesAreDistinctSeries(t *testing.T) {
	const (
		group = "bitbucket-rest"
		org   = "org-distinct"
		op    = "fetch_reviews"
	)

	recordAcquire(group, org, op, acquireOutcomeGranted)
	recordAcquire(group, org, op, acquireOutcomeLimited)
	recordAcquire(group, org, op, acquireOutcomeLimited)
	recordAcquire(group, org, op, acquireOutcomeError)

	assert.Equal(t, 1.0, testutil.ToFloat64(
		bitbucketRateLimitAcquireTotal.WithLabelValues(group, org, op, acquireOutcomeGranted)))
	assert.Equal(t, 2.0, testutil.ToFloat64(
		bitbucketRateLimitAcquireTotal.WithLabelValues(group, org, op, acquireOutcomeLimited)))
	assert.Equal(t, 1.0, testutil.ToFloat64(
		bitbucketRateLimitAcquireTotal.WithLabelValues(group, org, op, acquireOutcomeError)))

	// A call refused by an open circuit spends no token but is still demand. It is
	// also the outcome that dominates during a 429 storm, so omitting it would make
	// the series fall silent exactly when share-sizing needs it most.
	recordAcquire(group, org, op, acquireOutcomeCircuitOpen)
	assert.Equal(t, 1.0, testutil.ToFloat64(
		bitbucketRateLimitAcquireTotal.WithLabelValues(group, org, op, acquireOutcomeCircuitOpen)))

	// Sum over outcomes is the operation's attempted demand — the quantity a reserved
	// share is sized against, and what the Help text promises a reader. The sum must
	// cover every outcome the gateway can record; a new one added without extending
	// this list would silently under-report demand.
	var demand float64
	for _, outcome := range allAcquireOutcomes {
		demand += testutil.ToFloat64(
			bitbucketRateLimitAcquireTotal.WithLabelValues(group, org, op, outcome))
	}
	assert.Equal(t, 5.0, demand, "granted+limited+error+circuit_open is attempted demand")
}

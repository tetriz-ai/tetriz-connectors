// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordFetched_CreatesZeroSeriesAndGuardsNegative(t *testing.T) {
	// Unique label values keep this independent of other tests touching the
	// global counter.
	const op, org = "fetch_issues", "org-metrics-test"

	// A zero-fetch run must still materialize the series (at 0) so a
	// "ran but emitted nothing" run reads as zero — an absent series does not.
	recordFetched(op, org, 0)
	assert.Equal(t, float64(0), testutil.ToFloat64(recordsFetchedTotal.WithLabelValues(op, org)))

	before := testutil.ToFloat64(recordsFetchedTotal.WithLabelValues(op, org))
	recordFetched(op, org, 3)
	recordFetched(op, org, -1) // negative guarded: no change
	after := testutil.ToFloat64(recordsFetchedTotal.WithLabelValues(op, org))

	assert.Equal(t, float64(3), after-before)
}

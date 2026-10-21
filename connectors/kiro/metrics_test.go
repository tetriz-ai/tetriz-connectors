// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTheClientTypeLabelIsBoundedToTheDocumentedSet(t *testing.T) {
	t.Parallel()

	// The label's source is a filename in a bucket the customer controls, so an unbounded
	// label lets one organisation mint a Prometheus series per distinct filename. That is
	// cardinality the monitoring system cannot be talked out of afterwards.
	for _, documented := range []string{"KIRO_IDE", "KIRO_CLI", "PLUGIN"} {
		assert.Equal(t, documented, metricClientType(documented),
			"a documented client type must reach the label as the vendor spells it")
	}

	// Everything else collapses — including values the key pattern happily accepts.
	for _, unexpected := range []string{
		"KIRO_FUTURE", "kiro_ide", "", "A", "KIRO_IDE_2",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	} {
		assert.Equal(t, metricClientTypeOther, metricClientType(unexpected),
			"an undocumented client type must not become its own series: %q", unexpected)
	}
}

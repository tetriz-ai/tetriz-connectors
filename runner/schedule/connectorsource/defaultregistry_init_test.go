// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

// The package's tests gather the default registry, so expose the scheduler's
// metrics and this package's there, unprefixed.
func init() {
	prometheus.DefaultRegisterer.MustRegister(schedule.Collectors()...)
	prometheus.DefaultRegisterer.MustRegister(Collectors()...)
}

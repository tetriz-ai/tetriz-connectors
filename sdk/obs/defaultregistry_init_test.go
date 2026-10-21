// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package obs

import "github.com/prometheus/client_golang/prometheus"

// The package's tests gather the default registry, so expose the package's
// metrics there, unprefixed.
func init() {
	prometheus.DefaultRegisterer.MustRegister(Collectors()...)
}

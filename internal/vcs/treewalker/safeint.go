// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package treewalker

import "math"

// SafeInt32 clamps n to the int32 range. Vendor counters (stars, forks,
// watchers, …) come back as untyped int; the proto Stats type stores int32.
// Real-world values fit in int32 by many orders of magnitude, but a clamp
// keeps gosec G115 honest and avoids silent overflow on pathological inputs.
func SafeInt32(n int) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n) //#nosec G115 -- bounded above by explicit range check.
}

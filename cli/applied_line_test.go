// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli

import "testing"

// migrate reports the count it applied in a sentence that reads right for
// none, one and many.
func TestAppliedLine(t *testing.T) {
	for n, want := range map[int]string{0: "applied 0 migrations", 1: "applied 1 migration", 2: "applied 2 migrations"} {
		if got := appliedLine(n); got != want {
			t.Errorf("appliedLine(%d) = %q, want %q", n, got, want)
		}
	}
}

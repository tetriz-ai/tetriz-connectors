// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/demoseed"
)

// TestBoardIDFormulaMatchesSeed pins the load-bearing cross-package invariant: the
// board-id formula this package uses to LIST boards (deriveBoardID) must equal the
// one demoseed uses to STAMP a sprint's originBoardId (demoseed.DeriveBoardID).
// They are independent copies in two non-importing packages; if either drifts, the
// simulator lists a board id no seeded sprint matches, so handleBoardSprints returns
// zero sprints with no error — the silent-zero this test exists to prevent. A
// per-package test that used its own copy on both sides of the assertion could not
// catch this; this test is the only one that touches both copies.
func TestBoardIDFormulaMatchesSeed(t *testing.T) {
	// Cover the seeded project-id range plus the non-numeric / boundary cases.
	for _, projectID := range []string{"10000", "10001", "10007", "12345", "999", "1000", "abc", ""} {
		if got, want := deriveBoardID(projectID), demoseed.DeriveBoardID(projectID); got != want {
			t.Errorf("board-id formulas diverged for project %q: simulator=%d, demoseed=%d", projectID, got, want)
		}
	}
	// Guard the concrete value too, so a synchronized drift in BOTH copies is still
	// caught against the agreed contract (project 10001 → board 9001).
	if got := deriveBoardID("10001"); got != 9001 {
		t.Errorf("deriveBoardID(10001) = %d, want 9001", got)
	}
}

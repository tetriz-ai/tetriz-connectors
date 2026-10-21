// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// existingCursorStates is the test-side view of loadCursorStates without the
// dropped-pinned report.
func existingCursorStates(ctx context.Context, cursor *sdk.Cursor) map[string]repoCursorState {
	states, _ := loadCursorStates(ctx, cursor)
	return states
}

// prsCursor builds a fetch_prs cursor holding one watermark per repository.
func prsCursor(timestamps map[string]time.Time) *sdk.Cursor {
	states := make(map[string]repoCursorState, len(timestamps))
	for repo, ts := range timestamps {
		states[repo] = repoCursorState{Timestamp: ts}
	}
	return buildStateCursor(context.Background(), states)
}

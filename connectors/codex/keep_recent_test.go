// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// oldest is the window's first day: a listed day exactly at it is kept, one before it
// is dropped.
func TestKeepRecentDaysKeepsTheWindowsFirstDay(t *testing.T) {
	oldest := time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC)
	got := keepRecentDays(context.Background(), "org", []string{"2025-03-09", "2025-03-10", "2025-03-11"}, oldest)
	assert.Equal(t, []string{"2025-03-10", "2025-03-11"}, got)
}

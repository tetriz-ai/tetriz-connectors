// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A host's cursor-rewind tool (a job that rewinds persisted cursors) decodes and
// re-encodes this package's persisted cursor entries, and it cannot import these
// unexported types — it hand-declares the same JSON tags. If a tag here is renamed,
// that job silently stops recognising the entry, rewrites it in the wrong encoding,
// and the next run drops the cursor and re-walks the repo from the lookback floor.
//
// These assertions are the contract that makes such a rename fail here instead. On a
// deliberate change, update the job's codec in the same commit.
func TestCursorEntryJSONContract_FetchPRs(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(repoCursorState{
		Timestamp:  time.Date(2026, 8, 6, 7, 0, 0, 0, time.UTC),
		PageCursor: "3",
		Since:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &keyed))
	assert.Contains(t, keyed, "timestamp", "the rewind job keys the fetch_prs shape off \"timestamp\"")
	assert.Contains(t, keyed, "page_cursor", "the rewind job reports dropped resume state from \"page_cursor\"")
	assert.Contains(t, keyed, "since")
	// A repo with no hold must contribute no hold bytes. `omitempty` would NOT achieve
	// this — encoding/json never treats a struct as empty, so it is a silent no-op on
	// time.Time and every entry would carry "0001-01-01T00:00:00Z". omitzero is what
	// makes the fields actually disappear; this pins that they are not swapped back.
	assert.NotContains(t, keyed, "hf", "a repo with no repair floor must not serialize one")
	assert.NotContains(t, keyed, "hr", "a repo that never released a hold must not serialize one")
	assert.NotContains(t, string(raw), "0001-01-01")
}

// The hold fields are part of the same persisted shape, and the rewind job drops them
// deliberately (a rewind supersedes the connector's own floor). It finds them
// structurally rather than by name, but a rename still changes what an operator sees
// in the dry-run preview, so pin the tags alongside the rest of the entry.
func TestCursorEntryJSONContract_FetchPRsHoldFields(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(repoCursorState{
		Timestamp:      time.Date(2026, 8, 6, 7, 0, 0, 0, time.UTC),
		HoldFloor:      time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
		HoldCount:      3,
		HoldReleasedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &keyed))
	assert.Contains(t, keyed, "hf")
	assert.Contains(t, keyed, "hc")
	assert.Contains(t, keyed, "hr")

	// And the whole thing must still round-trip, so a held cursor survives a
	// persist/reload cycle rather than silently losing its streak.
	var back repoCursorState
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.Equal(t, 3, back.HoldCount)
	assert.Equal(t, 2026, back.HoldFloor.UTC().Year())
	assert.False(t, back.HoldReleasedAt.IsZero())
}

// fetch_commits / fetch_reviews / fetch_pr_comments persist a BARE timestamp via
// buildCompositeCursor, not an object. The rewind job must keep writing bare for
// those, so pin that too.
func TestCursorEntryJSONContract_BareTimestampOps(t *testing.T) {
	t.Parallel()

	cursor := buildCompositeCursor(map[string]time.Time{
		"ws/repo": time.Date(2026, 8, 6, 7, 0, 0, 0, time.UTC),
	})
	var parts map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(cursor.Data, &parts))
	require.Contains(t, parts, "ws/repo")

	// A bare JSON string, not an object — decodes straight into time.Time.
	var bare time.Time
	require.NoError(t, json.Unmarshal(parts["ws/repo"], &bare),
		"buildCompositeCursor must keep emitting a bare timestamp; the rewind job's bare-shape branch depends on it")
	assert.Equal(t, 2026, bare.UTC().Year())
}

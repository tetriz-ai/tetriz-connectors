// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package obs

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCursorEntryWatermark pins that a watermark is read from every encoding the
// connectors write. A shape this cannot read must yield the zero time — reading it as an
// actual zero timestamp would make every entry look like a regression to year 1.
func TestCursorEntryWatermark(t *testing.T) {
	t.Parallel()
	want := time.Date(2026, 4, 30, 5, 46, 24, 0, time.UTC)

	tests := []struct {
		name string
		raw  string
		want time.Time
	}{
		{
			name: "bare timestamp (bitbucket fetch_commits/reviews/pr_comments)",
			raw:  `"2026-04-30T05:46:24Z"`,
			want: want,
		},
		{
			name: "timestamp-tagged object (bitbucket fetch_prs state)",
			raw:  `{"timestamp":"2026-04-30T05:46:24Z","page_cursor":"abc","since":"2026-01-01T00:00:00Z"}`,
			want: want,
		},
		{
			name: "ts-tagged object (github, gitlab)",
			raw:  `{"ts":"2026-04-30T05:46:24Z","pc":"abc","prs":[1,2]}`,
			want: want,
		},
		{
			name: "object carrying neither watermark key",
			raw:  `{"page_cursor":"abc"}`,
			want: time.Time{},
		},
		{
			name: "unreadable value",
			raw:  `[1,2,3]`,
			want: time.Time{},
		},
		{
			// A composite whose values are not watermarks at all is legitimate:
			// fetch_repo_tree keys per repo (so the keys DO pass the per-entry test) and
			// stores a head SHA. It must read as "no watermark", never as a fault.
			name: "non-timestamp string (a repo-tree head SHA)",
			raw:  `"9d7601856c0f4a2b1e3d5f7a9c1b3d5f7a9c1b3d"`,
			want: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := cursorEntryWatermark(json.RawMessage(tt.raw))
			assert.True(t, got.Equal(tt.want), "got %s want %s", got, tt.want)
		})
	}
}

// compositeCursor builds a per-entry composite cursor from bare watermarks.
func compositeCursor(t *testing.T, entries map[string]time.Time) *sdk.Cursor {
	t.Helper()
	parts := make(map[string]json.RawMessage, len(entries))
	for key, ts := range entries {
		raw, err := json.Marshal(ts.UTC())
		require.NoError(t, err)
		parts[key] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// TestRecordCursorHealthCountsRegressions pins the monotonicity assertion: a watermark
// replaced by an earlier one — a genuine vendor timestamp overwritten by a synthetic
// first-sync floor — must count, since that shape re-scans the full window on every
// subsequent tick.
func TestRecordCursorHealthCountsRegressions(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	realWatermark := time.Date(2026, 4, 30, 5, 46, 24, 0, time.UTC)
	syntheticFloor := now.Add(-360 * 24 * time.Hour)

	tests := []struct {
		name            string
		before          map[string]time.Time
		after           map[string]time.Time
		wantRegressions int
	}{
		{
			name:            "a real watermark replaced by a synthetic floor is a regression",
			before:          map[string]time.Time{"ws/repo1": realWatermark},
			after:           map[string]time.Time{"ws/repo1": syntheticFloor},
			wantRegressions: 1,
		},
		{
			name:            "advancing is not a regression",
			before:          map[string]time.Time{"ws/repo": now.Add(-48 * time.Hour)},
			after:           map[string]time.Time{"ws/repo": now.Add(-1 * time.Hour)},
			wantRegressions: 0,
		},
		{
			name:            "unchanged is not a regression",
			before:          map[string]time.Time{"ws/repo": realWatermark},
			after:           map[string]time.Time{"ws/repo": realWatermark},
			wantRegressions: 0,
		},
		{
			// A newly-tracked entry has nothing to regress from; counting it would
			// make every first sync of every new repo look like a defect.
			name:            "a newly-tracked entry is not a regression",
			before:          map[string]time.Time{"ws/old": realWatermark},
			after:           map[string]time.Time{"ws/old": realWatermark, "ws/new": syntheticFloor},
			wantRegressions: 0,
		},
		{
			name:            "each regressed entry counts once",
			before:          map[string]time.Time{"ws/a": realWatermark, "ws/b": realWatermark},
			after:           map[string]time.Time{"ws/a": syntheticFloor, "ws/b": syntheticFloor},
			wantRegressions: 2,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// A distinct operation label per case keeps the shared counter's children
			// independent, so parallel cases cannot read each other's increments.
			opID := "op-regress-" + string(rune('a'+i))
			before := compositeCursor(t, tt.before)
			after := compositeCursor(t, tt.after)

			RecordCursorHealth(before, after, "bitbucket", opID, "org-test")

			got := counterValue(t, "bitbucket", opID, "org-test")
			assert.Equal(t, float64(tt.wantRegressions), got, "regression count")
		})
	}
}

// TestRecordCursorHealthIgnoresNonComparableCursors pins that the recorder never
// fabricates a regression from a cursor it cannot compare. A flat (non-composite) cursor
// is what most non-VCS connectors write, and it has no per-entry watermark at all.
func TestRecordCursorHealthIgnoresNonComparableCursors(t *testing.T) {
	t.Parallel()
	after := compositeCursor(t, map[string]time.Time{"ws/repo": time.Now().UTC().Add(-time.Hour)})

	tests := []struct {
		name   string
		before *sdk.Cursor
		after  *sdk.Cursor
	}{
		{name: "no prior cursor at all", before: nil, after: after},
		{name: "flat prior cursor", before: sdk.NewTimestampCursor(time.Now().UTC()), after: after},
		{name: "flat new cursor", before: after, after: sdk.NewTimestampCursor(time.Now().UTC())},
		{name: "no new cursor", before: after, after: nil},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opID := "op-noncomparable-" + string(rune('a'+i))
			RecordCursorHealth(tt.before, tt.after, "bitbucket", opID, "org-test")
			assert.Zero(t, counterValue(t, "bitbucket", opID, "org-test"),
				"a cursor pair that cannot be compared must record no regression")
		})
	}
}

// TestIsPerEntryCursorKey pins which composite keys are measured. The upper-case form is
// load-bearing for jira, whose per-project cursors are keyed by project key ("PROJ") and
// carry the same 12-month first-sync window as the VCS ops — excluding them would leave
// jira pins with no samples at all. The lower-case rejections are the framework's own flat
// cursor field names, which decode as one-entry "composites" and must stay out.
func TestIsPerEntryCursorKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key  string
		want bool
	}{
		{"myworkspace/myrepo", true}, // bitbucket, github
		{"12345", true},              // gitlab numeric project id
		{"PROJ", true},               // jira project key
		{"AB1", true},                // jira key with digits
		{"MY_PROJ", true},            // jira key with an underscore
		{"last_updated_at", false},   // flat timestamp cursor field
		{"page", false},              // flat page cursor field
		{"page_token", false},        // flat page cursor field
		{"", false},
		{"_LEADING", false}, // an underscore-led key is no project key
		{"1PROJ", false},    // mixed alnum starting with a digit
		{"MixedCase", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isPerEntryCursorKey(tt.key))
		})
	}
}

// TestRecordCursorHealthIgnoresFlatCursors is the guard against instrumenting the wrong
// connectors. sdk.CompositeFromCursor decodes ANY top-level JSON object into a map, so a FLAT
// cursor does NOT error — it parses as a one-entry "composite" keyed by a struct field
// name, whose value is a perfectly readable timestamp. Most connectors write a flat cursor
// (jira, linear, github's fetch_projects), and the orchestrator
// calls this for every operation, so without a per-entry key test they would all report a
// one-entry cursor age against thresholds derived from the VCS lookback windows.
//
// This asserts the GAUGE, not just the counter: the counter stays zero for a flat cursor
// by accident (the field-name key does not match any other cursor's key), so a
// counter-only assertion passes while the gauge is polluted.
func TestRecordCursorHealthIgnoresFlatCursors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		cursor *sdk.Cursor
	}{
		{name: "flat timestamp cursor", cursor: sdk.NewTimestampCursor(time.Now().UTC().Add(-500 * 24 * time.Hour))},
		{name: "flat page cursor", cursor: sdk.NewPageCursor(2, "tok")},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opID := "op-flat-" + string(rune('a'+i))
			RecordCursorHealth(nil, tt.cursor, "slack", opID, "org-test")
			assert.Zero(t, gaugeValue(t, "slack", opID, "org-test"),
				"a flat cursor has no per-entry watermark and must not set the age gauge")
			assert.Zero(t, counterValue(t, "slack", opID, "org-test"),
				"a flat cursor must not record a regression")
		})
	}
}

// TestCursorHealthTrackerCountsEachRegressionOnce pins why the tracker exists. A run that
// checkpoints per page persists several times, and each write must be measured against the
// last cursor that LANDED. Comparing every write against the run's starting cursor would
// re-count one early regression on every later page, against a counter whose premise is
// that any increment is a defect.
func TestCursorHealthTrackerCountsEachRegressionOnce(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	const opID = "op-tracker-once"

	tracker := NewCursorHealthTracker(compositeCursor(t, map[string]time.Time{
		"ws/repo": now.Add(-1 * time.Hour),
	}))

	// One regression, then two further pages that advance from the regressed value.
	// Every one of these is below the run's STARTING watermark.
	tracker.Record(compositeCursor(t, map[string]time.Time{"ws/repo": now.Add(-300 * 24 * time.Hour)}), "bitbucket", opID, "org-test")
	tracker.Record(compositeCursor(t, map[string]time.Time{"ws/repo": now.Add(-299 * 24 * time.Hour)}), "bitbucket", opID, "org-test")
	tracker.Record(compositeCursor(t, map[string]time.Time{"ws/repo": now.Add(-298 * 24 * time.Hour)}), "bitbucket", opID, "org-test")

	assert.Equal(t, float64(1), counterValue(t, "bitbucket", opID, "org-test"),
		"only the write that actually moved backward counts; later pages advance from it")
}

// TestRecordCursorHealthIgnoresNonWatermarkComposites pins that a composite whose keys pass
// the per-entry test but whose VALUES are not watermarks leaves both series untouched.
// fetch_repo_tree is exactly that shape — keyed by repo full name, holding a head SHA — and
// it syncs on its own schedule, so treating its entries as measurable would put a bogus age
// on the gauge for every repo it tracks.
func TestRecordCursorHealthIgnoresNonWatermarkComposites(t *testing.T) {
	t.Parallel()
	const opID = "op-nonwatermark"

	parts := map[string]json.RawMessage{
		"myworkspace/repo-a": json.RawMessage(`"9d7601856c0f4a2b1e3d5f7a9c1b3d5f7a9c1b3d"`),
		"myworkspace/repo-b": json.RawMessage(`"1f2edaa68f0c3b5d7e9a1c3b5d7e9a1c3b5d7e9a"`),
	}

	RecordCursorHealth(nil, sdk.NewCompositeCursor(parts), "bitbucket", opID, "org-test")

	assert.Zero(t, gaugeValue(t, "bitbucket", opID, "org-test"),
		"a composite of non-watermark values must not set the age gauge")
	assert.Zero(t, counterValue(t, "bitbucket", opID, "org-test"),
		"a composite of non-watermark values must not record a regression")
}

// TestRecordCursorHealthTracksOldestEntry pins the pinned-vs-idle discriminator: the gauge
// follows the OLDEST entry, so one repo stuck at a floor is visible even when every other
// repo in the same operation is current.
func TestRecordCursorHealthTracksOldestEntry(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	after := compositeCursor(t, map[string]time.Time{
		"ws/current": now.Add(-30 * time.Minute),
		"ws/pinned":  now.Add(-360 * 24 * time.Hour),
	})

	RecordCursorHealth(nil, after, "bitbucket", "op-oldest", "org-test")

	age := gaugeValue(t, "bitbucket", "op-oldest", "org-test")
	// ~360d in seconds, allowing generous slack for test execution time.
	assert.InDelta(t, (360 * 24 * time.Hour).Seconds(), age, 600,
		"gauge must track the oldest entry, not the newest")
}

// TestRecordCursorHealthClearsGaugeWhenNothingMeasurable pins the gauge's lifecycle on a
// cursor that stops carrying measurable watermarks — the state a save reaches after its
// aged entries are dropped. The gauge is process-local and holds its last Set forever,
// so without an explicit clear the healed cursor keeps asserting the old age forever.
func TestRecordCursorHealthClearsGaugeWhenNothingMeasurable(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	const opID = "op-clears-on-empty"

	populated := compositeCursor(t, map[string]time.Time{"ws/aged": now.Add(-400 * 24 * time.Hour)})
	RecordCursorHealth(nil, populated, "bitbucket", opID, "org-test")
	require.True(t, gaugeSeriesExists(t, "bitbucket", opID, "org-test"),
		"a measurable save must publish the series this test then expects to be cleared")

	emptied := sdk.NewCompositeCursor(map[string]json.RawMessage{})
	RecordCursorHealth(populated, emptied, "bitbucket", opID, "org-test")
	assert.False(t, gaugeSeriesExists(t, "bitbucket", opID, "org-test"),
		"a save with no measurable watermark must clear the series, not leave the last age standing")
}

// gaugeSeriesExists reports whether CursorOldestEntryAgeSeconds currently exports a
// series for the given labels. It gathers from the registry rather than calling
// WithLabelValues, which would CREATE the very series being probed.
func gaugeSeriesExists(t *testing.T, connectorID, operationID, orgID string) bool {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "connector_cursor_oldest_entry_age_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["connector_id"] == connectorID && labels["operation_id"] == operationID && labels["org_id"] == orgID {
				return true
			}
		}
	}
	return false
}

// counterValue reads one child of CursorRegressionsTotal.
func counterValue(t *testing.T, connectorID, operationID, orgID string) float64 {
	t.Helper()
	return testutil.ToFloat64(CursorRegressionsTotal.WithLabelValues(connectorID, operationID, orgID))
}

// gaugeValue reads one child of CursorOldestEntryAgeSeconds.
func gaugeValue(t *testing.T, connectorID, operationID, orgID string) float64 {
	t.Helper()
	return testutil.ToFloat64(CursorOldestEntryAgeSeconds.WithLabelValues(connectorID, operationID, orgID))
}

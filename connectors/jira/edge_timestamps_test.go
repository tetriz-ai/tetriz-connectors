// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
)

// TestEdge_JiraTimeAcceptsVendorFormats covers the timestamp shapes Jira Cloud
// and its proxies send: the documented "+0200" offset with millis, a UTC "+0000",
// a "Z" suffix, an RFC3339 colon offset, no fractional part, and microsecond
// precision. Each must decode to the same instant; a mis-parse would move the
// high-water mark and with it the next run's JQL boundary.
func TestEdge_JiraTimeAcceptsVendorFormats(t *testing.T) {
	t.Parallel()
	want := time.Date(2026, 1, 16, 13, 3, 31, 0, time.UTC)
	tests := []struct {
		raw  string
		want time.Time
	}{
		{`"2026-01-16T15:03:31.606+0200"`, want.Add(606 * time.Millisecond)},
		{`"2026-01-16T13:03:31.606+0000"`, want.Add(606 * time.Millisecond)},
		{`"2026-01-16T13:03:31.606Z"`, want.Add(606 * time.Millisecond)},
		{`"2026-01-16T15:03:31.606+02:00"`, want.Add(606 * time.Millisecond)},
		{`"2026-01-16T08:03:31-0500"`, want},
		{`"2026-01-16T13:03:31.606123+0000"`, want.Add(606123 * time.Microsecond)},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			t.Parallel()
			var jt jiraTime
			require.NoError(t, json.Unmarshal([]byte(tt.raw), &jt))
			assert.True(t, tt.want.Equal(jt.Time), "got %s, want %s", jt.UTC(), tt.want)
		})
	}

	t.Run("null and empty decode to zero, not an error", func(t *testing.T) {
		t.Parallel()
		var f jiraIssueFields
		require.NoError(t, json.Unmarshal([]byte(`{"updated":null,"created":"","resolutiondate":null}`), &f))
		assert.True(t, f.Updated.IsZero())
		assert.True(t, f.Created.IsZero())
		assert.True(t, f.ResolutionDate == nil || f.ResolutionDate.IsZero(),
			"a null resolutiondate must not read as resolved")
	})
}

// TestEdge_ResolveCursorOverlapRejectsNonPositive: a zero or negative YAML value
// must not shrink or invert the safety margin. A negative overlap added to the
// cursor would push the JQL boundary into the future and skip updates.
func TestEdge_ResolveCursorOverlapRejectsNonPositive(t *testing.T) {
	t.Parallel()
	assert.Equal(t, defaultCursorOverlapWindow, resolveCursorOverlap(nil))
	assert.Equal(t, defaultCursorOverlapWindow, resolveCursorOverlap(&connectorconfig.JiraConfig{}))
	assert.Equal(t, defaultCursorOverlapWindow, resolveCursorOverlap(&connectorconfig.JiraConfig{CursorOverlapWindow: -time.Hour}))
	assert.Equal(t, 30*time.Minute, resolveCursorOverlap(&connectorconfig.JiraConfig{CursorOverlapWindow: 30 * time.Minute}))

	cursor := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	got := effectiveSince(cursor, resolveCursorOverlap(&connectorconfig.JiraConfig{CursorOverlapWindow: -time.Hour}))
	assert.False(t, got.After(cursor), "the JQL boundary must never be later than the stored high-water mark")
}

// TestEdge_CursorFromNewerVersionWithUnknownFieldsResumes: a cursor written by
// a newer build (an extra field) or carrying a non-UTC offset must still resume
// at the same instant with its page token, not be dropped into a full re-walk.
func TestEdge_CursorFromNewerVersionWithUnknownFieldsResumes(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"timestamp":"2026-05-01T14:00:00+02:00","page_token":"p-2","since":"2026-05-01T11:55:00Z","added_later":{"x":1}}`)
	var s projectCursorState
	require.NoError(t, json.Unmarshal(raw, &s))
	assert.True(t, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).Equal(s.Timestamp))
	assert.Equal(t, "p-2", s.PageToken)
	assert.True(t, time.Date(2026, 5, 1, 11, 55, 0, 0, time.UTC).Equal(s.Since))
}

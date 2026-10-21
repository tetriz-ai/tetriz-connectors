// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInjectLineCounts pins the cache-hit re-injection path. The downstream
// Bitbucket consumer reads `additions` / `deletions` straight off the PR
// payload, so cache hits MUST
// produce the same shape as fresh enrichPR runs — otherwise MERGED/DECLINED/
// SUPERSEDED PRs emit zero LOC (BLOCKER from bot review).
func TestInjectLineCounts(t *testing.T) {
	tests := []struct {
		name      string
		prRaw     string
		additions int
		deletions int
		// keyChecks asserts specific keys on the resulting payload.
		keyChecks map[string]any
	}{
		{
			name:      "writes additions + deletions onto bare PR",
			prRaw:     `{"id": 42, "title": "fix"}`,
			additions: 100,
			deletions: 25,
			keyChecks: map[string]any{
				"id":        float64(42),
				"title":     "fix",
				"additions": float64(100),
				"deletions": float64(25),
			},
		},
		{
			name:      "overwrites stale additions/deletions if already present",
			prRaw:     `{"id": 1, "additions": 0, "deletions": 0}`,
			additions: 7,
			deletions: 3,
			keyChecks: map[string]any{
				"id":        float64(1),
				"additions": float64(7),
				"deletions": float64(3),
			},
		},
		{
			name:      "zero additions/deletions explicitly written",
			prRaw:     `{"id": 9}`,
			additions: 0,
			deletions: 0,
			keyChecks: map[string]any{
				"id":        float64(9),
				"additions": float64(0),
				"deletions": float64(0),
			},
		},
		{
			name:      "preserves nested + array fields verbatim",
			prRaw:     `{"id": 5, "author": {"display_name": "x"}, "labels": ["a", "b"]}`,
			additions: 1,
			deletions: 1,
			keyChecks: map[string]any{
				"additions": float64(1),
				"deletions": float64(1),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := injectLineCounts(json.RawMessage(tt.prRaw), tt.additions, tt.deletions)
			var asMap map[string]any
			require.NoError(t, json.Unmarshal(got, &asMap))
			for k, want := range tt.keyChecks {
				assert.Equal(t, want, asMap[k], "key %q", k)
			}
		})
	}
}

// TestInjectLineCountsFallback proves the at-least-once emit guarantee: on
// JSON unmarshal failure the original payload is returned, never dropped.
func TestInjectLineCountsFallback(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "invalid JSON string returns as-is", input: `not json`},
		{name: "JSON array (not object) returns as-is", input: `[1, 2, 3]`},
		{name: "empty payload returns as-is", input: ``},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := injectLineCounts(json.RawMessage(tt.input), 99, 99)
			assert.Equal(t, tt.input, string(got),
				"fallback must return input verbatim so the record isn't dropped")
		})
	}
}

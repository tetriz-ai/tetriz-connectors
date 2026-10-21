// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package parity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoad_EmbeddedManifest asserts the checked-in parity.yaml loads and carries
// the seeded vendor limits — the manifest a vendor-comparing check reads.
func TestLoad_EmbeddedManifest(t *testing.T) {
	t.Parallel()
	m, err := Load()
	require.NoError(t, err, "the checked-in parity.yaml must load")
	require.NotNil(t, m)
	assert.Equal(t, 1, m.Version)
	require.NotEmpty(t, m.Exceptions)

	// Spot-check the seeded TRUE limits. Only genuinely API-impossible gaps are recorded; the GitLab
	// PRMetrics gaps are buildable gaps, not limits, so they are
	// intentionally absent here.
	assert.True(t, m.IsException("model.PullRequest.IsDraft", "bitbucket"), "Bitbucket no-draft limit must be recorded")
	assert.True(t, m.IsException("reviewerIsBot", "bitbucket"), "Bitbucket no-bot-flag limit must be recorded")
	// Negatives: GitHub is exempt from nothing, and the GitLab files_changed gap
	// is a buildable gap, not a recorded limit.
	assert.False(t, m.IsException("model.PullRequest.IsDraft", "github"), "GitHub is not exempt from draft state")
	assert.False(t, m.IsException("PRMetrics.FilesChanged", "gitlab"), "GitLab files_changed is a build item, not a recorded limit")

	// The seed is behavioral-only today (the only true limits are Bitbucket's);
	// struct-field and dispatch remain valid kinds awaiting a genuine limit.
	kinds := map[string]int{}
	for _, e := range m.Exceptions {
		kinds[e.Kind]++
	}
	assert.Positive(t, kinds[KindBehavioral], "expect the behavioral true limits")
}

// TestIsExceptionOfKind confirms the kind-scoped lookup: a behavioral record is
// matched only when queried as behavioral, never as struct-field/dispatch — so a
// doc-only entry can never clear a struct-field or dispatch drift.
func TestIsExceptionOfKind(t *testing.T) {
	t.Parallel()
	m, err := Load()
	require.NoError(t, err)
	assert.True(t, m.IsExceptionOfKind(KindBehavioral, "model.PullRequest.IsDraft", "bitbucket"),
		"behavioral limit matches when queried as behavioral")
	assert.False(t, m.IsExceptionOfKind(KindStructField, "model.PullRequest.IsDraft", "bitbucket"),
		"a behavioral record must NOT clear a struct-field drift")
	assert.False(t, m.IsExceptionOfKind(KindDispatch, "model.PullRequest.IsDraft", "bitbucket"),
		"a behavioral record must NOT clear a dispatch drift")
}

// TestParse_FailsClosed is the load-closed contract: a malformed, empty, or
// under-specified manifest is a hard error, never a silent empty exception set
// (which would disable the parity guards).
func TestParse_FailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty file", raw: ""},
		{name: "whitespace only", raw: "   \n"},
		{name: "malformed yaml", raw: "version: 1\nexceptions: [oops"},
		{name: "no version", raw: "exceptions:\n  - kind: struct-field\n    symbol: X\n    absent_for: [gitlab]\n    reason: r\n"},
		{name: "no exceptions", raw: "version: 1\nexceptions: []\n"},
		{name: "entry missing kind", raw: "version: 1\nexceptions:\n  - symbol: X\n    absent_for: [gitlab]\n    reason: r\n"},
		{name: "entry unknown kind", raw: "version: 1\nexceptions:\n  - kind: nonsense\n    symbol: X\n    absent_for: [gitlab]\n    reason: r\n"},
		{name: "entry missing symbol", raw: "version: 1\nexceptions:\n  - kind: struct-field\n    absent_for: [gitlab]\n    reason: r\n"},
		{name: "entry missing absent_for", raw: "version: 1\nexceptions:\n  - kind: struct-field\n    symbol: X\n    reason: r\n"},
		{name: "entry missing reason", raw: "version: 1\nexceptions:\n  - kind: struct-field\n    symbol: X\n    absent_for: [gitlab]\n"},
		{name: "unknown vendor", raw: "version: 1\nexceptions:\n  - kind: struct-field\n    symbol: X\n    absent_for: [gitlub]\n    reason: r\n"},
		{name: "stray unknown field", raw: "version: 1\nexceptions:\n  - kind: struct-field\n    symbol: X\n    absent_for: [gitlab]\n    reason: r\n    typo_field: oops\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m, err := parse([]byte(tt.raw))
			require.Error(t, err, "%s must be a hard error", tt.name)
			assert.Nil(t, m, "%s must not return a partial manifest", tt.name)
		})
	}
}

// TestParse_Valid confirms a well-formed manifest parses and IsException works.
func TestParse_Valid(t *testing.T) {
	t.Parallel()
	raw := "version: 1\n" +
		"exceptions:\n" +
		"  - kind: struct-field\n" +
		"    symbol: Foo.Bar\n" +
		"    absent_for: [gitlab, bitbucket]\n" +
		"    reason: a genuine limit\n"
	m, err := parse([]byte(raw))
	require.NoError(t, err)
	assert.True(t, m.IsException("Foo.Bar", "gitlab"))
	assert.True(t, m.IsException("Foo.Bar", "bitbucket"))
	assert.False(t, m.IsException("Foo.Bar", "github"))
	assert.False(t, m.IsException("Other", "gitlab"))
}

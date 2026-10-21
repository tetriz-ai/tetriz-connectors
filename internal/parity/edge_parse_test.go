// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package parity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const edgeValidDoc = "version: 1\nexceptions:\n  - kind: behavioral\n    symbol: X\n    absent_for: [gitlab]\n    reason: r\n"

// parse fails closed "on any problem". A second YAML document carrying a typo is a
// problem: decoding only the first document would load clean and silently ignore it.
func TestEdge_Parse_SecondDocumentIsNotIgnored(t *testing.T) {
	t.Parallel()
	raw := edgeValidDoc + "---\nversion: 1\nexceptions:\n  - kind: behavioral\n    symbol: Y\n    absent_for: [gitlub]\n    reason: r\n"
	m, err := parse([]byte(raw))
	require.Error(t, err, "a multi-document manifest must not load only its first document")
	assert.Nil(t, m)
}

// A duplicated key (two `symbol:` lines) is a hard error, not last-one-wins.
func TestEdge_Parse_DuplicateKeyFails(t *testing.T) {
	t.Parallel()
	raw := "version: 1\nexceptions:\n  - kind: behavioral\n    symbol: X\n    symbol: Y\n    absent_for: [gitlab]\n    reason: r\n"
	_, err := parse([]byte(raw))
	require.Error(t, err)
}

// A null list entry decodes to a zero Exception and must be rejected.
func TestEdge_Parse_NullEntryFails(t *testing.T) {
	t.Parallel()
	_, err := parse([]byte("version: 1\nexceptions:\n  - ~\n"))
	require.Error(t, err)
}

// Vendor names are matched exactly; "GitHub" is not "github".
func TestEdge_Parse_VendorCaseIsStrict(t *testing.T) {
	t.Parallel()
	_, err := parse([]byte("version: 1\nexceptions:\n  - kind: behavioral\n    symbol: X\n    absent_for: [GitLab]\n    reason: r\n"))
	require.Error(t, err)
}

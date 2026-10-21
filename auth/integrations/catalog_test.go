// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestEveryConnectorConstantHasACatalogEntry keeps the product catalog and the connector ids in
// step, in the direction that fails silently.
//
// A catalog row is what a host's UI renders and what the connect RPC is asked for. An id in
// one place and not the other does not fail to build: the tile renders with an id nothing
// serves, so the RPC refuses a connector the operator can see on screen.
//
// The list is derived from this package's own connector-id constants rather than restated, so a
// connector that gains a constant here is covered without anyone remembering to extend a list.
func TestEveryConnectorConstantHasACatalogEntry(t *testing.T) {
	t.Parallel()

	inCatalog := map[string]bool{}
	for _, cat := range Catalog().GetCategories() {
		for _, integ := range cat.GetIntegrations() {
			inCatalog[integ.GetId()] = true
		}
	}
	require.NotEmpty(t, inCatalog, "the catalog must have entries, or this gate checks nothing")

	// Every connector-id constant this package declares. Adding a constant without adding it
	// here is the one gap left, and it is a compile-adjacent edit in the same file.
	for _, id := range []sdk.ConnectorID{
		connectorIDClaudeCode,
		connectorIDCodex,
		connectorIDGitHub,
		connectorIDGitLab,
		connectorIDBitbucket,
	} {
		assert.Truef(t, inCatalog[string(id)],
			"connector %q has a constant here but no catalog entry — the tile and the connect RPC disagree", id)
	}
}

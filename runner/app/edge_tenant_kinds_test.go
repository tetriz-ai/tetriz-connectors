// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Comments (indented too), blank lines, CRLF line endings, tabs and the
// UUID's other spellings all load, each id under its canonical form.
func TestEdge_TenantKindsFileTolerantShapes(t *testing.T) {
	p := writeKindsFile(t, "# header\r\n\r\n   # indented comment\r\n"+
		"\tAAAAAAAA-0000-4000-8000-0000000000E1\t individual \r\n"+
		"urn:uuid:aaaaaaaa-0000-4000-8000-0000000000e2 organization\r\n"+
		"{aaaaaaaa-0000-4000-8000-0000000000e3} individual\r\n"+
		"aaaaaaaa000040008000000000000e40 individual") // no trailing newline, no dashes
	kinds, err := LoadTenantKinds(p)
	require.NoError(t, err)
	assert.Equal(t, map[string]sdk.TenantKind{
		"aaaaaaaa-0000-4000-8000-0000000000e1": sdk.TenantKindIndividual,
		"aaaaaaaa-0000-4000-8000-0000000000e2": sdk.TenantKindOrganization,
		"aaaaaaaa-0000-4000-8000-0000000000e3": sdk.TenantKindIndividual,
		"aaaaaaaa-0000-4000-8000-000000000e40": sdk.TenantKindIndividual,
	}, kinds)
}

// An empty file is a valid file naming no organization.
func TestEdge_TenantKindsFileEmpty(t *testing.T) {
	kinds, err := LoadTenantKinds(writeKindsFile(t, ""))
	require.NoError(t, err)
	assert.Empty(t, kinds)
}

// A malformed line fails the whole load, naming the file and the line, so a
// typo cannot silently leave an individual scheduled as an organization.
func TestEdge_TenantKindsFileMalformedLines(t *testing.T) {
	const id = "aaaaaaaa-0000-4000-8000-0000000000e9"
	for _, tc := range []struct{ name, body, want string }{
		{"trailing comment", "# ok\n" + id + " individual # me\n", ":2: want an organization id and a kind"},
		{"missing kind", "\n" + id + "\n", ":2: want an organization id and a kind"},
		{"capitalised kind", id + " Individual\n", ":1: tenant kind must be organization or individual"},
		{"unknown kind", id + " team\n", ":1: tenant kind must be organization or individual"},
		{"comma separated", id + ",individual\n", ":1: want an organization id and a kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := writeKindsFile(t, tc.body)
			_, err := LoadTenantKinds(p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), p+tc.want)
		})
	}
}

// With an individual default, an organization the file does not name
// resolves individual, one the file names organization stays organization,
// and only the named individuals are published to the live set.
func TestEdge_IndividualDefaultWithAnOrganizationOverride(t *testing.T) {
	t.Cleanup(func() { tenantlabel.SetLiveIndividualSet(nil) })
	p := writeKindsFile(t, orgA+" organization\n"+orgB+" individual\n")
	d, err := NewTenantDirectory(Config{DefaultTenantKind: sdk.TenantKindIndividual, TenantKindsFile: p})
	require.NoError(t, err)
	ctx := context.Background()
	for org, want := range map[string]sdk.TenantKind{
		orgA:                                   sdk.TenantKindOrganization,
		orgB:                                   sdk.TenantKindIndividual,
		"00000000-0000-4000-8000-00000000c003": sdk.TenantKindIndividual,
	} {
		got, err := d.TenantType(ctx, org)
		require.NoError(t, err)
		assert.Equal(t, want, got, org)
	}
	assert.Equal(t, map[string]struct{}{orgB: {}}, d.Individuals())
}

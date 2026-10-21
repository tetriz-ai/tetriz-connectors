// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// An organization listed in upper case resolves under its canonical id.
func TestTenantKindsFileCanonicalizesTheOrg(t *testing.T) {
	p := filepath.Join(t.TempDir(), "kinds")
	require.NoError(t, os.WriteFile(p, []byte("AAAAAAAA-0000-4000-8000-0000000000F3 individual\n"), 0o600))
	d, err := NewTenantDirectory(Config{TenantKindsFile: p})
	require.NoError(t, err)
	k, err := d.TenantType(context.Background(), "aaaaaaaa-0000-4000-8000-0000000000f3")
	require.NoError(t, err)
	assert.Equal(t, sdk.TenantKindIndividual, k, "the listed individual org resolves as individual")
}

// A line whose organization id is not a UUID fails the load, naming the file
// and the line.
func TestTenantKindsFileRejectsANonUUIDOrg(t *testing.T) {
	p := filepath.Join(t.TempDir(), "kinds")
	require.NoError(t, os.WriteFile(p, []byte("# kinds\nnot-a-uuid individual\n"), 0o600))
	_, err := NewTenantDirectory(Config{TenantKindsFile: p})
	require.Error(t, err)
	assert.Contains(t, err.Error(), p+":2")
	assert.Contains(t, err.Error(), "not a UUID")
}

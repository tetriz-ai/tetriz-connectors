// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"crypto/rand"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tenant_isolation policies bind the tables' owner too, so a runner that
// connects as the owner sees only the tenant it scoped to. A superuser or a
// BYPASSRLS role still bypasses row-level security; the test takes the owner
// role that the migrations would run as.
func TestTableOwnerSeesOnlyItsTenant(t *testing.T) {
	if testDB == nil {
		t.Skip("no test database")
	}
	tx := newTestTx(t)

	var rls []string
	require.NoError(t, tx.Raw(`SELECT relname FROM pg_class WHERE relrowsecurity AND relnamespace = 'public'::regnamespace ORDER BY relname`).Scan(&rls).Error)
	require.NotEmpty(t, rls)
	for _, table := range rls {
		var forced bool
		require.NoError(t, tx.Raw(`SELECT relforcerowsecurity FROM pg_class WHERE relname = ? AND relnamespace = 'public'::regnamespace`, table).Scan(&forced).Error)
		assert.True(t, forced, "%s forces row-level security on its owner", table)
	}

	var super bool
	require.NoError(t, tx.Raw(`SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user`).Scan(&super).Error)
	if super {
		// Take a plain owner role for this transaction; the rollback drops it.
		var b [4]byte
		_, _ = rand.Read(b[:])
		owner := "rls_owner_" + hex.EncodeToString(b[:])
		require.NoError(t, tx.Exec("CREATE ROLE "+owner+" NOSUPERUSER NOBYPASSRLS").Error)
		require.NoError(t, tx.Exec("ALTER TABLE connector_configs OWNER TO "+owner).Error)
		require.NoError(t, tx.Exec("SET LOCAL ROLE "+owner).Error)
	}
	const orgA, orgB = "00000000-0000-4000-8000-0000000f0a01", "00000000-0000-4000-8000-0000000f0b01"
	require.NoError(t, tx.Exec("SET LOCAL app.current_tenant = '__system__'").Error)
	require.NoError(t, tx.Exec(`INSERT INTO connector_configs (connector_id, organization_id) VALUES ('github', ?), ('github', ?)`, orgA, orgB).Error)

	require.NoError(t, tx.Exec("SET LOCAL app.current_tenant = '"+orgA+"'").Error)
	var visible []string
	require.NoError(t, tx.Raw(`SELECT organization_id FROM connector_configs WHERE organization_id IN (?, ?)`, orgA, orgB).Scan(&visible).Error)
	assert.Equal(t, []string{orgA}, visible, "tenant a sees its own row and not tenant b's")

	require.NoError(t, tx.Exec("SET LOCAL app.current_tenant = ''").Error)
	var unscoped int64
	require.NoError(t, tx.Raw(`SELECT count(*) FROM connector_configs WHERE organization_id IN (?, ?)`, orgA, orgB).Scan(&unscoped).Error)
	assert.Zero(t, unscoped, "without a tenant scope the owner sees no rows")
}

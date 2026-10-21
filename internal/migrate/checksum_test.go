// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package migrate_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
)

const checksumFile = "000001_create_t.up.sql"

func checksumFS(sql string) fstest.MapFS {
	return fstest.MapFS{checksumFile: &fstest.MapFile{Data: []byte(sql)}}
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func storedChecksum(t *testing.T, db *sql.DB) sql.NullString {
	t.Helper()
	var sum sql.NullString
	require.NoError(t, db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version = 1`).Scan(&sum))
	return sum
}

const createT = `CREATE TABLE IF NOT EXISTS t (id int);`

// An applied migration records its file's SHA-256, and a run over the same,
// unchanged file passes and applies nothing.
func TestUnchangedAppliedMigrationPasses(t *testing.T) {
	db := emptyDB(t)
	ctx := context.Background()
	_, err := migrate.Up(ctx, db, checksumFS(createT))
	require.NoError(t, err)
	assert.Equal(t, sha(createT), storedChecksum(t, db).String)
	applied, err := migrate.Up(ctx, db, checksumFS(createT))
	require.NoError(t, err)
	assert.Empty(t, applied)
}

// An applied migration whose file was edited afterwards is refused, naming
// the migration, instead of silently no-oping.
func TestEditedAppliedMigrationIsRefused(t *testing.T) {
	db := emptyDB(t)
	ctx := context.Background()
	_, err := migrate.Up(ctx, db, checksumFS(createT))
	require.NoError(t, err)
	_, err = migrate.Up(ctx, db, checksumFS(createT+"\nALTER TABLE t ADD COLUMN IF NOT EXISTS name text;"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), checksumFile)
	assert.Contains(t, err.Error(), "an applied migration changed; add a new numbered migration (before v0.1.0, reset a local stack with docker compose down -v)")
}

// A version table from before checksums existed (no checksum column, or a
// NULL in it) gets the column and the applied file's checksum, not a refusal.
func TestNullChecksumIsFilledIn(t *testing.T) {
	db := emptyDB(t)
	ctx := context.Background()
	_, err := db.Exec(createT + `
CREATE TABLE schema_migrations (version bigint PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
INSERT INTO schema_migrations (version) VALUES (1);`)
	require.NoError(t, err)
	applied, err := migrate.Up(ctx, db, checksumFS(createT))
	require.NoError(t, err)
	assert.Empty(t, applied)
	assert.Equal(t, sha(createT), storedChecksum(t, db).String, "the NULL checksum is filled in")
	_, err = migrate.Up(ctx, db, checksumFS(createT+" "))
	require.Error(t, err, "once filled in, the checksum guards the file")
}

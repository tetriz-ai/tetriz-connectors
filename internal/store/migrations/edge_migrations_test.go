// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package migrations_test

import (
	"context"
	"io/fs"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
	"github.com/tetriz-ai/tetriz-connectors/internal/store/migrations"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// The package had no tests, so it had no guard; this file adds one.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// migrated returns a fresh database with every up migration applied.
func migrated(t *testing.T) *gorm.DB {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		if v := os.Getenv(testdb.RequireEnv); v != "" && v != "0" {
			t.Fatalf("no test database: %v", err)
		}
		t.Skipf("no test database: %v", err)
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return db
}

func publicTables(t *testing.T, db *gorm.DB) []string {
	t.Helper()
	var out []string
	require.NoError(t, db.Raw(`SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`).Scan(&out).Error)
	return out
}

// Every up file has a down file of the same number, and the reverse.
func TestEdge_EveryUpHasADown(t *testing.T) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	require.NoError(t, err)
	ups, downs := map[string]bool{}, map[string]bool{}
	for _, e := range entries {
		n := e.Name()
		switch {
		case strings.HasSuffix(n, ".up.sql"):
			ups[strings.TrimSuffix(n, ".up.sql")] = true
		case strings.HasSuffix(n, ".down.sql"):
			downs[strings.TrimSuffix(n, ".down.sql")] = true
		}
	}
	assert.Equal(t, ups, downs)
}

// migrate's package doc asks for migrations whose statements are idempotent,
// so one that fails part way can be retried after the fix: the retry re-runs
// the statements that already landed. Each up must therefore run cleanly a
// second time on a schema that already has it.
func TestEdge_EveryUpMigrationCanBeReRun(t *testing.T) {
	db := migrated(t)
	migs, err := migrate.Load(migrations.FS)
	require.NoError(t, err)
	for _, m := range migs {
		t.Run(m.Name, func(t *testing.T) {
			assert.NoError(t, db.Exec(m.SQL).Error, "re-running %s", m.Name)
		})
	}
}

// Applying the downs newest first removes everything the ups made, and the
// ups then apply again from scratch.
func TestEdge_DownsReverseTheUps(t *testing.T) {
	db := migrated(t)
	entries, err := fs.ReadDir(migrations.FS, ".")
	require.NoError(t, err)
	var downs []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".down.sql") {
			downs = append(downs, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(downs)))
	for _, name := range downs {
		b, err := fs.ReadFile(migrations.FS, name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(b)).Error, "applying %s", name)
	}
	assert.Equal(t, []string{"schema_migrations"}, publicTables(t, db), "the downs leave only the version table")
	var policies int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM pg_policies WHERE schemaname = 'public'`).Scan(&policies).Error)
	assert.Zero(t, policies)

	require.NoError(t, db.Exec(`DELETE FROM schema_migrations`).Error)
	raw, err := db.DB()
	require.NoError(t, err)
	applied, err := migrate.Up(context.Background(), raw, migrations.FS)
	require.NoError(t, err, "the ups apply again after the downs")
	assert.NotEmpty(t, applied)
}

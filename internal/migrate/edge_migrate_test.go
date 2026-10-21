// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package migrate_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
	"github.com/tetriz-ai/tetriz-connectors/internal/store/migrations"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// The package had no tests, so it had no guard; this file adds one.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// emptyDB creates an empty database on the test server and returns a handle.
func emptyDB(t *testing.T) *sql.DB {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		if v := os.Getenv(testdb.RequireEnv); v != "" && v != "0" {
			t.Fatalf("no test database: %v", err)
		}
		t.Skipf("no test database: %v", err)
	}
	server := os.Getenv(testdb.DSNEnv)
	if server == "" {
		server = "host=localhost port=55432 user=tetriz password=tetriz-local-only dbname=postgres sslmode=disable"
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "tetriz_edge_migrate_" + hex.EncodeToString(b[:])
	admin, err := sql.Open("pgx", server)
	require.NoError(t, err)
	_, err = admin.Exec("CREATE DATABASE " + name)
	require.NoError(t, err)
	parts := strings.Fields(server)
	kept := parts[:0]
	for _, p := range parts {
		if !strings.HasPrefix(p, "dbname=") {
			kept = append(kept, p)
		}
	}
	db, err := sql.Open("pgx", strings.Join(append(kept, "dbname="+name), " "))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		_ = admin.Close()
	})
	return db
}

func recorded(t *testing.T, db *sql.DB) []int64 {
	t.Helper()
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck // test read
	var out []int64
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		out = append(out, v)
	}
	return out
}

// Load rejects names it cannot order and ignores what is not an up file.
func TestEdge_LoadRejectsMalformedNames(t *testing.T) {
	ok := &fstest.MapFile{Data: []byte("SELECT 1;")}
	for name, fsys := range map[string]fstest.MapFS{
		"no version prefix":          {"create.up.sql": ok},
		"non-numeric version":        {"v1_create.up.sql": ok},
		"two files share a version":  {"1_a.up.sql": ok, "000001_b.up.sql": ok},
		"version overflows an int64": {"99999999999999999999_a.up.sql": ok},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := migrate.Load(fsys)
			assert.Error(t, err)
		})
	}

	migs, err := migrate.Load(fstest.MapFS{
		"000010_b.up.sql":     ok,
		"000002_a.up.sql":     ok,
		"000002_a.down.sql":   ok,
		"README.md":           ok,
		"sub/000001_x.up.sql": ok,
	})
	require.NoError(t, err)
	require.Len(t, migs, 2, "down files, other files and subdirectories are not migrations")
	assert.Equal(t, []int64{2, 10}, []int64{migs[0].Version, migs[1].Version}, "ordered numerically, not lexically")
}

// The version table name is interpolated into SQL, so anything but a plain
// lower-case identifier is refused before a statement runs.
func TestEdge_UpTableRefusesUnsafeTableNames(t *testing.T) {
	for _, name := range []string{"", "Schema", "schema-migrations", "x; DROP TABLE y", "a.b", `"q"`, "naïve"} {
		_, err := migrate.UpTable(context.Background(), nil, fstest.MapFS{}, name)
		assert.Error(t, err, "%q", name)
	}
}

// Applying the store's migrations twice is a no-op the second time.
func TestEdge_UpIsIdempotent(t *testing.T) {
	db := emptyDB(t)
	first, err := migrate.Up(context.Background(), db, migrations.FS)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	second, err := migrate.Up(context.Background(), db, migrations.FS)
	require.NoError(t, err)
	assert.Empty(t, second)
	assert.Equal(t, first, recorded(t, db))
}

// A migration that fails is not recorded; the ones before it are, and after
// the fix the next Up applies only the fixed one.
func TestEdge_FailedMigrationIsRetriedAfterTheFix(t *testing.T) {
	db := emptyDB(t)
	fsys := fstest.MapFS{
		"000001_a.up.sql": {Data: []byte("CREATE TABLE IF NOT EXISTS edge_a (id int);")},
		"000002_b.up.sql": {Data: []byte("CREATE TABLE edge_b (id int); SELECT * FROM missing_table;")},
	}
	applied, err := migrate.Up(context.Background(), db, fsys)
	require.Error(t, err)
	assert.Equal(t, []int64{1}, applied)
	assert.Equal(t, []int64{1}, recorded(t, db))

	fsys["000002_b.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE IF NOT EXISTS edge_b (id int);")}
	applied, err = migrate.Up(context.Background(), db, fsys)
	require.NoError(t, err)
	assert.Equal(t, []int64{2}, applied)
}

// `serve` applies the migrations on start, and several instances can start
// at once against one database. Concurrent Ups on a fresh database must all
// succeed and record each version once.
func TestEdge_ConcurrentUpsOnAFreshDatabase(t *testing.T) {
	db := emptyDB(t)
	const instances = 4
	var wg sync.WaitGroup
	errs := make([]error, instances)
	start := make(chan struct{})
	for i := range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = migrate.Up(context.Background(), db, migrations.FS)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		assert.NoError(t, err, "instance %d", i)
	}
	all, err := migrate.Load(migrations.FS)
	require.NoError(t, err)
	want := make([]int64, 0, len(all))
	for _, m := range all {
		want = append(want, m.Version)
	}
	assert.Equal(t, want, recorded(t, db), fmt.Sprintf("each of the %d versions recorded once", len(want)))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package testdb gives store tests a fresh Postgres database per test
// package, with the schema's migrations applied.
//
// The server comes from $TETRIZ_TEST_POSTGRES_DSN (a libpq keyword DSN for a
// role that may create databases). Without it the default is the local
// docker-compose Postgres. An unreachable server skips the suite, unless
// $TETRIZ_REQUIRE_TEST_DB is set, which turns the skip into a failure.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
	simmigrations "github.com/tetriz-ai/tetriz-connectors/internal/simulator/migrations"
	connmigrations "github.com/tetriz-ai/tetriz-connectors/internal/store/migrations"
)

const (
	// DSNEnv names the server DSN variable.
	DSNEnv = "TETRIZ_TEST_POSTGRES_DSN"
	// RequireEnv turns an unreachable server into a failure.
	RequireEnv = "TETRIZ_REQUIRE_TEST_DB"
	// defaultDSN matches the docker-compose Postgres (local-only password).
	defaultDSN = "host=localhost port=55432 user=tetriz password=tetriz-local-only dbname=postgres sslmode=disable"
	// rlsOptions sets the session to the system tenant, so a test that does
	// not open a tenant scope sees every row, and pins the time zone to UTC so
	// date truncation matches production.
	rlsOptions = "options='-c TimeZone=UTC -c app.current_tenant=__system__'"
)

// Schema names a set of migrations.
type Schema string

const (
	// Connector is the runner store schema.
	Connector Schema = "connector"
	// Simulator is the simulator's schema.
	Simulator Schema = "simulator"
)

// dropTimeout bounds a test database's cleanup. A drop that waits on a busy
// server would otherwise give up and leave the database behind.
const dropTimeout = 2 * time.Minute

// dropOrLog runs one cleanup statement and reports a failure on stderr, so a
// database or role a test leaves behind is visible.
func dropOrLog(ctx context.Context, admin *sql.DB, stmt string) {
	if _, err := admin.ExecContext(ctx, stmt); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: cleanup %q failed: %v\n", stmt, err)
	}
}

func (s Schema) migrations() (fs.FS, error) {
	switch s {
	case Connector:
		return connmigrations.FS, nil
	case Simulator:
		return simmigrations.FS, nil
	default:
		return nil, fmt.Errorf("testdb: unknown schema %q", s)
	}
}

func serverDSN() string {
	if v := os.Getenv(DSNEnv); v != "" {
		return v
	}
	return defaultDSN
}

// withDB replaces or appends the dbname keyword of a keyword DSN.
func withDB(dsn, name string) string {
	parts := strings.Fields(dsn)
	out := parts[:0]
	for _, p := range parts {
		if !strings.HasPrefix(p, "dbname=") {
			out = append(out, p)
		}
	}
	return strings.Join(append(out, "dbname="+name), " ")
}

// Ping reports whether the test server is reachable.
func Ping() error {
	db, err := sql.Open("pgx", serverDSN())
	if err != nil {
		return fmt.Errorf("testdb: open: %w", err)
	}
	defer db.Close() //nolint:errcheck // best-effort close of a probe connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("testdb: ping %s: %w", DSNEnv, err)
	}
	return nil
}

// SkipOrFail reports an unreachable server for suite. With RequireEnv set it
// exits 1; otherwise it prints that the suite ran no tests and returns.
func SkipOrFail(suite string, err error) {
	if v := os.Getenv(RequireEnv); v != "" && v != "0" {
		fmt.Fprintf(os.Stderr, "FAIL %s tests: 0 tests ran; %s is set but no test database is reachable: %v\n", suite, RequireEnv, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "SKIP %s tests: 0 tests ran; no test database reachable: %v (set %s=1 to fail instead)\n", suite, err, RequireEnv)
}

// Setup creates a new database, applies the schema's migrations, and returns
// a gorm handle, its DSN, and a cleanup that drops the database.
func Setup(ctx context.Context, schema Schema) (*gorm.DB, string, func(), error) {
	migs, err := schema.migrations()
	if err != nil {
		return nil, "", nil, err
	}
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, "", nil, fmt.Errorf("testdb: name: %w", err)
	}
	name := "tetriz_test_" + string(schema) + "_" + hex.EncodeToString(suffix[:])
	admin, err := sql.Open("pgx", serverDSN())
	if err != nil {
		return nil, "", nil, fmt.Errorf("testdb: open server: %w", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		_ = admin.Close()
		return nil, "", nil, fmt.Errorf("testdb: create %s: %w", name, err)
	}
	drop := func() {
		dctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
		defer cancel()
		dropOrLog(dctx, admin, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close()
	}
	dsn := withDB(serverDSN(), name) + " " + rlsOptions
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		drop()
		return nil, "", nil, fmt.Errorf("testdb: open %s: %w", name, err)
	}
	table := "schema_migrations"
	if schema == Simulator {
		table = "simulator_schema_migrations"
	}
	if _, err := migrate.UpTable(ctx, raw, migs, table); err != nil {
		_ = raw.Close()
		drop()
		return nil, "", nil, fmt.Errorf("testdb: migrate %s: %w", name, err)
	}
	gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: raw}), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		_ = raw.Close()
		drop()
		return nil, "", nil, fmt.Errorf("testdb: gorm %s: %w", name, err)
	}
	cleanup := func() {
		_ = raw.Close()
		drop()
	}
	return gdb, dsn, cleanup, nil
}

// withKey replaces or appends one keyword of a keyword DSN.
func withKey(dsn, key, value string) string {
	parts := strings.Fields(dsn)
	out := parts[:0]
	for _, p := range parts {
		if !strings.HasPrefix(p, key+"=") {
			out = append(out, p)
		}
	}
	return strings.Join(append(out, key+"="+value), " ")
}

// SetupAsOwner is Setup for the deployment the README recommends: a new
// NOSUPERUSER NOBYPASSRLS role owns the database, runs the migrations and is
// the one the returned handle and DSN connect as. The session sets no tenant,
// so a query outside a tenant scope sees no rows. The server role must be
// able to create roles.
func SetupAsOwner(ctx context.Context, schema Schema) (*gorm.DB, string, func(), error) {
	migs, err := schema.migrations()
	if err != nil {
		return nil, "", nil, err
	}
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return nil, "", nil, fmt.Errorf("testdb: name: %w", err)
	}
	name := "tetriz_owner_" + string(schema) + "_" + hex.EncodeToString(suffix[:])
	password := hex.EncodeToString(suffix[:]) + "-local-only"
	admin, err := sql.Open("pgx", serverDSN())
	if err != nil {
		return nil, "", nil, fmt.Errorf("testdb: open server: %w", err)
	}
	for _, stmt := range []string{
		"CREATE ROLE " + name + " LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '" + password + "'",
		"CREATE DATABASE " + name + " OWNER " + name,
	} {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			_ = admin.Close()
			return nil, "", nil, fmt.Errorf("testdb: %s: %w", strings.Fields(stmt)[1], err)
		}
	}
	drop := func() {
		dctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
		defer cancel()
		dropOrLog(dctx, admin, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		dropOrLog(dctx, admin, "DROP ROLE IF EXISTS "+name)
		_ = admin.Close()
	}
	dsn := withKey(withKey(withDB(serverDSN(), name), "user", name), "password", password) + " options='-c TimeZone=UTC'"
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		drop()
		return nil, "", nil, fmt.Errorf("testdb: open %s: %w", name, err)
	}
	table := "schema_migrations"
	if schema == Simulator {
		table = "simulator_schema_migrations"
	}
	if _, err := migrate.UpTable(ctx, raw, migs, table); err != nil {
		_ = raw.Close()
		drop()
		return nil, "", nil, fmt.Errorf("testdb: migrate %s: %w", name, err)
	}
	gdb, err := gorm.Open(postgres.New(postgres.Config{Conn: raw}), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		_ = raw.Close()
		drop()
		return nil, "", nil, fmt.Errorf("testdb: gorm %s: %w", name, err)
	}
	return gdb, dsn, func() { _ = raw.Close(); drop() }, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package migrate applies numbered SQL migrations to a Postgres database.
//
// Files are named NNNNNN_<anything>.up.sql. Applied versions are recorded in
// schema_migrations, one row per version, with the SHA-256 of the file that
// was applied. Every run compares each applied migration's file with its
// recorded checksum and refuses to go on when one changed, so an edited
// migration cannot silently no-op; a row recorded before checksums existed
// (NULL) gets its checksum filled in. A migration runs as one
// multi-statement exec outside a transaction, because some contain CREATE
// INDEX CONCURRENTLY; its version is recorded only after it succeeds. A run
// holds a Postgres advisory lock (keyed by the version table) on one
// connection, so concurrent runs against one database apply in turn. A
// migration that fails part way leaves its earlier statements applied and its
// version unrecorded, so write migrations whose statements are idempotent
// (IF NOT EXISTS) and it can be retried after the fix.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Migration is one up migration.
type Migration struct {
	Version int64
	Name    string
	SQL     string
}

// Checksum is the hex SHA-256 of the migration's file, as recorded when it is
// applied.
func (m Migration) Checksum() string {
	sum := sha256.Sum256([]byte(m.SQL))
	return hex.EncodeToString(sum[:])
}

// Load reads the up migrations from fsys, sorted by version.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("migrate: read migrations: %w", err)
	}
	var out []Migration
	seen := map[int64]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		num, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migrate: %s: no version prefix", name)
		}
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migrate: %s: version %q is not a number", name, num)
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrate: %s and %s share version %d", prev, name, v)
		}
		seen[v] = name
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", name, err)
		}
		out = append(out, Migration{Version: v, Name: name, SQL: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Up applies every migration in fsys newer than the latest version recorded
// in schema_migrations and returns the versions it applied.
func Up(ctx context.Context, db *sql.DB, fsys fs.FS) ([]int64, error) {
	return UpTable(ctx, db, fsys, "schema_migrations")
}

// UpTable is Up with the version table named, so two migration sets can
// share one database.
func UpTable(ctx context.Context, db *sql.DB, fsys fs.FS, table string) ([]int64, error) {
	if !validTable(table) {
		return nil, fmt.Errorf("migrate: invalid table name %q", table)
	}
	migs, err := Load(fsys)
	if err != nil {
		return nil, err
	}
	// One connection holds a session advisory lock for the whole run, so
	// instances starting at once against one database apply the migrations
	// one after another; each reads the recorded version only once it holds
	// the lock.
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: acquire connection: %w", err)
	}
	// Closing returns the connection to the pool; a failed unlock is then
	// released when the session ends.
	defer func() { _ = conn.Close() }()
	if err := lock(ctx, conn, lockPrefix+table); err != nil {
		return nil, fmt.Errorf("migrate: lock %s: %w", table, err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, lockPrefix+table)
	}()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (
		version bigint PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return nil, fmt.Errorf("migrate: create %s: %w", table, err)
	}
	// A table created before checksums were recorded gains the column.
	if _, err := conn.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN IF NOT EXISTS checksum text`); err != nil {
		return nil, fmt.Errorf("migrate: add checksum to %s: %w", table, err)
	}
	if err := verify(ctx, conn, table, migs); err != nil {
		return nil, err
	}
	var current sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT max(version) FROM `+table).Scan(&current); err != nil {
		return nil, fmt.Errorf("migrate: read version: %w", err)
	}
	var applied []int64
	for _, m := range migs {
		if current.Valid && m.Version <= current.Int64 {
			continue
		}
		if err := apply(ctx, conn, table, m); err != nil {
			return applied, err
		}
		applied = append(applied, m.Version)
	}
	return applied, nil
}

// lock takes the session advisory lock for key, polling with
// pg_try_advisory_lock. A waiter blocked inside pg_advisory_lock would sit in
// an open statement, and the holder's CREATE INDEX CONCURRENTLY waits for every
// open transaction to finish: the two would deadlock.
func lock(ctx context.Context, conn *sql.Conn, key string) error {
	for {
		var got bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&got); err != nil {
			return fmt.Errorf("try advisory lock: %w", err)
		}
		if got {
			return nil
		}
		t := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("wait for advisory lock: %w", ctx.Err())
		case <-t.C:
		}
	}
}

// lockPollInterval is how often a waiting run retries the lock.
const lockPollInterval = 100 * time.Millisecond

// lockPrefix namespaces the advisory lock key, which is derived from the
// version table's name.
const lockPrefix = "tetriz-migrate:"

func apply(ctx context.Context, conn *sql.Conn, table string, m Migration) error {
	if _, err := conn.ExecContext(ctx, m.SQL); err != nil {
		return fmt.Errorf("migrate: apply %s: %w", m.Name, err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO `+table+` (version, checksum) VALUES ($1, $2)`, m.Version, m.Checksum()); err != nil {
		return fmt.Errorf("migrate: record %s: %w", m.Name, err)
	}
	return nil
}

// ErrChanged is wrapped by the error verify returns when an applied
// migration's file no longer matches its recorded checksum.
var ErrChanged = errors.New("an applied migration changed; add a new numbered migration (before v0.1.0, reset a local stack with docker compose down -v)")

// verify compares each applied migration's file with the checksum recorded
// when it was applied. A mismatch is refused, naming the migration. A row
// with no checksum (recorded before checksums existed) gets the file's
// checksum recorded. An applied version with no file is left alone.
func verify(ctx context.Context, conn *sql.Conn, table string, migs []Migration) error {
	rows, err := conn.QueryContext(ctx, `SELECT version, checksum FROM `+table)
	if err != nil {
		return fmt.Errorf("migrate: read %s: %w", table, err)
	}
	stored := map[int64]sql.NullString{}
	for rows.Next() {
		var v int64
		var sum sql.NullString
		if err := rows.Scan(&v, &sum); err != nil {
			_ = rows.Close()
			return fmt.Errorf("migrate: read %s: %w", table, err)
		}
		stored[v] = sum
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("migrate: read %s: %w", table, err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("migrate: read %s: %w", table, err)
	}
	for _, m := range migs {
		sum, ok := stored[m.Version]
		switch {
		case !ok:
		case !sum.Valid:
			if _, err := conn.ExecContext(ctx, `UPDATE `+table+` SET checksum = $1 WHERE version = $2 AND checksum IS NULL`, m.Checksum(), m.Version); err != nil {
				return fmt.Errorf("migrate: record checksum of %s: %w", m.Name, err)
			}
		case sum.String != m.Checksum():
			return fmt.Errorf("migrate: %s: %w", m.Name, ErrChanged)
		}
	}
	return nil
}

func validTable(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

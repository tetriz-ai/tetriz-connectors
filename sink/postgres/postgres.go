// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package postgres publishes connector envelopes into a Postgres table, so
// records can be read with SQL.
//
// Every event becomes one row of connector_records, keyed on (organization_id,
// connector_id, operation_id, external_id, data_date): the key the sink
// documents for deduplication. Delivery is at least once, and incremental runs
// emit the records at their cursor boundary again, so a record arrives more
// than once. The upsert absorbs that:
//
//   - A record with a later fetched_at replaces the row's payload, metadata,
//     fetched_at and sync_run_id.
//   - A record with an earlier fetched_at is ignored, so a replay never
//     overwrites a newer copy.
//   - A record with the same fetched_at replaces the row only when its
//     content differs; an exact replay changes nothing.
//   - first_seen_at is set once, when the row is inserted. updated_at moves
//     only when payload, payload_encoding or metadata change, so
//     "updated_at > $since" finds records whose content changed.
//
// Payloads are stored as jsonb when they are valid JSON that Postgres accepts
// (UTF-8, no \u0000, no unpaired surrogate escapes); every operation in this
// repository emits JSON, including the protojson of repository records.
// Anything else is stored as a JSON string holding the payload's standard
// base64, with payload_encoding 'base64'. An empty payload is stored as JSON
// null.
//
// The table lives beside its own version table, sink_schema_migrations, so it
// can share a database with the runner store or use one of its own. It has no
// row-level security: the publisher writes every organization's records.
package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
)

// Table is the table records are written to.
const Table = "connector_records"

// MigrationsTable records which of this package's migrations are applied. It
// is separate from the runner store's schema_migrations.
const MigrationsTable = "sink_schema_migrations"

// Payload encodings stored in payload_encoding.
const (
	EncodingJSON   = "json"
	EncodingBase64 = "base64"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations holds the table's migrations, for a host that applies them
// itself.
var Migrations = mustSub(migrationFiles, "migrations")

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// maxParams is Postgres's limit on bind parameters in one statement.
const maxParams = 65535

// columnsPerRow is the number of bind parameters each record takes.
const columnsPerRow = 10

// rowsPerStatement is the most records one INSERT carries.
const rowsPerStatement = maxParams / columnsPerRow

// Publisher writes envelopes to connector_records. It is safe for concurrent
// use.
type Publisher struct {
	db    *sql.DB
	owned bool
}

// Open connects to dsn (any DSN the pgx driver accepts), applies the
// migrations and returns a Publisher that owns the connection pool; Close
// closes it. Open is safe to call from several processes at once: the
// migrations run under an advisory lock.
func Open(ctx context.Context, dsn string) (*Publisher, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres sink: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres sink: connect: %w", err)
	}
	if _, err := Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Publisher{db: db, owned: true}, nil
}

// New returns a Publisher writing through db, which the caller keeps and
// closes. It does not migrate; call Migrate first.
func New(db *sql.DB) *Publisher {
	return &Publisher{db: db}
}

// Migrate applies the migrations not yet recorded in sink_schema_migrations
// and returns the versions it applied. It is idempotent and safe to run
// concurrently.
func Migrate(ctx context.Context, db *sql.DB) ([]int64, error) {
	applied, err := migrate.UpTable(ctx, db, Migrations, MigrationsTable)
	if err != nil {
		return applied, fmt.Errorf("postgres sink: migrate: %w", err)
	}
	return applied, nil
}

// Close closes the connection pool when Open created it, and does nothing
// for a Publisher from New.
func (p *Publisher) Close() error {
	if !p.owned {
		return nil
	}
	if err := p.db.Close(); err != nil {
		return fmt.Errorf("postgres sink: close: %w", err)
	}
	return nil
}

// row is one event, ready to bind.
type row struct {
	org, connector, operation, externalID string
	dataDate                              *time.Time
	payload                               string
	encoding                              string
	metadata                              string
	fetchedAt                             *time.Time
	syncRunID                             string
}

// Publish implements sink.Publisher. It upserts the envelope's events in one
// transaction, so either all of them land or none do, and returns an empty
// message id: a row has no id beyond its key.
//
// An event without an organization takes the envelope's; an event naming a
// different organization from the envelope is an error. An event without
// fetched_at takes the transaction's time.
func (p *Publisher) Publish(ctx context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error) {
	if envelope == nil {
		return "", errors.New("postgres sink: envelope must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("postgres sink: %w", err)
	}
	rows, err := toRows(envelope)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("postgres sink: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for start := 0; start < len(rows); start += rowsPerStatement {
		chunk := rows[start:min(start+rowsPerStatement, len(rows))]
		query, args := upsert(chunk)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return "", fmt.Errorf("postgres sink: upsert %d records: %w", len(chunk), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("postgres sink: commit: %w", err)
	}
	return "", nil
}

// toRows converts the events, keeps the latest copy of any key repeated
// within the envelope (one INSERT cannot touch a row twice), and sorts by key
// so concurrent publishers lock rows in the same order.
func toRows(envelope *connectorpb.ConnectorDataEnvelope) ([]row, error) {
	byKey := make(map[string]int, len(envelope.GetEvents()))
	rows := make([]row, 0, len(envelope.GetEvents()))
	for i, ev := range envelope.GetEvents() {
		if ev == nil {
			return nil, fmt.Errorf("postgres sink: event %d is nil", i)
		}
		r, err := toRow(envelope.GetOrganizationId(), ev)
		if err != nil {
			return nil, fmt.Errorf("postgres sink: event %d (%s): %w", i, ev.GetExternalId(), err)
		}
		k := r.key()
		if j, dup := byKey[k]; dup {
			if !r.fetchedBefore(rows[j]) {
				rows[j] = r
			}
			continue
		}
		byKey[k] = len(rows)
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b row) int { return cmp.Compare(a.key(), b.key()) })
	return rows, nil
}

func toRow(envelopeOrg string, ev *connectorpb.ConnectorDataEvent) (row, error) {
	org := ev.GetOrganizationId()
	switch {
	case org == "":
		org = envelopeOrg
	case envelopeOrg != "" && org != envelopeOrg:
		return row{}, fmt.Errorf("organization %q differs from the envelope's %q", org, envelopeOrg)
	}
	r := row{
		org:        org,
		connector:  ev.GetConnectorId(),
		operation:  ev.GetOperationId(),
		externalID: ev.GetExternalId(),
		syncRunID:  ev.GetSyncRunId(),
	}
	for _, col := range []struct {
		name, value string
		required    bool
	}{
		{"organization_id", r.org, true},
		{"connector_id", r.connector, true},
		{"operation_id", r.operation, true},
		{"external_id", r.externalID, true},
		{"sync_run_id", r.syncRunID, false},
	} {
		// The first four are the row's key: an empty or whitespace-only one
		// would make every such record of the organization land on one row.
		if col.required && strings.TrimSpace(col.value) == "" {
			return row{}, fmt.Errorf("%s is empty or blank, and it is part of the record's key", col.name)
		}
		if !textColumnAccepts(col.value) {
			return row{}, fmt.Errorf("%s %q is not valid UTF-8 or contains NUL, which a Postgres text column rejects", col.name, col.value)
		}
	}
	if ts := ev.GetDataDate(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return row{}, fmt.Errorf("data_date: %w", err)
		}
		t := ts.AsTime()
		r.dataDate = &t
	}
	if ts := ev.GetFetchedAt(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return row{}, fmt.Errorf("fetched_at: %w", err)
		}
		t := ts.AsTime()
		r.fetchedAt = &t
	}
	r.payload, r.encoding = encodePayload(ev.GetPayload())
	meta, err := encodeMetadata(ev.GetMetadata())
	if err != nil {
		return row{}, err
	}
	r.metadata = meta
	return r, nil
}

// key is the row's primary key as one comparable string.
func (r row) key() string {
	date := "-"
	if r.dataDate != nil {
		date = strconv.FormatInt(r.dataDate.UnixNano(), 10)
	}
	return strings.Join([]string{r.org, r.connector, r.operation, r.externalID, date}, "\x00")
}

// fetchedBefore reports whether r was fetched strictly before o. A missing
// fetched_at counts as the latest, as the database gives it now().
func (r row) fetchedBefore(o row) bool {
	switch {
	case r.fetchedAt == nil:
		return false
	case o.fetchedAt == nil:
		return true
	default:
		return r.fetchedAt.Before(*o.fetchedAt)
	}
}

// encodePayload returns the jsonb text for payload and its encoding.
func encodePayload(payload []byte) (string, string) {
	if len(payload) == 0 {
		return "null", EncodingJSON
	}
	if jsonbAccepts(payload) {
		return string(payload), EncodingJSON
	}
	quoted, _ := json.Marshal(base64.StdEncoding.EncodeToString(payload)) // a string always marshals
	return string(quoted), EncodingBase64
}

// jsonbAccepts reports whether Postgres's jsonb input accepts b: valid JSON
// in valid UTF-8 whose string escapes contain no \u0000 and no unpaired
// surrogate.
func jsonbAccepts(b []byte) bool {
	if !utf8.Valid(b) || !json.Valid(b) {
		return false
	}
	// In valid JSON a backslash appears only inside a string, as an escape.
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		i++
		if b[i] != 'u' {
			continue
		}
		c := hex4(b[i+1 : i+5])
		i += 4
		switch {
		case c == 0:
			return false
		case c >= 0xDC00 && c <= 0xDFFF:
			return false
		case c >= 0xD800 && c <= 0xDBFF:
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			lo := hex4(b[i+3 : i+7])
			if lo < 0xDC00 || lo > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}

// textColumnAccepts reports whether a Postgres text column in a UTF-8
// database accepts s: valid UTF-8 with no NUL byte.
func textColumnAccepts(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// hex4 decodes four hex digits that json.Valid has already checked.
func hex4(b []byte) rune {
	v, _ := strconv.ParseUint(string(b), 16, 32)
	return rune(v)
}

// encodeMetadata renders the metadata map as a jsonb object. NUL, which jsonb
// rejects, becomes U+FFFD.
func encodeMetadata(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	clean := m
	for k, v := range m {
		if strings.ContainsRune(k, 0) || strings.ContainsRune(v, 0) {
			clean = make(map[string]string, len(m))
			for k, v := range m {
				clean[strings.ReplaceAll(k, "\x00", "�")] = strings.ReplaceAll(v, "\x00", "�")
			}
			break
		}
	}
	b, err := json.Marshal(clean)
	if err != nil {
		return "", fmt.Errorf("metadata: %w", err)
	}
	return string(b), nil
}

const upsertHead = `INSERT INTO ` + Table + ` AS r (
	organization_id, connector_id, operation_id, external_id, data_date,
	payload, payload_encoding, metadata, fetched_at, sync_run_id
) VALUES `

// upsertTail applies the replacement rule in the package doc.
const upsertTail = `
ON CONFLICT (organization_id, connector_id, operation_id, external_id, data_date_key) DO UPDATE SET
	payload = EXCLUDED.payload,
	payload_encoding = EXCLUDED.payload_encoding,
	metadata = EXCLUDED.metadata,
	fetched_at = EXCLUDED.fetched_at,
	sync_run_id = EXCLUDED.sync_run_id,
	updated_at = CASE
		WHEN (r.payload, r.payload_encoding, r.metadata)
			IS DISTINCT FROM (EXCLUDED.payload, EXCLUDED.payload_encoding, EXCLUDED.metadata)
		THEN now() ELSE r.updated_at END
WHERE r.fetched_at < EXCLUDED.fetched_at
	OR (r.fetched_at = EXCLUDED.fetched_at
		AND (r.payload, r.payload_encoding, r.metadata)
			IS DISTINCT FROM (EXCLUDED.payload, EXCLUDED.payload_encoding, EXCLUDED.metadata))`

// upsert builds one INSERT for rows, which number at most rowsPerStatement.
func upsert(rows []row) (string, []any) {
	var q strings.Builder
	q.Grow(len(upsertHead) + len(upsertTail) + len(rows)*120)
	q.WriteString(upsertHead)
	args := make([]any, 0, len(rows)*columnsPerRow)
	for i, r := range rows {
		if i > 0 {
			q.WriteString(",")
		}
		n := i * columnsPerRow
		fmt.Fprintf(&q, "($%d,$%d,$%d,$%d,$%d::timestamptz,$%d::jsonb,$%d,$%d::jsonb,COALESCE($%d::timestamptz,now()),NULLIF($%d,''))",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10)
		args = append(args, r.org, r.connector, r.operation, r.externalID, nullTime(r.dataDate),
			r.payload, r.encoding, r.metadata, nullTime(r.fetchedAt), r.syncRunID)
	}
	q.WriteString(upsertTail)
	return q.String(), args
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

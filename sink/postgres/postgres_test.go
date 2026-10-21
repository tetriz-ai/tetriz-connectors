// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/sink/postgres"
)

// open returns a publisher on the shared test database and a plain handle to
// read it with.
func open(t *testing.T) (*postgres.Publisher, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	p, err := postgres.Open(ctx, testDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	db, err := sql.Open("pgx", testDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return p, db
}

// newOrg returns an organization id no other test uses.
func newOrg() string { return "org-" + uuid.NewString() }

func event(org, externalID string, payload []byte, fetched time.Time) *connectorpb.ConnectorDataEvent {
	return &connectorpb.ConnectorDataEvent{
		ConnectorId:    "github",
		OperationId:    "fetch_prs",
		OrganizationId: org,
		ExternalId:     externalID,
		Payload:        payload,
		FetchedAt:      timestamppb.New(fetched),
		DataDate:       timestamppb.New(time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)),
		Metadata:       map[string]string{"event_type": "opened"},
	}
}

func envelope(org string, events ...*connectorpb.ConnectorDataEvent) *connectorpb.ConnectorDataEnvelope {
	return &connectorpb.ConnectorDataEnvelope{OrganizationId: org, Events: events}
}

type stored struct {
	payload, encoding, metadata string
	dataDate                    sql.NullTime
	fetchedAt                   time.Time
	syncRunID                   sql.NullString
	firstSeen, updated          time.Time
}

func readRow(t *testing.T, db *sql.DB, org, externalID string) stored {
	t.Helper()
	var s stored
	require.NoError(t, db.QueryRow(`SELECT payload::text, payload_encoding, metadata::text, data_date,
		fetched_at, sync_run_id, first_seen_at, updated_at
		FROM connector_records WHERE organization_id = $1 AND external_id = $2`, org, externalID).
		Scan(&s.payload, &s.encoding, &s.metadata, &s.dataDate, &s.fetchedAt, &s.syncRunID, &s.firstSeen, &s.updated))
	return s
}

func count(t *testing.T, db *sql.DB, org string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM connector_records WHERE organization_id = $1`, org).Scan(&n))
	return n
}

// A record emitted through sink.ConnectorSink lands as one row with every
// field of its event.
func TestPublish_RoundTripsARealEnvelope(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	fetched := time.Date(2026, 3, 1, 12, 30, 0, 123456000, time.UTC)
	dataDate := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	payload := []byte(`{"number": 42, "title": "Fix the build", "state": "MERGED"}`)
	ctx := sdk.WithSyncRunID(context.Background(), "run-1")

	require.NoError(t, sink.NewConnectorSink(p).Emit(ctx, &sdk.RawRecord{
		ConnectorID:    "github",
		OperationID:    "fetch_prs",
		OrganizationID: org,
		ExternalID:     "acme/api#PR-42#opened",
		Payload:        payload,
		FetchedAt:      fetched,
		DataDate:       dataDate,
		Metadata:       map[string]string{"event_type": "opened", "repo": "acme/api"},
	}))

	s := readRow(t, db, org, "acme/api#PR-42#opened")
	assert.JSONEq(t, string(payload), s.payload)
	assert.Equal(t, postgres.EncodingJSON, s.encoding)
	assert.JSONEq(t, `{"event_type":"opened","repo":"acme/api"}`, s.metadata)
	require.True(t, s.dataDate.Valid)
	assert.True(t, s.dataDate.Time.Equal(dataDate))
	assert.True(t, s.fetchedAt.Equal(fetched), "fetched_at %v", s.fetchedAt)
	assert.Equal(t, sql.NullString{String: "run-1", Valid: true}, s.syncRunID)
	assert.Equal(t, s.firstSeen, s.updated)

	var connector, operation string
	require.NoError(t, db.QueryRow(`SELECT connector_id, operation_id FROM connector_records WHERE organization_id = $1`, org).
		Scan(&connector, &operation))
	assert.Equal(t, "github", connector)
	assert.Equal(t, "fetch_prs", operation)
}

// The boundary re-emission of an incremental run delivers the same record
// again: it stays one row, and an exact replay changes nothing.
func TestPublish_ReplayIsIdempotent(t *testing.T) {
	p, db := open(t)
	ctx := context.Background()
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ev := event(org, "acme/api#PR-1#snapshot", []byte(`{"comments":0}`), t0)

	_, err := p.Publish(ctx, envelope(org, ev))
	require.NoError(t, err)
	first := readRow(t, db, org, ev.ExternalId)

	_, err = p.Publish(ctx, envelope(org, ev))
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, db, org))
	assert.Equal(t, first, readRow(t, db, org, ev.ExternalId), "an exact replay changes nothing")

	// The next run fetches it again unchanged: fetched_at moves, updated_at
	// does not.
	same := event(org, ev.ExternalId, []byte(`{"comments": 0}`), t0.Add(time.Hour))
	_, err = p.Publish(ctx, envelope(org, same))
	require.NoError(t, err)
	got := readRow(t, db, org, ev.ExternalId)
	assert.True(t, got.fetchedAt.Equal(t0.Add(time.Hour)))
	assert.Equal(t, first.firstSeen, got.firstSeen)
	assert.Equal(t, first.updated, got.updated, "the content did not change")

	// A later fetch with new counters replaces the payload and moves
	// updated_at, keeping first_seen_at.
	newer := event(org, ev.ExternalId, []byte(`{"comments":8}`), t0.Add(2*time.Hour))
	_, err = p.Publish(ctx, envelope(org, newer))
	require.NoError(t, err)
	got = readRow(t, db, org, ev.ExternalId)
	assert.Equal(t, 1, count(t, db, org))
	assert.JSONEq(t, `{"comments":8}`, got.payload)
	assert.Equal(t, first.firstSeen, got.firstSeen)
	assert.True(t, got.updated.After(first.updated), "updated_at %v after %v", got.updated, first.updated)
}

// A replay of an older copy, such as a re-run that finishes after a newer
// one, never overwrites the newer row.
func TestPublish_OlderReplayDoesNotOverwriteNewer(t *testing.T) {
	p, db := open(t)
	ctx := context.Background()
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	_, err := p.Publish(ctx, envelope(org, event(org, "pr", []byte(`{"v":"new"}`), t0.Add(time.Hour))))
	require.NoError(t, err)
	before := readRow(t, db, org, "pr")

	old := event(org, "pr", []byte(`{"v":"old"}`), t0)
	old.SyncRunId = "older-run"
	_, err = p.Publish(ctx, envelope(org, old))
	require.NoError(t, err)

	assert.Equal(t, before, readRow(t, db, org, "pr"))
}

// Within one envelope a repeated key keeps the latest copy, whatever its
// position.
func TestPublish_RepeatedKeyInOneEnvelopeKeepsTheLatest(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	_, err := p.Publish(context.Background(), envelope(org,
		event(org, "pr", []byte(`{"v":2}`), t0.Add(time.Minute)),
		event(org, "pr", []byte(`{"v":1}`), t0),
	))
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, db, org))
	assert.JSONEq(t, `{"v":2}`, readRow(t, db, org, "pr").payload)
}

// The data date is part of the key: an operation that reuses an external id
// per day (Kiro's client type) keeps one row per day, and records without a
// date still collapse to one row.
func TestPublish_DataDateIsPartOfTheKey(t *testing.T) {
	p, db := open(t)
	ctx := context.Background()
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	day1 := event(org, "IDE", []byte(`{"day":1}`), t0)
	day1.DataDate = timestamppb.New(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	day2 := event(org, "IDE", []byte(`{"day":2}`), t0)
	day2.DataDate = timestamppb.New(time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC))
	undated := event(org, "members", []byte(`{}`), t0)
	undated.DataDate = nil

	_, err := p.Publish(ctx, envelope(org, day1, day2, undated))
	require.NoError(t, err)
	_, err = p.Publish(ctx, envelope(org, undated))
	require.NoError(t, err)

	assert.Equal(t, 3, count(t, db, org))
	assert.False(t, readRow(t, db, org, "members").dataDate.Valid, "data_date is NULL when the event has none")
}

// Bytes Postgres's jsonb rejects are kept as a JSON string of their base64.
func TestPublish_NonJSONPayloadIsStoredAsBase64(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string][]byte{
		"binary":                       {0xff, 0x00, 0xfe, 'x'},
		"text":                         []byte("not json"),
		"nul escape":                   []byte(`{"a":"\u0000"}`),
		"lone surrogate":               []byte(`{"a":"\ud800"}`),
		"lone low surrogate":           []byte(`{"a":"\udc00"}`),
		"high surrogate then a letter": []byte(`{"a":"\ud800A"}`),
		"invalid utf8":                 []byte("{\"a\":\"\xff\"}"),
	}
	var events []*connectorpb.ConnectorDataEvent
	for name, b := range cases {
		events = append(events, event(org, name, b, t0))
	}
	events = append(events,
		event(org, "surrogate pair", []byte(`{"a":"😀"}`), t0),
		event(org, "empty", nil, t0),
		event(org, "metadata nul", []byte(`{}`), t0),
	)
	events[len(events)-1].Metadata = map[string]string{"k": "a\x00b"}

	_, err := p.Publish(context.Background(), envelope(org, events...))
	require.NoError(t, err)

	for name, b := range cases {
		s := readRow(t, db, org, name)
		assert.Equal(t, postgres.EncodingBase64, s.encoding, name)
		var encoded string
		require.NoError(t, json.Unmarshal([]byte(s.payload), &encoded), name)
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err, name)
		assert.Equal(t, b, decoded, name)
	}
	pair := readRow(t, db, org, "surrogate pair")
	assert.Equal(t, postgres.EncodingJSON, pair.encoding)
	assert.JSONEq(t, `{"a":"😀"}`, pair.payload)
	empty := readRow(t, db, org, "empty")
	assert.Equal(t, postgres.EncodingJSON, empty.encoding)
	assert.Equal(t, "null", empty.payload)
	assert.JSONEq(t, `{"k":"a�b"}`, readRow(t, db, org, "metadata nul").metadata)
}

// A replay with the same fetched_at and the same content changes nothing,
// not even the sync run id: the row is not rewritten.
func TestPublish_SameFetchedAtSameContentChangesNothing(t *testing.T) {
	p, db := open(t)
	ctx := context.Background()
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	first := event(org, "pr", []byte(`{"v":1}`), t0)
	first.SyncRunId = "run-1"
	_, err := p.Publish(ctx, envelope(org, first))
	require.NoError(t, err)
	xmin := func() string {
		var x string
		require.NoError(t, db.QueryRow(`SELECT xmin::text FROM connector_records WHERE organization_id = $1 AND external_id = 'pr'`, org).Scan(&x))
		return x
	}
	before := xmin()

	replay := event(org, "pr", []byte(`{"v":1}`), t0)
	replay.SyncRunId = "run-2"
	_, err = p.Publish(ctx, envelope(org, replay))
	require.NoError(t, err)

	got := readRow(t, db, org, "pr")
	assert.Equal(t, "run-1", got.syncRunID.String, "an identical replay keeps the first run's id")
	assert.Equal(t, before, xmin(), "the row was not rewritten")
}

// A key column (organization, connector, operation, external id or sync run
// id) that is not valid UTF-8 or holds a NUL is rejected with an error naming
// the record and the column, before any SQL runs, like an organization that
// differs from the envelope's. The envelope is not stored.
func TestPublish_KeyColumnsMustBeValidText(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	type bad struct {
		column string
		edit   func(*connectorpb.ConnectorDataEvent)
	}
	cases := map[string]bad{}
	for _, v := range []struct{ name, value string }{{"nul", "a\x00b"}, {"invalid utf8", "b\xff"}} {
		v := v
		cases[v.name+" in external id"] = bad{"external_id", func(e *connectorpb.ConnectorDataEvent) { e.ExternalId = v.value }}
		cases[v.name+" in connector id"] = bad{"connector_id", func(e *connectorpb.ConnectorDataEvent) { e.ConnectorId = v.value }}
		cases[v.name+" in operation id"] = bad{"operation_id", func(e *connectorpb.ConnectorDataEvent) { e.OperationId = v.value }}
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ev := event(org, "bad", []byte(`{}`), t0)
			c.edit(ev)
			_, err := p.Publish(context.Background(), envelope(org, event(org, "ok", []byte(`{}`), t0), ev))
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.column)
			assert.Contains(t, err.Error(), "is not valid UTF-8 or contains NUL")
			assert.Equal(t, 0, count(t, db, org), "the envelope is rejected whole")
		})
	}
	// The organization comes from the event when the envelope names none.
	for name, v := range map[string]string{"nul in organization id": "o\x00", "invalid utf8 in organization id": "o\xff"} {
		t.Run(name, func(t *testing.T) {
			_, err := p.Publish(context.Background(), envelope("", event(v, "x", []byte(`{}`), t0)))
			require.ErrorContains(t, err, "organization_id")
			assert.Contains(t, err.Error(), "is not valid UTF-8 or contains NUL")
		})
	}
	badRun := event(org, "run", []byte(`{}`), t0)
	badRun.SyncRunId = "r\x00"
	_, err := p.Publish(context.Background(), envelope(org, badRun))
	require.ErrorContains(t, err, "sync_run_id")

	// Multi-byte text is valid UTF-8 and is stored.
	_, err = p.Publish(context.Background(), envelope(org, event(org, "pr-é-日本-🙂", []byte(`{}`), t0)))
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, db, org))
}

// An event with an empty organization, connector, operation or external id
// is refused: those four are the row's key, so an empty one would make every
// such record land on one row. An empty sync run id is allowed (a record
// emitted outside a run carries none).
func TestPublish_EmptyKeyColumnsAreRefused(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for column, edit := range map[string]func(*connectorpb.ConnectorDataEvent){
		"connector_id": func(e *connectorpb.ConnectorDataEvent) { e.ConnectorId = "" },
		"operation_id": func(e *connectorpb.ConnectorDataEvent) { e.OperationId = "" },
		"external_id":  func(e *connectorpb.ConnectorDataEvent) { e.ExternalId = "" },
	} {
		t.Run(column, func(t *testing.T) {
			ev := event(org, "x", []byte(`{}`), t0)
			edit(ev)
			_, err := p.Publish(context.Background(), envelope(org, ev))
			require.ErrorContains(t, err, column+" is empty")
			assert.Equal(t, 0, count(t, db, org))
		})
	}
	t.Run("organization_id", func(t *testing.T) {
		_, err := p.Publish(context.Background(), envelope("", event("", "x", []byte(`{}`), t0)))
		require.ErrorContains(t, err, "organization_id is empty")
	})
	for column, edit := range map[string]func(*connectorpb.ConnectorDataEvent){
		"connector_id": func(e *connectorpb.ConnectorDataEvent) { e.ConnectorId = " " },
		"operation_id": func(e *connectorpb.ConnectorDataEvent) { e.OperationId = "\t" },
		"external_id":  func(e *connectorpb.ConnectorDataEvent) { e.ExternalId = " \n " },
	} {
		t.Run("blank "+column, func(t *testing.T) {
			ev := event(org, "x", []byte(`{}`), t0)
			edit(ev)
			_, err := p.Publish(context.Background(), envelope(org, ev))
			require.ErrorContains(t, err, column+" is empty or blank")
			assert.Equal(t, 0, count(t, db, org))
		})
	}
	t.Run("blank organization_id", func(t *testing.T) {
		_, err := p.Publish(context.Background(), envelope("  ", event("  ", "x", []byte(`{}`), t0)))
		require.ErrorContains(t, err, "organization_id is empty or blank")
	})
	t.Run("an empty sync_run_id is stored", func(t *testing.T) {
		_, err := p.Publish(context.Background(), envelope(org, event(org, "no-run", []byte(`{}`), t0)))
		require.NoError(t, err)
		assert.Equal(t, 1, count(t, db, org))
	})
}

// An envelope too large for one statement's 65535 bind parameters is split
// into several statements in one transaction.
func TestPublish_ManyRecordsInOneEnvelope(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const n = 15000 // 150000 parameters: three statements
	events := make([]*connectorpb.ConnectorDataEvent, n)
	for i := range events {
		events[i] = event(org, fmt.Sprintf("pr-%05d", i), []byte(fmt.Sprintf(`{"i":%d}`, i)), t0)
	}
	_, err := p.Publish(context.Background(), envelope(org, events...))
	require.NoError(t, err)
	assert.Equal(t, n, count(t, db, org))
	assert.JSONEq(t, `{"i":14999}`, readRow(t, db, org, "pr-14999").payload)
}

// A failing event rolls back the whole envelope, including statements that
// already ran.
func TestPublish_OneTransactionPerEnvelope(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const n = 7000
	events := make([]*connectorpb.ConnectorDataEvent, n)
	for i := range events {
		events[i] = event(org, fmt.Sprintf("pr-%05d", i), []byte(`{}`), t0)
	}
	// A NUL in a text column fails the second statement.
	events[n-1].ExternalId = "pr-99999\x00" // sorts last
	_, err := p.Publish(context.Background(), envelope(org, events...))
	require.Error(t, err)
	assert.Equal(t, 0, count(t, db, org))
}

// Publishers writing overlapping keys at once neither deadlock nor lose the
// newest copy.
func TestPublish_ConcurrentPublishers(t *testing.T) {
	p, db := open(t)
	other, err := postgres.Open(context.Background(), testDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	org := newOrg()
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const workers, keys = 8, 200

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			pub := p
			if w%2 == 1 {
				pub = other
			}
			events := make([]*connectorpb.ConnectorDataEvent, 0, keys)
			for k := range keys {
				// Opposite orders, so unsorted writes would deadlock.
				if w%2 == 1 {
					k = keys - 1 - k
				}
				events = append(events, event(org, fmt.Sprintf("pr-%03d", k),
					[]byte(fmt.Sprintf(`{"worker":%d}`, w)), t0.Add(time.Duration(w)*time.Minute)))
			}
			for range 5 {
				if _, err := pub.Publish(context.Background(), envelope(org, events...)); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, keys, count(t, db, org))
	var notLatest int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM connector_records
		WHERE organization_id = $1 AND payload <> '{"worker":7}'::jsonb`, org).Scan(&notLatest))
	assert.Zero(t, notLatest, "every key holds the copy with the latest fetched_at")
}

// A cancelled context publishes nothing and says why.
func TestPublish_ContextCancelled(t *testing.T) {
	p, db := open(t)
	org := newOrg()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Publish(ctx, envelope(org, event(org, "pr", []byte(`{}`), time.Now())))
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, count(t, db, org))
}

func TestPublish_RejectsBadEnvelopes(t *testing.T) {
	p, db := open(t)
	ctx := context.Background()
	org := newOrg()

	_, err := p.Publish(ctx, nil)
	require.Error(t, err)

	_, err = p.Publish(ctx, envelope(org, event("someone-else", "pr", []byte(`{}`), time.Now())))
	require.ErrorContains(t, err, "differs from the envelope")

	id, err := p.Publish(ctx, envelope(org))
	require.NoError(t, err, "an empty envelope is a no-op")
	assert.Empty(t, id)

	// An event without an organization takes the envelope's.
	ev := event("", "pr", []byte(`{}`), time.Now())
	_, err = p.Publish(ctx, envelope(org, ev))
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, db, org))
}

// Migrations re-run as a no-op, use their own version table beside the
// runner store's, and serialise when several processes start at once.
func TestMigrate_ReRunsAndConcurrentStartup(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup, err := testdb.Setup(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	const starts = 6
	var wg sync.WaitGroup
	errs := make(chan error, starts)
	for range starts {
		wg.Go(func() {
			p, err := postgres.Open(ctx, dsn)
			if err != nil {
				errs <- err
				return
			}
			errs <- p.Close()
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	applied, err := postgres.Migrate(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, applied, "a re-run applies nothing")

	var sinkVersions, storeVersions int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM `+postgres.MigrationsTable).Scan(&sinkVersions))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&storeVersions))
	assert.Equal(t, 1, sinkVersions)
	assert.Positive(t, storeVersions, "the runner store's migrations are untouched")

	// New does not own the pool.
	require.NoError(t, postgres.New(db).Close())
	require.NoError(t, db.PingContext(ctx))
}

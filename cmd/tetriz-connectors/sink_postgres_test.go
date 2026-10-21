// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// TETRIZ_SINK=postgres writes each record of a `sync` to connector_records:
// in TETRIZ_SINK_POSTGRES_DSN when it is set, and otherwise in the store's
// own database.
func TestSyncWritesRecordsToThePostgresSink(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("postgres sink", err)
		t.Skip("no test database")
	}
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "name": "core", "full_name": "acme/core"},
			{"id": 2, "name": "web", "full_name": "acme/web"},
		})
	}))
	defer vendor.Close()
	const org = "00000000-0000-4000-8000-0000000e0e51"
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = vendor.URL
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	for _, separate := range []bool{false, true} {
		name := "store database"
		if separate {
			name = "TETRIZ_SINK_POSTGRES_DSN"
		}
		t.Run(name, func(t *testing.T) {
			_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
			require.NoError(t, err)
			t.Cleanup(cleanup)
			sinkDSN := dsn
			t.Setenv("TETRIZ_SINK_POSTGRES_DSN", "")
			if separate {
				_, other, cleanupOther, err := testdb.Setup(context.Background(), testdb.Connector)
				require.NoError(t, err)
				t.Cleanup(cleanupOther)
				sinkDSN = other
				t.Setenv("TETRIZ_SINK_POSTGRES_DSN", other)
			}
			t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
			t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
			t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
			t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
			t.Setenv("TETRIZ_SINK", "postgres")

			code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`, "--credentials", `{}`)
			require.Equal(t, 0, code, errOut)
			code, out, errOut := runCLI(t, "sync", "--org", org, "--connector", "github", "--op", "fetch_repos")
			require.Equal(t, 0, code, "stdout=%s stderr=%s", out, errOut)
			assert.Contains(t, out, `"records_emitted": 2`)

			db, err := sql.Open("pgx", sinkDSN)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			rows, err := db.Query(`SELECT external_id FROM connector_records WHERE organization_id = $1 AND connector_id = 'github' AND operation_id = 'fetch_repos' ORDER BY external_id`, org)
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()
			var ids []string
			for rows.Next() {
				var id string
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			assert.Len(t, ids, 2, "both repositories land in connector_records: %v", ids)
			if separate {
				store, err := sql.Open("pgx", dsn)
				require.NoError(t, err)
				defer func() { _ = store.Close() }()
				var table sql.NullString
				require.NoError(t, store.QueryRow(`SELECT to_regclass('connector_records')::text`).Scan(&table))
				assert.False(t, table.Valid, "the store's database gets no records table")
			}
		})
	}
}

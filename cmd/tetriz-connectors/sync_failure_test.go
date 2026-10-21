// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

// syncEnv points the CLI at a fresh test database and a private Redis.
func syncEnv(t *testing.T) *gorm.DB {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("sync failure", err)
		t.Skip("no test database")
	}
	db, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	return db
}

// A run that fails after Execute and whose row cannot be closed: `sync`
// prints the run as failed with the error and exits 1. The stored row keeps
// reading running with no error, which the README lists under "Before you
// rely on it".
func TestSyncFailsWhenTheRunRowCannotBeClosed(t *testing.T) {
	db := syncEnv(t)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "name": "core", "full_name": "acme/core"}})
	}))
	defer vendor.Close()
	const org = "00000000-0000-4000-8000-0000000e0e09"
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = vendor.URL
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`, "--credentials", `{}`)
	require.Equal(t, 0, code, errOut)
	// The store refuses every write that closes a run row (a stand-in for a
	// database that times out after Execute); lease renewals still land.
	require.NoError(t, db.Exec(`CREATE FUNCTION refuse_close() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'store unavailable'; END $$;
CREATE TRIGGER refuse_close BEFORE UPDATE ON connector_sync_runs FOR EACH ROW WHEN (NEW.status <> 'running') EXECUTE FUNCTION refuse_close();`).Error)

	code, out, errOut := runCLI(t, "sync", "--org", org, "--connector", "github", "--op", "fetch_repos")
	assert.Equal(t, 1, code, "the run failed: its cursor was not saved; stderr=%s", errOut)
	var res struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &res), "stdout is the run's JSON: %s", out)
	assert.Equal(t, "failed", res.Status)
	assert.NotEmpty(t, res.RunID)
	assert.NotEmpty(t, res.Error)

	var row struct {
		Status       string
		ErrorMessage *string
	}
	require.NoError(t, db.Raw(`SELECT status, error_message FROM connector_sync_runs WHERE id = ?`, res.RunID).Scan(&row).Error)
	assert.Equal(t, "running", row.Status, "pinned: the stored row stays running")
	assert.True(t, row.ErrorMessage == nil || *row.ErrorMessage == "", "pinned: no error is recorded on the row")
}

// `sync` for a connector with no stored config exits 1 with the load error.
func TestSyncWithoutAStoredConfigExits1(t *testing.T) {
	syncEnv(t)
	code, _, errOut := runCLI(t, "sync", "--org", "00000000-0000-4000-8000-0000000e0e0a", "--connector", "cursor", "--op", "fetch_usage_events")
	assert.Equal(t, 1, code)
	assert.Contains(t, errOut, "load config: record not found")
}

// `sync` reports its own run row. A scheduled run of the same operation that
// starts while the manual run is in flight is the latest row, but `sync`
// still prints the run it started.
func TestSyncReportsItsOwnRunRow(t *testing.T) {
	db := syncEnv(t)
	const org = "00000000-0000-4000-8000-0000000e0e0b"
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A scheduled run of the same operation starts during the manual one.
		require.NoError(t, db.Exec(`INSERT INTO connector_sync_runs (id, organization_id, connector_id, operation_id, status, started_at, trigger)
VALUES ('00000000-0000-4000-8000-00000000c0cc', ?, 'github', 'fetch_repos', 'running', now() + interval '1 minute', 'scheduled')
ON CONFLICT (id) DO NOTHING`, org).Error)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "name": "core", "full_name": "acme/core"}})
	}))
	defer vendor.Close()
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = vendor.URL
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`, "--credentials", `{}`)
	require.Equal(t, 0, code, errOut)
	code, out, errOut := runCLI(t, "sync", "--org", org, "--connector", "github", "--op", "fetch_repos")
	require.Equal(t, 0, code, errOut)
	var res struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &res), "stdout is the run's JSON: %s", out)
	assert.NotEqual(t, "00000000-0000-4000-8000-00000000c0cc", res.RunID, "sync does not print the concurrent run's row")
	assert.Equal(t, "completed", res.Status)
	var trigger string
	require.NoError(t, db.Raw(`SELECT trigger FROM connector_sync_runs WHERE id = ?`, res.RunID).Scan(&trigger).Error)
	assert.Equal(t, "manual", trigger, "the printed row is the manual run")
}

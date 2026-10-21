// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/cli"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

const org = "00000000-0000-4000-8000-00000000c0de"

// todoVendor is the todo API: three todos, behind a bearer token.
func todoVendor(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer todo-token" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/todos" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]Todo{{1, "write the connector", true}, {2, "sync it", false}, {3, "serve it", false}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// runnerEnv points the command at a fresh Postgres database and a miniredis.
func runnerEnv(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("custom connector example", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	mr := miniredis.RunT(t)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", mr.Addr())
	t.Setenv("TETRIZ_REDIS_URL", "")
	t.Setenv("TETRIZ_NAMESPACE", "")
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_BLOB_DIR", t.TempDir())
	t.Setenv("TETRIZ_HTTP_ADDR", "127.0.0.1:0")
	return mr
}

func run(t *testing.T, ctx context.Context, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := cli.Main(ctx, args, &out, &errb, Options()...)
	return code, out.String(), errb.String()
}

func putTodoConfig(t *testing.T, baseURL string) {
	t.Helper()
	code, out, errOut := run(t, context.Background(), "config", "put", "--org", org, "--connector", "todo",
		"--settings", `{"base_url":"`+baseURL+`"}`, "--credentials", `{"token":"todo-token"}`)
	require.Equal(t, 0, code, errOut)
	assert.Equal(t, "stored todo config for "+org+" (enabled=true)\n", out)
}

// `sync` of the custom operation runs it through the custom gateway and
// writes its records to the sink, here connector_records in Postgres
// (TETRIZ_SINK=postgres), like any built-in operation.
func TestSyncRunsTheCustomConnector(t *testing.T) {
	runnerEnv(t)
	t.Setenv("TETRIZ_SINK", "postgres")
	putTodoConfig(t, todoVendor(t).URL)

	code, out, errOut := run(t, context.Background(), "sync", "--org", org, "--connector", "todo", "--op", "fetch_todos")
	require.Equal(t, 0, code, "stdout=%s stderr=%s", out, errOut)
	var res app.SyncResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	assert.Equal(t, "completed", res.Status)
	assert.EqualValues(t, 3, res.Records)

	db, err := sql.Open("pgx", os.Getenv("TETRIZ_POSTGRES_DSN"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT external_id, payload->>'title' FROM connector_records
		WHERE organization_id = $1 AND connector_id = 'todo' AND operation_id = 'fetch_todos' ORDER BY external_id`, org)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var id, title string
		require.NoError(t, rows.Scan(&id, &title))
		got = append(got, id+" "+title)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"1 write the connector", "2 sync it", "3 serve it"}, got)

	// The connectors listing and the cursor include the custom connector.
	code, out, _ = run(t, context.Background(), "connectors")
	require.Equal(t, 0, code)
	assert.Contains(t, out, "todo\n  fetch_todos                  */15 * * * *\n")
	code, out, errOut = run(t, context.Background(), "cursor", "show", "--org", org, "--connector", "todo", "--op", "fetch_todos")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, `"count": 3`)

	// The stock command, with no options, does not know the connector.
	var stockOut, stockErr bytes.Buffer
	code = cli.Main(context.Background(), []string{"sync", "--org", org, "--connector", "todo", "--op", "fetch_todos"}, &stockOut, &stockErr)
	assert.Equal(t, 2, code)
	assert.Contains(t, stockErr.String(), `unknown connector "todo"`)
}

// `serve` schedules the custom operation: its leader registers a periodic
// task for todo/fetch_todos.
func TestServeSchedulesTheCustomConnector(t *testing.T) {
	mr := runnerEnv(t)
	t.Setenv("TETRIZ_SINK", "log")
	putTodoConfig(t, todoVendor(t).URL)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		code, _, _ := run(t, ctx, "serve")
		done <- code
	}()
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rc.Close() }()
	insp := asynq.NewInspectorFromRedisClient(rc)
	scheduled := func() bool {
		entries, err := insp.SchedulerEntries()
		if err != nil {
			return false
		}
		for _, e := range entries {
			var p struct {
				OrgID       string          `json:"org_id"`
				ConnectorID sdk.ConnectorID `json:"connector_id"`
				OperationID sdk.OperationID `json:"operation_id"`
			}
			if json.Unmarshal(e.Task.Payload(), &p) == nil && p.OrgID == org && p.ConnectorID == ConnectorID && p.OperationID == OpFetchTodos {
				return true
			}
		}
		return false
	}
	deadline := time.Now().Add(30 * time.Second)
	for !scheduled() && time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("serve exited %d before scheduling", code)
		case <-time.After(200 * time.Millisecond):
		}
	}
	assert.True(t, scheduled(), "serve schedules todo/fetch_todos")
	cancel()
	select {
	case code := <-done:
		assert.Equal(t, 0, code)
	case <-time.After(60 * time.Second):
		t.Fatal("serve did not stop")
	}
}

// A custom connector whose id is a built-in connector's is refused.
func TestCustomConnectorCannotReplaceABuiltIn(t *testing.T) {
	_, err := app.NewRegistryWith(nil, app.WithConnector(builtinNamed{Connector{}}, NewGateway))
	require.ErrorIs(t, err, app.ErrConnectorCollision)
	var out, errb bytes.Buffer
	code := cli.Main(context.Background(), []string{"connectors"}, &out, &errb, app.WithConnector(builtinNamed{Connector{}}, NewGateway))
	assert.Equal(t, 1, code)
	assert.Contains(t, errb.String(), `"github"`)
}

type builtinNamed struct{ Connector }

func (builtinNamed) ID() sdk.ConnectorID { return "github" }

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/cli"
	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// `sync` exits 1 with "run failed:" when the run fails, and 0 when the
// connector is paused and the run is skipped.
func TestSyncExitCodes(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("sync exit codes", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"down"}`, http.StatusInternalServerError)
	}))
	defer vendor.Close()
	const org = "00000000-0000-4000-8000-0000000e0e01"
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = vendor.URL
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`, "--credentials", `{}`)
	require.Equal(t, 0, code, errOut)
	code, out, errOut := runCLI(t, "sync", "--org", org, "--connector", "github", "--op", "fetch_repos")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, `"status": "failed"`)
	assert.Contains(t, errOut, "run failed:")

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`, "--disabled")
	require.Equal(t, 0, code, errOut)
	code, out, _ = runCLI(t, "sync", "--org", org, "--connector", "github", "--op", "fetch_repos")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, `"status": "skipped"`)

	// A connector whose operation panics fails its run: exit 1, not a crash.
	opts := []app.Option{app.WithConnector(panicConnector{}, func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return nil, nil
	})}
	runWith := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := cli.Main(context.Background(), args, &out, &errb, opts...)
		return code, out.String(), errb.String()
	}
	code, _, errOut = runWith("config", "put", "--org", org, "--connector", "panics", "--settings", `{}`, "--credentials", `{}`)
	require.Equal(t, 0, code, errOut)
	code, out, errOut = runWith("sync", "--org", org, "--connector", "panics", "--op", "fetch")
	assert.Equal(t, 1, code, errOut)
	assert.Contains(t, out, `"status": "failed"`)
	assert.Contains(t, out, "operation panicked: connector bug")
	assert.Contains(t, errOut, "run failed:")
}

// panicConnector has one operation, whose Execute panics.
type panicConnector struct{}

func (panicConnector) ID() sdk.ConnectorID                                        { return "panics" }
func (panicConnector) DisplayName() string                                        { return "Panics" }
func (panicConnector) Categories() []sdk.ConnectorCategory                        { return nil }
func (panicConnector) ValidateConfig(context.Context, *sdk.ConnectorConfig) error { return nil }
func (panicConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{{
		ID: "fetch", DisplayName: "Fetch", IngestionType: sdk.IngestionPull,
		Factory: func() sdk.Operation { return panicOp{} },
	}}
}

type panicOp struct{}

func (panicOp) OperationID() sdk.OperationID { return "fetch" }
func (panicOp) Execute(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error) {
	panic("connector bug")
}

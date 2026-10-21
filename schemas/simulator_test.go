// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	simserver "github.com/tetriz-ai/tetriz-connectors/internal/simulator/server"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// simOrg is the organization the gateway routes to the simulator.
const simOrg = "00000000-0000-4000-8000-00000000d3e0"

// simVendor is one simulated connector config and the operations a run of
// it covers.
type simVendor struct {
	connector string
	settings  string
	// first runs before the others: repository and project discovery, which
	// later operations read from the connector's cache.
	first []string
}

// Each ops_<connector>_test.go declares, for the simulated connectors:
//
//   - declareSimGap: the operation fails against the simulator (an endpoint
//     it does not serve, a host service the in-process runner lacks, a
//     webhook-only operation). Its payloads are checked by a fixture test in
//     the connector package (schema_*_test.go) instead.
//   - declareSimEmpty: the operation succeeds but emits nothing against the
//     seeded catalog.
var (
	simGaps  = map[string]map[string]string{}
	simEmpty = map[string]map[string]string{}
)

func declareSimGap(connector, op, reason string) { declareIn(simGaps, connector, op, reason) }

func declareSimEmpty(connector, op, reason string) { declareIn(simEmpty, connector, op, reason) }

func declareIn(m map[string]map[string]string, connector, op, reason string) {
	if reason == "" {
		panic(fmt.Sprintf("%s/%s: empty reason", connector, op))
	}
	if m[connector] == nil {
		m[connector] = map[string]string{}
	}
	m[connector][op] = reason
}

// simRun is what one simulated sync left.
type simRun struct {
	result app.SyncResult
	err    error
	events []*connectorpb.ConnectorDataEvent
}

// simHarness runs operations in process against a seeded simulator and a
// fresh store, writing records to one JSON-lines file.
type simHarness struct {
	t    *testing.T
	app  *app.App
	sink string
}

func newSimHarness(t *testing.T) *simHarness {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("schemas simulator", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, simDSN, simCleanup, err := testdb.Setup(ctx, testdb.Simulator)
	if err != nil {
		t.Fatalf("simulator database: %v", err)
	}
	t.Cleanup(simCleanup)
	if err := simserver.Seed(ctx, simDSN); err != nil {
		t.Fatalf("seed simulator: %v", err)
	}
	pool, err := pgxpool.New(ctx, simDSN)
	if err != nil {
		t.Fatalf("simulator pool: %v", err)
	}
	t.Cleanup(pool.Close)
	sim := httptest.NewServer(simserver.Handler(pool))
	t.Cleanup(sim.Close)

	_, dsn, cleanup, err := testdb.Setup(ctx, testdb.Connector)
	if err != nil {
		t.Fatalf("connector database: %v", err)
	}
	t.Cleanup(cleanup)

	demo, err := os.ReadFile("../deploy/gateway.demo.yaml")
	if err != nil {
		t.Fatal(err)
	}
	gwPath := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(gwPath, []byte(strings.ReplaceAll(string(demo), "http://simulator:8080", sim.URL)), 0o600); err != nil {
		t.Fatal(err)
	}
	gcfg, err := gateway.LoadConfigFile(gwPath)
	if err != nil {
		t.Fatal(err)
	}
	// The in-process simulator answers in well under a millisecond, so the
	// demo's 1000 requests a second is reachable on a big seeded listing (Jira
	// sprints across every board). This test checks payload shapes, not pacing:
	// give every simulated vendor a rate the seeded catalog cannot exhaust.
	for _, eps := range [][]gateway.APIEndpointConfig{gcfg.GitHub, gcfg.GitLab, gcfg.Bitbucket, gcfg.Jira} {
		for i := range eps {
			eps[i].RequestsPerSecond, eps[i].BurstSize = 1_000_000, 1_000_000
		}
	}
	gateway.SetConfig(gcfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	sinkPath := filepath.Join(t.TempDir(), "records.jsonl")
	oauth := map[string]app.OAuthClient{}
	for _, v := range app.OAuthVendors {
		oauth[v] = app.OAuthClient{ClientID: "simulated", ClientSecret: "simulated"}
	}
	a, err := app.New(ctx, app.Config{
		PostgresDSN:               dsn,
		RedisAddr:                 miniredis.RunT(t).Addr(),
		AllowPlaintextCredentials: true,
		SinkPath:                  sinkPath,
		BlobDir:                   t.TempDir(),
		DefaultTenantKind:         sdk.TenantKindOrganization,
		OAuth:                     oauth,
	})
	if err != nil {
		t.Fatalf("app: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return &simHarness{t: t, app: a, sink: sinkPath}
}

func (h *simHarness) put(connector, settings string) {
	h.t.Helper()
	err := h.app.PutConfig(context.Background(), &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connector),
		OrganizationID: simOrg,
		Settings:       json.RawMessage(settings),
		Credentials:    json.RawMessage(`{}`),
		Enabled:        true,
	})
	if err != nil {
		h.t.Fatalf("put %s config: %v", connector, err)
	}
}

// sync runs one operation and returns the events it appended to the sink.
func (h *simHarness) sync(connector, op string) simRun {
	h.t.Helper()
	before := h.lines()
	res, err := h.app.SyncOnce(context.Background(), simOrg, sdk.ConnectorID(connector), sdk.OperationID(op), sdk.TenantKindOrganization)
	run := simRun{result: res, err: err}
	for _, line := range h.lines()[len(before):] {
		var env connectorpb.ConnectorDataEnvelope
		if uErr := protojson.Unmarshal([]byte(line), &env); uErr != nil {
			h.t.Fatalf("%s/%s: decode envelope: %v", connector, op, uErr)
		}
		run.events = append(run.events, env.GetEvents()...)
	}
	return run
}

func (h *simHarness) lines() []string {
	h.t.Helper()
	f, err := os.Open(h.sink)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read-only
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	if err := sc.Err(); err != nil {
		h.t.Fatal(err)
	}
	return out
}

// dumpEvents writes each event's payload under TETRIZ_SCHEMA_DUMP, when set,
// one file per record operationId, for writing docs and schemas.
func dumpEvents(t *testing.T, connector, op string, events []*connectorpb.ConnectorDataEvent) {
	dir := os.Getenv("TETRIZ_SCHEMA_DUMP")
	if dir == "" {
		return
	}
	byOp := map[string][]map[string]any{}
	for _, e := range events {
		var payload any
		_ = json.Unmarshal(e.GetPayload(), &payload)
		byOp[e.GetOperationId()] = append(byOp[e.GetOperationId()], map[string]any{
			"ranOperation": op,
			"externalId":   e.GetExternalId(),
			"dataDate":     e.GetDataDate().AsTime(),
			"hasDataDate":  e.GetDataDate() != nil,
			"metadata":     e.GetMetadata(),
			"payload":      payload,
		})
	}
	for recOp, recs := range byOp {
		b, _ := json.MarshalIndent(recs, "", "  ")
		p := filepath.Join(dir, connector, op+"--"+recOp+".json")
		_ = os.MkdirAll(filepath.Dir(p), 0o750)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Logf("dump %s: %v", p, err)
		}
	}
}

var simVendors = []simVendor{
	{
		connector: "github",
		settings:  `{"organization":"demo-gh-startup"}`,
		first:     []string{"fetch_repos"},
	},
	{
		connector: "gitlab",
		settings:  `{"group_id":91002}`,
		first:     []string{"fetch_projects", "fetch_repos"},
	},
	{
		connector: "bitbucket",
		settings:  `{"workspace":"demo-bb-startup"}`,
		first:     []string{"fetch_repos"},
	},
	{
		connector: "jira",
		settings:  `{"cloud_id":"demo-bb-startup"}`,
		first:     []string{"fetch_projects"},
	},
}

// TestSimulatedPayloadsMatchSchemas runs every operation of the GitHub,
// GitLab, Bitbucket and Jira connectors against the seeded simulator and
// validates every emitted payload against its schema.
func TestSimulatedPayloadsMatchSchemas(t *testing.T) {
	h := newSimHarness(t)
	reg := app.NewRegistry(nil)
	for _, v := range simVendors {
		t.Run(v.connector, func(t *testing.T) {
			h.put(v.connector, v.settings)
			c, err := reg.Get(sdk.ConnectorID(v.connector))
			if err != nil {
				t.Fatal(err)
			}
			order := append([]string(nil), v.first...)
			for _, op := range c.Operations() {
				if !contains(v.first, string(op.ID)) {
					order = append(order, string(op.ID))
				}
			}
			for _, op := range order {
				run := h.sync(v.connector, op)
				dumpEvents(t, v.connector, op, run.events)
				if testing.Verbose() {
					t.Logf("%s/%s: status=%s records=%d err=%v", v.connector, op, run.result.Status, len(run.events), run.err)
				}
				checkRun(t, v, op, run)
			}
		})
	}
}

func checkRun(t *testing.T, v simVendor, op string, run simRun) {
	t.Helper()
	if reason, gap := simGaps[v.connector][op]; gap {
		if run.err == nil {
			t.Errorf("%s/%s is listed as a simulator gap (%s) but succeeded: move it off the list", v.connector, op, reason)
		}
		return
	}
	if run.err != nil {
		t.Errorf("%s/%s: run failed: %v", v.connector, op, run.err)
		return
	}
	reason, empty := simEmpty[v.connector][op]
	_, noRecs := noRecords[v.connector][op]
	switch {
	case (empty || noRecs) && len(run.events) > 0:
		t.Errorf("%s/%s emitted %d records but is listed as emitting none (%s%s)", v.connector, op, len(run.events), reason, noRecords[v.connector][op])
	case !empty && !noRecs && len(run.events) == 0:
		t.Errorf("%s/%s emitted no records against the simulator: seed data for it, or list it with a reason", v.connector, op)
	}
	allowed := emits[v.connector][op]
	if allowed == nil {
		allowed = []string{op}
	}
	failed := 0
	for _, e := range run.events {
		if e.GetConnectorId() != v.connector {
			t.Errorf("%s/%s: record %s has connectorId %q", v.connector, op, e.GetExternalId(), e.GetConnectorId())
		}
		if !contains(allowed, e.GetOperationId()) {
			t.Errorf("%s/%s: record %s has operationId %q, want one of %v (declareEmits)", v.connector, op, e.GetExternalId(), e.GetOperationId(), allowed)
		}
		if e.GetExternalId() == "" {
			t.Errorf("%s/%s: a record has no externalId", v.connector, op)
		}
		if err := schemas.Validate(v.connector, e.GetOperationId(), e.GetPayload()); err != nil {
			failed++
			if failed <= maxReported {
				t.Errorf("%s/%s: record %s: %v", v.connector, op, e.GetExternalId(), err)
			}
		}
	}
	if failed > maxReported {
		t.Errorf("%s/%s: %d more records failed validation", v.connector, op, failed-maxReported)
	}
}

// maxReported caps the validation failures reported per operation.
const maxReported = 5

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

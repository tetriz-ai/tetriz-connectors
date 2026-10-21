// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/connectors/github"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

const (
	orgA = "00000000-0000-4000-8000-00000000a001"
	orgB = "00000000-0000-4000-8000-00000000b002"
)

func writeKindsFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "tenant-kinds")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func directoryFromEnv(t *testing.T, env map[string]string) (StaticTenantDirectory, error) {
	t.Helper()
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	for k, v := range env {
		t.Setenv(k, v)
	}
	t.Cleanup(func() { tenantlabel.SetLiveIndividualSet(nil) })
	cfg, err := FromEnv()
	if err != nil {
		return StaticTenantDirectory{}, err
	}
	return NewTenantDirectory(cfg)
}

// The runner's tenant directory defaults to organization, and
// TETRIZ_DEFAULT_TENANT_KIND and TETRIZ_TENANT_KINDS_FILE override it.
func TestTenantDirectoryFromEnv(t *testing.T) {
	ctx := context.Background()
	kind := func(d StaticTenantDirectory, org string) sdk.TenantKind {
		k, err := d.TenantType(ctx, org)
		require.NoError(t, err)
		return k
	}
	t.Run("default", func(t *testing.T) {
		d, err := directoryFromEnv(t, nil)
		require.NoError(t, err)
		assert.Equal(t, sdk.TenantKindOrganization, kind(d, orgA))
	})
	t.Run("zero config", func(t *testing.T) {
		d, err := NewTenantDirectory(Config{})
		require.NoError(t, err)
		assert.Equal(t, sdk.TenantKindOrganization, kind(d, orgA), "a host that builds Config itself gets organization")
	})
	t.Run("default kind from the environment", func(t *testing.T) {
		d, err := directoryFromEnv(t, map[string]string{"TETRIZ_DEFAULT_TENANT_KIND": "individual"})
		require.NoError(t, err)
		assert.Equal(t, sdk.TenantKindIndividual, kind(d, orgA))
	})
	t.Run("file overrides the default", func(t *testing.T) {
		d, err := directoryFromEnv(t, map[string]string{
			"TETRIZ_TENANT_KINDS_FILE": writeKindsFile(t, "# org kinds\n\n"+orgA+" individual\n"+orgB+"   organization\n"),
		})
		require.NoError(t, err)
		assert.Equal(t, sdk.TenantKindIndividual, kind(d, orgA))
		assert.Equal(t, sdk.TenantKindOrganization, kind(d, orgB))
		assert.Equal(t, sdk.TenantKindOrganization, kind(d, "00000000-0000-4000-8000-00000000c003"))
		assert.Contains(t, tenantlabel.LiveIndividualSet(), orgA, "the file's individuals are published")
		assert.NotContains(t, tenantlabel.LiveIndividualSet(), orgB)
	})
	t.Run("bad default kind", func(t *testing.T) {
		_, err := directoryFromEnv(t, map[string]string{"TETRIZ_DEFAULT_TENANT_KIND": "team"})
		assert.ErrorContains(t, err, "TETRIZ_DEFAULT_TENANT_KIND")
	})
	t.Run("bad file line", func(t *testing.T) {
		_, err := directoryFromEnv(t, map[string]string{"TETRIZ_TENANT_KINDS_FILE": writeKindsFile(t, orgA+" team\n")})
		assert.ErrorContains(t, err, ":1:")
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := directoryFromEnv(t, map[string]string{"TETRIZ_TENANT_KINDS_FILE": filepath.Join(t.TempDir(), "absent")})
		assert.ErrorContains(t, err, "tenant kinds file")
	})
}

// listingGateway answers every GitHub listing with an empty page and records
// which listing the operation asked for.
type listingGateway struct {
	mu    sync.Mutex
	calls []string
}

func (g *listingGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, io.ErrUnexpectedEOF
}
func (g *listingGateway) DoJSON(context.Context, string, string, any, any) error { return nil }
func (g *listingGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }
func (g *listingGateway) ForGroup(string) sdk.VendorGateway                      { return g }
func (g *listingGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, string(proto.MessageName(req).Name()))
	switch req.(type) {
	case *vgpb.ListOrgReposRequest:
		return &vgpb.ListOrgReposResponse{}, nil
	case *vgpb.ListInstallationReposRequest:
		return &vgpb.ListInstallationReposResponse{}, nil
	}
	return nil, io.ErrUnexpectedEOF
}

func stampedKind(t *testing.T, defs []schedule.ScheduleDefinition, org string, op sdk.OperationID) sdk.TenantKind {
	t.Helper()
	for _, d := range defs {
		var p connectorsource.ConnectorSyncPayload
		require.NoError(t, json.Unmarshal(d.Payload, &p))
		if p.OrgID == org && p.OperationID == op {
			return p.TenantType
		}
	}
	t.Fatalf("no schedule for %s %s", org, op)
	return sdk.TenantKindUnspecified
}

// A scheduled GitHub fetch_prs, stamped by the default tenant directory,
// passes the tenant guard and completes. For a GitHub App install, fetch_repos
// lists the endpoint the stamped kind selects.
func TestScheduledGitHubRunsWithTheDefaultDirectory(t *testing.T) {
	ctx := context.Background()
	d, err := directoryFromEnv(t, map[string]string{"TETRIZ_TENANT_KINDS_FILE": writeKindsFile(t, orgB+" individual\n")})
	require.NoError(t, err)
	mem := memstore.New()
	for _, org := range []string{orgA, orgB} {
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github",
			Credentials: json.RawMessage(`{"installation_id":"1"}`), Settings: json.RawMessage(`{"organization":"acme"}`), Enabled: true}))
	}
	reg := sdk.NewConnectorRegistry()
	reg.Register(github.NewConnector(nil, 0))
	defs, err := connectorsource.NewConnectorScheduleSource(mem, mem, reg, d, nil, nil).FetchSchedules(ctx)
	require.NoError(t, err)
	gw := &listingGateway{}
	orch := runner.NewOrchestrator(reg, mem, mem, mem, sink.NewChannelSink(), func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { return gw, nil })

	kindA := stampedKind(t, defs, orgA, "fetch_prs")
	assert.Equal(t, sdk.TenantKindOrganization, kindA)
	require.NoError(t, orch.RunOperation(ctx, orgA, "github", "fetch_prs", kindA, "scheduled"))
	run, err := mem.GetLatest(ctx, orgA, "github", "fetch_prs")
	require.NoError(t, err)
	assert.Equal(t, "completed", string(run.Status))

	for org, want := range map[string]string{orgA: "ListOrgReposRequest", orgB: "ListInstallationReposRequest"} {
		gw.calls = nil
		k := stampedKind(t, defs, org, "fetch_repos")
		require.NoError(t, orch.RunOperation(ctx, org, "github", "fetch_repos", k, "scheduled"))
		assert.Contains(t, gw.calls, want, org)
	}
}

// yearly is the fake vendor with a schedule that does not fire during a test.
type yearly struct{ fakevendor.Connector }

func (y yearly) Operations() []sdk.OperationDescriptor {
	ops := y.Connector.Operations()
	for i := range ops {
		ops[i].DefaultSchedule = "0 0 1 1 *"
	}
	return ops
}

// lockedBuffer is a bytes.Buffer safe for concurrent writers.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The App's schedule source stamps each organization with the kind its
// tenant directory resolves, and a healthy serve start and shutdown log no
// error, from the runner or from asynq.
func TestServeStampsTenantKindsAndLogsNoError(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("serve", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	logs := &lockedBuffer{}
	logger.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logger.SetLogger(nil) })
	r, w, err := os.Pipe()
	require.NoError(t, err)
	stderr := os.Stderr
	os.Stderr = w
	asynqLogs := &lockedBuffer{}
	copied := make(chan struct{})
	go func() { _, _ = io.Copy(asynqLogs, r); close(copied) }()
	restore := func() {
		os.Stderr = stderr
		_ = w.Close()
		<-copied
	}

	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_TENANT_KINDS_FILE", writeKindsFile(t, orgB+" individual\n"))
	t.Cleanup(func() { tenantlabel.SetLiveIndividualSet(nil) })
	cfg, err := FromEnv()
	require.NoError(t, err)
	ctx := context.Background()
	a, err := New(ctx, cfg)
	require.NoError(t, err)
	defer a.Close() //nolint:errcheck // test cleanup
	a.Registry = sdk.NewConnectorRegistry()
	a.Registry.Register(yearly{})
	for _, org := range []string{orgA, orgB} {
		require.NoError(t, WriteConfig(ctx, store.PostgresTxRunner{}, a.Configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: fakevendor.ConnectorID,
			Credentials: json.RawMessage(`{"token":"t"}`), Settings: json.RawMessage(`{}`), Enabled: true}))
	}

	sctx := tenantdb.NewContext(ctx, tenantdb.SystemTenantID, tenantdb.ScopeSystem)
	defs, err := a.ScheduleSource().FetchSchedules(sctx)
	require.NoError(t, tenantdb.CommitOrRollback(sctx, err))
	assert.Equal(t, sdk.TenantKindOrganization, stampedKind(t, defs, orgA, fakevendor.OpFetchItems))
	assert.Equal(t, sdk.TenantKindIndividual, stampedKind(t, defs, orgB, fakevendor.OpFetchItems))

	sctxServe, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Serve(sctxServe) }()
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(logs.String(), "leader started with periodic task manager") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.Contains(t, logs.String(), "leader started with periodic task manager")
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(40 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	restore()

	assert.NotContains(t, logs.String(), "level=ERROR", "runner log:\n%s", logs.String())
	require.Contains(t, asynqLogs.String(), "Scheduler stopped", "asynq's log is captured")
	assert.NotContains(t, asynqLogs.String(), "ERROR", "asynq log:\n%s", asynqLogs.String())
}

// An individual default kind, with no file, skips the multi-user operations
// and routes the rest to the individual lane, the same as a file entry does.
func TestIndividualDefaultSkipsMultiUserOperations(t *testing.T) {
	ctx := context.Background()
	d, err := directoryFromEnv(t, map[string]string{"TETRIZ_DEFAULT_TENANT_KIND": "individual"})
	require.NoError(t, err)
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: orgA, ConnectorID: "github",
		Credentials: json.RawMessage(`{"installation_id":"1"}`), Settings: json.RawMessage(`{"organization":"acme"}`), Enabled: true}))
	reg := sdk.NewConnectorRegistry()
	reg.Register(github.NewConnector(nil, 0))
	defs, err := connectorsource.NewConnectorScheduleSource(mem, mem, reg, d, nil, nil).FetchSchedules(ctx)
	require.NoError(t, err)
	ops := map[sdk.OperationID]schedule.ScheduleDefinition{}
	for _, def := range defs {
		var p connectorsource.ConnectorSyncPayload
		require.NoError(t, json.Unmarshal(def.Payload, &p))
		ops[p.OperationID] = def
	}
	assert.NotContains(t, ops, sdk.OperationID("fetch_org_members"))
	assert.NotContains(t, ops, sdk.OperationID("fetch_copilot_usage"))
	require.Contains(t, ops, sdk.OperationID("fetch_repos"))
	assert.Equal(t, schedule.QueueIndividualConnectorSync, ops["fetch_repos"].Queue)
	assert.Equal(t, sdk.TenantKindIndividual, stampedKind(t, defs, orgA, "fetch_repos"))
}

// A schedule refresh over every shipped connector, with a VCS config each,
// logs no WARN or ERROR: the defaults that fire together are noted at DEBUG.
func TestScheduleRefreshLogsNoWarning(t *testing.T) {
	ctx := context.Background()
	logs := &lockedBuffer{}
	logger.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logger.SetLogger(nil) })
	d, err := directoryFromEnv(t, nil)
	require.NoError(t, err)
	mem := memstore.New()
	for _, c := range []sdk.ConnectorID{"github", "gitlab", "bitbucket"} {
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: orgA, ConnectorID: c,
			Credentials: json.RawMessage(`{"installation_id":"1"}`), Settings: json.RawMessage(`{"organization":"acme","workspace":"acme","group_id":"1"}`), Enabled: true}))
	}
	defs, err := connectorsource.NewConnectorScheduleSource(mem, mem, NewRegistry(nil), d, nil, nil).FetchSchedules(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, defs)
	assert.NotContains(t, logs.String(), "level=WARN", "log:\n%s", logs.String())
	assert.NotContains(t, logs.String(), "level=ERROR", "log:\n%s", logs.String())
	assert.Contains(t, logs.String(), "cannot be per-key staggered", "the drain_defect defaults are noted at DEBUG")
}

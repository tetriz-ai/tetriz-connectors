// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Under forced row-level security, serve's schedule read runs in the system
// scope, so a runner connected as the tables' owner schedules every
// organization's configs.
func TestServeSchedulesAsTableOwner(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("serve schedules as owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, dsn, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	a, err := New(ctx, Config{PostgresDSN: dsn, RedisAddr: miniredis.RunT(t).Addr(), AllowPlaintextCredentials: true,
		SinkPath: "log", BlobDir: t.TempDir(), Concurrency: 1, DefaultTenantKind: sdk.TenantKindOrganization, OAuth: map[string]OAuthClient{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	const org = "00000000-0000-4000-8000-0000000c0c01"
	require.NoError(t, WriteConfig(ctx, store.PostgresTxRunner{}, a.Configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github",
		Settings: []byte(`{"organization":"acme"}`), Credentials: []byte(`{"personal_access_token":"x"}`), Enabled: true, HealthState: sdk.HealthStateHealthy}))

	cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{a.ScheduleSource()}, time.Minute, sourceScope).GetConfigs()
	require.NoError(t, err)
	assert.NotEmpty(t, cfgs, "the stored config's operations are scheduled")
}

// serve wires no host services, so it never schedules an operation whose
// RequiresHost names one (a scheduled run would only fail), and it passes no
// content gate, so it never schedules fetch_file_content.
func TestServeWithholdsHostOnlyOperations(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("serve withholds host-only operations", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, dsn, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	a, err := New(ctx, Config{PostgresDSN: dsn, RedisAddr: miniredis.RunT(t).Addr(), AllowPlaintextCredentials: true,
		SinkPath: "log", BlobDir: t.TempDir(), Concurrency: 1, DefaultTenantKind: sdk.TenantKindOrganization, OAuth: map[string]OAuthClient{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	const org = "00000000-0000-4000-8000-0000000c0c02"
	for _, c := range []struct{ id, settings string }{
		{"github", `{"organization":"acme"}`},
		{"gitlab", `{"group_id":1,"group_path":"acme"}`},
		{"bitbucket", `{"workspace":"acme"}`},
	} {
		require.NoError(t, WriteConfig(ctx, store.PostgresTxRunner{}, a.Configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: sdk.ConnectorID(c.id),
			Settings: []byte(c.settings), Credentials: []byte(`{"personal_access_token":"x"}`), Enabled: true, HealthState: sdk.HealthStateHealthy}))
	}

	sctx, done := sourceScope(ctx)
	defs, err := a.ScheduleSource().FetchSchedules(sctx)
	require.NoError(t, done(err))
	scheduled := map[string]bool{}
	for _, d := range defs {
		scheduled[d.ID] = true
	}
	assert.True(t, scheduled[org+"/github/fetch_prs"], "ordinary operations are scheduled")
	for _, key := range []string{"github/fetch_pr_diffs", "bitbucket/fetch_pr_diffs", "gitlab/fetch_mr_diffs",
		"github/drain_defect_lineage", "gitlab/drain_defect_lineage", "bitbucket/drain_defect_lineage",
		"github/drain_defect_scoring", "gitlab/drain_defect_scoring", "bitbucket/drain_defect_scoring"} {
		assert.False(t, scheduled[org+"/"+key], "%s needs a host and is not scheduled", key)
	}
	// Every operation that declares a host service is withheld, so this test
	// covers a new one without a list to update.
	for _, conn := range a.Registry.List() {
		for _, op := range conn.Operations() {
			key := org + "/" + string(conn.ID()) + "/" + string(op.ID)
			if len(op.RequiresHost) > 0 || op.ID == vendorids.OpFetchFileContent {
				assert.False(t, scheduled[key], "%s needs a host or the content gate and is not scheduled", key)
			}
		}
	}
	assert.False(t, scheduled[org+"/github/fetch_file_content"], "fetch_file_content stays behind the content gate")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A GitHub App reinstall over a live row, as the table owner, writes the
// settings keys the install produces and keeps every other stored key and the
// operation overrides. A first install stores only the produced keys.
func TestReinstallKeepsOperatorSettingsAndOverrides(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("reinstall keeps settings", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs := store.NewConnectorConfigRepo(db, nil)
	ctrl := gomock.NewController(t)
	gh := intgithubmocks.NewMockGitHubAppClient(ctrl)
	gh.EXPECT().GetOrgAvatarURL(gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	tenants := sdkmocks.NewMockTenantDirectory(ctrl)
	const org = "00000000-0000-4000-8000-0000000c1c01"
	tenants.EXPECT().TenantType(gomock.Any(), org).Return(sdk.TenantKindOrganization, nil).AnyTimes()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	svc := NewService(Deps{Redis: rc, Tx: store.PostgresTxRunner{}, Cfg: testConfig(), Registry: testRegistry(),
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		GHClient: gh, Tenants: tenants})
	get := func() *sdk.ConnectorConfig {
		var out *sdk.ConnectorConfig
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			c, err := configs.Get(s, org, "github")
			out = c
			return err
		}))
		return out
	}
	install := func() {
		gh.EXPECT().GetInstallation(gomock.Any(), "12345").Return(&intgithub.InstallationInfo{ID: 12345, AccountLogin: "test-org", AccountID: 67890, AccountType: "Organization"}, nil)
		_, err := svc.SaveInstallation(ctx, &connintv1.SaveInstallationRequest{OrgId: org, InstallationId: "12345", ValidationToken: seedInstallationValidation(t, rc, org, "12345", 67890)})
		require.NoError(t, err)
	}

	install()
	first := get()
	assert.JSONEq(t, `{"organization":"test-org","include_private":true,"installation_id":"12345"}`, string(first.Settings), "a first install stores the produced keys")

	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(first.Settings, &m))
	m["repositories"] = json.RawMessage(`["test-org/core"]`)
	m["tracked_branch"] = json.RawMessage(`"develop"`)
	first.Settings, _ = json.Marshal(m)
	first.OperationOverrides = map[sdk.OperationID]json.RawMessage{"fetch_prs": json.RawMessage(`{"schedule":"0 * * * *"}`)}
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return configs.Save(s, first) }))

	install()
	got := get()
	assert.JSONEq(t, `{"organization":"test-org","include_private":true,"installation_id":"12345","repositories":["test-org/core"],"tracked_branch":"develop"}`, string(got.Settings),
		"the operator's settings survive the reinstall")
	assert.Contains(t, got.OperationOverrides, sdk.OperationID("fetch_prs"), "the override survives")
	assert.True(t, got.Enabled)
	assert.Equal(t, sdk.HealthStateHealthy, got.HealthState)
}

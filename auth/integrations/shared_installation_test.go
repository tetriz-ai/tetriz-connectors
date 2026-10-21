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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A second organization's claim on a GitHub App installation another holds is
// refused as AlreadyExists, and a "deleted" webhook for it disables the
// organization that holds it.
func TestSecondOrganizationCannotClaimAnInstallation(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("shared installation as the table owner", err)
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
	gh.EXPECT().GetInstallation(gomock.Any(), "777").Return(&intgithub.InstallationInfo{ID: 777, AccountLogin: "victim-org", AccountID: 1, AccountType: "Organization"}, nil).AnyTimes()
	tenants := sdkmocks.NewMockTenantDirectory(ctrl)
	tenants.EXPECT().TenantType(gomock.Any(), gomock.Any()).Return(sdk.TenantKindOrganization, nil).AnyTimes()
	cfg := testConfig()
	cfg.GitHubAppWebhookSecret = testWebhookSecret
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	svc := NewService(Deps{Redis: rc, Tx: store.PostgresTxRunner{}, Cfg: cfg, Registry: testRegistry(),
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		GHClient: gh, Tenants: tenants})
	const orgA, orgB = "00000000-0000-4000-8000-0000000d0a01", "00000000-0000-4000-8000-0000000d0b02"
	_, err = svc.SaveInstallation(ctx, &connintv1.SaveInstallationRequest{OrgId: orgA, InstallationId: "777", ValidationToken: seedInstallationValidation(t, rc, orgA, "777", 1)})
	require.NoError(t, err)
	_, err = svc.SaveInstallation(ctx, &connintv1.SaveInstallationRequest{OrgId: orgB, InstallationId: "777", ValidationToken: seedInstallationValidation(t, rc, orgB, "777", 1)})
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "err=%v", err)
	// A reinstall by the organization that holds it still succeeds.
	_, err = svc.SaveInstallation(ctx, &connintv1.SaveInstallationRequest{OrgId: orgA, InstallationId: "777", ValidationToken: seedInstallationValidation(t, rc, orgA, "777", 1)})
	require.NoError(t, err)
	enabled := func(org string) bool {
		var e bool
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			c, err := configs.Get(s, org, "github")
			if err == nil {
				e = c.Enabled
			}
			return err
		}))
		return e
	}
	require.True(t, enabled(orgA))
	// A row with no installation id in its settings names the same
	// installation in its credentials; the webhook must reach it too.
	unstamped, _ := json.Marshal(map[string]string{"installation_id": "777"})
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, orgB, store.ScopeEnforced, func(s context.Context) error {
		return configs.Save(s, &sdk.ConnectorConfig{OrganizationID: orgB, ConnectorID: "github", Credentials: unstamped, Settings: []byte(`{"organization":"victim-org"}`), Enabled: true})
	}))
	p := installationPayload("deleted", 777)
	_, err = svc.HandleWebhook(ctx, &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err)
	assert.False(t, enabled(orgA), "the holder is disabled when the installation is deleted")
	assert.False(t, enabled(orgB), "every row naming the installation is disabled")
}

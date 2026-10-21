// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// individualTenants is a tenant directory in which every organization is an
// individual tenant.
type individualTenants struct{}

func (individualTenants) TenantType(context.Context, string) (sdk.TenantKind, error) {
	return sdk.TenantKindIndividual, nil
}

// twoKeyStores is a Postgres database with a service reading under key A and
// a repository writing under key B, whose rows the service cannot decrypt.
func twoKeyStores(t *testing.T, tenants sdk.TenantDirectory) (*Service, *store.ConnectorConfigRepo, *store.ConnectorConfigRepo) {
	t.Helper()
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("webhook as the table owner", err)
		t.Skip("no test database")
	}
	db, _, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	encB, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	require.NoError(t, err)
	readable := store.NewConnectorConfigRepo(db, encA)
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: webhookConfig(), Registry: testRegistry(), Tenants: tenants,
		ConnectorDAO: readable, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	return svc, readable, store.NewConnectorConfigRepo(db, encB)
}

func saveRow(t *testing.T, repo *store.ConnectorConfigRepo, org, credsInstallation, settings string, enabled bool) {
	t.Helper()
	c := configWithInstallation(credsInstallation, enabled)
	c.OrganizationID, c.Settings = org, []byte(settings)
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(context.Background(), org, store.ScopeEnforced, func(s context.Context) error { return repo.Save(s, c) }))
}

func rowEnabled(t *testing.T, repo *store.ConnectorConfigRepo, org string) bool {
	t.Helper()
	var enabled bool
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(context.Background(), org, store.ScopeEnforced, func(s context.Context) error {
		c, err := repo.Get(s, org, "github")
		if err == nil {
			enabled = c.Enabled
		}
		return err
	}))
	return enabled
}

// A `deleted` event that matches a readable row while another row, which
// cannot be read and whose settings name no installation, may hold the same
// installation in its credentials: the readable row is disabled, and the event
// is refused (Unavailable) for redelivery rather than acknowledged.
func TestWebhookRefusesWhenAnUnreadableRowMayHoldTheInstallation(t *testing.T) {
	svc, readable, unreadable := twoKeyStores(t, allOrganizations{})
	const orgA, orgB = "00000000-0000-4000-8000-0000000c1c01", "00000000-0000-4000-8000-0000000c1c02"
	saveRow(t, readable, orgA, "6161", `{}`, true)
	saveRow(t, unreadable, orgB, "6161", `{}`, true)

	p := installationPayload("deleted", 6161)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	assert.Equal(t, codes.Unavailable, status.Code(err), "the event is refused: %v", err)
	assert.False(t, rowEnabled(t, readable, orgA), "the readable row is disabled")
}

// An unsuspend that matches an unreadable row on its settings applies the
// tenant gate like any other row: an individual tenant's row stays disabled.
func TestWebhookUnsuspendGatesAnUnreadableRow(t *testing.T) {
	svc, _, unreadable := twoKeyStores(t, individualTenants{})
	const org = "00000000-0000-4000-8000-0000000c1c03"
	saveRow(t, unreadable, org, "7171", `{"installation_id":"7171"}`, false)

	p := installationPayload("unsuspend", 7171)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err)
	assert.False(t, rowEnabled(t, unreadable, org), "an individual tenant's row stays disabled")
}

// Settings may hold the installation id as a JSON number: it still matches.
func TestWebhookMatchesANumericSettingsInstallationID(t *testing.T) {
	mem := memstore.New()
	const org = "00000000-0000-4000-8000-0000000c1c04"
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github",
		Settings: []byte(`{"installation_id":8888}`), Credentials: []byte(`{}`), Enabled: true}))
	svc := NewService(Deps{Cfg: webhookConfig(), Registry: testRegistry(), ConnectorDAO: mem, Tenants: allOrganizations{}})

	p := installationPayload("suspend", 8888)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err)
	got, err := mem.Get(context.Background(), org, "github")
	require.NoError(t, err)
	assert.False(t, got.Enabled)
}

// A replayed `deleted` event for a row it already disabled matches the row
// again and is acknowledged; the row stays disabled.
func TestWebhookReplayedDeletedIsAcknowledged(t *testing.T) {
	mem := memstore.New()
	const org = "00000000-0000-4000-8000-0000000c1c05"
	c := configWithInstallation("9191", true)
	c.OrganizationID = org
	require.NoError(t, mem.Save(context.Background(), c))
	svc := NewService(Deps{Cfg: webhookConfig(), Registry: testRegistry(), ConnectorDAO: mem, Tenants: allOrganizations{}})

	p := installationPayload("deleted", 9191)
	for i := range 2 {
		_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
		require.NoError(t, err, "delivery %d", i+1)
		got, err := mem.Get(context.Background(), org, "github")
		require.NoError(t, err)
		assert.False(t, got.Enabled, "delivery %d", i+1)
	}
}

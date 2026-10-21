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
)

// One unrelated organization's row that does not decrypt (written under
// another key), whose plain-text settings name another installation, does not
// stop an installation event for another organization: the row is ruled out,
// and the event is applied and acknowledged.
func TestWebhookSkipsARowThatDoesNotDecrypt(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("webhook as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	encB, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	require.NoError(t, err)
	configs := store.NewConnectorConfigRepo(db, encA)
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: webhookConfig(), Registry: testRegistry(),
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	const org = "00000000-0000-4000-8000-0000000c1b01"
	save := func(repo *store.ConnectorConfigRepo, o, inst string) {
		c := configWithInstallation(inst, true)
		c.OrganizationID = o
		c.Settings = []byte(`{"installation_id":"` + inst + `"}`)
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, o, store.ScopeEnforced, func(s context.Context) error { return repo.Save(s, c) }))
	}
	save(configs, org, "4242")
	save(store.NewConnectorConfigRepo(db, encB), "00000000-0000-4000-8000-0000000c1b02", "999")

	p := installationPayload("suspend", 4242)
	_, err = svc.HandleWebhook(ctx, &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err, "an unrelated row that does not decrypt must not fail the event")
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
		c, err := configs.Get(s, org, "github")
		if err == nil {
			assert.False(t, c.Enabled, "the suspend is applied")
		}
		return err
	}))
}

// A validly signed installation event whose body does not parse is rejected
// as InvalidArgument, not acknowledged.
func TestWebhookRejectsAnUnparseableInstallationEvent(t *testing.T) {
	svc := NewService(Deps{Cfg: webhookConfig(), Registry: testRegistry()})
	body := []byte(`{"action":`)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: body, SignatureHeader: signPayload(testWebhookSecret, body), EventType: "installation"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// When the installation's own row does not decrypt and its settings do not
// carry the installation id, the event matches no connector and is refused
// (Unavailable), not acknowledged, so the failed delivery shows in the App's
// settings and can be redelivered once the row reads again.
func TestWebhookRefusesAnEventWhoseRowCannotBeRead(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("webhook as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	encB, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	require.NoError(t, err)
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: webhookConfig(), Registry: testRegistry(),
		ConnectorDAO: store.NewConnectorConfigRepo(db, encA), SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	const org = "00000000-0000-4000-8000-0000000c1b03"
	other := store.NewConnectorConfigRepo(db, encB)
	c := configWithInstallation("4343", true)
	c.OrganizationID = org
	c.Settings = []byte(`{}`)
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return other.Save(s, c) }))

	p := installationPayload("suspend", 4343)
	_, err = svc.HandleWebhook(ctx, &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	assert.Equal(t, codes.Unavailable, status.Code(err), "the event is refused, not acknowledged")
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
		got, err := other.Get(s, org, "github")
		if err == nil {
			assert.True(t, got.Enabled, "the suspend is not applied")
		}
		return err
	}))
}

// A row whose credentials do not decrypt is still matched on the
// installation id its plain-text settings carry: a suspend disables it and
// the event is acknowledged.
func TestWebhookDisablesAnUnreadableRowItsSettingsName(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("webhook as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	encA, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	encB, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{2}, 32))
	require.NoError(t, err)
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: webhookConfig(), Registry: testRegistry(),
		ConnectorDAO: store.NewConnectorConfigRepo(db, encA), SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db)})
	const org = "00000000-0000-4000-8000-0000000c1b05"
	other := store.NewConnectorConfigRepo(db, encB)
	c := configWithInstallation("4444", true)
	c.OrganizationID = org
	c.Settings = []byte(`{"installation_id":"4444"}`)
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return other.Save(s, c) }))

	for _, action := range []string{"suspend", "deleted"} {
		p := installationPayload(action, 4444)
		_, err = svc.HandleWebhook(ctx, &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
		require.NoError(t, err, action)
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			got, err := other.Get(s, org, "github")
			require.NoError(t, err)
			assert.False(t, got.Enabled, "%s disables the unreadable row", action)
			return other.SetEnabled(s, org, "github", true)
		}))
	}
}

// An installation event that matches no connector is refused (NotFound), not
// acknowledged, and nothing in the service retries it: GitHub records the
// failed delivery and does not send it again by itself, so it is applied
// only when someone redelivers it from the App's settings.
func TestWebhookRefusesAnEventForAnUnknownInstallation(t *testing.T) {
	mem := memstore.New()
	const org = "00000000-0000-4000-8000-0000000c1b04"
	c := configWithInstallation("5151", true)
	c.OrganizationID = org
	require.NoError(t, mem.Save(context.Background(), c))
	svc := NewService(Deps{Cfg: webhookConfig(), Registry: testRegistry(), ConnectorDAO: mem})

	p := installationPayload("suspend", 5150)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	assert.Equal(t, codes.NotFound, status.Code(err))
	got, err := mem.Get(context.Background(), org, "github")
	require.NoError(t, err)
	assert.True(t, got.Enabled, "an unmatched event changes nothing")

	// An action the service does not act on is acknowledged whether or not it matches.
	p = installationPayload("new_permissions_accepted", 5150)
	_, err = svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: p, SignatureHeader: signPayload(testWebhookSecret, p), EventType: "installation"})
	require.NoError(t, err)
}

// With no webhook secret configured, every request is rejected, including one
// whose HMAC is keyed with the empty string.
func TestUnsetWebhookSecretRejectsEveryRequest(t *testing.T) {
	cfg := webhookConfig()
	cfg.GitHubAppWebhookSecret = ""
	svc := NewService(Deps{Cfg: cfg, Registry: testRegistry()})
	body := installationPayload("deleted", 1)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{Body: body, SignatureHeader: signPayload("", body), EventType: "installation"})
	require.Error(t, err)
	assert.False(t, verifySignature("", body, signPayload("", body)))
}

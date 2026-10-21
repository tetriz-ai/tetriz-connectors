// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// writeAfterRead lands a concurrent write right after the service reads the
// github row, as a reconnect or a settings change would.
type writeAfterRead struct {
	store.IConnectorConfigDAO
	write func(ctx context.Context)
}

func (w *writeAfterRead) land(ctx context.Context) {
	if w.write != nil {
		f := w.write
		w.write = nil
		f(ctx)
	}
}

func (w *writeAfterRead) ListByConnector(ctx context.Context, id sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	out, err := w.IConnectorConfigDAO.ListByConnector(ctx, id)
	w.land(ctx)
	return out, err //nolint:wrapcheck // test double
}

func (w *writeAfterRead) Get(ctx context.Context, org string, id sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	out, err := w.IConnectorConfigDAO.Get(ctx, org, id)
	w.land(ctx)
	return out, err //nolint:wrapcheck // test double
}

const raceOrg = "00000000-0000-4000-8000-0000000f0e11"

func seedGitHub(t *testing.T, enabled bool) *memstore.Store {
	t.Helper()
	ms := memstore.New()
	require.NoError(t, ms.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: raceOrg, ConnectorID: "github", Enabled: enabled,
		Credentials: json.RawMessage(`{"installation_id":"4242"}`), Settings: json.RawMessage(`{"organization":"acme"}`)}))
	return ms
}

func addSettingsKey(t *testing.T, ms *memstore.Store) func(context.Context) {
	return func(ctx context.Context) {
		require.NoError(t, ms.PatchSettings(ctx, raceOrg, "github", func(json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"organization":"acme","repositories":["acme/core"]}`), nil
		}))
	}
}

func rotateCredentials(t *testing.T, ms *memstore.Store) func(context.Context) {
	return func(ctx context.Context) {
		require.NoError(t, ms.SaveCredentialsEnablePreservingSettings(ctx, &sdk.ConnectorConfig{OrganizationID: raceOrg, ConnectorID: "github",
			Enabled: true, Credentials: json.RawMessage(`{"installation_id":"4242","rotated":true}`), Settings: json.RawMessage(`{}`)}))
	}
}

func sendInstallationEvent(t *testing.T, svc *Service, action string) {
	t.Helper()
	body := []byte(`{"action":"` + action + `","installation":{"id":4242,"account":{"login":"acme"}}}`)
	m := hmac.New(sha256.New, []byte("test-secret"))
	m.Write(body)
	_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{
		Body: body, SignatureHeader: "sha256=" + hex.EncodeToString(m.Sum(nil)), EventType: "installation"})
	require.NoError(t, err)
}

func stored(t *testing.T, ms *memstore.Store) *sdk.ConnectorConfig {
	t.Helper()
	got, err := ms.Get(context.Background(), raceOrg, "github")
	require.NoError(t, err)
	return got
}

// The webhook suspend, deleted and unsuspend branches write only enabled: a
// settings write that lands after the service's read survives.
func TestWebhookEnabledWriteKeepsAConcurrentSettingsWrite(t *testing.T) {
	for _, tc := range []struct {
		action string
		seed   bool
		want   bool
	}{{"suspend", true, false}, {"deleted", true, false}, {"unsuspend", false, true}} {
		t.Run(tc.action, func(t *testing.T) {
			ms := seedGitHub(t, tc.seed)
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: &writeAfterRead{IConnectorConfigDAO: ms, write: addSettingsKey(t, ms)}, Registry: testRegistry(), Tenants: allOrganizations{}})
			sendInstallationEvent(t, svc, tc.action)
			got := stored(t, ms)
			assert.Equal(t, tc.want, got.Enabled)
			assert.Contains(t, string(got.Settings), "acme/core", "the concurrent settings write survives")
		})
	}
}

// The github-issues toggle writes only its settings key: a credential write
// that lands after the service's read survives.
func TestGitHubIssuesToggleKeepsAConcurrentCredentialWrite(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(map[bool]string{true: "connect", false: "disconnect"}[on], func(t *testing.T) {
			ms := seedGitHub(t, true)
			cfg := testConfig()
			cfg.EnabledConnectors["github-issues"] = true
			svc := NewService(Deps{Cfg: cfg, ConnectorDAO: &writeAfterRead{IConnectorConfigDAO: ms, write: rotateCredentials(t, ms)}, Registry: testRegistry()})
			var err error
			if on {
				_, err = svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{OrgId: raceOrg, ConnectorId: "github-issues"})
			} else {
				_, err = svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{OrgId: raceOrg, ConnectorId: "github-issues"})
			}
			require.NoError(t, err)
			got := stored(t, ms)
			assert.Contains(t, string(got.Credentials), "rotated", "the concurrent credential write survives")
			var settings map[string]any
			require.NoError(t, json.Unmarshal(got.Settings, &settings))
			assert.Equal(t, on, settings["github_issues_enabled"])
			assert.Equal(t, "acme", settings["organization"])
		})
	}
}

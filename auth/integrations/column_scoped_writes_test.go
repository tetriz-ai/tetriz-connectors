// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// UpdateVCSSettings writes only the VCS settings keys: a credential write that
// lands after the service reads the row survives.
func TestUpdateVCSSettingsKeepsAConcurrentCredentialWrite(t *testing.T) {
	ms := seedGitHub(t, true)
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: &writeAfterRead{IConnectorConfigDAO: ms, write: rotateCredentials(t, ms)}, Registry: testRegistry()})
	_, err := svc.UpdateVCSSettings(context.Background(), &connintv1.UpdateVCSSettingsRequest{OrgId: raceOrg, ConnectorId: "github",
		Settings: &connintv1.VCSSettings{TrackedBranch: "develop"}})
	require.NoError(t, err)
	got := stored(t, ms)
	assert.Contains(t, string(got.Credentials), "rotated", "the concurrent credential write survives")
	var settings map[string]any
	require.NoError(t, json.Unmarshal(got.Settings, &settings))
	assert.Equal(t, "develop", settings["tracked_branch"])
	assert.Equal(t, "acme", settings["organization"])
}

// failingDelete makes the disconnect's soft delete fail, so what the
// credential wipe wrote stays visible.
type failingDelete struct{ *writeAfterRead }

func (f failingDelete) Delete(context.Context, string, sdk.ConnectorID) error {
	return errors.New("db down")
}

// The disconnect's credential wipe writes only the credentials it read: a
// reconnect that lands after the read keeps its credentials and settings.
func TestDisconnectWipeKeepsAConcurrentReconnect(t *testing.T) {
	ctx := context.Background()
	ms := memstore.New()
	require.NoError(t, ms.Save(ctx, &sdk.ConnectorConfig{OrganizationID: raceOrg, ConnectorID: "bitbucket", Enabled: true,
		Credentials: json.RawMessage(`{"oauth_refresh_token":"old"}`), Settings: json.RawMessage(`{"workspace":"acme"}`)}))
	reconnect := func(ctx context.Context) {
		require.NoError(t, ms.SaveCredentialsEnableWithSettings(ctx, &sdk.ConnectorConfig{OrganizationID: raceOrg, ConnectorID: "bitbucket",
			Enabled: true, Credentials: json.RawMessage(`{"oauth_refresh_token":"new"}`)},
			func(json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage(`{"workspace":"acme","repositories":["acme/core"]}`), nil
			}))
		_, err := ms.ResetHealthState(ctx, raceOrg, "bitbucket")
		require.NoError(t, err)
	}
	dao := failingDelete{&writeAfterRead{IConnectorConfigDAO: ms, write: reconnect}}
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})
	_, err := svc.DisconnectIntegration(ctx, &connintv1.DisconnectIntegrationRequest{OrgId: raceOrg, ConnectorId: "bitbucket"})
	require.Error(t, err, "the soft delete failed")
	got, err := ms.Get(ctx, raceOrg, "bitbucket")
	require.NoError(t, err)
	assert.JSONEq(t, `{"oauth_refresh_token":"new"}`, string(got.Credentials), "the reconnect's credentials survive")
	assert.Contains(t, string(got.Settings), "acme/core", "the reconnect's settings survive")
}

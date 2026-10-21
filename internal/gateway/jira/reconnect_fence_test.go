// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jiragateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A reconnect's credential write advances the credential fence in the same
// statement, so a refresh whose swap lands between the connect flow's save
// and its later health reset fails its fence, and the reconnect's grant
// stands.
func TestRefreshBetweenConnectSaveAndResetKeepsTheReconnect(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	limSeed(t, mem)
	fresh, err := json.Marshal(jiraconfig.JiraCredentials{OAuthRefreshToken: "account-B-grant"})
	require.NoError(t, err)
	srv := raceTokenServer(t, func() {
		require.NoError(t, mem.SaveCredentialsEnableWithSettings(ctx, &sdk.ConnectorConfig{OrganizationID: limOrg, ConnectorID: "jira", Enabled: true,
			Credentials: fresh}, func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"cloud_id":"cloud-B"}`), nil }))
	})
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})

	_, err = limDoer(rc, mem, srv.URL).refreshToken(ctx)
	require.NoError(t, err)
	_, err = mem.ResetHealthState(ctx, limOrg, "jira") // resetConnectorHealth, after the save
	require.NoError(t, err)

	cfg, token := storedRefreshToken(t, mem)
	require.Equal(t, "account-B-grant", token, "the reconnect's grant must survive")
	require.JSONEq(t, `{"cloud_id":"cloud-B"}`, string(cfg.Settings))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineargateway

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A reconnect (a fresh grant) that lands during the refresh exchange stands:
// the old grant's rotated tokens are dropped, the reconnect's credentials,
// settings and enabled flag are kept, and the exchanged access token is not
// cached.
func TestRefreshKeepsAReconnectThatLandsDuringTheExchange(t *testing.T) {
	rc := newTestRedis(t)
	var hits int
	srv := refreshServer(t, "old-grant-access-2", "old-grant-rotated", &hits)
	fresh, err := json.Marshal(linearconfig.LinearCredentials{AccessToken: "fresh-access", RefreshToken: "fresh-grant"})
	require.NoError(t, err)
	live := liveCfg(t)
	live.CredentialsUpdatedAt = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	reconnected := &sdk.ConnectorConfig{ConnectorID: "linear", OrganizationID: testOrgID, Enabled: true,
		Credentials: fresh, Settings: json.RawMessage(`{"team_ids":["T2"]}`),
		CredentialsUpdatedAt: live.CredentialsUpdatedAt.Add(time.Second)} // a credential write advances the fence
	var saved *sdk.ConnectorConfig
	creds := &mockConfigStore{
		getQueue: []getResult{
			{cfg: live},        // the refresh token read before the exchange
			{cfg: reconnected}, // the persist re-read: the reconnect landed
		},
		saveFn: func(c *sdk.ConnectorConfig) { saved = c },
	}
	d := newTestDoer(&sequenceDoer{codes: []int{200}}, rc, creds, testEnc, srv.URL)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.linear.app/graphql", nil)
	_, err = d.Do(context.Background(), req)
	require.NoError(t, err)

	if saved != nil {
		assert.Contains(t, string(saved.Credentials), "fresh-grant", "the reconnect's credentials survive; saved=%s", saved.Credentials)
	}
	assert.JSONEq(t, string(fresh), string(reconnected.Credentials), "the reconnect's credentials survive")
	assert.JSONEq(t, `{"team_ids":["T2"]}`, string(reconnected.Settings), "the reconnect's settings are untouched")
	assert.True(t, reconnected.Enabled, "the reconnect's enabled flag is untouched")
	n, err := rc.Exists(context.Background(), linearconfig.AccessTokenKey(testOrgID)).Result()
	require.NoError(t, err)
	assert.Zero(t, n, "the dropped grant's access token is not cached")
}

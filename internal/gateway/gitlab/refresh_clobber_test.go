// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package glgateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A reconnect (fresh grant, new settings) or an operator disable that lands
// while a scheduled run is refreshing stands: the refresher swaps only the
// credentials, and only if no other credential write landed first.
func TestRefreshDoesNotClobberConcurrentWrites(t *testing.T) {
	for _, tc := range []string{"reconnect", "disable"} {
		t.Run(tc, func(t *testing.T) {
			mem := memstore.New()
			limSeed(t, mem)
			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				ctx := context.Background()
				if tc == "reconnect" {
					creds, _ := json.Marshal(glconfig.GitLabCredentials{OAuthRefreshToken: "fresh-grant"})
					// what app.WriteConfig and the connect flows do: Save, then ResetHealthState
					require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{
						OrganizationID: limOrg, ConnectorID: "gitlab", Enabled: true, Credentials: creds,
						Settings: json.RawMessage(`{"group_id":2}`)}))
					_, err := mem.ResetHealthState(ctx, limOrg, "gitlab")
					require.NoError(t, err)
				} else {
					// what config put --disabled (app.UpdateSettings) does
					cur, err := mem.Get(ctx, limOrg, "gitlab")
					require.NoError(t, err)
					cur.Enabled = false
					require.NoError(t, mem.Save(ctx, cur))
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "a-2", "refresh_token": "old-grant-rotated", "expires_in": 3600, "token_type": "Bearer"})
			}))
			defer tokenServer.Close()
			rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
			_, err := limDoer(rc, mem, tokenServer.URL).refreshToken(context.Background())
			require.NoError(t, err)
			cfg, err := mem.Get(context.Background(), limOrg, "gitlab")
			require.NoError(t, err)
			if tc == "reconnect" {
				assert.Contains(t, string(cfg.Credentials), "fresh-grant", "the reconnect's credentials survive")
				assert.JSONEq(t, `{"group_id":2}`, string(cfg.Settings), "the reconnect's settings survive")
			} else {
				assert.False(t, cfg.Enabled, "the operator's disable survives")
			}
		})
	}
}

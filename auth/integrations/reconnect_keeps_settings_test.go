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

	intjira "github.com/tetriz-ai/tetriz-connectors/auth/integrations/jira"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	jiramocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/jira"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A Jira or GitLab reconnect over a live row writes the settings keys the
// connect flow produces and keeps every other stored key and the operation
// overrides. A first connect stores only the produced keys.
func TestReconnectKeepsOperatorSettingsAndOverrides(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("reconnect keeps settings", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs := store.NewConnectorConfigRepo(db, nil)
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	ctrl := gomock.NewController(t)
	jc := jiramocks.NewMockOAuthClient(ctrl)
	gl := NewMockGitLabOAuthClient(ctrl)
	cfg := testConfig()
	cfg.JiraOAuth = &connectorconfig.JiraOAuthConfig{ClientID: "c", CallbackURL: "http://x/cb"}
	cfg.GitLabOAuth = &connectorconfig.GitLabOAuthConfig{ClientID: "c", CallbackURL: "http://x/cb"}
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: cfg, Registry: testRegistry(), Redis: rc,
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		JiraClient: jc, GLClient: gl})
	get := func(org string, id sdk.ConnectorID) *sdk.ConnectorConfig {
		var out *sdk.ConnectorConfig
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			c, err := configs.Get(s, org, id)
			out = c
			return err
		}))
		return out
	}
	// The operator adds a setting of its own and a schedule override.
	customize := func(org string, id sdk.ConnectorID, key, value string, op sdk.OperationID) {
		c := get(org, id)
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(c.Settings, &m))
		m[key] = json.RawMessage(value)
		c.Settings, _ = json.Marshal(m)
		c.OperationOverrides = map[sdk.OperationID]json.RawMessage{op: json.RawMessage(`{"schedule":"0 * * * *"}`)}
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return configs.Save(s, c) }))
	}

	t.Run("jira", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000e0a01"
		connect := func(rt string) {
			state := "s" + rt
			require.NoError(t, rc.Set(ctx, jiraconfig.OAuthStateKey(state), `{"org_id":"`+org+`"}`, 0).Err())
			jc.EXPECT().ExchangeAuthCode(gomock.Any(), "code").Return(&intjira.TokenResponse{AccessToken: "a", RefreshToken: rt}, nil)
			jc.EXPECT().FetchAccessibleResources(gomock.Any(), "a").Return([]intjira.CloudSite{{ID: "cloud-1", Name: "n", URL: "u"}}, nil)
			jc.EXPECT().FetchCurrentUser(gomock.Any(), "a").Return("me", nil)
			_, err := svc.ValidateAndSaveJiraConnection(ctx, &connintv1.ValidateAndSaveJiraConnectionRequest{OrgId: org, AuthCode: "code", State: state})
			require.NoError(t, err)
		}
		connect("rt-1")
		first := get(org, "jira")
		assert.JSONEq(t, `{"cloud_id":"cloud-1","site_url":"u","cloud_name":"n","connected_by_name":"me"}`, string(first.Settings), "a first connect stores the produced keys")

		customize(org, "jira", "project_keys", `["WORK"]`, "fetch_issues")
		connect("rt-2")
		got := get(org, "jira")
		assert.JSONEq(t, `{"cloud_id":"cloud-1","site_url":"u","cloud_name":"n","connected_by_name":"me","project_keys":["WORK"]}`, string(got.Settings))
		assert.Contains(t, got.OperationOverrides, sdk.OperationID("fetch_issues"), "the override survives")
		assert.Contains(t, string(got.Credentials), "rt-2")
		assert.True(t, got.Enabled)
		assert.Equal(t, sdk.HealthStateHealthy, got.HealthState)
	})

	t.Run("gitlab", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000e0a02"
		connect := func(rt string) {
			state := "s" + rt
			require.NoError(t, rc.Set(ctx, glconfig.OAuthStateKey(state), `{"org_id":"`+org+`"}`, 0).Err())
			gl.EXPECT().ExchangeAuthCode(gomock.Any(), "code").Return(&GitLabTokenResponse{AccessToken: "a", RefreshToken: rt}, nil)
			gl.EXPECT().FetchCurrentUser(gomock.Any(), "a").Return(&GitLabUser{ID: 1, Username: "u"}, nil)
			gl.EXPECT().FetchGroups(gomock.Any(), "a").Return([]GitLabGroup{{ID: 91002, Path: "p", FullPath: "p", Name: "n"}}, nil)
			_, err := svc.ValidateAndSaveGitLabConnection(ctx, &connintv1.ValidateAndSaveGitLabConnectionRequest{OrgId: org, AuthCode: "code", State: state})
			require.NoError(t, err)
		}
		connect("g-1")
		first := get(org, "gitlab")
		var produced map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(first.Settings, &produced))
		assert.Contains(t, produced, "group_id")
		assert.NotContains(t, produced, "repositories", "a first connect stores only the produced keys")

		customize(org, "gitlab", "repositories", `["p/widgets"]`, "fetch_prs")
		connect("g-2")
		got := get(org, "gitlab")
		var settings map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(got.Settings, &settings))
		assert.JSONEq(t, `["p/widgets"]`, string(settings["repositories"]), "the repository filter survives")
		assert.JSONEq(t, `91002`, string(settings["group_id"]))
		assert.Contains(t, got.OperationOverrides, sdk.OperationID("fetch_prs"), "the override survives")
		assert.Contains(t, string(got.Credentials), "g-2")
	})
}

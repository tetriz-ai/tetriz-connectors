// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	intlinear "github.com/tetriz-ai/tetriz-connectors/auth/integrations/linear"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intlinearmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/linear"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fakeSlackCommsAPI answers the two Slack Web API calls the slack_comms
// connect makes: the code exchange (a bot token, bot per call) and users.info
// (an admin installer).
func fakeSlackCommsAPI(t *testing.T, bot *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth.v2.access":
			_, _ = fmt.Fprintf(w, `{"ok":true,"access_token":%q,"scope":"chat:write","team":{"id":"T1","name":"Acme"},"authed_user":{"id":"U1"}}`, *bot)
		case "/users.info":
			_, _ = w.Write([]byte(`{"ok":true,"user":{"is_admin":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A Bitbucket, Linear or slack_comms reconnect over a
// live row, as the table owner, keeps every settings key the flow does not
// produce and the operation overrides, as Jira and GitLab do.
func TestReconnectKeepsOperatorSettingsAndOverridesInEveryFlow(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("reconnect keeps settings in every flow", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	configs := store.NewConnectorConfigRepo(db, nil)
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	ctrl := gomock.NewController(t)
	bb := NewMockBitbucketOAuthClient(ctrl)
	lin := intlinearmocks.NewMockOAuthClient(ctrl)
	bot := "xoxb-1"
	cfg := testLinearConfig()
	cfg.BitbucketOAuth = &connectorconfig.BitbucketOAuthConfig{ClientID: "c", CallbackURL: "http://x/cb"}
	cfg.EnabledConnectors[connectorIDSlackComms] = true
	cfg.SlackCommsAppClientSecret = "fake-secret" // a placeholder for the fake Slack API
	cfg.SlackCommsApp = &connectorconfig.SlackAppStaticConfig{ClientID: "c", RedirectURI: "http://x/cb", APIBaseURL: fakeSlackCommsAPI(t, &bot).URL}
	svc := NewService(Deps{Tx: store.PostgresTxRunner{}, Cfg: cfg, Registry: testRegistry(), Redis: rc,
		ConnectorDAO: configs, SyncStateDAO: store.NewSyncStateRepo(db), SyncRunDAO: store.NewSyncRunRepo(db),
		BBClient: bb, LinearClient: lin})
	get := func(org string, id sdk.ConnectorID) *sdk.ConnectorConfig {
		var out *sdk.ConnectorConfig
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error {
			c, err := configs.Get(s, org, id)
			out = c
			return err
		}))
		return out
	}
	// The operator adds settings of its own and a schedule override.
	customize := func(org string, id sdk.ConnectorID, keys map[string]string, op sdk.OperationID) {
		c := get(org, id)
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(c.Settings, &m))
		for k, v := range keys {
			m[k] = json.RawMessage(v)
		}
		c.Settings, _ = json.Marshal(m)
		c.OperationOverrides = map[sdk.OperationID]json.RawMessage{op: json.RawMessage(`{"schedule":"0 * * * *"}`)}
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) error { return configs.Save(s, c) }))
	}
	check := func(t *testing.T, org string, id sdk.ConnectorID, keys map[string]string, op sdk.OperationID, credential string) {
		t.Helper()
		got := get(org, id)
		var settings map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(got.Settings, &settings))
		for k, v := range keys {
			assert.JSONEq(t, v, string(settings[k]), "the operator's %s survives; settings=%s", k, got.Settings)
		}
		assert.Contains(t, got.OperationOverrides, op, "the override survives")
		assert.Contains(t, string(got.Credentials), credential, "the reconnect's credentials are stored")
		assert.True(t, got.Enabled)
		assert.Equal(t, sdk.HealthStateHealthy, got.HealthState)
	}

	t.Run("bitbucket", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000b0b01"
		connect := func(rt string) {
			state := "s" + rt
			require.NoError(t, rc.Set(ctx, bbconfig.OAuthStateKey(state), `{"org_id":"`+org+`"}`, 0).Err())
			bb.EXPECT().ExchangeAuthCode(gomock.Any(), "code").Return(&BitbucketTokenResponse{AccessToken: "a", RefreshToken: rt}, nil)
			bb.EXPECT().FetchCurrentUser(gomock.Any(), "a").Return(&BitbucketUser{UUID: "{u}", DisplayName: "me"}, nil)
			bb.EXPECT().FetchWorkspaces(gomock.Any(), "a").Return([]BitbucketWorkspace{{UUID: "{w}", Slug: "acme", Name: "Acme"}}, nil)
			_, err := svc.ValidateAndSaveBitbucketConnection(ctx, &connintv1.ValidateAndSaveBitbucketConnectionRequest{OrgId: org, AuthCode: "code", State: state})
			require.NoError(t, err)
		}
		connect("bb-1")
		keys := map[string]string{"repositories": `["acme/core"]`, "tracked_branch": `"develop"`}
		customize(org, "bitbucket", keys, "fetch_prs")
		connect("bb-2")
		keys["workspace"] = `"acme"`
		check(t, org, "bitbucket", keys, "fetch_prs", "bb-2")
	})

	t.Run("linear", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000b0b03"
		connect := func(rt string) {
			state := "s" + rt
			require.NoError(t, rc.Set(ctx, linearconfig.OAuthStateKey(state), `{"org_id":"`+org+`"}`, 0).Err())
			lin.EXPECT().ExchangeAuthCode(gomock.Any(), "code").Return(&intlinear.TokenResponse{AccessToken: "a", RefreshToken: rt}, nil)
			lin.EXPECT().FetchWorkspaceInfo(gomock.Any(), "a").Return(&intlinear.WorkspaceInfo{ID: "ws-1", Name: "Acme", URLKey: "acme"}, nil)
			lin.EXPECT().FetchCurrentUser(gomock.Any(), "a").Return("Sam", nil)
			_, err := svc.ValidateAndSaveLinearConnection(ctx, &connintv1.ValidateAndSaveLinearConnectionRequest{OrgId: org, Code: "code", State: state})
			require.NoError(t, err)
		}
		connect("l-1")
		keys := map[string]string{"team_ids": `["T2"]`}
		customize(org, "linear", keys, "fetch_issues")
		connect("l-2")
		keys["workspace_id"] = `"ws-1"`
		check(t, org, "linear", keys, "fetch_issues", "l-2")
	})

	t.Run("slack_comms", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000b0b05"
		connect := func(tok string) {
			bot = tok
			state := "s" + tok
			require.NoError(t, rc.Set(ctx, slackOAuthStateKey(state), `{"org_id":"`+org+`","connector_id":"`+connectorIDSlackComms+`"}`, 0).Err())
			_, err := svc.ValidateAndSaveSlackConnection(ctx, &connintv1.ValidateAndSaveSlackConnectionRequest{OrgId: org, Code: "code", State: state})
			require.NoError(t, err)
		}
		connect("xoxb-1")
		keys := map[string]string{"default_channel": `"C9"`}
		customize(org, connectorIDSlackComms, keys, "send_message")
		connect("xoxb-2")
		keys["team_name"] = `"Acme"`
		check(t, org, connectorIDSlackComms, keys, "send_message", "xoxb-2")
	})
}

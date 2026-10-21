// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intlinearmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/linear"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/internal/tokencache"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A credential write forgets the cached access token under the runner's
// namespace, which is where a namespaced runner caches it.
func TestResetConnectorHealthForgetsTheNamespacedToken(t *testing.T) {
	for _, spelling := range []string{"lower", "upper"} {
		t.Run(spelling, func(t *testing.T) { resetForgetsTheNamespacedToken(t, spelling == "upper") })
	}
}

func resetForgetsTheNamespacedToken(t *testing.T, upper bool) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	const org = "00000000-0000-4000-8000-0000000a5e01"
	key, ok := tokencache.Key("jira", org)
	require.True(t, ok)
	require.NoError(t, mr.Set(schedule.Namespaced("ns", key), "token"))
	require.NoError(t, mr.Set(key, "other-deployment"))

	dao := daomocks.NewMockIConnectorConfigDAO(gomock.NewController(t))
	dao.EXPECT().ResetHealthState(gomock.Any(), org, sdk.ConnectorID("jira")).Return(false, nil)
	s := &Service{connectorDAO: dao, redis: rdb, namespace: "ns"}
	asked := org
	if upper {
		asked = strings.ToUpper(org)
	}
	s.resetConnectorHealth(context.Background(), asked, "jira")

	assert.False(t, mr.Exists(schedule.Namespaced("ns", key)), "the namespaced token is forgotten")
	assert.True(t, mr.Exists(key), "another deployment's unprefixed token is left alone")
}

// Disconnecting a connector whose gateway caches an access token deletes the
// token under the runner's namespace, where a namespaced runner caches it,
// and leaves another deployment's unprefixed key alone.
func TestDisconnectForgetsTheNamespacedToken(t *testing.T) {
	for _, tc := range []struct{ connectorID, spelling string }{
		{"jira", "lower"}, {"bitbucket", "lower"}, {"gitlab", "lower"}, {"linear", "lower"},
		{"jira", "upper"}, {"bitbucket", "upper"}, {"gitlab", "upper"}, {"linear", "upper"},
	} {
		connectorID := tc.connectorID
		t.Run(connectorID+"/"+tc.spelling, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			const org = "00000000-0000-4000-8000-0000000a5e02"
			key, ok := tokencache.Key(sdk.ConnectorID(connectorID), org)
			require.True(t, ok)
			require.NoError(t, mr.Set(schedule.Namespaced("ns", key), "token"))
			require.NoError(t, mr.Set(key, "other-deployment"))

			mem := memstore.New()
			require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: sdk.ConnectorID(connectorID),
				Enabled: true, Credentials: []byte(`{}`)}))
			ctrl := gomock.NewController(t)
			linear := intlinearmocks.NewMockOAuthClient(ctrl)
			linear.EXPECT().RevokeToken(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: mem, SyncStateDAO: mem, SyncRunDAO: mem, Redis: rdb, Namespace: "ns",
				Registry: testRegistry(), LinearClient: linear})
			asked := org
			if tc.spelling == "upper" {
				asked = strings.ToUpper(org)
			}
			_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{OrgId: asked, ConnectorId: connectorID})
			require.NoError(t, err)

			assert.False(t, mr.Exists(schedule.Namespaced("ns", key)), "the namespaced token is forgotten")
			assert.True(t, mr.Exists(key), "another deployment's unprefixed token is left alone")
		})
	}
}

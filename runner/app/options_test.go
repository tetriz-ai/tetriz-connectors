// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/inprocess"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// thingsConnector is a connector of a host's own.
type thingsConnector struct{ id sdk.ConnectorID }

func (c thingsConnector) ID() sdk.ConnectorID                                      { return c.id }
func (thingsConnector) DisplayName() string                                        { return "Things" }
func (thingsConnector) Categories() []sdk.ConnectorCategory                        { return nil }
func (thingsConnector) ValidateConfig(context.Context, *sdk.ConnectorConfig) error { return nil }
func (thingsConnector) Operations() []sdk.OperationDescriptor {
	return []sdk.OperationDescriptor{{ID: "fetch_things", DefaultSchedule: "0 * * * *", IngestionType: sdk.IngestionPull,
		Factory: func() sdk.Operation { return nil }}}
}

type stubGateway struct{ sdk.VendorGateway }

func stubBuilder(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
	return stubGateway{}, nil
}

// An added connector joins the built-in ones; one whose id is a built-in's,
// or another added one's, or that has no gateway builder, is refused.
func TestNewRegistryWith(t *testing.T) {
	r, err := NewRegistryWith(nil, WithConnector(thingsConnector{"things"}, stubBuilder))
	require.NoError(t, err)
	_, err = r.Get("things")
	require.NoError(t, err)
	_, err = r.Get("github")
	require.NoError(t, err)

	_, err = NewRegistryWith(nil, WithConnector(thingsConnector{"github"}, stubBuilder))
	require.ErrorIs(t, err, ErrConnectorCollision)
	_, err = NewRegistryWith(nil, WithConnector(thingsConnector{"things"}, stubBuilder), WithConnector(thingsConnector{"things"}, stubBuilder))
	require.ErrorIs(t, err, ErrConnectorCollision)
	_, err = NewRegistryWith(nil, WithConnector(thingsConnector{"things"}, nil))
	require.ErrorContains(t, err, "no gateway builder")
}

// The factory New builds sends an added connector to its builder, every
// built-in one to the built-in factory, and applies the wrappers in order.
func TestGatewayFactoryRoutesAddedConnectors(t *testing.T) {
	builtinCalls := 0
	builtin := func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		builtinCalls++
		return nil, errors.New("built-in")
	}
	var order []string
	wrap := func(name string) func(sdk.VendorGatewayFactory) sdk.VendorGatewayFactory {
		return func(next sdk.VendorGatewayFactory) sdk.VendorGatewayFactory {
			return func(ctx context.Context, cc *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
				order = append(order, name)
				return next(ctx, cc)
			}
		}
	}
	f := CollectOptions(WithConnector(thingsConnector{"things"}, stubBuilder), WithGatewayWrapper(wrap("inner")), WithGatewayWrapper(wrap("outer"))).gatewayFactory(builtin)
	gw, err := f(context.Background(), &sdk.ConnectorConfig{ConnectorID: "things"})
	require.NoError(t, err)
	assert.IsType(t, stubGateway{}, gw)
	assert.Equal(t, 0, builtinCalls)
	_, err = f(context.Background(), &sdk.ConnectorConfig{ConnectorID: "github"})
	require.EqualError(t, err, "built-in")
	assert.Equal(t, 1, builtinCalls)
	assert.Equal(t, []string{"outer", "inner", "outer", "inner"}, order)
}

// The built-in gateway factory on in-process stores builds the gateways
// that keep state in Redis (GitHub, GitLab, Bitbucket, Jira) with no Redis.
func TestInProcessGatewayFactoryNeedsNoRedis(t *testing.T) {
	const org = "00000000-0000-4000-8000-0000000c0c10"
	cfg := gateway.DefaultConfig()
	cfg.SimulatorOrgIDs = []string{org}
	cfg.SimulatorBaseURL = "http://127.0.0.1:1"
	gateway.SetConfig(cfg)
	t.Cleanup(func() { gateway.SetConfig(gateway.DefaultConfig()) })

	f, err := NewGatewayFactory(InProcessGatewayDeps(Config{}, inprocess.New()))
	require.NoError(t, err)
	for _, id := range []sdk.ConnectorID{"github", "gitlab", "bitbucket", "jira"} {
		gw, err := f(context.Background(), &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: id})
		require.NoError(t, err, id)
		assert.NotNil(t, gw, id)
	}
	// A PAT gateway outside the simulator too.
	gw, err := f(context.Background(), &sdk.ConnectorConfig{OrganizationID: "00000000-0000-4000-8000-0000000c0c11", ConnectorID: "github",
		Credentials: []byte(`{"personal_access_token":"x"}`)})
	require.NoError(t, err)
	assert.NotNil(t, gw)

	// An OAuth vendor without the credential store to refresh into fails to
	// build, naming what is missing, instead of panicking mid-run.
	f, err = NewGatewayFactory(InProcessGatewayDeps(Config{OAuth: map[string]OAuthClient{"gitlab": {ClientID: "id", ClientSecret: "s"}}}, inprocess.New()))
	require.NoError(t, err)
	_, err = f(context.Background(), &sdk.ConnectorConfig{OrganizationID: "00000000-0000-4000-8000-0000000c0c12", ConnectorID: "gitlab"})
	require.ErrorContains(t, err, "GatewayDeps.Refresh needs")

	_, err = NewGatewayFactory(GatewayDeps{})
	require.ErrorContains(t, err, "RateLimits is nil")
}

type hostQueue struct{ sdk.WorkQueue }

// serve schedules an added connector's operations, and the operations that
// need only the host services WithHostServices gives. New uses the tenant
// directory WithTenantDirectory gives.
func TestServeSchedulesAddedConnectorsAndHostOperations(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("serve with options", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, dsn, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	tenants := StaticTenantDirectory{Default: sdk.TenantKindOrganization}
	a, err := New(ctx, Config{PostgresDSN: dsn, RedisAddr: miniredis.RunT(t).Addr(), AllowPlaintextCredentials: true,
		SinkPath: "log", BlobDir: t.TempDir(), Concurrency: 1, OAuth: map[string]OAuthClient{}},
		WithConnector(thingsConnector{"things"}, stubBuilder),
		WithHostServices(sdk.HostServices{WorkQueue: hostQueue{}}),
		WithTenantDirectory(tenants))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	assert.Equal(t, sdk.TenantDirectory(tenants), a.Tenants)

	const org = "00000000-0000-4000-8000-0000000c0c03"
	for _, c := range []struct{ id, settings string }{
		{"github", `{"organization":"acme"}`},
		{"things", `{}`},
	} {
		require.NoError(t, a.PutConfig(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: sdk.ConnectorID(c.id),
			Settings: []byte(c.settings), Credentials: []byte(`{"personal_access_token":"x"}`), Enabled: true, HealthState: sdk.HealthStateHealthy}))
	}
	sctx, done := sourceScope(ctx)
	defs, err := a.ScheduleSource().FetchSchedules(sctx)
	require.NoError(t, done(err))
	scheduled := map[string]bool{}
	for _, d := range defs {
		scheduled[d.ID] = true
	}
	assert.True(t, scheduled[org+"/things/fetch_things"], "the added connector's operation is scheduled")
	assert.True(t, scheduled[org+"/github/fetch_pr_diffs"], "an operation that needs only the work queue is scheduled")
	assert.False(t, scheduled[org+"/github/drain_defect_lineage"], "an operation that needs services not given is not")
}

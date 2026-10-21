// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc_test

// Tests of the secret broker's refusals: who may borrow a token, and which
// grants are lent. They run on the in-memory store, which returns not-found
// the way the Postgres store does, rather than on mocks that return (nil, nil).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const limOrg = "00000000-0000-4000-8000-00000000cccc"

// allowAll is a SecretAuthorizer that lends to every caller, for tests that
// exercise what GetSecret lends rather than who may borrow it.
func allowAll(context.Context, string, string) error { return nil }

func lendableGrant(t *testing.T) *memstore.Store {
	t.Helper()
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{
		OrganizationID: limOrg, ConnectorID: "github", Enabled: true,
		Credentials: json.RawMessage(`{"access_token":"fake-token-for-test"}`),
	}))
	return mem
}

// GetSecret denies by default: a service built without a SecretAuthorizer,
// directly or through NewService, lends no token to any caller.
func TestGetSecretDeniesWithoutAnAuthorizer(t *testing.T) {
	mem := lendableGrant(t)
	for name, svc := range map[string]*connectorsvc.Service{
		"struct":     {ConfigDAO: mem},
		"NewService": connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: "github"})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Empty(t, resp.GetToken())
		})
	}
}

// The authorizer decides per call: a denied caller gets PermissionDenied, and
// one it allows gets the token. AllowConnectors lends only the listed
// connectors.
func TestGetSecretAppliesTheAuthorizer(t *testing.T) {
	mem := lendableGrant(t)
	ctx := context.Background()
	req := &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: "github"}

	deny := connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
		connectorsvc.WithSecretAuthorizer(func(context.Context, string, string) error { return errors.New("unknown caller") }))
	resp, err := deny.GetSecret(ctx, req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, resp.GetToken())
	require.NotContains(t, err.Error(), "unknown caller", "the denial reason is logged, not sent")

	other := connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
		connectorsvc.WithSecretAuthorizer(connectorsvc.AllowConnectors(commsConnectorID)))
	resp, err = other.GetSecret(ctx, req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, resp.GetToken())

	allowed := connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
		connectorsvc.WithSecretAuthorizer(connectorsvc.AllowConnectors("github")))
	resp, err = allowed.GetSecret(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "fake-token-for-test", resp.GetToken())
}

// ReportGrantInvalid and PurgeOrgGrants answer NotFound for a grant that does
// not exist.
func TestMissingGrantIsNotFound(t *testing.T) {
	svc := &connectorsvc.Service{ConfigDAO: memstore.New()}
	_, err := svc.ReportGrantInvalid(context.Background(), &connectorpb.ReportGrantInvalidRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = svc.PurgeOrgGrants(context.Background(), &connectorpb.PurgeOrgGrantsRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// GetSecret refuses an enabled grant whose connector is paused as
// auth_revoked: its credential is confirmed dead, so it answers
// FailedPrecondition and lends no token. GetConnectionStatus reports it as
// needing a reconnect.
func TestGetSecretRefusesAnAuthRevokedGrant(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{
		OrganizationID: limOrg, ConnectorID: commsConnectorID, Enabled: true,
		Credentials: json.RawMessage(`{"bot_token":"xoxb-revoked"}`),
	}))
	got, err := mem.Get(ctx, limOrg, commsConnectorID)
	require.NoError(t, err)
	revoked, err := mem.MarkAuthRevoked(ctx, limOrg, commsConnectorID, got.CredentialsUpdatedAt)
	require.NoError(t, err)
	require.True(t, revoked)
	svc := &connectorsvc.Service{ConfigDAO: mem, SecretAuthorizer: allowAll}
	resp, err := svc.GetSecret(ctx, &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Empty(t, resp.GetToken())

	st, err := svc.GetConnectionStatus(ctx, &connectorpb.GetConnectionStatusRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.NoError(t, err)
	require.True(t, st.GetConnected())
	require.True(t, st.GetNeedsReconnect())
	require.False(t, st.GetUsable())
}

// GetSecret does not lend a disabled grant's token: it answers NotFound.
func TestGetSecretOnADisabledGrantIsNotFound(t *testing.T) {
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{
		OrganizationID: limOrg, ConnectorID: commsConnectorID, Enabled: false,
		Credentials: json.RawMessage(`{"bot_token":"xoxb-disabled"}`),
	}))
	svc := &connectorsvc.Service{ConfigDAO: mem, SecretAuthorizer: allowAll}
	resp, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Empty(t, resp.GetToken())
}

// An enabled grant whose stored token is empty is an error, not a success
// lending an empty token.
func TestGetSecretOnAnEmptyStoredTokenIsAnError(t *testing.T) {
	mem := memstore.New()
	require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{
		OrganizationID: limOrg, ConnectorID: commsConnectorID, Enabled: true,
		Credentials: json.RawMessage(`{"bot_token":""}`),
	}))
	svc := &connectorsvc.Service{ConfigDAO: mem, SecretAuthorizer: allowAll}
	resp, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: commsConnectorID})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Empty(t, resp.GetToken())
}

// countingConfigs counts the grant reads GetSecret makes.
type countingConfigs struct {
	store.IConnectorConfigDAO
	gets int
}

func (c *countingConfigs) Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	c.gets++
	return c.IConnectorConfigDAO.Get(ctx, orgID, connectorID)
}

// A denied GetSecret reveals nothing: it is refused before the grant is read,
// so a grant that does not exist and one that does both answer
// PermissionDenied, and the store is not asked.
func TestDeniedGetSecretReadsNoGrant(t *testing.T) {
	for name, mem := range map[string]*memstore.Store{"no grant": memstore.New(), "a grant": lendableGrant(t)} {
		t.Run(name, func(t *testing.T) {
			configs := &countingConfigs{IConnectorConfigDAO: mem}
			svc := connectorsvc.NewService(store.NoopTxRunner{}, configs, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
				connectorsvc.WithSecretAuthorizer(func(context.Context, string, string) error { return errors.New("deny all") }))
			_, err := svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: "github"})
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Zero(t, configs.gets, "the grant is not read for a denied caller")
		})
	}
}

// AllowConnectors matches connector ids exactly: another case or a trailing
// space is a different id and is denied.
func TestAllowConnectorsMatchesExactly(t *testing.T) {
	allow := connectorsvc.AllowConnectors("github")
	require.NoError(t, allow(context.Background(), limOrg, "github"))
	for _, id := range []string{"GitHub", "github ", " github", "githu"} {
		require.Error(t, allow(context.Background(), limOrg, id), "%q", id)
	}
}

// Every spelling of an organization meets the same policy: GetSecret hands
// the authorizer the canonical form, so an upper-case, braced or urn:uuid:
// spelling of a denied organization is still denied, and an organization id
// that is not a UUID is InvalidArgument.
func TestGetSecretAppliesTheAuthorizerToEverySpelling(t *testing.T) {
	mem := lendableGrant(t)
	ctx := context.Background()
	var seen []string
	svc := connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
		connectorsvc.WithSecretAuthorizer(func(_ context.Context, orgID, _ string) error {
			seen = append(seen, orgID)
			if orgID == limOrg {
				return errors.New("this organization may not borrow")
			}
			return nil
		}))
	for _, spelling := range []string{limOrg, strings.ToUpper(limOrg), "{" + limOrg + "}", "urn:uuid:" + limOrg} {
		resp, err := svc.GetSecret(ctx, &connectorpb.GetSecretRequest{OrganizationId: spelling, ConnectorId: "github"})
		require.Equal(t, codes.PermissionDenied, status.Code(err), spelling)
		require.Empty(t, resp.GetToken(), spelling)
	}
	for _, got := range seen {
		require.Equal(t, limOrg, got, "the authorizer sees the canonical form")
	}
	_, err := svc.GetSecret(ctx, &connectorpb.GetSecretRequest{OrganizationId: "not-a-uuid", ConnectorId: "github"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// A SecretAuthorizer that panics denies the call instead of crashing the
// server.
func TestGetSecretTreatsAnAuthorizerPanicAsADenial(t *testing.T) {
	mem := lendableGrant(t)
	svc := connectorsvc.NewService(store.NoopTxRunner{}, mem, mem, mem, sdk.NewConnectorRegistry(), nil, nil, nil,
		connectorsvc.WithSecretAuthorizer(func(context.Context, string, string) error { panic("policy bug") }))
	var resp *connectorpb.GetSecretResponse
	var err error
	require.NotPanics(t, func() {
		resp, err = svc.GetSecret(context.Background(), &connectorpb.GetSecretRequest{OrganizationId: limOrg, ConnectorId: "github"})
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, resp.GetToken())
}

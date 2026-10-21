// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// newTokenDelegate builds a connector-service Service wired with a connector DAO
// mock, a management-service mock (for tenant_type resolution), and a gateway
// factory that returns the supplied VendorGateway mock. The mgmt mock has no
// default tenant directory expectation; a test that reaches the tenant_type lookup
// (ValidateConnectorToken always; SubmitConnectorToken only when
// expected_account_id is set) sets its own expectation via expectTenantType.
func newTokenDelegate(t *testing.T, gw sdk.VendorGateway, gwErr error) (*Service, *daomocks.MockIConnectorConfigDAO, *fwmocks.MockTenantDirectory) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	mgmt := fwmocks.NewMockTenantDirectory(ctrl)
	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, gwErr
	})
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Tenants: mgmt, Registry: testRegistry(), GatewayFactory: factory})
	return svc, dao, mgmt
}

// expectTenantType sets a single tenant directory expectation returning the given
// tenant_type, for tests that reach the tenant_type lookup.
func expectTenantType(mgmt *fwmocks.MockTenantDirectory, tt commonv1.TenantType) {
	mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
		Return(sdk.TenantKind(tt), nil)
}

func TestValidateConnectorToken_Delegate(t *testing.T) {
	t.Parallel()

	t.Run("resolve returns account and saves NOTHING", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.GetAuthenticatedUserRequest{})).
			Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 583231}), nil)

		svc, dao, mgmt := newTokenDelegate(t, gw, nil)
		// The single-VCS guard runs, but no persist may happen — no Save /
		// SaveCredentialsPreserveEnabled expectation is set, so a persist would fail
		// the test (the "resolve has no side effect" invariant).
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		expectTenantType(mgmt, commonv1.TenantType_TENANT_TYPE_INDIVIDUAL)

		resp, err := svc.ValidateConnectorToken(context.Background(), &connintv1.ValidateConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.NoError(t, err)
		assert.Equal(t, "octocat", resp.GetAccount().GetHandle())
		assert.Equal(t, "583231", resp.GetAccount().GetAccountId())
		// tenant_type is surfaced so a caller can gate the owner pin on INDIVIDUAL.
		assert.Equal(t, commonv1.TenantType_TENANT_TYPE_INDIVIDUAL, resp.GetTenantType())
	})

	t.Run("invalid token (401) is rejected with InvalidArgument and nothing is saved", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, Cause: fmt.Errorf("status 401")})

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)

		_, err := svc.ValidateConnectorToken(context.Background(), &connintv1.ValidateConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "bad",
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("unsupported connector is rejected", func(t *testing.T) {
		t.Parallel()
		svc, _, _ := newTokenDelegate(t, nil, nil)
		_, err := svc.ValidateConnectorToken(context.Background(), &connintv1.ValidateConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "slack", Token: "tok",
		})
		require.Error(t, err)
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestSubmitConnectorToken_Delegate(t *testing.T) {
	t.Parallel()

	t.Run("first connect saves disabled config", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.GetAuthenticatedUserRequest{})).
			Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 583231}), nil)

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		// No other enabled VCS → the single-VCS guard passes.
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		// The credential write is a single atomic upsert that never touches the
		// `enabled` column on conflict — no Get-then-Save, which would race between the read and the write.
		dao.EXPECT().SaveCredentialsPreserveEnabled(gomock.Any(), gomock.AssignableToTypeOf(&sdk.ConnectorConfig{})).
			DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig) error {
				assert.Equal(t, sdk.ConnectorID("github"), cfg.ConnectorID)
				assert.Equal(t, testOrgID, cfg.OrganizationID)
				assert.Contains(t, string(cfg.Credentials), "personal_access_token")
				return nil
			})
		dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)

		resp, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.NoError(t, err)
		assert.Equal(t, "octocat", resp.GetAccount().GetHandle())
		assert.Equal(t, "583231", resp.GetAccount().GetAccountId())
	})

	t.Run("individual torn-state (expected_account_id mismatch) persists nothing", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// The re-resolve returns a DIFFERENT account (999) than the pinned 583231.
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "intruder", Id: 999}), nil)

		svc, dao, mgmt := newTokenDelegate(t, gw, nil)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		expectTenantType(mgmt, commonv1.TenantType_TENANT_TYPE_INDIVIDUAL)
		// SaveCredentialsPreserveEnabled must NOT be called — the guard rejects before
		// any persist (gomock fails the test if it is).

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx", ExpectedAccountId: "583231",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		// The torn-state reject carries the dedicated errdetails reason — the ONLY
		// FailedPrecondition a caller maps to the different-account reject.
		assert.True(t, hasTornStateReason(err), "torn-state reject must carry the dedicated reason")
	})

	t.Run("org ignores expected_account_id mismatch and saves (preserve-enabled upsert, not enable-on-install)", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 999}), nil)

		svc, dao, mgmt := newTokenDelegate(t, gw, nil)
		// An ORG tenant has no owner to guard, so the mismatch is ignored and the
		// credential still persists via the preserve-enabled upsert (lands disabled on
		// first connect — the PAT path is never enable-on-install, unlike the App path).
		expectTenantType(mgmt, commonv1.TenantType_TENANT_TYPE_ORGANIZATION)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		dao.EXPECT().SaveCredentialsPreserveEnabled(gomock.Any(), gomock.AssignableToTypeOf(&sdk.ConnectorConfig{})).Return(nil)
		dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx", ExpectedAccountId: "583231",
		})
		require.NoError(t, err)
	})

	t.Run("re-submit goes through the enabled-preserving atomic upsert (PAT rotation)", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "octocat", Id: 583231}), nil)

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		// The only enabled VCS is github itself (the connector being re-submitted),
		// so the single-VCS guard passes.
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).
			Return([]*sdk.ConnectorConfig{{ConnectorID: "github", Enabled: true}}, nil)
		// Rotating the PAT must NOT read-then-save: the handler must call the atomic
		// upsert that leaves `enabled` untouched on conflict, so a concurrent
		// ConfirmConnectorRepos enabling the connector between calls is never
		// clobbered. Get/Save being absent here is enforced by gomock (an
		// unexpected call fails the test). The DAO itself preserves enabled at the
		// SQL level; see TestConnectorConfigRepo_SaveCredentialsPreserveEnabled.
		dao.EXPECT().SaveCredentialsPreserveEnabled(gomock.Any(), gomock.AssignableToTypeOf(&sdk.ConnectorConfig{})).
			DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig) error {
				assert.Contains(t, string(cfg.Credentials), "personal_access_token")
				return nil
			})
		dao.EXPECT().ResetHealthState(gomock.Any(), testOrgID, sdk.ConnectorID("github")).Return(true, nil)

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_rotated",
		})
		require.NoError(t, err)
	})

	t.Run("invalid token (401) is rejected with InvalidArgument and nothing is saved", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(nil, &sdk.SyncError{Kind: sdk.ErrKindAuth, Cause: fmt.Errorf("status 401")})

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		// dao.Save must NOT be called — an invalid credential never lands in the DB.

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "bad",
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("transient vendor error (rate limit) maps to Unavailable, not InvalidArgument", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			Return(nil, &sdk.SyncError{Kind: sdk.ErrKindRateLimit, Cause: fmt.Errorf("status 429")})

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("plain (unclassified) vendor error maps to Unavailable, not InvalidArgument", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// A bare error (e.g. a context timeout) is not a SyncError — it must not be
		// mistaken for a bad token.
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("dial tcp: i/o timeout"))

		svc, dao, _ := newTokenDelegate(t, gw, nil)
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("unsupported connector is rejected", func(t *testing.T) {
		t.Parallel()
		svc, _, _ := newTokenDelegate(t, nil, nil)
		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "slack", Token: "tok",
		})
		require.Error(t, err)
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("missing fields are rejected", func(t *testing.T) {
		t.Parallel()
		svc, _, _ := newTokenDelegate(t, nil, nil)
		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github",
		})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ops-disabled connector is rejected before any vendor call or save", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
		// The off switch fires before the gateway/DAO, so no factory, ListByOrg,
		// Get or Save is exercised.
		factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
			t.Fatal("gateway factory must not be called for an ops-disabled connector")
			return nil, nil
		})
		svc := NewService(Deps{Cfg: testConfigWithDisabledConnector("github"), ConnectorDAO: dao, Registry: testRegistry(), GatewayFactory: factory})

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		// The off switch is a FailedPrecondition too, but NOT the torn state — it must
		// not carry the reason, so a caller surfaces it as itself, not different-account.
		assert.False(t, hasTornStateReason(err), "ops off switch must not be mistaken for the torn-state reject")
	})

	t.Run("another active VCS rejects the token submit (single-VCS guard)", func(t *testing.T) {
		t.Parallel()
		svc, dao, _ := newTokenDelegate(t, nil, nil)
		// GitLab is already enabled → submitting a GitHub token must be rejected
		// before the gateway is built or any credential is persisted.
		dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).
			Return([]*sdk.ConnectorConfig{{ConnectorID: "gitlab", Enabled: true}}, nil)

		_, err := svc.SubmitConnectorToken(context.Background(), &connintv1.SubmitConnectorTokenRequest{
			OrgId: testOrgID, ConnectorId: "github", Token: "ghp_xxx",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		// The single-VCS guard is a FailedPrecondition too, but NOT the torn state — it
		// must not carry the reason, so a caller does not mislabel it as different-account.
		assert.False(t, hasTornStateReason(err), "single-VCS guard must not be mistaken for the torn-state reject")
	})
}

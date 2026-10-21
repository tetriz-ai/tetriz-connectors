// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	connectordao "github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestPinBeforeSave_DifferentOwnerPersistsNothing pins that a
// different owner's credential is never stored. It drives the connect flow's
// resolve step (ValidateConnectorToken) against a REAL connector DAO (real pg) and
// proves the invariant a caller's orchestration relies on: the resolve step never
// persists. A different owner is rejected by the caller's owner pin, which the
// caller's own tests cover (the save delegate is never called on !Pinned); this
// test closes the loop on the connector side — after a resolve, connectorDAO.Get
// returns NotFound, so no credential and no listable repos exist for the org.
//
// "Nothing was persisted" is asserted directly against the DB, which is cleaner
// and stronger than the abandoned rollback approach ("saved then deleted"):
// nothing is ever written, so there is no window where a credential exists.
func TestPinBeforeSave_DifferentOwnerPersistsNothing(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		t.Skipf("skipping: cannot reach test database: %v", err)
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	const orgID = "org-pin-before-save"
	connectorID := sdk.ConnectorID("github")

	tx := db.Begin()
	require.NoError(t, tx.Error)
	t.Cleanup(func() { tx.Rollback() })

	realDAO := connectordao.NewConnectorConfigRepo(tx, nil)

	ctrl := gomock.NewController(t)
	// The token resolves to "identity-Y" (id 999) — a DIFFERENT account than would
	// be the tenant's pinned owner. A caller pins this account, the CAS
	// returns Pinned=false, and the caller never calls the save delegate. Here we drive
	// only the resolve step and prove it persists nothing regardless.
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
	gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.GetAuthenticatedUserRequest{})).
		Return(proto.Message(&vgpb.GetAuthenticatedUserResponse{Login: "intruder", Id: 999}), nil)

	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, nil
	})
	// ValidateConnectorToken resolves tenant_type through the tenant directory (so a caller can
	// gate the owner pin on INDIVIDUAL); the resolve step still persists nothing.
	mgmt := fwmocks.NewMockTenantDirectory(ctrl)
	mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
		Return(sdk.TenantKind(commonv1.TenantType_TENANT_TYPE_INDIVIDUAL), nil)
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: realDAO, Tenants: mgmt, Registry: testRegistry(), GatewayFactory: factory})

	// Resolve the token (no save). This is the only side-effect-free step a caller
	// runs before the owner pin.
	resp, err := svc.ValidateConnectorToken(context.Background(), &connintv1.ValidateConnectorTokenRequest{
		OrgId: orgID, ConnectorId: "github", Token: "ghp_intruder",
	})
	require.NoError(t, err)
	assert.Equal(t, "intruder", resp.GetAccount().GetHandle())
	assert.Equal(t, "999", resp.GetAccount().GetAccountId())

	// The invariant: NOTHING was persisted for the org. A credential (and therefore
	// any listable repo) only exists after the owner-verified save step, which the
	// caller skips on a different-owner pin rejection.
	_, getErr := realDAO.Get(context.Background(), orgID, connectorID)
	require.Error(t, getErr, "resolve must not persist a connector config")
	assert.True(t, connectordao.IsNotFound(getErr),
		"connectorDAO.Get must return NotFound after a resolve-only connect: %v", getErr)

	// Belt-and-suspenders: the org has no connector config row at all.
	cfgs, err := realDAO.ListByOrg(context.Background(), orgID)
	require.NoError(t, err)
	assert.Empty(t, cfgs, "no connector config may exist for the org after a resolve-only connect")
}

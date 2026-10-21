// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
)

// TestDisableConnectorConfigsForOrg covers DisableConnectorConfigsForOrg:
// it validates org_id, delegates to the DAO, returns the disabled count, and
// maps a DAO failure to Internal. The DAO's idempotency is proven at the DB
// layer (connector_config_repo_test); here the count is just passed through.
func TestDisableConnectorConfigsForOrg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		orgID     string
		daoCount  int64
		daoErr    error
		callDAO   bool
		wantErr   bool
		wantCode  codes.Code
		wantCount int64
	}{
		{
			name:      "disables and returns the count",
			orgID:     testOrgID,
			daoCount:  2,
			callDAO:   true,
			wantCount: 2,
		},
		{
			name:      "no enabled configs returns zero (idempotent re-run)",
			orgID:     testOrgID,
			daoCount:  0,
			callDAO:   true,
			wantCount: 0,
		},
		{
			name:     "empty org_id fails InvalidArgument without touching the DAO",
			orgID:    "",
			callDAO:  false,
			wantErr:  true,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "DAO error surfaces Internal",
			orgID:    testOrgID,
			daoErr:   errors.New("db down"),
			callDAO:  true,
			wantErr:  true,
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			if tt.callDAO {
				dao.EXPECT().DisableConfigsForOrg(gomock.Any(), tt.orgID).
					Return(tt.daoCount, tt.daoErr)
			}

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry()})

			resp, err := svc.DisableConnectorConfigsForOrg(context.Background(), &connintv1.DisableConnectorConfigsForOrgRequest{
				OrgId: tt.orgID,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, resp.GetDisabledCount())
		})
	}
}

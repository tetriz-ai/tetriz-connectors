// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	// Aliased to keep the bare name free for this package's own helpers.
	prommetrics "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestDisconnectIntegrationMetric pins DisconnectsTotal to the successful-removal
// edge: a completed config delete increments it once, and a delete that fails does
// not — the counter counts only completed removals, so a bump on a failed
// disconnect would report a removal that never happened. Each case uses its
// own (org, connector) labels because the counter is process-global and other cases
// in this package touch DisconnectIntegration.
func TestDisconnectIntegrationMetric(t *testing.T) {
	tests := []struct {
		name        string
		orgID       string
		connectorID sdk.ConnectorID
		// deleteErr, when set, is what the config Delete returns instead of success.
		deleteErr error
		wantDelta float64
	}{
		{
			name:        "successful removal increments the counter",
			orgID:       "org_disconnect_metric_ok",
			connectorID: "linear",
			wantDelta:   1,
		},
		{
			name:        "failed config delete leaves the counter untouched",
			orgID:       "org_disconnect_metric_delete_fail",
			connectorID: "linear",
			deleteErr:   fmt.Errorf("db connection lost"),
			wantDelta:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().Get(gomock.Any(), tt.orgID, tt.connectorID).Return(&sdk.ConnectorConfig{
				ConnectorID:    tt.connectorID,
				OrganizationID: tt.orgID,
				Enabled:        true,
				Credentials:    json.RawMessage(`{}`),
			}, nil)
			dao.EXPECT().Delete(gomock.Any(), tt.orgID, tt.connectorID).Return(tt.deleteErr)
			if tt.deleteErr == nil {
				// resetConnectorHealth runs only after a successful delete.
				dao.EXPECT().ResetHealthState(gomock.Any(), tt.orgID, tt.connectorID).Return(true, nil)
			}

			labelOrg := tenantlabel.OrgID(tt.orgID)
			before := prommetrics.ToFloat64(obs.DisconnectsTotal.WithLabelValues(labelOrg, string(tt.connectorID)))

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry()})
			_, err := svc.DisconnectIntegration(context.Background(), &connintv1.DisconnectIntegrationRequest{
				OrgId: tt.orgID, ConnectorId: string(tt.connectorID),
			})
			if tt.deleteErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			after := prommetrics.ToFloat64(obs.DisconnectsTotal.WithLabelValues(labelOrg, string(tt.connectorID)))
			assert.Equal(t, tt.wantDelta, after-before, "connector_disconnect_total delta")
		})
	}
}

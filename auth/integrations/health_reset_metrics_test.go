// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	// Aliased to keep the bare name free for this package's own helpers.
	prommetrics "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestResetConnectorHealthWriteFailureMetric pins the two reconnect outcomes as
// distinguishable series: a lost auth_revoked->healthy write moves only
// HealthWriteFailuresTotal{direction=reset}, a successful one moves only
// HealthStateTransitionsTotal{to=healthy}. Asserting both deltas per case is the
// point — the failure leaves the connector gated after the customer already fixed
// their credential, and without the counter that state is indistinguishable from a
// reset that landed. Each case uses its own (org, connector) labels because these
// counters are process-global and other cases in this package touch them.
func TestResetConnectorHealthWriteFailureMetric(t *testing.T) {
	tests := []struct {
		name        string
		orgID       string
		connectorID sdk.ConnectorID
		// resetErr, when set, is what ResetHealthState returns instead of a
		// successful transition.
		resetErr            error
		wantWriteFailDelta  float64
		wantTransitionDelta float64
	}{
		{
			name:                "reset write fails: only the write-failure counter moves",
			orgID:               "org_reset_write_fail",
			connectorID:         "github",
			resetErr:            fmt.Errorf("connector db unreachable"),
			wantWriteFailDelta:  1,
			wantTransitionDelta: 0,
		},
		{
			name:                "reset write lands: only the transition counter moves",
			orgID:               "org_reset_write_ok",
			connectorID:         "slack",
			wantWriteFailDelta:  0,
			wantTransitionDelta: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDAO := daomocks.NewMockIConnectorConfigDAO(ctrl)
			mockDAO.EXPECT().
				ResetHealthState(gomock.Any(), tt.orgID, tt.connectorID).
				Return(tt.resetErr == nil, tt.resetErr)

			writeFailBefore := prommetrics.ToFloat64(obs.HealthWriteFailuresTotal.WithLabelValues(tt.orgID, string(tt.connectorID), obs.HealthWriteReset))
			transitionBefore := prommetrics.ToFloat64(obs.HealthStateTransitionsTotal.WithLabelValues(tt.orgID, string(tt.connectorID), string(sdk.HealthStateHealthy)))

			s := &Service{connectorDAO: mockDAO}
			s.resetConnectorHealth(context.Background(), tt.orgID, tt.connectorID)

			writeFailAfter := prommetrics.ToFloat64(obs.HealthWriteFailuresTotal.WithLabelValues(tt.orgID, string(tt.connectorID), obs.HealthWriteReset))
			transitionAfter := prommetrics.ToFloat64(obs.HealthStateTransitionsTotal.WithLabelValues(tt.orgID, string(tt.connectorID), string(sdk.HealthStateHealthy)))

			assert.Equal(t, tt.wantWriteFailDelta, writeFailAfter-writeFailBefore,
				"connector_health_write_failures_total{direction=reset} delta")
			assert.Equal(t, tt.wantTransitionDelta, transitionAfter-transitionBefore,
				"connector_health_state_transitions_total{to=healthy} delta")
		})
	}
}

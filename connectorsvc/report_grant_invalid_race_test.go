// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// reconnectAfterGet lands a reconnect (a fresh credential write) right after
// the RPC reads the row, as a concurrent connect flow would.
type reconnectAfterGet struct {
	store.IConnectorConfigDAO
	once bool
}

func (r *reconnectAfterGet) Get(ctx context.Context, org string, id sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	cfg, err := r.IConnectorConfigDAO.Get(ctx, org, id)
	if !r.once {
		r.once = true
		_ = r.SaveCredentialsEnablePreservingSettings(ctx, &sdk.ConnectorConfig{
			OrganizationID: org, ConnectorID: id, Credentials: json.RawMessage(`{"bot_token":"xoxb-new"}`), Settings: json.RawMessage(`{}`), Enabled: true})
		_, _ = r.ResetHealthState(ctx, org, id)
	}
	return cfg, err //nolint:wrapcheck // test double
}

// A reconnect that lands after ReportGrantInvalid reads the grant stands: its
// credential is kept and the grant stays enabled.
func TestReportGrantInvalidKeepsAReconnectThatLandsAfterTheRead(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000a0a1"
	ms := memstore.New()
	require.NoError(t, ms.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "slack_comms",
		Credentials: json.RawMessage(`{"bot_token":"xoxb-dead"}`), Settings: json.RawMessage(`{}`), Enabled: true}))
	svc := &connectorsvc.Service{ConfigDAO: &reconnectAfterGet{IConnectorConfigDAO: ms}}

	_, err := svc.ReportGrantInvalid(ctx, &connectorpb.ReportGrantInvalidRequest{OrganizationId: org, ConnectorId: "slack_comms", Reason: "invalid_auth"})
	require.NoError(t, err)
	got, err := ms.Get(ctx, org, "slack_comms")
	require.NoError(t, err)
	assert.Contains(t, string(got.Credentials), "xoxb-new", "the reconnect's credential stands")
	assert.True(t, got.Enabled, "the reconnected grant stays enabled")
}

// Without a concurrent write the grant is disabled and nothing else changes.
func TestReportGrantInvalidDisablesAnUnchangedGrant(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000a0a2"
	ms := memstore.New()
	require.NoError(t, ms.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "slack_comms",
		Credentials: json.RawMessage(`{"bot_token":"xoxb-dead"}`), Settings: json.RawMessage(`{"team_name":"Acme"}`), Enabled: true}))
	svc := &connectorsvc.Service{ConfigDAO: ms}

	_, err := svc.ReportGrantInvalid(ctx, &connectorpb.ReportGrantInvalidRequest{OrganizationId: org, ConnectorId: "slack_comms", Reason: "invalid_auth"})
	require.NoError(t, err)
	got, err := ms.Get(ctx, org, "slack_comms")
	require.NoError(t, err)
	assert.False(t, got.Enabled)
	assert.JSONEq(t, `{"team_name":"Acme"}`, string(got.Settings))
}

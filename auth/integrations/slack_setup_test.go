// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
)

const testStateToken = "test-state-token-abcdef1234"

// seedState writes a CSRF state entry to miniredis for orgID and connectorID
// (empty connectorID writes a state without one).
func seedState(t *testing.T, rc *redis.Client, state, orgID, connectorID string) {
	t.Helper()
	data, err := json.Marshal(slackOAuthState{OrgID: orgID, ConnectorID: connectorID})
	require.NoError(t, err)
	require.NoError(t, rc.Set(context.Background(), slackOAuthStateKey(state), data, time.Minute).Err())
}

// The Slack OAuth callback validates the request and the CSRF state before it
// routes to the slack_comms save path.
func TestValidateAndSaveSlackConnection_RequestAndState(t *testing.T) {
	tests := []struct {
		name      string
		code      string
		orgID     string
		seedOrg   string // org stored with the state (empty = no state)
		connector string // connector id stored with the state
		wantCode  codes.Code
	}{
		{name: "MissingCode", orgID: testOrgID, seedOrg: testOrgID, connector: connectorIDSlackComms, wantCode: codes.InvalidArgument},
		{name: "MissingOrgID", code: "c", seedOrg: testOrgID, connector: connectorIDSlackComms, wantCode: codes.InvalidArgument},
		{name: "StateNotInRedis", code: "c", orgID: testOrgID, wantCode: codes.InvalidArgument},
		{name: "StateOrgMismatch", code: "c", orgID: testOrgID, seedOrg: "00000000-0000-4000-8000-00000000beef", connector: connectorIDSlackComms, wantCode: codes.InvalidArgument},
		// A state that names no connector, or another one, is not a slack_comms
		// connect; there is no other Slack connection to save.
		{name: "StateWithoutConnector", code: "c", orgID: testOrgID, seedOrg: testOrgID, wantCode: codes.InvalidArgument},
		{name: "StateForSlackData", code: "c", orgID: testOrgID, seedOrg: testOrgID, connector: "slack", wantCode: codes.InvalidArgument},
		// slack_comms is not enabled in testConfig, so a valid state reaches the
		// save path and stops at its enabled check.
		{name: "SlackCommsNotEnabled", code: "c", orgID: testOrgID, seedOrg: testOrgID, connector: connectorIDSlackComms, wantCode: codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
			if tt.seedOrg != "" {
				seedState(t, rc, testStateToken, tt.seedOrg, tt.connector)
			}
			svc := NewService(Deps{Cfg: testConfig(), Redis: rc, Registry: testRegistry()})
			_, err := svc.ValidateAndSaveSlackConnection(context.Background(), &connintv1.ValidateAndSaveSlackConnectionRequest{
				Code: tt.code, OrgId: tt.orgID, State: testStateToken,
			})
			require.Error(t, err)
			st, _ := status.FromError(err)
			assert.Equal(t, tt.wantCode, st.Code(), st.Message())
		})
	}
}

// A state is single-use: the callback consumes it even when it refuses the
// connection, so it cannot be replayed.
func TestValidateAndSaveSlackConnection_StateConsumed(t *testing.T) {
	rc := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	seedState(t, rc, testStateToken, testOrgID, "slack")
	svc := NewService(Deps{Cfg: testConfig(), Redis: rc, Registry: testRegistry()})
	_, err := svc.ValidateAndSaveSlackConnection(context.Background(), &connintv1.ValidateAndSaveSlackConnectionRequest{
		Code: "c", OrgId: testOrgID, State: testStateToken,
	})
	require.Error(t, err)
	assert.Equal(t, int64(0), rc.Exists(context.Background(), slackOAuthStateKey(testStateToken)).Val(), "the state is consumed")
}

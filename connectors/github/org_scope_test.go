// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// runOrgScoped runs opID through the orchestrator with every vendor request
// answered by vendorErr, and returns the connector's health and the run row.
func runOrgScoped(t *testing.T, org string, opID sdk.OperationID, vendorErr error) (sdk.HealthState, string) {
	t.Helper()
	return runOrgScopedWith(t, org, opID, func(context.Context, proto.Message) (proto.Message, error) { return nil, vendorErr })
}

// runOrgScopedWith is runOrgScoped with every vendor request answered by answer.
func runOrgScopedWith(t *testing.T, org string, opID sdk.OperationID, answer func(context.Context, proto.Message) (proto.Message, error)) (sdk.HealthState, string) {
	t.Helper()
	ctx := context.Background()
	mem := memstore.New()
	cfg := testParams(nil, nil, nil).Config
	cfg.OrganizationID = org
	require.NoError(t, mem.Save(ctx, cfg))
	gw := &mockVendorGateway{executeFunc: answer}
	reg := sdk.NewConnectorRegistry()
	reg.Register(NewConnector(nil, 0))
	orch := runner.NewOrchestrator(reg, mem, mem, mem, testkit.NewChannelSink(),
		func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { return gw, nil }).WithTxRunner(store.NoopTxRunner{})
	require.Error(t, orch.RunOperation(ctx, org, "github", opID, sdk.TenantKindOrganization, "scheduled"))
	got, err := mem.Get(ctx, org, "github")
	require.NoError(t, err)
	run, err := mem.GetLatest(ctx, org, "github", opID)
	require.NoError(t, err)
	return got.HealthState, run.ErrorKind
}

// A token without access to an organization-scoped endpoint (Copilot usage,
// organization members, projects) gets a 403 there. That fails the operation
// as config_invalid and does not pause the connector, whose repository
// operations the token can still run. A 401 still pauses it.
func TestOrgScopedForbiddenDoesNotPauseTheConnector(t *testing.T) {
	forbidden := vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"Resource not accessible by personal access token"}`), nil)
	unauthorized := vendorapi.ClassifyHTTPErrorWithHeaders(401, []byte(`{"message":"Bad credentials"}`), nil)
	for i, opID := range []sdk.OperationID{"fetch_copilot_usage", "fetch_org_members", "fetch_projects"} {
		t.Run(string(opID), func(t *testing.T) {
			health, kind := runOrgScoped(t, fmt.Sprintf("00000000-0000-4000-8000-0000000b040%d", i), opID, forbidden)
			assert.Equal(t, sdk.HealthStateHealthy, health, "a 403 on an organization-scoped endpoint does not pause the connector")
			assert.Equal(t, "config_invalid", kind)

			health, kind = runOrgScoped(t, fmt.Sprintf("00000000-0000-4000-8000-0000000b041%d", i), opID, unauthorized)
			assert.Equal(t, sdk.HealthStateAuthRevoked, health, "a 401 pauses the connector")
			assert.Equal(t, "auth", kind)
		})
	}
}

// A throttle 403 stays a retryable rate limit, and other errors pass
// through unchanged.
func TestOrgScopeForbiddenLeavesOtherErrorsAlone(t *testing.T) {
	throttle := vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"You have exceeded a secondary rate limit"}`), nil)
	assert.Same(t, error(throttle), orgScopeForbidden(throttle, "fetch_copilot_usage", "o", "x"))
	assert.NoError(t, orgScopeForbidden(nil, "fetch_copilot_usage", "o", "x"))
}

// GitHub answers a GraphQL query the token may not run with HTTP 200 and
// errors of type INSUFFICIENT_SCOPES or FORBIDDEN. For the organization-scoped
// GraphQL operations that is the same 403: config_invalid, and the connector
// is not paused.
func TestOrgScopedGraphQLForbiddenDoesNotPauseTheConnector(t *testing.T) {
	for i, opID := range []sdk.OperationID{"fetch_org_members", "fetch_projects"} {
		for j, typ := range []string{"INSUFFICIENT_SCOPES", "FORBIDDEN"} {
			t.Run(string(opID)+"/"+typ, func(t *testing.T) {
				answer := func(context.Context, proto.Message) (proto.Message, error) {
					return &connectorpb.GitHubGraphQLResponse{Errors: []*connectorpb.GitHubGraphQLError{
						{Type: typ, Message: "Your token has not been granted the required scopes to execute this query."},
					}}, nil
				}
				health, kind := runOrgScopedWith(t, fmt.Sprintf("00000000-0000-4000-8000-0000000b05%d%d", i, j), opID, answer)
				assert.Equal(t, sdk.HealthStateHealthy, health)
				assert.Equal(t, "config_invalid", kind)
			})
		}
	}
}

// The mapped error is terminal: the token lacks the access until someone grants it, so
// the scheduler fails that fire once instead of retrying it.
func TestOrgScopeForbiddenIsTerminal(t *testing.T) {
	forbidden := vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"Resource not accessible by personal access token"}`), nil)
	var se *sdk.SyncError
	require.ErrorAs(t, orgScopeForbidden(forbidden, "fetch_projects", "o", "the organization's projects"), &se)
	assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	assert.True(t, se.Terminal)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
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

// runFetchIssues runs fetch_issues over two repositories through the
// orchestrator, every GraphQL request answered by answer, and returns the
// run's row's kind and message, the connector's health and the run's error.
func runFetchIssues(t *testing.T, org string, answer func(context.Context, proto.Message) (proto.Message, error)) (kind, message string, health sdk.HealthState, runErr error) {
	t.Helper()
	ctx := context.Background()
	mem := memstore.New()
	cfg := testParams(nil, nil, nil).Config
	cfg.OrganizationID = org
	cfg.Settings = json.RawMessage(`{"organization":"acme","repositories":["acme/widgets","acme/gadgets"],"include_private":false}`)
	require.NoError(t, mem.Save(ctx, cfg))
	reg := sdk.NewConnectorRegistry()
	reg.Register(NewConnector(nil, 0))
	orch := runner.NewOrchestrator(reg, mem, mem, mem, testkit.NewChannelSink(),
		func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
			return &mockVendorGateway{executeFunc: answer}, nil
		}).WithTxRunner(store.NoopTxRunner{})
	runErr = orch.RunOperation(ctx, org, "github", "fetch_issues", sdk.TenantKindOrganization, "scheduled")
	run, err := mem.GetLatest(ctx, org, "github", "fetch_issues")
	require.NoError(t, err)
	got, err := mem.Get(ctx, org, "github")
	require.NoError(t, err)
	return run.ErrorKind, run.ErrorMessage, got.HealthState, runErr
}

// A token without a scope the issue query needs (read:project, for each
// issue's projects) gets GitHub's INSUFFICIENT_SCOPES on every repository.
// No retry fixes that and the token still works, so fetch_issues fails as a
// terminal config_invalid naming the scope, and the connector is not paused:
// repositories and pull requests keep syncing.
func TestScopeErrorOnEveryRepositoryIsTerminalConfigInvalid(t *testing.T) {
	answer := func(_ context.Context, m proto.Message) (proto.Message, error) {
		require.IsType(t, &connectorpb.GitHubGraphQLRequest{}, m)
		return &connectorpb.GitHubGraphQLResponse{Errors: []*connectorpb.GitHubGraphQLError{{Type: "INSUFFICIENT_SCOPES",
			Message: "Your token has not been granted the required scopes to execute this query. The 'title' field requires one of the following scopes: ['read:project'], but your token has only been granted the: ['repo'] scopes."}}}, nil
	}
	kind, message, health, runErr := runFetchIssues(t, "00000000-0000-4000-8000-0000000b0a01", answer)
	require.Error(t, runErr)
	var se *sdk.SyncError
	require.True(t, errors.As(runErr, &se))
	assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	assert.True(t, se.Terminal, "a missing scope is not retried")
	assert.Equal(t, "config_invalid", kind)
	assert.Contains(t, message, "needs read:project")
	assert.Equal(t, sdk.HealthStateHealthy, health, "a missing scope does not pause the connector")
}

// A plain 403 (not a scope problem) on every repository, with none left
// unread, still pauses the connector, as the README's vendor-answer table says.
func TestForbiddenOnEveryRepositoryPausesTheConnector(t *testing.T) {
	forbidden := vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"Resource not accessible by personal access token"}`), nil)
	answer := func(context.Context, proto.Message) (proto.Message, error) { return nil, forbidden }
	kind, _, health, runErr := runFetchIssues(t, "00000000-0000-4000-8000-0000000b0a02", answer)
	require.Error(t, runErr)
	assert.Equal(t, "auth", kind)
	assert.Equal(t, sdk.HealthStateAuthRevoked, health)
}

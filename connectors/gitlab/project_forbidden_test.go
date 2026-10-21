// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
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

// runFetchPRs runs fetch_prs through the orchestrator for two projects, each
// answering its merge-request listing with the given error (nil = empty page),
// and returns the connector's health after the run.
func runFetchPRs(t *testing.T, org string, errA, errB error) (sdk.HealthState, error) {
	t.Helper()
	ctx := context.Background()
	mem := memstore.New()
	base := testParams(nil, nil, nil).Config
	base.OrganizationID = org
	require.NoError(t, mem.Save(ctx, base))
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.GitLabListProjectsRequest:
			return glProjectsResponse([]glProject{testGLProject(1, "mygroup/a"), testGLProject(2, "mygroup/b")}), nil
		case *connectorpb.GitLabListProjectMergeRequestsRequest:
			if r.GetProjectId() == 1 && errA != nil {
				return nil, errA
			}
			if r.GetProjectId() == 2 && errB != nil {
				return nil, errB
			}
			return &connectorpb.GitLabListProjectMergeRequestsResponse{}, nil
		}
		return nil, errors.New("unexpected request")
	}}
	reg := sdk.NewConnectorRegistry()
	reg.Register(NewConnector())
	orch := runner.NewOrchestrator(reg, mem, mem, mem, testkit.NewChannelSink(),
		func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) { return gw, nil }).WithTxRunner(store.NoopTxRunner{})
	runErr := orch.RunOperation(ctx, org, "gitlab", "fetch_prs", sdk.TenantKindOrganization, "scheduled")
	cfg, err := mem.Get(ctx, org, "gitlab")
	require.NoError(t, err)
	return cfg.HealthState, runErr
}

func glForbidden() error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"403 Forbidden"}`), nil)
}

// One project that answers 403 to its merge-request listing is that
// project's failure: the run completes and the connector is not paused.
// Every project answering 403, or a 401, still pauses it.
func TestOneForbiddenProjectDoesNotPauseTheConnector(t *testing.T) {
	health, err := runFetchPRs(t, "00000000-0000-4000-8000-0000000a0403", glForbidden(), nil)
	require.NoError(t, err)
	assert.NotEqual(t, sdk.HealthStateAuthRevoked, health, "one project's 403 must not pause the connector")

	health, err = runFetchPRs(t, "00000000-0000-4000-8000-0000000a0404", glForbidden(), glForbidden())
	require.Error(t, err)
	assert.Equal(t, sdk.HealthStateAuthRevoked, health, "every project answering 403 pauses the connector")

	health, err = runFetchPRs(t, "00000000-0000-4000-8000-0000000a0405",
		vendorapi.ClassifyHTTPErrorWithHeaders(401, []byte(`{"message":"401 Unauthorized"}`), nil), nil)
	require.Error(t, err)
	assert.Equal(t, sdk.HealthStateAuthRevoked, health, "a 401 pauses the connector")
}

// fetch_commits and fetch_pr_comments skip a forbidden project the same way,
// and the project after it is read.
func TestGitLabProjectLoopsSkipAForbiddenProject(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   sdk.APIFetchOperation
	}{
		{"fetch_commits", &FetchCommitsOp{}},
		{"fetch_pr_comments", &FetchPRCommentsOp{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var read []int64
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				switch r := req.(type) {
				case *connectorpb.GitLabListProjectsRequest:
					return glProjectsResponse([]glProject{testGLProject(1, "mygroup/a"), testGLProject(2, "mygroup/b")}), nil
				case *connectorpb.GitLabListCommitsRequest:
					read = append(read, r.GetProjectId())
					if r.GetProjectId() == 1 {
						return nil, glForbidden()
					}
					return &connectorpb.GitLabListCommitsResponse{}, nil
				case *connectorpb.GitLabListProjectMergeRequestsRequest:
					read = append(read, r.GetProjectId())
					if r.GetProjectId() == 1 {
						return nil, glForbidden()
					}
					return &connectorpb.GitLabListProjectMergeRequestsResponse{}, nil
				}
				return nil, errors.New("unexpected request")
			}}
			_, err := tc.op.Execute(context.Background(), testParams(testkit.NewChannelSink(), gw, nil))
			require.NoError(t, err)
			assert.Contains(t, read, int64(2), "the project after the forbidden one is read")
		})
	}
}

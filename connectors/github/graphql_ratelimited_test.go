// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A GraphQL answer whose errors carry type RATE_LIMITED fails the run as a
// retryable rate_limit error, not internal.
func TestGraphQLRateLimitedIsARateLimit(t *testing.T) {
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.ListOrgReposRequest:
			return &connectorpb.ListOrgReposResponse{Repos: [][]byte{mustJSON(ghRepository{ID: 1, FullName: "acme/a", DefaultBranch: "main"})}}, nil
		case *connectorpb.GitHubGraphQLRequest:
			return &connectorpb.GitHubGraphQLResponse{Errors: []*connectorpb.GitHubGraphQLError{{Type: "RATE_LIMITED", Message: "API rate limit exceeded for installation ID 1."}}}, nil
		}
		return nil, errors.New("unexpected request")
	}}
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org-test", Enabled: true,
			Credentials: mustJSON(GitHubCredentials{InstallationID: "1"}), Settings: mustJSON(GitHubSettings{Organization: "acme"})},
		Sink: testkit.NewChannelSink(), VendorGateway: gw,
	}
	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindRateLimit, se.Kind, "a GraphQL RATE_LIMITED error must be rate_limit")
	assert.True(t, se.Retryable)
}

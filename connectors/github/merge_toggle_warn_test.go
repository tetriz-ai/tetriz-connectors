// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A failed merge-method query is logged with the connector, operation and
// organization it belongs to, and the repository is still emitted.
func TestFetchRepos_MergeToggleFailureNamesTheRun(t *testing.T) {
	var logs bytes.Buffer
	logger.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { logger.SetLogger(nil) })

	repoJSON, _ := json.Marshal(ghRepository{ID: 1, Name: "widgets", FullName: "acme/widgets", Owner: struct {
		Login string `json:"login"`
	}{Login: "acme"}})
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *vgpb.ListOrgReposRequest:
				return &vgpb.ListOrgReposResponse{Repos: [][]byte{repoJSON}}, nil
			case *vgpb.GitHubGraphQLRequest:
				return nil, fmt.Errorf("handling vendor request %q: %w", "graphql", vendorapi.ClassifyHTTPErrorWithHeaders(400, []byte(`{"message":"no"}`), nil))
			}
			return nil, nil
		},
	}
	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.TenantType = sdk.TenantKindOrganization
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", IncludePrivate: true})

	_, err := (&FetchReposOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Len(t, sink.Records(), 1)
	assert.Contains(t, logs.String(), "connector=github operation=fetch_repos org=org-test")
}

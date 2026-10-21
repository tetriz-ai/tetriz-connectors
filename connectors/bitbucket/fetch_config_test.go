// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func TestFetchConfigOp_EmitsRepoConfig(t *testing.T) {
	repoJSON := `{"uuid":"{repo-uuid}","slug":"api","full_name":"ws/api","mainbranch":{"name":"main","type":"branch"}}`
	branchJSON := `{"name":"main","default_merge_strategy":"squash","merge_strategies":["merge_commit","squash","fast_forward"]}`

	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.BitbucketGetRepoRequest:
			return &connectorpb.BitbucketGetRepoResponse{Repo: []byte(repoJSON)}, nil
		case *connectorpb.BitbucketGetBranchRequest:
			return &connectorpb.BitbucketGetBranchResponse{RawJson: []byte(branchJSON)}, nil
		default:
			return nil, nil
		}
	}}
	sink := testkit.NewChannelSink()

	op := &FetchConfigOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "tok"}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:       "ws",
				DiscoveredRepos: []BitbucketDiscoveredRepo{{FullName: "ws/api", DefaultBranch: "main"}},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: gw,
	})
	require.NoError(t, err)

	recs := sink.Records()
	require.Len(t, recs, 1)
	require.Equal(t, "fetch_config", recs[0].OperationID)
	require.Equal(t, "{repo-uuid}", recs[0].ExternalID)
	require.Equal(t, "REPO", recs[0].Metadata["scope"])
	require.Equal(t, "ws/api", recs[0].Metadata["full_name"])

	var blob struct {
		RepoSettings struct {
			DefaultBranch        string   `json:"default_branch"`
			DefaultMergeStrategy string   `json:"default_merge_strategy"`
			MergeStrategies      []string `json:"merge_strategies"`
		} `json:"repo_settings"`
	}
	require.NoError(t, json.Unmarshal(recs[0].Payload, &blob))
	require.Equal(t, "main", blob.RepoSettings.DefaultBranch)
	require.Equal(t, "squash", blob.RepoSettings.DefaultMergeStrategy)
	require.Contains(t, blob.RepoSettings.MergeStrategies, "squash")
}

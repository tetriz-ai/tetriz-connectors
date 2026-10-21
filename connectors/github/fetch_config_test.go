// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func TestFetchConfigOp_EmitsRepoConfig(t *testing.T) {
	repoJSON := `{"id":12345,"full_name":"acme/widgets","default_branch":"master"}`
	rulesJSON := `[{"type":"pull_request","parameters":{"required_approving_review_count":2,"allowed_merge_methods":["squash"]}}]`

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *vgpb.GetRepositoryRequest:
				return &vgpb.GetRepositoryResponse{RawJson: []byte(repoJSON)}, nil
			case *vgpb.GetBranchRulesRequest:
				return &vgpb.GetBranchRulesResponse{RawJson: []byte(rulesJSON)}, nil
			default:
				return nil, nil
			}
		},
	}
	sink := testkit.NewChannelSink()

	op := &FetchConfigOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-test",
			Credentials:    mustJSON(GitHubCredentials{InstallationID: "12345678"}),
			Settings:       mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/widgets"}}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	})
	require.NoError(t, err)

	recs := sink.Records()
	require.Len(t, recs, 1)
	require.Equal(t, "fetch_config", recs[0].OperationID)
	require.Equal(t, "12345", recs[0].ExternalID)
	require.Equal(t, "REPO", recs[0].Metadata["scope"])
	require.Equal(t, "acme/widgets", recs[0].Metadata["full_name"])

	var blob struct {
		RepoSettings struct {
			DefaultBranch string `json:"default_branch"`
		} `json:"repo_settings"`
		BranchRules []struct {
			Type string `json:"type"`
		} `json:"branch_rules"`
	}
	require.NoError(t, json.Unmarshal(recs[0].Payload, &blob))
	require.Equal(t, "master", blob.RepoSettings.DefaultBranch)
	require.Len(t, blob.BranchRules, 1)
	require.Equal(t, "pull_request", blob.BranchRules[0].Type)
}

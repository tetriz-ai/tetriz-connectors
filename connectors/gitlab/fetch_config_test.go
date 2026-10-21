// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

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

func TestFetchConfigOp_EmitsProjectAndGroupConfig(t *testing.T) {
	projectJSON := `{"id":42,"path_with_namespace":"grp/api","default_branch":"main","merge_method":"ff","squash_option":"default_on"}`
	groupJSON := `{"id":7,"full_path":"grp","default_branch_protection_defaults":{"allow_force_push":false}}`

	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.GitLabGetProjectRequest:
			return &connectorpb.GitLabGetProjectResponse{RawJson: []byte(projectJSON)}, nil
		case *connectorpb.GitLabGetGroupRequest:
			return &connectorpb.GitLabGetGroupResponse{RawJson: []byte(groupJSON)}, nil
		default:
			return nil, nil
		}
	}}
	sink := testkit.NewChannelSink()

	op := &FetchConfigOp{}
	_, err := op.Execute(context.Background(), &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-test",
			Credentials:    mustJSON(GitLabCredentials{OAuthRefreshToken: "rt"}),
			Settings: mustJSON(GitLabSettings{
				GroupID:         7,
				DiscoveredRepos: []GitLabDiscoveredRepo{{FullName: "grp/api", ProjectID: 42, DefaultBranch: "main"}},
			}),
			Enabled: true,
		},
		Sink:          sink,
		VendorGateway: gw,
	})
	require.NoError(t, err)

	recs := sink.Records()
	require.Len(t, recs, 2, "one ACCOUNT (group) + one REPO (project)")

	byScope := map[string]*sdk.RawRecord{}
	for _, r := range recs {
		byScope[r.Metadata["scope"]] = r
	}

	repo := byScope["REPO"]
	require.NotNil(t, repo)
	require.Equal(t, "42", repo.ExternalID)
	require.Equal(t, "grp/api", repo.Metadata["full_name"])
	var rb struct {
		RepoSettings struct {
			MergeMethod   string `json:"merge_method"`
			SquashOption  string `json:"squash_option"`
			DefaultBranch string `json:"default_branch"`
		} `json:"repo_settings"`
	}
	require.NoError(t, json.Unmarshal(repo.Payload, &rb))
	require.Equal(t, "ff", rb.RepoSettings.MergeMethod)
	require.Equal(t, "default_on", rb.RepoSettings.SquashOption)
	require.Equal(t, "main", rb.RepoSettings.DefaultBranch)

	acct := byScope["ACCOUNT"]
	require.NotNil(t, acct)
	require.Equal(t, "group-7", acct.ExternalID)
	var ab struct {
		GroupSettings json.RawMessage `json:"group_settings"`
	}
	require.NoError(t, json.Unmarshal(acct.Payload, &ab))
	require.Contains(t, string(ab.GroupSettings), "default_branch_protection_defaults")
}

// Group and project ids are separate sequences: a group and a project can
// share a number, and the ACCOUNT record must not take the REPO record's key.
func TestFetchConfigOp_GroupAndProjectWithSameIDDoNotCollide(t *testing.T) {
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch req.(type) {
		case *connectorpb.GitLabGetProjectRequest:
			return &connectorpb.GitLabGetProjectResponse{RawJson: []byte(`{"id":7,"default_branch":"main"}`)}, nil
		case *connectorpb.GitLabGetGroupRequest:
			return &connectorpb.GitLabGetGroupResponse{RawJson: []byte(`{"id":7}`)}, nil
		default:
			return nil, nil
		}
	}}
	sink := testkit.NewChannelSink()
	_, err := (&FetchConfigOp{}).Execute(context.Background(), &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: "gitlab", OrganizationID: "org-test",
			Credentials: mustJSON(GitLabCredentials{OAuthRefreshToken: "rt"}),
			Settings: mustJSON(GitLabSettings{
				GroupID:         7,
				DiscoveredRepos: []GitLabDiscoveredRepo{{FullName: "grp/api", ProjectID: 7}},
			}),
			Enabled: true,
		},
		Sink: sink, VendorGateway: gw,
	})
	require.NoError(t, err)
	recs := sink.Records()
	require.Len(t, recs, 2)
	require.NotEqual(t, recs[0].ExternalID, recs[1].ExternalID, "ACCOUNT and REPO records share no key")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

// Tests of how repository loops treat one failing repository, and of the
// commit cursor's advance.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// One repository whose commit query fails is skipped by fetch_commits: the
// repositories after it are read, and the run completes.
func TestOneFailingRepoIsSkippedByFetchCommits(t *testing.T) {
	var queried []string
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.ListOrgReposRequest:
				return &connectorpb.ListOrgReposResponse{Repos: [][]byte{
					mustJSON(ghRepository{ID: 1, FullName: "acme/a", DefaultBranch: "main"}),
					mustJSON(ghRepository{ID: 2, FullName: "acme/b", DefaultBranch: "main"}),
					mustJSON(ghRepository{ID: 3, FullName: "acme/c", DefaultBranch: "main"}),
				}}, nil
			case *connectorpb.GitHubGraphQLRequest:
				for _, name := range []string{`"a"`, `"b"`, `"c"`} {
					if strings.Contains(r.GetQuery(), name) {
						queried = append(queried, name)
						if name == `"b"` {
							return nil, errors.New("repository b: 502")
						}
					}
				}
				return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{DatabaseID: 1, CustomRef: &ghGQLRef{Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{}}}}}), nil
			}
			return nil, errors.New("unexpected request")
		},
	}
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: "github", OrganizationID: "org-test", Enabled: true,
			Credentials: mustJSON(GitHubCredentials{InstallationID: "1"}),
			Settings:    mustJSON(GitHubSettings{Organization: "acme"}),
		},
		Sink: testkit.NewChannelSink(), VendorGateway: gw,
	}
	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Contains(t, queried, `"b"`)
	assert.Contains(t, queried, `"c"`, "the repository after the failing one is read")
}

// One failing repository is skipped by each of these operations: the
// repositories after it are read, and the run completes.
func TestOneFailingRepoIsSkippedByTheOperation(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   sdk.Operation
	}{
		{"fetch_issues", &FetchIssuesOp{}},
		{"fetch_reviews", &FetchReviewsOp{}},
		{"fetch_pr_comments", &FetchPRCommentsOp{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queried []string
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
				if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok {
					for _, name := range []string{`name: "a"`, `name: "b"`} {
						if strings.Contains(r.GetQuery(), name) {
							queried = append(queried, name)
						}
					}
					if strings.Contains(r.GetQuery(), `name: "a"`) {
						return nil, errors.New("repository a: 502")
					}
				}
				return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{Issues: &ghGQLConnection[ghGQLIssueNode]{}}}), nil
			}}
			params := testParams(testkit.NewChannelSink(), gw, nil)
			params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/a", "acme/b"}})
			_, err := tc.op.(sdk.APIFetchOperation).Execute(context.Background(), params)
			require.NoError(t, err)
			assert.Contains(t, queried, `name: "a"`)
			assert.Contains(t, queried, `name: "b"`, "the repository after the failing one is read")
		})
	}
}

// fetch_commits advances each repository's cursor to the newest commit it read.
func TestFetchCommitsAdvancesTheCursor(t *testing.T) {
	newest := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if _, ok := req.(*connectorpb.GitHubGraphQLRequest); ok {
			history := &ghGQLConnection[ghGQLCommitNode]{Nodes: []ghGQLCommitNode{{OID: "c1", CommittedDate: newest}}}
			ref := &ghGQLRef{Target: ghGQLTarget{History: history}}
			return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{DatabaseID: 1, Ref: ref, CustomRef: ref}}), nil
		}
		return nil, errors.New("unexpected request")
	}}
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/a"}})
	cur, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Contains(t, string(cur.Data), newest.Format(time.RFC3339), "the cursor holds the newest commit's date: %s", cur.Data)
}

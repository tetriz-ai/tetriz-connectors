// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func forbidden() error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"Resource not accessible by integration"}`), nil)
}

// graphQLRepoOps are the GitHub operations that walk repositories over
// GraphQL and stop on a repository's failure.
var graphQLRepoOps = []struct {
	name string
	op   sdk.Operation
}{
	{"fetch_issues", &FetchIssuesOp{}},
	{"fetch_reviews", &FetchReviewsOp{}},
	{"fetch_pr_comments", &FetchPRCommentsOp{}},
}

func runRepoOp(t *testing.T, op sdk.Operation, failA, failB error) ([]string, error) {
	t.Helper()
	var queried []string
	gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		if r, ok := req.(*connectorpb.GitHubGraphQLRequest); ok {
			for _, name := range []string{`name: "a"`, `name: "b"`} {
				if strings.Contains(r.GetQuery(), name) {
					queried = append(queried, name)
				}
			}
			if failA != nil && strings.Contains(r.GetQuery(), `name: "a"`) {
				return nil, failA
			}
			if failB != nil && strings.Contains(r.GetQuery(), `name: "b"`) {
				return nil, failB
			}
		}
		// An empty repository: a non-null issues connection with no nodes.
		return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{Issues: &ghGQLConnection[ghGQLIssueNode]{}}}), nil
	}}
	params := testParams(testkit.NewChannelSink(), gw, nil)
	params.Config.Settings = mustJSON(GitHubSettings{Organization: "acme", Repositories: []string{"acme/a", "acme/b"}})
	_, err := op.(sdk.APIFetchOperation).Execute(context.Background(), params)
	return queried, err
}

func requireTerminalAuth(t *testing.T, err error) {
	t.Helper()
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindAuth, se.Kind)
	assert.True(t, se.Terminal, "the run pauses the connector")
}

// One repository that answers 403 is that repository's failure: it is skipped
// and the next repository is read. Every repository answering 403 is the
// credential's, so the run fails as a terminal auth error. A 401 still stops
// the run as auth, and a 403 the gateway reads as a secondary rate limit
// stays a rate limit.
func TestGitHubRepoLoopsSkipAForbiddenRepository(t *testing.T) {
	for _, tc := range graphQLRepoOps {
		t.Run(tc.name+"/one 403", func(t *testing.T) {
			queried, err := runRepoOp(t, tc.op, forbidden(), nil)
			require.NoError(t, err)
			assert.Contains(t, queried, `name: "b"`, "the repository after the forbidden one is read")
		})
		t.Run(tc.name+"/all 403", func(t *testing.T) {
			_, err := runRepoOp(t, tc.op, forbidden(), forbidden())
			requireTerminalAuth(t, err)
		})
		t.Run(tc.name+"/401", func(t *testing.T) {
			_, err := runRepoOp(t, tc.op, vendorapi.ClassifyHTTPErrorWithHeaders(401, []byte(`{"message":"Bad credentials"}`), nil), nil)
			requireTerminalAuth(t, err)
		})
		t.Run(tc.name+"/secondary rate limit", func(t *testing.T) {
			_, err := runRepoOp(t, tc.op, vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"message":"You have exceeded a secondary rate limit"}`), nil), nil)
			var se *sdk.SyncError
			require.True(t, errors.As(err, &se), "err=%v", err)
			assert.Equal(t, sdk.ErrKindRateLimit, se.Kind)
		})
	}
}

// fetch_commits lists the organization's repositories, then walks each; one
// forbidden repository is skipped and an all-forbidden walk fails as auth.
func TestGitHubFetchCommitsSkipsAForbiddenRepository(t *testing.T) {
	run := func(fail func(name string) error) ([]string, error) {
		var queried []string
		gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.ListOrgReposRequest:
				return &connectorpb.ListOrgReposResponse{Repos: [][]byte{
					mustJSON(ghRepository{ID: 1, FullName: "acme/a", DefaultBranch: "main"}),
					mustJSON(ghRepository{ID: 2, FullName: "acme/b", DefaultBranch: "main"}),
				}}, nil
			case *connectorpb.GitHubGraphQLRequest:
				for _, name := range []string{`"a"`, `"b"`} {
					if strings.Contains(r.GetQuery(), name) {
						queried = append(queried, name)
						if err := fail(name); err != nil {
							return nil, err
						}
					}
				}
				return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{DatabaseID: 1, CustomRef: &ghGQLRef{Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{}}}}}), nil
			}
			return nil, errors.New("unexpected request")
		}}
		params := &sdk.ExecutionParams{
			Config: &sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org-test", Enabled: true,
				Credentials: mustJSON(GitHubCredentials{InstallationID: "1"}), Settings: mustJSON(GitHubSettings{Organization: "acme"})},
			Sink: testkit.NewChannelSink(), VendorGateway: gw,
		}
		_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
		return queried, err
	}
	queried, err := run(func(name string) error {
		if name == `"a"` {
			return forbidden()
		}
		return nil
	})
	require.NoError(t, err)
	assert.Contains(t, queried, `"b"`)
	_, err = run(func(string) error { return forbidden() })
	requireTerminalAuth(t, err)
}

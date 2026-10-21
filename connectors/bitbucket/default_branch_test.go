// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// bbGetRepoJSON builds a single-repo JSON payload as the BitbucketGetRepo
// endpoint returns it, carrying the given default branch and UUID.
func bbGetRepoJSON(t *testing.T, mainbranch, uuid string) []byte {
	t.Helper()
	b, err := json.Marshal(bbRepository{
		FullName:   "myworkspace/myrepo",
		UUID:       uuid,
		Mainbranch: &bbBranchRef{Name: mainbranch, Type: "branch"},
	})
	require.NoError(t, err)
	return b
}

// branchDispatchGateway is a VendorGateway that records BitbucketGetRepo calls
// and routes Execute by request type. getRepoFn supplies the GetRepo response
// (or error); a nil getRepoFn means "GetRepo must not be called".
type branchDispatchGateway struct {
	getRepoCalls atomic.Int32
	getRepoFn    func() (*connectorpb.BitbucketGetRepoResponse, error)
	commits      []bbCommit
	// branchSHAByBranch maps a branch name to the SHA returned by the
	// /refs/branches/{name} DoJSON call; used by the tree path.
	branchSHAByBranch map[string]string
}

func (g *branchDispatchGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	switch req.(type) {
	case *connectorpb.BitbucketGetRepoRequest:
		g.getRepoCalls.Add(1)
		if g.getRepoFn == nil {
			return nil, fmt.Errorf("unexpected BitbucketGetRepo call")
		}
		return g.getRepoFn()
	case *connectorpb.BitbucketListCommitsRequest:
		commitBytes := make([][]byte, len(g.commits))
		for i, c := range g.commits {
			commitBytes[i], _ = json.Marshal(c)
		}
		return &connectorpb.BitbucketListCommitsResponse{Commits: commitBytes}, nil
	default:
		return nil, fmt.Errorf("unexpected Execute request type: %T", req)
	}
}

func (g *branchDispatchGateway) DoJSON(_ context.Context, _, urlStr string, _, respBody any) error {
	if strings.Contains(urlStr, "/refs/branches/") {
		r, ok := respBody.(*bbBranchSHAResponse)
		if !ok {
			return fmt.Errorf("unexpected DoJSON respBody %T", respBody)
		}
		for branch, sha := range g.branchSHAByBranch {
			if strings.Contains(urlStr, "/refs/branches/"+branch) {
				r.Target.Hash = sha
				return nil
			}
		}
		return fmt.Errorf("no branch SHA registered for url %s", urlStr)
	}
	return fmt.Errorf("unregistered DoJSON call: %s", urlStr)
}

func (g *branchDispatchGateway) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("Do not used")
}

func (g *branchDispatchGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *branchDispatchGateway) ForGroup(_ string) sdk.VendorGateway { return g }

// commitsParams builds ExecutionParams for fetch_commits with the given settings.
func commitsParams(sink sdk.DataSink, gw sdk.VendorGateway, settings BitbucketSettings) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "tok"}),
			Settings:       mustJSON(settings),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}
}

// TestFetchCommits_ExplicitRepo_ResolvesRealDefaultBranch verifies that on the
// explicit settings.Repositories path, fetch_commits issues a BitbucketGetRepo
// call and walks the repo's REAL default branch (master), never the literal
// "main" guess.
func TestFetchCommits_ExplicitRepo_ResolvesRealDefaultBranch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var includeSeen string
	gw := &branchDispatchGateway{
		getRepoFn: func() (*connectorpb.BitbucketGetRepoResponse, error) {
			return &connectorpb.BitbucketGetRepoResponse{Repo: bbGetRepoJSON(t, "master", "{uuid-1}")}, nil
		},
		commits: []bbCommit{{Hash: "abc123", Date: now, Message: "c"}},
	}
	// Wrap Execute to capture the Include the commits request used.
	captureGW := &captureIncludeGateway{branchDispatchGateway: gw, includeSeen: &includeSeen}

	sink := testkit.NewChannelSink()
	params := commitsParams(sink, captureGW, BitbucketSettings{
		Workspace:    "myworkspace",
		Repositories: []string{"myworkspace/myrepo"},
	})

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, int32(1), gw.getRepoCalls.Load(), "explicit repo must trigger exactly one GetRepo")
	assert.Equal(t, "master", includeSeen, "commits must walk the real default branch, not main")
	require.Len(t, sink.Records(), 1)
}

// TestFetchCommits_GetRepoFailure_FallsBackToMain verifies that when the
// BitbucketGetRepo fetch errors, fetch_commits falls back to the literal "main"
// branch (the load-bearing last resort that keeps the commits query
// single-branch instead of git log --all).
func TestFetchCommits_GetRepoFailure_FallsBackToMain(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var includeSeen string
	gw := &branchDispatchGateway{
		getRepoFn: func() (*connectorpb.BitbucketGetRepoResponse, error) {
			return nil, fmt.Errorf("boom")
		},
		commits: []bbCommit{{Hash: "abc123", Date: now, Message: "c"}},
	}
	captureGW := &captureIncludeGateway{branchDispatchGateway: gw, includeSeen: &includeSeen}

	sink := testkit.NewChannelSink()
	params := commitsParams(sink, captureGW, BitbucketSettings{
		Workspace:    "myworkspace",
		Repositories: []string{"myworkspace/myrepo"},
	})

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, int32(1), gw.getRepoCalls.Load(), "GetRepo is attempted once")
	assert.Equal(t, "main", includeSeen, "GetRepo failure must fall back to the literal main branch")
}

// TestFetchCommits_DiscoveredRepoCache_NoGetRepoCall verifies the auto-discovery
// path makes NO extra GetRepo call — the default branch is already known from
// the workspace listing cached in DiscoveredRepos. This guards the API budget.
func TestFetchCommits_DiscoveredRepoCache_NoGetRepoCall(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var includeSeen string
	gw := &branchDispatchGateway{
		getRepoFn: nil, // any GetRepo call returns an error and fails the assertion below
		commits:   []bbCommit{{Hash: "abc123", Date: now, Message: "c"}},
	}
	captureGW := &captureIncludeGateway{branchDispatchGateway: gw, includeSeen: &includeSeen}

	sink := testkit.NewChannelSink()
	params := commitsParams(sink, captureGW, BitbucketSettings{
		Workspace: "myworkspace",
		DiscoveredRepos: []BitbucketDiscoveredRepo{
			{FullName: "myworkspace/myrepo", DefaultBranch: "develop"},
		},
	})

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, int32(0), gw.getRepoCalls.Load(), "auto-discovery must not issue a GetRepo call")
	assert.Equal(t, "develop", includeSeen, "commits must use the cached default branch")
}

// captureIncludeGateway wraps branchDispatchGateway to record the Include field
// of the first BitbucketListCommitsRequest seen.
type captureIncludeGateway struct {
	*branchDispatchGateway
	includeSeen *string
}

func (g *captureIncludeGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if c, ok := req.(*connectorpb.BitbucketListCommitsRequest); ok && *g.includeSeen == "" {
		*g.includeSeen = c.GetInclude()
	}
	return g.branchDispatchGateway.Execute(ctx, req)
}
func (g *captureIncludeGateway) ForGroup(_ string) sdk.VendorGateway { return g }
func (g *captureIncludeGateway) WithRateLimits(_ []sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}

// TestFetchRepoTree_ExplicitRepo_ResolvesRealDefaultBranch verifies that on the
// explicit settings.Repositories path, fetch_repo_tree issues a BitbucketGetRepo
// call, walks the repo's real default branch (master), and records the resolved
// UUID as platform_repo_id.
func TestFetchRepoTree_ExplicitRepo_ResolvesRealDefaultBranch(t *testing.T) {
	const headSHA = "treehead123"

	ctrl := gomock.NewController(t)
	s3 := sdkmocks.NewMockBlobStore(ctrl)
	// SHA matches cursor → no_change path, no S3 write expected.

	gw := &branchDispatchGateway{
		getRepoFn: func() (*connectorpb.BitbucketGetRepoResponse, error) {
			return &connectorpb.BitbucketGetRepoResponse{Repo: bbGetRepoJSON(t, "master", "{uuid-tree}")}, nil
		},
		branchSHAByBranch: map[string]string{"master": headSHA},
	}

	sink := testkit.NewChannelSink()
	shaBytes, _ := json.Marshal(headSHA)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		"myworkspace/myrepo": shaBytes,
	})
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials:    mustJSON(BitbucketCredentials{OAuthAccessToken: "tok"}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{"myworkspace/myrepo"},
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gw,
		BlobStore:     s3,
	}

	_, err := (&FetchRepoTreeOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, int32(1), gw.getRepoCalls.Load(), "explicit repo must trigger exactly one GetRepo")
	records := sink.Records()
	require.Len(t, records, 1)
	rec := decodeBBTreeRecord(t, records[0].Payload)
	assert.Equal(t, "master", rec.TreeBranch, "tree must walk the real default branch, not main")
	assert.Equal(t, "{uuid-tree}", rec.PlatformRepoId, "explicit-repo UUID must come from GetRepo")
}

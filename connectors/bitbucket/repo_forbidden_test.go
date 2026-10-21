// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func bbForbidden() error {
	return vendorapi.ClassifyHTTPErrorWithHeaders(403, []byte(`{"type":"error","error":{"message":"Forbidden"}}`), nil)
}

func requireBBTerminalAuth(t *testing.T, err error) {
	t.Helper()
	var se *sdk.SyncError
	require.True(t, errors.As(err, &se), "err=%v", err)
	assert.Equal(t, sdk.ErrKindAuth, se.Kind)
	assert.True(t, se.Terminal)
}

// fetch_prs: one repository answering 403 is that repository's failure and
// the run completes; every repository answering 403, or a 401, fails the run
// as a terminal auth error.
func TestBitbucketFetchPRsSkipsAForbiddenRepository(t *testing.T) {
	repos := []string{"myworkspace/a", "myworkspace/b"}
	run := func(a, b repoPRFixture) error {
		gw := &mockVendorGateway{executeFunc: multiRepoExecutor(map[string]repoPRFixture{"myworkspace/a": a, "myworkspace/b": b})}
		_, err := (&FetchPRsOp{}).Execute(context.Background(), testParamsMultiRepo(testkit.NewChannelSink(), gw, nil, repos, nil))
		return err
	}
	require.NoError(t, run(repoPRFixture{err: bbForbidden()}, repoPRFixture{}))
	requireBBTerminalAuth(t, run(repoPRFixture{err: bbForbidden()}, repoPRFixture{err: bbForbidden()}))
	requireBBTerminalAuth(t, run(repoPRFixture{err: vendorapi.ClassifyHTTPErrorWithHeaders(401, nil, nil)}, repoPRFixture{}))
}

// fetch_issues stops on a repository's failure, but skips one that answered
// 403 and reads the next.
func TestBitbucketFetchIssuesSkipsAForbiddenRepository(t *testing.T) {
	var read []string
	run := func(failB bool) error {
		read = nil
		gw := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			r, ok := req.(*connectorpb.BitbucketListIssuesRequest)
			if !ok {
				return nil, errors.New("unexpected request")
			}
			read = append(read, r.GetRepoSlug())
			if r.GetRepoSlug() == "a" || failB {
				return nil, bbForbidden()
			}
			return &connectorpb.BitbucketListIssuesResponse{}, nil
		}}
		params := testParamsMultiRepo(testkit.NewChannelSink(), gw, nil, []string{"myworkspace/a", "myworkspace/b"}, nil)
		params.Config.Settings = mustJSON(BitbucketSettings{Workspace: "myworkspace", Repositories: []string{"myworkspace/a", "myworkspace/b"}, IssueTrackerEnabled: true})
		_, err := (&FetchIssuesOp{}).Execute(context.Background(), params)
		return err
	}
	require.NoError(t, run(false))
	assert.Contains(t, read, "b", "the repository after the forbidden one is read")
	requireBBTerminalAuth(t, run(true))
}

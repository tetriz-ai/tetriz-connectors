// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

var sinceInQuery = regexp.MustCompile(`since: "([^"]+)"`)

// TestEdge_MultiRefInterruptedWalkDoesNotRaiseTheSinceFloor covers a repository whose
// tracked refs are main and dev, interrupted on main's second history page.
//
// The per-page checkpoint stores the walk's in-progress high-water (main's newest commit)
// as the repo's Timestamp. A multi-ref walk cannot resume from a page cursor, so the next
// run restarts "from the watermark" — but that watermark is now main's newest commit, not
// the floor the interrupted run started from. Main's unread older pages and every commit
// on dev older than main's head then fall below `since` and are never fetched.
//
// The correct restart floor is the one the interrupted run started from (state.Since).
func TestEdge_MultiRefInterruptedWalkDoesNotRaiseTheSinceFloor(t *testing.T) {
	floor := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Second)
	mainHead := floor.Add(48 * time.Hour)

	resolverFor := func(t *testing.T) *sdkmocks.MockTrackedRefsResolver {
		m := sdkmocks.NewMockTrackedRefsResolver(gomock.NewController(t))
		m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
			Return(trackedRefsResponse([]string{"main", "dev"}, false), nil).AnyTimes()
		return m
	}
	listRepos := func() proto.Message {
		repo := mustJSON(ghRepository{ID: 42, FullName: "acme/repo", DefaultBranch: "main"})
		return &connectorpb.ListOrgReposResponse{Repos: [][]byte{repo}}
	}
	params := func(t *testing.T, gw sdk.VendorGateway, cursor *sdk.Cursor) *sdk.ExecutionParams {
		return &sdk.ExecutionParams{
			Config: &sdk.ConnectorConfig{
				ConnectorID:    "github",
				OrganizationID: "org-test",
				Credentials:    mustJSON(GitHubCredentials{InstallationID: "12345678"}),
				Settings:       mustJSON(GitHubSettings{Organization: "acme"}),
				Enabled:        true,
			},
			Cursor:        cursor,
			Sink:          testkit.NewChannelSink(),
			VendorGateway: gw,
			Host:          sdk.HostServices{TrackedRefs: resolverFor(t)},
		}
	}

	// Run 1: main page 1 succeeds with a next page, main page 2 fails.
	var saved *sdk.Cursor
	run1 := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.ListOrgReposRequest:
			return listRepos(), nil
		case *connectorpb.GitHubGraphQLRequest:
			if refFromQuery(r.GetQuery()) == "main" && !regexp.MustCompile(`after: "p2"`).MatchString(r.GetQuery()) {
				return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{DatabaseID: 42, CustomRef: &ghGQLRef{
					Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{
						Nodes:    []ghGQLCommitNode{{OID: "head", CommittedDate: mainHead}},
						PageInfo: ghPageInfo{HasNextPage: true, EndCursor: "p2"},
					}},
				}}}), nil
			}
			return nil, errors.New("connection reset")
		}
		return nil, fmt.Errorf("unexpected %T", req)
	}}
	p1 := params(t, run1, buildStateCursor(map[string]repoCursorState{"acme/repo": {Timestamp: floor}}))
	p1.Checkpoint = func(_ context.Context, c *sdk.Cursor) error { saved = c; return nil }
	_, _ = (&FetchCommitsOp{}).Execute(context.Background(), p1)
	require.NotNil(t, saved, "run 1 must checkpoint after main's first page")

	// Run 2 resumes from the checkpoint run 1 left behind.
	var sinces []time.Time
	run2 := &mockVendorGateway{executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
		switch r := req.(type) {
		case *connectorpb.ListOrgReposRequest:
			return listRepos(), nil
		case *connectorpb.GitHubGraphQLRequest:
			if m := sinceInQuery.FindStringSubmatch(r.GetQuery()); m != nil {
				ts, err := time.Parse(time.RFC3339, m[1])
				require.NoError(t, err)
				sinces = append(sinces, ts)
			}
			return graphQLDataResponse(ghGraphQLData{Repository: ghGQLRepository{DatabaseID: 42, CustomRef: &ghGQLRef{
				Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{}},
			}}}), nil
		}
		return nil, fmt.Errorf("unexpected %T", req)
	}}
	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params(t, run2, saved))
	require.NoError(t, err)
	require.NotEmpty(t, sinces)
	for _, s := range sinces {
		assert.False(t, s.After(floor),
			"restarting an interrupted multi-ref walk must query from the run's original floor %s, got since=%s — "+
				"commits between them on main's unread pages and on dev are otherwise never fetched", floor, s)
	}
}

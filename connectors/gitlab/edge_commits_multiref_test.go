// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// staticRefsResolver answers ResolveTrackedRefs with a fixed response.
type staticRefsResolver struct {
	resp *workqueuev1.ResolveTrackedRefsResponse
}

func (s staticRefsResolver) ResolveTrackedRefs(context.Context, *workqueuev1.ResolveTrackedRefsRequest) (*workqueuev1.ResolveTrackedRefsResponse, error) {
	return s.resp, nil
}

// TestEdge_CommitReachedFromTwoRefsKeepsItsEnrichedPayload covers a commit that
// both tracked refs contain (main's history inside a release branch, the ordinary
// case). The first sighting is emitted with the detail payload, which carries
// line stats. The second sighting skips the detail call, and the record upsert
// assigns wholesale, so the LAST emitted record is what the row ends up holding:
// it must still carry the stats, not fall back to the stats-less list payload.
func TestEdge_CommitReachedFromTwoRefsKeepsItsEnrichedPayload(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	const projectPath = "mygroup/myrepo"
	project := glProject{ID: 7, Path: "myrepo", PathWithNamespace: projectPath, DefaultBranch: "main"}

	listJSON, err := json.Marshal(glCommit{ID: "c1", CommittedDate: now})
	require.NoError(t, err)
	detailJSON := []byte(fmt.Sprintf(`{"id":"c1","committed_date":%q,"stats":{"additions":12,"deletions":3,"total":15}}`,
		now.Format(time.RFC3339)))

	detailCalls := 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *connectorpb.GitLabListProjectsRequest:
				return glProjectsResponse([]glProject{project}), nil
			case *connectorpb.GitLabListCommitsRequest:
				return &connectorpb.GitLabListCommitsResponse{Commits: [][]byte{listJSON}}, nil
			case *connectorpb.GitLabGetCommitDetailRequest:
				detailCalls++
				return &connectorpb.GitLabGetCommitDetailResponse{Commit: detailJSON}, nil
			default:
				return nil, fmt.Errorf("unexpected request %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.Host.TrackedRefs = staticRefsResolver{resp: &workqueuev1.ResolveTrackedRefsResponse{
		Repos: []*workqueuev1.RepoTrackedRefs{{
			PlatformRepoId: "7",
			RepoFullName:   projectPath,
			Refs:           []string{"main", "release/2026.10"},
		}},
	}}

	_, err = (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 2, "one sighting per ref")
	assert.Equal(t, 1, detailCalls, "the detail call is paid once per commit, not once per ref")

	last := records[len(records)-1]
	assert.Equal(t, "main,release/2026.10", last.Metadata[metaKeyRefs], "the last write carries the union of refs")
	assert.Contains(t, string(last.Payload), `"stats"`,
		"the last write wins the upsert, so it must carry the enriched payload; otherwise "+
			"every commit on two tracked refs loses its line stats")
}

// TestEdge_ProjectListingIsBoundedWhenNextPageNeverEnds covers a vendor (or a
// proxy) whose pagination header never clears. Every other GitLab loop that pages
// a list is bounded (maxBranchPages, maxTreePages) "so a pathological input must
// not run the sync indefinitely"; the group project listing is the first call of
// almost every operation, so it needs the same guarantee.
func TestEdge_ProjectListingIsBoundedWhenNextPageNeverEnds(t *testing.T) {
	const safetyStop = 5000 // the fake ends the loop itself here so the test cannot hang
	calls := 0
	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*connectorpb.GitLabListProjectsRequest); !ok {
				return nil, fmt.Errorf("unexpected request %T", req)
			}
			calls++
			resp := glProjectsResponse([]glProject{{ID: calls, PathWithNamespace: fmt.Sprintf("g/p%d", calls)}})
			if calls < safetyStop {
				resp.NextPageUrl = "https://gitlab.example/api/v4/groups/42/projects?page=next"
			}
			return resp, nil
		},
	}

	_, err := fetchAllProjects(context.Background(), gw, GitLabSettings{GroupID: 42})
	assert.Less(t, calls, safetyStop,
		"fetchAllProjects must stop at a page cap rather than follow a never-ending next page")
	assert.Error(t, err, "hitting the cap must be reported, not returned as a complete listing")
}

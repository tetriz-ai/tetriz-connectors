// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/tetriz-ai/tetriz-connectors/internal/vcs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// commitsByRef drives FetchCommitsOp with the resolver stubbed, and returns the qualified
// ref each commit query targeted alongside the emitted records. commitsPerRef maps a ref to
// the commit OIDs the vendor should return for it, so one OID can appear under two refs —
// which is the multiplicity case the accumulator exists for.
func commitsByRef(t *testing.T, resolver func(m *sdkmocks.MockTrackedRefsResolver), commitsPerRef map[string][]string) (refsQueried []string, records []*sdk.RawRecord) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)

	gw := &mockVendorGateway{
		executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
			switch r := req.(type) {
			case *connectorpb.ListOrgReposRequest:
				repo := mustJSON(ghRepository{ID: 42, FullName: "acme/repo", DefaultBranch: "main"})
				return &connectorpb.ListOrgReposResponse{Repos: [][]byte{repo}, HasNextPage: false}, nil
			case *connectorpb.GitHubGraphQLRequest:
				// The query embeds refs/heads/<ref>; recover which ref this page is for.
				ref := refFromQuery(r.GetQuery())
				refsQueried = append(refsQueried, ref)
				nodes := make([]ghGQLCommitNode, 0, len(commitsPerRef[ref]))
				for _, oid := range commitsPerRef[ref] {
					nodes = append(nodes, ghGQLCommitNode{OID: oid, CommittedDate: now})
				}
				return graphQLDataResponse(ghGraphQLData{
					Repository: ghGQLRepository{
						DatabaseID: 42,
						CustomRef:  &ghGQLRef{Target: ghGQLTarget{History: &ghGQLConnection[ghGQLCommitNode]{Nodes: nodes}}},
					},
				}), nil
			default:
				return nil, fmt.Errorf("unexpected request %T", req)
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "github",
			OrganizationID: "org-test",
			Credentials:    mustJSON(GitHubCredentials{InstallationID: "12345678"}),
			Settings:       mustJSON(GitHubSettings{Organization: "acme"}),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}
	if resolver != nil {
		ctrl := gomock.NewController(t)
		client := sdkmocks.NewMockTrackedRefsResolver(ctrl)
		resolver(client)
		params.Host.TrackedRefs = client
	}

	_, err := (&FetchCommitsOp{}).Execute(context.Background(), params)
	require.NoError(t, err)
	return refsQueried, sink.Records()
}

// refFromQuery pulls the ref out of a commits query's qualifiedName.
func refFromQuery(q string) string {
	const marker = `refs/heads/`
	i := strings.Index(q, marker)
	if i < 0 {
		return ""
	}
	rest := q[i+len(marker):]
	if j := strings.IndexAny(rest, `"\`+"\n"); j >= 0 {
		return rest[:j]
	}
	return rest
}

func trackedRefsResponse(refs []string, allBranches bool) *workqueuev1.ResolveTrackedRefsResponse {
	return &workqueuev1.ResolveTrackedRefsResponse{
		Repos: []*workqueuev1.RepoTrackedRefs{{
			PlatformRepoId: "42",
			RepoFullName:   "acme/repo",
			Refs:           refs,
			AllBranches:    allBranches,
		}},
	}
}

// TestTrackedBranchCommits_WalksEveryResolvedRef is the slice's headline proof: the walk
// visits the administrator's set rather than one pinned branch, and each
// record names the ref it was reached from.
func TestTrackedBranchCommits_WalksEveryResolvedRef(t *testing.T) {
	refs, records := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(trackedRefsResponse([]string{"main", "release/2026-09"}, false), nil).Times(1)
		},
		map[string][]string{
			"main":            {"c-main"},
			"release/2026-09": {"c-release"},
		})

	assert.ElementsMatch(t, []string{"main", "release/2026-09"}, refs,
		"every resolved ref must be walked, not just the default branch")
	require.Len(t, records, 2)
	byRef := map[string]string{}
	for _, r := range records {
		byRef[r.Metadata[metaKeyRefs]] = r.ExternalID
	}
	assert.Contains(t, byRef, "main")
	assert.Contains(t, byRef, "release/2026-09")
}

// TestTrackedBranchCommits_OneCommitOnTwoRefsConvergesToAllRefs pins how the ref union
// survives without buffering the walk.
//
// The record is re-emitted on each sighting so that pages stream to the sink before the
// cursor advances past them. Those emits share one ExternalID, and the downstream dedup key
// does not vary by ref, so they collapse to a single row — and because refs only ever
// accumulate, the LAST emit carries the complete set and the upsert's wholesale metadata
// assignment makes it the one that survives.
func TestTrackedBranchCommits_OneCommitOnTwoRefsConvergesToAllRefs(t *testing.T) {
	_, records := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(trackedRefsResponse([]string{"main", "release/2026-09"}, false), nil).Times(1)
		},
		map[string][]string{
			"main":            {"shared"},
			"release/2026-09": {"shared"},
		})

	require.NotEmpty(t, records)
	// Every emit for this commit is the same dedup key, so the database holds one row.
	first := records[0].ExternalID
	for _, r := range records {
		assert.Equal(t, first, r.ExternalID, "all emits must share one dedup key, so they collapse to a single row")
	}
	// And the surviving write — the last one — carries every ref.
	assert.Equal(t, "main,release/2026-09", records[len(records)-1].Metadata[metaKeyRefs],
		"the last write wins, so it must name every ref the commit was reached from")
}

// TestTrackedBranchCommits_ResolverFailureUsesDefaultBranchOnly pins the safety property.
// Falling back to every branch on a settings outage would turn it into a provider-budget
// incident, so the assertion is on equality — Contains would pass while also walking others.
func TestTrackedBranchCommits_ResolverFailureUsesDefaultBranchOnly(t *testing.T) {
	refs, records := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("resolver unavailable")).Times(1)
		},
		map[string][]string{"main": {"c-main"}})

	assert.Equal(t, []string{"main"}, refs,
		"a resolver failure walks the default branch and nothing else — never all")
	require.Len(t, records, 1)
}

// TestTrackedBranchCommits_MatchNoneFetchesNothing covers the resolver's documented empty
// case: refs empty with all_branches false means the repo resolved to no countable refs, so
// fetching nothing is the answer rather than falling back to a branch nobody asked for.
func TestTrackedBranchCommits_MatchNoneFetchesNothing(t *testing.T) {
	refs, records := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(trackedRefsResponse(nil, false), nil).Times(1)
		},
		map[string][]string{"main": {"c-main"}})

	assert.Empty(t, refs, "MatchNone must query no ref at all")
	assert.Empty(t, records, "and emit nothing")
}

// TestTrackedBranchCommits_CapBoundsTheWalk proves the per-repo bound holds. Cost is
// strictly refs × pages because no provider call covers several refs and attributes each
// commit, so an unbounded set is a budget incident waiting on one administrator's config.
func TestTrackedBranchCommits_CapBoundsTheWalk(t *testing.T) {
	many := make([]string, 0, vcs.MaxRefsPerRepo+3)
	commits := map[string][]string{}
	for i := 0; i < vcs.MaxRefsPerRepo+3; i++ {
		ref := fmt.Sprintf("branch-%02d", i)
		many = append(many, ref)
		commits[ref] = []string{fmt.Sprintf("c-%02d", i)}
	}

	refs, _ := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(trackedRefsResponse(many, false), nil).Times(1)
		}, commits)

	assert.Len(t, refs, vcs.MaxRefsPerRepo,
		"the walk must stop at the cap however many refs an administrator lists")
	// Sorted, so the subset is the same on every run rather than map order.
	assert.Equal(t, []string{"branch-00", "branch-01", "branch-02", "branch-03", "branch-04"}, refs)
}

// TestTrackedBranchCommits_CapNeverDropsTheDefaultBranch pins a case a
// plain sorted truncation hides in an ordinary configuration: "feature/a".."feature/e" plus
// "main" sorts main into sixth place, so a cap of five would silently drop the default
// branch while walking five feature branches.
func TestTrackedBranchCommits_CapNeverDropsTheDefaultBranch(t *testing.T) {
	tracked := []string{"feature/a", "feature/b", "feature/c", "feature/d", "feature/e", "main"}
	commits := map[string][]string{}
	for _, r := range tracked {
		commits[r] = []string{"c-" + r}
	}

	refs, _ := commitsByRef(t,
		func(m *sdkmocks.MockTrackedRefsResolver) {
			m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).
				Return(trackedRefsResponse(tracked, false), nil).Times(1)
		}, commits)

	require.Len(t, refs, vcs.MaxRefsPerRepo)
	assert.Contains(t, refs, "main",
		"the default branch must survive the cap however the rest sort")
}

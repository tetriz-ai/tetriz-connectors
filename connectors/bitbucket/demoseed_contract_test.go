// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

// This internal test pins the seed↔connector contract: it feeds the demoseed
// Bitbucket builders' output through the connector's own (unexported) bb* structs.
// A renamed or retyped connector field breaks it at build/test time here, not in a
// silently-zeroed field downstream. It lives in this package — not demoseed — because the bb* structs are
// unexported; the reverse import is test-only, so there is no cycle.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/demoseed"
)

func bbTestOrg() demoseed.OrgCatalog {
	return demoseed.OrgCatalog{
		OrgName: "demo-bb-enterprise",
		Vendor:  "bitbucket",
		Repos: []demoseed.RepoSpec{
			{Name: "ledger-core", Flavor: demoseed.FlavorMonorepo},
			{Name: "tools", Flavor: demoseed.FlavorDirectPush},
		},
		MemberNames: []string{"alice", "bob", "carol"},
	}
}

// reencode round-trips a builder's map[string]any payload through JSON into the
// connector struct T — exactly the path a real sync takes (simulator serves JSON,
// connector unmarshals). A shape mismatch surfaces as a decode error or a zero field.
func reencode[T any](t *testing.T, payload any) T {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func TestDemoseedMembersMatchConnector(t *testing.T) {
	for _, m := range demoseed.BuildBitbucketMembers(bbTestOrg()) {
		member := reencode[bbWorkspaceMember](t, m)
		require.NotNil(t, member.User, "member.user must be present")
		require.NotEmpty(t, member.User.AccountID, "every member carries account_id (consumer attribution key)")
		require.NotEmpty(t, member.User.DisplayName)
	}
}

func TestDemoseedReposMatchConnector(t *testing.T) {
	for _, r := range demoseed.BuildBitbucketRepos(bbTestOrg(), time.Now().Add(-90*24*time.Hour)) {
		repo := reencode[bbRepository](t, r)
		require.NotEmpty(t, repo.Slug, "repo slug drives repo-scoped queries")
		require.NotEmpty(t, repo.UUID)
		require.NotNil(t, repo.Mainbranch, "single-repo route resolves the default branch from mainbranch")
		require.NotEmpty(t, repo.Mainbranch.Name)
		require.NotNil(t, repo.Workspace)
	}
}

func TestDemoseedPRsMatchConnector(t *testing.T) {
	org := bbTestOrg()
	repo := org.Repos[0] // ledger-core (monorepo, has PRs)
	prs := demoseed.BuildBitbucketPRs(org, repo, demoseed.BuildBitbucketMembers(org), 5, 0, time.Now().Add(-30*24*time.Hour), time.Now())
	require.NotEmpty(t, prs)

	for _, p := range prs {
		pr := reencode[bbPullRequest](t, p)
		require.Positive(t, pr.ID, "PR keyed on numeric id, not github's number")
		require.NotNil(t, pr.Author)
		require.NotEmpty(t, pr.Author.AccountID)
		require.NotNil(t, pr.Source)
		require.NotNil(t, pr.Destination)

		// The nested arrays the per-PR routes serve must each decode into the
		// connector's struct for that route.
		for _, c := range reencodeArray[bbCommit](t, p, "commits") {
			require.NotEmpty(t, c.Hash)
			require.NotNil(t, c.Author)
			require.Contains(t, c.Author.Raw, "@"+org.OrgName+".example.com",
				"commit author.raw must match the seed email pattern so both identity-linking paths converge")
			require.NotNil(t, c.Author.User)
			require.NotEmpty(t, c.Author.User.AccountID)
		}
		for _, d := range reencodeArray[bbDiffstatEntry](t, p, "diffstat") {
			require.NotEmpty(t, d.Status)
			require.NotNil(t, d.New)
		}
		for _, a := range reencodeArray[bbActivity](t, p, "activity") {
			require.True(t, a.Approval != nil || a.Comment != nil, "activity entry is an approval or a comment")
		}
	}
}

func TestDemoseedDirectPushCommitsMatchConnector(t *testing.T) {
	org := bbTestOrg()
	repo := org.Repos[1] // tools (direct-push)
	commits := demoseed.BuildBitbucketDirectPushCommits(org, repo, demoseed.BuildBitbucketMembers(org), 4, time.Now().Add(-30*24*time.Hour), time.Now())
	require.Len(t, commits, 4)
	for _, c := range commits {
		commit := reencode[bbCommit](t, c)
		require.NotEmpty(t, commit.Hash)
		require.False(t, commit.Date.IsZero(), "commit date must parse (drives the fetch_commits cursor)")
		require.NotNil(t, commit.Author)
		require.NotNil(t, commit.Author.User)
		require.NotEmpty(t, commit.Author.User.AccountID)
	}
}

// TestDemoseedIdentityCorrelation pins the identity invariant: one login
// carries the SAME uuid and account_id everywhere it appears (member row, PR author,
// commit author). This is what lets linkIdentities' MANUALLY_LINKED write and the
// connector's runtime resolution converge on one person; a future change that
// threaded a per-PR index into the identity seed would de-correlate them and every
// per-entity assertion above would still pass — only this cross-entity check fails.
func TestDemoseedIdentityCorrelation(t *testing.T) {
	org := bbTestOrg()

	// account_id -> uuid, taken from the member rows as the source of truth.
	memberUUID := map[string]string{}
	for _, m := range demoseed.BuildBitbucketMembers(org) {
		member := reencode[bbWorkspaceMember](t, m)
		memberUUID[member.User.AccountID] = member.User.UUID
	}
	require.NotEmpty(t, memberUUID)

	assertMatchesMember := func(u *bbUser, where string) {
		require.NotNil(t, u, where)
		want, ok := memberUUID[u.AccountID]
		require.True(t, ok, "%s: account_id %q must belong to a seeded member", where, u.AccountID)
		require.Equal(t, want, u.UUID, "%s: a login must carry its member row's uuid everywhere", where)
	}

	prs := demoseed.BuildBitbucketPRs(org, org.Repos[0], demoseed.BuildBitbucketMembers(org), 8, 0, time.Now().Add(-30*24*time.Hour), time.Now())
	for _, p := range prs {
		pr := reencode[bbPullRequest](t, p)
		assertMatchesMember(pr.Author, "pr.author")
		for _, c := range reencodeArray[bbCommit](t, p, "commits") {
			require.NotNil(t, c.Author)
			assertMatchesMember(c.Author.User, "pr.commits[].author.user")
		}
	}
	for _, c := range demoseed.BuildBitbucketDirectPushCommits(org, org.Repos[1], demoseed.BuildBitbucketMembers(org), 4, time.Now().Add(-30*24*time.Hour), time.Now()) {
		commit := reencode[bbCommit](t, c)
		require.NotNil(t, commit.Author)
		assertMatchesMember(commit.Author.User, "direct-push commit.author.user")
	}
}

// reencodeArray extracts a named nested array from a payload map and decodes each
// element into T.
func reencodeArray[T any](t *testing.T, payload any, field string) []T {
	t.Helper()
	m, ok := payload.(map[string]any)
	require.True(t, ok, "payload must be a map")
	raw, ok := m[field]
	require.True(t, ok, "payload must carry %q", field)
	b, err := json.Marshal(raw)
	require.NoError(t, err)
	var out []T
	require.NoError(t, json.Unmarshal(b, &out))
	require.NotEmpty(t, out, "%q array must be non-empty", field)
	return out
}

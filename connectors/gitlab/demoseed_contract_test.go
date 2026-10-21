// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

// This internal test pins the seed↔connector contract: it feeds the demoseed
// GitLab builders' output through the connector's own (unexported) gl* structs.
// A renamed or retyped connector field breaks it at build/test time here, not in a
// silently-zeroed field downstream.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/demoseed"
)

func glTestOrg() demoseed.OrgCatalog {
	return demoseed.OrgCatalog{
		OrgName: "demo-gl-regulated",
		Vendor:  "gitlab",
		Repos: []demoseed.RepoSpec{
			{Name: "ledger", Flavor: demoseed.FlavorGitLabFlow},
			{Name: "scripts", Flavor: demoseed.FlavorDirectPush},
		},
		MemberNames: []string{"alice", "bob", "carol"},
	}
}

func reencode[T any](t *testing.T, payload any) T {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func TestDemoseedMembersMatchConnector(t *testing.T) {
	for _, m := range demoseed.BuildGitLabMembers(glTestOrg()) {
		member := reencode[glGroupMember](t, m)
		require.NotZero(t, member.ID, "every member carries numeric id (consumer attribution key)")
		require.NotEmpty(t, member.Username)
	}
}

func TestDemoseedReposMatchConnector(t *testing.T) {
	org := glTestOrg()
	for _, r := range demoseed.BuildGitLabRepos(org, time.Now().Add(-90*24*time.Hour)) {
		repo := reencode[glProject](t, r)
		require.NotZero(t, repo.ID, "project id is platform_repo_id")
		require.NotEmpty(t, repo.PathWithNamespace)
		require.NotEmpty(t, repo.Path)
		require.NotEmpty(t, repo.DefaultBranch)
	}
}

func TestDemoseedMRsMatchConnector(t *testing.T) {
	org := glTestOrg()
	repo := org.Repos[0]
	mrs := demoseed.BuildGitLabMRs(org, repo, 1, demoseed.BuildGitLabMembers(org), 5, 0, time.Now().Add(-30*24*time.Hour), time.Now())
	require.NotEmpty(t, mrs)

	for _, p := range mrs {
		mr := reencode[glMergeRequest](t, p)
		require.Positive(t, mr.IID, "MR keyed on iid, not github's number")
		require.Equal(t, int(demoseed.GitLabProjectID(org.OrgName, 1)), mr.ProjectID)
		require.NotZero(t, mr.Author.ID)
		require.NotEmpty(t, mr.Author.Username)

		for _, c := range reencodeArray[glCommit](t, p, "commits") {
			require.NotEmpty(t, c.ID)
			require.False(t, c.CommittedDate.IsZero())
		}
		for _, n := range reencodeArray[glNote](t, p, "notes") {
			require.NotZero(t, n.Author.ID)
		}
		for _, d := range reencodeArray[glMRDiffEntry](t, p, "diffs") {
			require.NotEmpty(t, d.NewPath)
		}
	}
}

func TestDemoseedCommitsMatchConnector(t *testing.T) {
	org := glTestOrg()
	repo := org.Repos[1]
	commits := demoseed.BuildGitLabDirectPushCommits(org, repo, 2, demoseed.BuildGitLabMembers(org), 4, time.Now().Add(-30*24*time.Hour), time.Now())
	require.NotEmpty(t, commits)
	for _, c := range commits {
		got := reencode[glCommit](t, c)
		require.NotEmpty(t, got.ID)
		require.NotEmpty(t, got.AuthorEmail)
		require.False(t, got.CommittedDate.IsZero())
	}
}

func TestDemoseedIssuesCarryAuthor(t *testing.T) {
	org := glTestOrg()
	org.PMTool = "gitlab-issues"
	issues := demoseed.BuildGitLabIssues(org, org.Repos[0], 1, demoseed.BuildGitLabMembers(org), 4, 0, time.Now().Add(-30*24*time.Hour), time.Now())
	require.NotEmpty(t, issues)
	for _, iss := range issues {
		got := reencode[glIssue](t, iss)
		require.Positive(t, got.IID)
		require.NotZero(t, got.ProjectID)
		author := iss["author"].(map[string]any)
		require.NotZero(t, author["id"])
		require.NotEmpty(t, author["username"])
	}
}

func TestGitLabIDsMatchRFCWorkedExample(t *testing.T) {
	require.Equal(t, int64(91001), demoseed.GitLabGroupID("demo-gl-regulated"))
	require.Equal(t, int64(9100101), demoseed.GitLabProjectID("demo-gl-regulated", 1))
	require.Equal(t, int64(91001001), demoseed.GitLabUserID("demo-gl-regulated", 1))
}

func reencodeArray[T any](t *testing.T, payload map[string]any, field string) []T {
	t.Helper()
	raw, ok := payload[field]
	if !ok {
		return nil
	}
	b, err := json.Marshal(raw)
	require.NoError(t, err)
	var out []T
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

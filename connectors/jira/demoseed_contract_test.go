// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

// This internal test pins the seed↔connector contract: it feeds the demoseed Jira
// builders' output through the connector's own (unexported) jira* structs, and
// asserts the parser-read field keys are present. A renamed/retyped connector field
// or a dropped custom-field id breaks it at build/test time here, not as a
// silently-zeroed field downstream. It lives
// in this package — not demoseed — because the jira* structs are unexported; the
// reverse import is test-only, so there is no cycle.

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/demoseed"
)

func jiraTestOrg() demoseed.OrgCatalog {
	return demoseed.OrgCatalog{
		OrgName: "demo-gh-enterprise",
		Vendor:  "github",
		PMTool:  "jira",
		Repos: []demoseed.RepoSpec{
			{Name: "platform-monorepo", Flavor: demoseed.FlavorMonorepo},
		},
		MemberNames: []string{"alice", "bob", "carol"},
	}
}

// github members are plain {login: …} maps (demoseed.buildMembers' shape); the Jira
// builders read only the login, so this minimal shape is enough for the contract.
func jiraTestMembers() []map[string]any {
	return []map[string]any{{"login": "alice"}, {"login": "bob"}, {"login": "carol"}}
}

// The Jira issue builder derives one issue per pull request, reading only the
// number and timestamps off each — so, as with jiraTestMembers, the minimal GitHub
// pull-request shape is enough for the contract. Enough of them that the role split
// yields epics, linked issues, unlinked issues, and parents.
func jiraTestPRRefs(t *testing.T, org demoseed.OrgCatalog, repo demoseed.RepoSpec) []demoseed.JiraPRRef {
	t.Helper()
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	prs := make([]map[string]any, 0, 40)
	for n := 1; n <= 40; n++ {
		opened := base.Add(time.Duration(n) * 24 * time.Hour)
		pr := map[string]any{
			"number":    n,
			"repo":      repo.Name,
			"createdAt": opened.Format(time.RFC3339),
			"updatedAt": opened.Add(2 * time.Hour).Format(time.RFC3339),
		}
		// Leave every third pull request unmerged, so the linked-and-open branch
		// (which must NOT emit a resolutiondate) is exercised too.
		if n%3 != 0 {
			pr["mergedAt"] = opened.Add(6 * time.Hour).Format(time.RFC3339)
		}
		prs = append(prs, pr)
	}
	refs, err := demoseed.BuildJiraPRRefs(org, prs)
	require.NoError(t, err)
	return refs
}

func reencode[T any](t *testing.T, payload any) T {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func TestDemoseedJiraProjectsMatchConnector(t *testing.T) {
	projects := demoseed.BuildJiraProjects(jiraTestOrg())
	require.NotEmpty(t, projects)
	for _, p := range projects {
		proj := reencode[jiraProject](t, p)
		require.NotEmpty(t, proj.Key, "project key drives fetch_projects + JQL project filter")
		require.NotEmpty(t, proj.Name)
	}
}

func TestDemoseedJiraIssuesMatchConnector(t *testing.T) {
	org := jiraTestOrg()
	repo := org.Repos[0]
	refs := jiraTestPRRefs(t, org, repo)
	issues := demoseed.BuildJiraIssues(org, jiraTestMembers(), map[string][]demoseed.JiraPRRef{repo.Name: refs})
	require.NotEmpty(t, issues)

	seededKeys := map[string]bool{}
	for _, raw := range issues {
		seededKeys[raw["key"].(string)] = true //nolint:forcetypeassert // demoseed built the map
	}
	sprintIDs := map[string]bool{}
	for _, sp := range demoseed.BuildJiraSprints(org) {
		sprintIDs[fmt.Sprint(sp["id"])] = true
	}

	var sawParent, sawSprint, sawResolved, sawUnresolved bool
	for _, raw := range issues {
		// Decoding into jiraIssue proves the payload shape AND that the seed's
		// timestamp format is accepted by the connector's jiraTime decoder (its
		// UnmarshalJSON errors on an unknown layout — which reencode would surface).
		issue := reencode[jiraIssue](t, raw)
		require.NotEmpty(t, issue.Key, "issue key is the issue's identity and the PR-linkage target")
		require.NotEmpty(t, issue.Fields.Summary)
		require.NotEmpty(t, issue.Fields.Project.Key)
		require.Contains(t, []string{"new", "indeterminate", "done"}, issue.Fields.Status.StatusCategory.Key,
			"status category must be a value the parser maps to a workflow state")

		// jiraTime.UnmarshalJSON tolerates an empty string by yielding the zero time,
		// so decoding alone does not prove a timestamp survived. a consumer anchors its
		// data-window RLS on created — a zero value there hides the issue from every
		// analytics read.
		require.False(t, issue.Fields.Created.IsZero(), "issue %s: fields.created decoded to the zero time", issue.Key)
		require.False(t, issue.Fields.Updated.IsZero(), "issue %s: fields.updated decoded to the zero time", issue.Key)

		// jiraIssueFields is lightweight — the parser reads most fields off the raw
		// map, so assert those keys explicitly (a rename there is not a build break).
		fields, ok := raw["fields"].(map[string]any)
		require.True(t, ok, "issue payload must carry a fields object")
		for _, key := range []string{"created", "updated", "duedate", "labels", "issuetype", "priority", "assignee", "reporter", "customfield_10015", "customfield_10016", "customfield_10020"} {
			require.Contains(t, fields, key, "parser reads fields.%s", key)
		}
		assignee, ok := fields["assignee"].(map[string]any)
		require.True(t, ok, "assignee must be an object")
		require.NotEmpty(t, assignee["accountId"], "assignee.accountId is the identity attribution key")

		// A resolutiondate exactly when the issue is done: present on an open issue it
		// would fabricate a completion, absent on a done one drops the completion.
		if issue.Fields.Status.StatusCategory.Key == "done" {
			require.NotNil(t, issue.Fields.ResolutionDate, "issue %s is done and must carry a resolutiondate", issue.Key)
			require.False(t, issue.Fields.ResolutionDate.IsZero(), "issue %s: resolutiondate decoded to the zero time", issue.Key)
			sawResolved = true
		} else {
			require.Nil(t, issue.Fields.ResolutionDate, "issue %s is not done and must omit resolutiondate", issue.Key)
			sawUnresolved = true
		}

		// A sprint reference must name a sprint that is actually seeded, or
		// pickJiraSprintKey resolves to a sprint the board handler never serves.
		sprintRef, ok := fields["customfield_10020"].([]any)
		require.True(t, ok, "customfield_10020 must be an array")
		for _, entry := range sprintRef {
			m, ok := entry.(map[string]any)
			require.True(t, ok, "sprint entry must be an object")
			require.True(t, sprintIDs[fmt.Sprint(m["id"])], "issue %s references unseeded sprint %v", issue.Key, m["id"])
			sawSprint = true
		}

		// A parent must name a seeded issue, or PMIssueMetadata carries a dangling
		// parent_issue_key.
		if parent, present := fields["parent"]; present {
			m, ok := parent.(map[string]any)
			require.True(t, ok, "parent must be an object")
			require.True(t, seededKeys[fmt.Sprint(m["key"])], "issue %s names unseeded parent %v", issue.Key, m["key"])
			sawParent = true
		}
	}

	// A per-issue assertion cannot catch a field that is empty on every issue.
	require.True(t, sawSprint, "no issue carried a sprint")
	require.True(t, sawParent, "no issue carried a parent — the epic hierarchy would be absent")
	require.True(t, sawResolved, "no issue carried a resolution date")
	require.True(t, sawUnresolved, "every issue was resolved — the open-issue state is unexercised")
}

func TestDemoseedJiraSprintsMatchConnector(t *testing.T) {
	org := jiraTestOrg()
	projects := demoseed.BuildJiraProjects(org)
	require.Len(t, projects, 1, "test org has one repo → one project")
	projID, err := strconv.ParseInt(projects[0]["id"].(string), 10, 64)
	require.NoError(t, err)
	// Pin the seeded originBoardId to the agreed contract value (projectID-1000).
	// The cross-package equality of the two deriveBoardID copies (demoseed's vs the
	// simulator handler's) is pinned separately by internal/simulator/jira's
	// TestBoardIDFormulaMatchesSeed — this assertion only covers the demoseed copy.
	wantBoardID := projID - 1000

	sprints := demoseed.BuildJiraSprints(org)
	require.NotEmpty(t, sprints)
	for _, s := range sprints {
		sp := reencode[jiraSprint](t, s)
		require.Positive(t, sp.ID, "sprint id drives the downstream sprint-<id> upsert key")
		ob, ok := s["originBoardId"].(int64)
		require.True(t, ok, "originBoardId must be an int64")
		require.Equal(t, wantBoardID, ob, "sprint.originBoardId must equal projectID-1000 (board-id invariant with the simulator)")
		require.NotEmpty(t, s["projectKey"], "sprint carries projectKey for downstream project resolution")
	}
}

func TestDemoseedJiraWorklogsMatchConnector(t *testing.T) {
	org := jiraTestOrg()
	repo := org.Repos[0]
	issues := demoseed.BuildJiraIssues(org, jiraTestMembers(),
		map[string][]demoseed.JiraPRRef{repo.Name: jiraTestPRRefs(t, org, repo)})
	sawWorklog := false
	for _, raw := range issues {
		fields, ok := raw["fields"].(map[string]any)
		require.True(t, ok, "issue payload must carry a fields object")
		wls, ok := fields["worklogs"].([]map[string]any)
		require.True(t, ok, "worklogs must stay []map[string]any — a type drift here would empty this loop silently")
		for _, w := range wls {
			sawWorklog = true
			wl := reencode[jiraWorklog](t, w)
			require.NotEmpty(t, wl.ID)
			require.False(t, wl.Created.IsZero(), "worklog created must decode (drives worklog attribution)")
			author, ok := w["author"].(map[string]any)
			require.True(t, ok, "worklog author must be an object")
			require.NotEmpty(t, author["accountId"], "worklog author.accountId attributes logged time to a person")
		}
	}
	require.True(t, sawWorklog, "some seeded issues must carry worklogs")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// jiraTestWindow is a fixed window, so nothing here depends on when it runs.
func jiraTestWindow() (start, end time.Time) {
	start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return start, start.Add(prBackfillDays * 24 * time.Hour)
}

func jiraOrgMembers(org OrgCatalog) []map[string]any {
	switch org.Vendor {
	case vendorBitbucket:
		return BuildBitbucketMembers(org)
	case vendorGitLab:
		return BuildGitLabMembers(org)
	default:
		return buildMembers(org)
	}
}

// buildPRsForRepo renders a repo's pull requests the way seedPRs' backfill does.
func buildPRsForRepo(t *testing.T, org OrgCatalog, repo RepoSpec, members []map[string]any, count, startNum int) []map[string]any {
	t.Helper()
	start, end := jiraTestWindow()
	return buildPRsForRepoInWindow(t, org, repo, members, count, startNum, start, end)
}

func buildPRsForRepoInWindow(t *testing.T, org OrgCatalog, repo RepoSpec, members []map[string]any, count, startNum int, start, end time.Time) []map[string]any {
	t.Helper()
	switch org.Vendor {
	case vendorBitbucket:
		return BuildBitbucketPRs(org, repo, members, count, startNum, start, end)
	case vendorGitLab:
		return BuildGitLabMRs(org, repo, gitLabRepoIndex(org, repo), members, count, startNum, start, end)
	default:
		return buildPRs(org, repo, members, count, startNum, start, end)
	}
}

func jiraRefsForRepo(t *testing.T, org OrgCatalog, repo RepoSpec, members []map[string]any, count, startNum int) []JiraPRRef {
	t.Helper()
	refs, err := BuildJiraPRRefs(org, buildPRsForRepo(t, org, repo, members, count, startNum))
	require.NoError(t, err)
	return refs
}

// jiraPMOrgs returns the catalog orgs that actually get a Jira overlay seeded.
func jiraPMOrgs() []OrgCatalog {
	var out []OrgCatalog
	for _, org := range DefaultCatalog {
		if org.PMTool == pmToolJira {
			out = append(out, org)
		}
	}
	return out
}

func jiraSeedableRepos(org OrgCatalog) []RepoSpec {
	var out []RepoSpec
	for _, repo := range org.Repos {
		if Profiles[repo.Flavor].PRsPerWeek > 0 {
			out = append(out, repo)
		}
	}
	return out
}

// TestJiraUnlinkedIssueFraction holds the coverage figure a consumer's attribution view
// renders. Both ends matter: no unlinked issues makes coverage a constant 100%, and
// too many makes the demo look like a team that never links its work.
func TestJiraUnlinkedIssueFraction(t *testing.T) {
	for _, org := range jiraPMOrgs() {
		members := jiraOrgMembers(org)
		linked, unlinked, noIssue := 0, 0, 0
		for _, repo := range jiraSeedableRepos(org) {
			for _, ref := range jiraRefsForRepo(t, org, repo, members, 200, 0) {
				switch jiraRoleForPR(org, repo, ref.Number) {
				case jiraRoleLinked:
					linked++
				case jiraRoleUnlinked:
					unlinked++
				case jiraRoleNoIssue:
					noIssue++
				}
			}
		}
		issues := linked + unlinked
		require.Positive(t, issues, "org %s seeded no issues", org.OrgName)
		require.Positive(t, noIssue, "org %s: every PR produced an issue", org.OrgName)

		frac := float64(unlinked) / float64(issues)
		require.Greater(t, frac, 0.10, "org %s: only %.1f%% of issues are unlinked", org.OrgName, frac*100)
		require.Less(t, frac, 0.30, "org %s: %.1f%% of issues are unlinked", org.OrgName, frac*100)
	}
}

// TestJiraIssuesAccumulateAcrossRuns is the property the previous design lacked: a
// later run must ADD issues without rewriting the ones already seeded. Provable with
// no database, because the builder is a pure function of the pull-request refs.
//
// The second run is modelled the way seedPRs actually behaves — it APPENDS a weekly
// batch continuing from the last number and never re-renders the backfill. That is
// load-bearing for the whole design: buildPRs spaces a batch by window/count, so a
// re-rendered backfill of a different size would move every pull request's
// timestamps and drag its issue's dates with them. Immutable pull-request rows are
// what make issue rows safe to write non-destructively.
func TestJiraIssuesAccumulateAcrossRuns(t *testing.T) {
	for _, org := range jiraPMOrgs() {
		members := jiraOrgMembers(org)
		repos := jiraSeedableRepos(org)
		require.NotEmpty(t, repos)

		const backfill = 60
		firstRun := map[string][]JiraPRRef{}
		secondRun := map[string][]JiraPRRef{}
		for _, repo := range repos {
			backfilled := jiraRefsForRepo(t, org, repo, members, backfill, 0)
			firstRun[repo.Name] = backfilled

			// The weekly append: new numbers, a trailing window, and the backfilled
			// rows carried forward untouched.
			weekly := Profiles[repo.Flavor].PRsPerWeek
			_, end := jiraTestWindow()
			appended, err := BuildJiraPRRefs(org, buildPRsForRepoInWindow(t, org, repo, members, weekly, backfill, end, end.Add(7*24*time.Hour)))
			require.NoError(t, err)
			secondRun[repo.Name] = append(append([]JiraPRRef{}, backfilled...), appended...)
		}

		before := BuildJiraIssues(org, members, firstRun)
		after := BuildJiraIssues(org, members, secondRun)
		require.Greater(t, len(after), len(before), "org %s: a later run added no issues", org.OrgName)

		// The already-seeded prefix must be byte-identical: upsertRow writes issues
		// with overwrite=false, so a payload that drifted here would be silently
		// dropped and the row would keep its older content.
		beforeByKey := map[string]string{}
		for _, iss := range before {
			beforeByKey[asStr(iss["key"])] = mustJSON(t, iss)
		}
		matched := 0
		for _, iss := range after {
			key := asStr(iss["key"])
			was, existed := beforeByKey[key]
			if !existed {
				continue
			}
			require.JSONEq(t, was, mustJSON(t, iss), "org %s: issue %s changed between runs", org.OrgName, key)
			matched++
		}
		require.Equal(t, len(beforeByKey), matched, "org %s: a first-run issue vanished on the second run", org.OrgName)
	}
}

// TestJiraIssueDatesTrackTheirPR pins the date derivation: an issue precedes the
// pull request that resolves it, never predates the oldest seeded pull request (the
// floor that keeps it inside the data window), and carries a resolutiondate exactly
// when it is done.
func TestJiraIssueDatesTrackTheirPR(t *testing.T) {
	for _, org := range jiraPMOrgs() {
		members := jiraOrgMembers(org)
		for _, repo := range jiraSeedableRepos(org) {
			refs := jiraRefsForRepo(t, org, repo, members, 60, 0)
			require.NotEmpty(t, refs)
			byNumber := map[int]JiraPRRef{}
			for _, r := range refs {
				byNumber[r.Number] = r
			}
			earliest := refs[0].CreatedAt

			for _, iss := range BuildJiraIssues(org, members, map[string][]JiraPRRef{repo.Name: refs}) {
				key := asStr(iss["key"])
				fields, ok := iss["fields"].(map[string]any)
				require.True(t, ok)

				prNum := jiraPRNumFromKey(t, key)
				ref, ok := byNumber[prNum]
				require.True(t, ok, "issue %s names pull request %d, which was not seeded", key, prNum)

				created := mustParseJiraTime(t, asStr(fields["created"]))
				updated := mustParseJiraTime(t, asStr(fields["updated"]))

				require.False(t, created.After(ref.CreatedAt), "issue %s was created after its pull request", key)
				require.False(t, created.Before(earliest), "issue %s predates the oldest seeded pull request and would fall outside the data window", key)
				require.False(t, updated.Before(created), "issue %s was updated before it was created", key)

				_, hasResolution := fields["resolutiondate"]
				status, ok := fields["status"].(map[string]any)
				require.True(t, ok)
				cat, ok := status["statusCategory"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, asStr(cat["key"]) == "done", hasResolution,
					"issue %s: resolutiondate must be present exactly when the issue is done", key)

				// A date-only field must stay date-only; the parser reads it with a
				// bare 2006-01-02 layout.
				for _, dateField := range []string{"duedate", jiraFieldStartDate} {
					_, err := time.Parse(jiraDateLayout, asStr(fields[dateField]))
					require.NoError(t, err, "issue %s: fields.%s is not date-only", key, dateField)
				}
			}
		}
	}
}

// TestJiraIssueBuilderReadsNoClock fails if a wall-clock read leaks back into the
// issue builder. Without it, issue dates would drift between a backfill and a
// re-backfill, moving a consumer's issue creation date — the date anchor —
// under a key that is never rewritten.
func TestJiraIssueBuilderReadsNoClock(t *testing.T) {
	org := jiraPMOrgs()[0]
	members := jiraOrgMembers(org)
	repo := jiraSeedableRepos(org)[0]
	refs := jiraRefsForRepo(t, org, repo, members, 40, 0)

	// Every ref sits in a window that ended long before now, so any timestamp near
	// the present could only have come from the clock.
	var latest time.Time
	for _, r := range refs {
		for _, ts := range []time.Time{r.CreatedAt, r.UpdatedAt, r.MergedAt} {
			if ts.After(latest) {
				latest = ts
			}
		}
	}
	ceiling := latest.Add(54 * time.Hour)

	for _, iss := range BuildJiraIssues(org, members, map[string][]JiraPRRef{repo.Name: refs}) {
		fields, ok := iss["fields"].(map[string]any)
		require.True(t, ok)
		for _, f := range []string{"created", "updated"} {
			ts := mustParseJiraTime(t, asStr(fields[f]))
			require.False(t, ts.After(ceiling),
				"issue %s: fields.%s is past every pull-request timestamp, so the builder read the clock", asStr(iss["key"]), f)
		}
	}
}

// TestNonJiraOrgsCarryNoJiraData is the other half of the PM-tool gate: an org whose
// PM tool is GitHub Issues or nothing must get no Jira keys and no Jira issues. An
// inverted gate or a catalog typo would otherwise ship a GitHub-Issues org with Jira
// keys in its pull requests, which no other assertion looks for.
func TestNonJiraOrgsCarryNoJiraData(t *testing.T) {
	checked := 0
	for _, org := range DefaultCatalog {
		if org.PMTool == pmToolJira {
			continue
		}
		checked++
		members := jiraOrgMembers(org)
		for _, repo := range jiraSeedableRepos(org) {
			prs := buildPRsForRepo(t, org, repo, members, 40, 0)
			for _, pr := range prs {
				for _, text := range prLinkTexts(org.Vendor, pr) {
					require.Empty(t, jiraKeyRe.FindAllString(text, -1),
						"org %s (pm=%q) carries a Jira key in its pull-request text", org.OrgName, org.PMTool)
				}
			}
			refs, err := BuildJiraPRRefs(org, prs)
			require.NoError(t, err)
			require.Empty(t, BuildJiraIssues(org, members, map[string][]JiraPRRef{repo.Name: refs}),
				"org %s (pm=%q) seeded Jira issues", org.OrgName, org.PMTool)
		}
	}
	require.Positive(t, checked, "catalog must contain non-Jira orgs for this gate to mean anything")
}

// TestClosingIssuesReferencesByPMTool guards the discriminator in both directions: a
// Jira org's linkage rides the issue key in the PR text, so its vendor-native
// closing references must stay empty — and a GitHub-Issues org's must not, or its
// (pre-existing) linkage path goes dark.
func TestClosingIssuesReferencesByPMTool(t *testing.T) {
	sawJira, sawGitHubIssues := false, false
	for _, org := range DefaultCatalog {
		if org.Vendor != vendorGitHub {
			continue // closingIssuesReferences is a GitHub payload field
		}
		members := jiraOrgMembers(org)
		for _, repo := range jiraSeedableRepos(org) {
			nonEmpty := 0
			for _, pr := range buildPRsForRepo(t, org, repo, members, 40, 0) {
				nodes := closingIssueNodes(t, pr)
				switch org.PMTool {
				case pmToolJira:
					require.Empty(t, nodes, "org %s: a Jira-PM org must not carry closingIssuesReferences", org.OrgName)
				default:
					if len(nodes) > 0 {
						nonEmpty++
					}
				}
			}
			if org.PMTool == pmToolJira {
				sawJira = true
				continue
			}
			if org.PMTool == pmToolGitHubIssues {
				sawGitHubIssues = true
				require.Positive(t, nonEmpty, "org %s: a github-issues org must link some closing issues", org.OrgName)
			}
		}
	}
	require.True(t, sawJira, "no GitHub Jira-PM org exercised")
	require.True(t, sawGitHubIssues, "no github-issues org exercised")
}

// TestJiraEntitiesFollowPRs pins the ordering seedJiraIssues depends on: issues are
// derived from pull-request rows a prior entity pass already committed.
func TestJiraEntitiesFollowPRs(t *testing.T) {
	for _, org := range jiraPMOrgs() {
		specs := jiraAwareEntities(org)
		prIdx, issueIdx := -1, -1
		for i, spec := range specs {
			switch spec.Type {
			case "pr":
				prIdx = i
			case entityTypeJiraIssue:
				issueIdx = i
			}
		}
		require.NotEqual(t, -1, prIdx, "org %s has no pr entity spec", org.OrgName)
		require.NotEqual(t, -1, issueIdx, "org %s has no jira issue entity spec", org.OrgName)
		require.Less(t, prIdx, issueIdx,
			"org %s: jira issues are seeded before pull requests, so they would derive from the previous run's", org.OrgName)
	}
}

// --- helpers ---

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func mustParseJiraTime(t *testing.T, v string) time.Time {
	t.Helper()
	ts, err := time.Parse(jiraTimestampLayout, v)
	require.NoError(t, err, "timestamp %q must use the seeded Jira layout", v)
	return ts.UTC()
}

// jiraPRNumFromKey reads the pull-request number back out of an issue key, which is
// only possible because the key names exactly one pull request.
func jiraPRNumFromKey(t *testing.T, key string) int {
	t.Helper()
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '-' {
			n := 0
			for _, c := range key[i+1:] {
				require.True(t, c >= '0' && c <= '9', "issue key %q does not end in a number", key)
				n = n*10 + int(c-'0')
			}
			return n
		}
	}
	t.Fatalf("issue key %q has no numeric suffix", key)
	return 0
}

func closingIssueNodes(t *testing.T, pr map[string]any) []any {
	t.Helper()
	refs, ok := pr["closingIssuesReferences"]
	if !ok {
		return nil
	}
	switch v := refs.(type) {
	case []any:
		return v
	case map[string]any:
		nodes, _ := v[keyNodes].([]any) //nolint:errcheck // absent/other shape → no nodes
		return nodes
	default:
		return nil
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

// TestJiraPRIssueCorrelation pins the load-bearing seed contract: for
// every jira-PM org, the Jira issue keys stamped into PR text/branches are a subset
// of the keys seeded as jira issues. Both derive from the same per-(org,repo)
// helper, so the prissue linkage engine resolves every stamped key against a real
// issue row a consumer stores — a de-correlating change (e.g. a different key formula in one
// builder) fails here rather than silently leaving a consumer's PR-issue links empty.

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// jiraKeyRe mirrors the prissue signal generator's Jira-key shape.
var jiraKeyRe = regexp.MustCompile(`\b[A-Z][A-Z0-9]{1,9}-[0-9]{1,7}\b`)

func asStr(v any) string { s, _ := v.(string); return s } //nolint:errcheck // test helper: absent/non-string → ""

// prLinkTexts returns the PR fields the linkage engine scans for issue keys
// (title, body/description, source branch), per vendor payload shape.
func prLinkTexts(vendor string, pr map[string]any) []string {
	switch vendor {
	case vendorBitbucket:
		branch := ""
		if src, ok := pr["source"].(map[string]any); ok {
			if br, ok := src[bbKeyBranch].(map[string]any); ok {
				branch = asStr(br[bbKeyName])
			}
		}
		return []string{asStr(pr[bbKeyTitle]), asStr(pr["description"]), branch}
	case vendorGitLab:
		return []string{asStr(pr[keyTitle]), asStr(pr[keyDescription]), asStr(pr["source_branch"])}
	default:
		return []string{asStr(pr[keyTitle]), asStr(pr["body"]), asStr(pr["headRefName"])}
	}
}

func TestJiraPRIssueCorrelation(t *testing.T) {
	windowEnd := time.Now().UTC()
	windowStart := windowEnd.Add(-prBackfillDays * 24 * time.Hour)

	sawJiraOrg := false
	for _, org := range DefaultCatalog {
		if org.PMTool != pmToolJira {
			continue
		}
		sawJiraOrg = true

		var members []map[string]any
		switch org.Vendor {
		case vendorBitbucket:
			members = BuildBitbucketMembers(org)
		case vendorGitLab:
			members = BuildGitLabMembers(org)
		default:
			members = buildMembers(org)
		}

		// Build the pull requests first, then derive the issues from them — the same
		// order seedJiraIssues uses, so this test exercises the real dependency
		// rather than two independently-seeded key sets.
		prsByRepo := map[string][]JiraPRRef{}
		prPayloads := map[string][]map[string]any{}
		for i, repo := range org.Repos {
			if Profiles[repo.Flavor].PRsPerWeek == 0 {
				continue // direct-push repos have no PRs
			}
			var prs []map[string]any
			switch org.Vendor {
			case vendorBitbucket:
				prs = BuildBitbucketPRs(org, repo, members, 30, 0, windowStart, windowEnd)
			case vendorGitLab:
				prs = BuildGitLabMRs(org, repo, i+1, members, 30, 0, windowStart, windowEnd)
			default:
				prs = buildPRs(org, repo, members, 30, 0, windowStart, windowEnd)
			}
			refs, err := BuildJiraPRRefs(org, prs)
			require.NoError(t, err, "org %s repo %s: projecting pr refs", org.OrgName, repo.Name)
			prsByRepo[repo.Name] = refs
			prPayloads[repo.Name] = prs
		}

		issueKeys := map[string]bool{}
		for _, iss := range BuildJiraIssues(org, members, prsByRepo) {
			issueKeys[asStr(iss["key"])] = true
		}
		require.NotEmpty(t, issueKeys, "org %s must seed jira issues", org.OrgName)

		stamped := map[string]int{}
		for _, prs := range prPayloads {
			for _, pr := range prs {
				seen := map[string]bool{}
				for _, text := range prLinkTexts(org.Vendor, pr) {
					for _, k := range jiraKeyRe.FindAllString(text, -1) {
						require.True(t, issueKeys[k],
							"org %s: PR-stamped key %q is not a seeded issue key (correlation broken)", org.OrgName, k)
						seen[k] = true
					}
				}
				for k := range seen {
					stamped[k]++
				}
			}
		}
		require.NotEmpty(t, stamped, "org %s: some PRs must carry a jira key — the correlation is the headline feature", org.OrgName)

		// One issue, one pull request. A key referenced by two PRs would mean an
		// issue's attribution is an average over several PRs rather than a single
		// PR's signal, which is what the per-issue AI% view reads.
		for key, n := range stamped {
			require.Equal(t, 1, n, "org %s: issue %s is referenced by %d pull requests, want exactly 1", org.OrgName, key, n)
		}

		// Some issues must have no pull request at all, or the attribution
		// unlinked-issue coverage figure is a constant 100%.
		require.Less(t, len(stamped), len(issueKeys),
			"org %s: every seeded issue has a linked PR; the unlinked bucket must be non-empty", org.OrgName)
	}
	require.True(t, sawJiraOrg, "DefaultCatalog must contain at least one jira-PM org")
}

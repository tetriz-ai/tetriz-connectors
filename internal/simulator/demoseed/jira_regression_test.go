// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package demoseed

import (
	"crypto/sha256"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nonJiraPRHistorySHA256 pins the pull-request payloads of each non-Jira-PM org,
// one entry per org.
//
// The Jira overlay decides which PRs carry an issue key from a dedicated rng seed
// and returns before drawing at all when the org's PM tool is not Jira. That is
// what keeps a Jira-side change from shifting an unrelated org's deterministic
// history — a shift nothing else would notice, because each org's payloads are
// individually plausible whatever the rng produced. These hashes are the only
// thing that would.
//
// Keyed per org rather than folded into one digest on purpose: a single digest
// changed whenever ANY org was added, so growing the catalog broke a guard that
// had nothing to say about the addition, and the failure could not distinguish a
// new org from a regression in an existing one. Per-org, an addition touches only
// its own entry and every other org stays protected.
//
// An org whose every repo is direct-push has no pull-request history and is
// deliberately absent: its digest would be the hash of no input, a pin that cannot
// fail.
var nonJiraPRHistorySHA256 = map[string]string{
	"demo-gh-oss":       "0e99e12c32909c7cc721418c1fe1137863e244b8f0999df6f8102df144db48c4",
	"demo-gh-startup":   "a1b1cded213d7daec451347301c27c9326cec8d3d0b1ffbf4c70d6b1e57f0856",
	"demo-gl-regulated": "d69fa7ad3447ec507478d195ff318245bd3b80c0814017fac1cecff46a70a7ed",
	"demo-gl-scaleup":   "56f8b1e53e483ba156789f2211e5c297978c83dde303d1ed30fd0066cd2bd85e",
}

func TestNonJiraPRHistoryIsPinned(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(prBackfillDays * 24 * time.Hour)

	got := map[string]string{}
	for _, org := range DefaultCatalog {
		if org.PMTool == pmToolJira {
			continue
		}
		hash, hasPRs := nonJiraPRHistoryHash(t, org, start, end)
		if !hasPRs {
			// Every repo is direct-push, so there is no pull-request history to
			// pin. Pinning the empty digest here would be a guard that cannot
			// fail — worse than none, because the entry reads as protection.
			continue
		}
		got[org.OrgName] = hash
	}
	require.NotEmpty(t, got, "catalog must contain non-Jira orgs for this pin to mean anything")

	for _, name := range sortedKeys(got) {
		pinned, ok := nonJiraPRHistorySHA256[name]
		if !ok {
			// A catalog addition, which is expected and not a regression — the
			// other orgs' entries are untouched. Stated as the one-line edit that
			// resolves it, so this never blocks on someone else's diagnosis.
			t.Errorf("%s has no pinned PR history.\n"+
				"If you just added this org to DefaultCatalog, that is expected: add this line to "+
				"nonJiraPRHistorySHA256 and you are done — no other org's entry changes.\n"+
				"\t%q: %q,", name, name, got[name])
			continue
		}
		require.Equalf(t, pinned, got[name],
			"%s's PR history changed.\n"+
				"If you did NOT intend a change to the shared PR builders, this is a real regression: "+
				"the Jira rng leaked across the PM-tool gate and shifted an unrelated org's history. "+
				"Do not re-pin — find the leak (jiraRoleForPR must return before drawing for a non-Jira org).\n"+
				"If a shared-builder change WAS intended, re-pin only this line:\n"+
				"\t%q: %q,", name, name, got[name])
	}

	for _, name := range sortedKeys(nonJiraPRHistorySHA256) {
		if _, ok := got[name]; !ok {
			t.Errorf("%s is pinned but is no longer a non-Jira org in DefaultCatalog.\n"+
				"If you removed it, or switched its PM tool to Jira, drop its line from "+
				"nonJiraPRHistorySHA256.", name)
		}
	}
}

// nonJiraPRHistoryHash hashes one org's pull-request payloads across its
// PR-producing repos, in catalog order so the digest is stable.
func nonJiraPRHistoryHash(t *testing.T, org OrgCatalog, start, end time.Time) (string, bool) {
	t.Helper()

	members := buildMembers(org)
	switch org.Vendor {
	case vendorBitbucket:
		members = BuildBitbucketMembers(org)
	case vendorGitLab:
		members = BuildGitLabMembers(org)
	}

	h := sha256.New()
	anyPRs := false
	for _, repo := range org.Repos {
		if Profiles[repo.Flavor].PRsPerWeek == 0 {
			continue // direct-push repo: no pull requests to pin
		}
		anyPRs = true
		var prs []map[string]any
		switch org.Vendor {
		case vendorBitbucket:
			prs = BuildBitbucketPRs(org, repo, members, 30, 0, start, end)
		case vendorGitLab:
			prs = BuildGitLabMRs(org, repo, gitLabRepoIndex(org, repo), members, 30, 0, start, end)
		default:
			prs = buildPRs(org, repo, members, 30, 0, start, end)
		}
		b, err := json.Marshal(prs)
		require.NoError(t, err)
		h.Write(b)
	}
	return hex(h.Sum(nil)), anyPRs
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func hex(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0x0f])
	}
	return string(out)
}

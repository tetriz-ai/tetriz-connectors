// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vcs

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"go.uber.org/zap"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// resolveTrackedRefsTimeout bounds the host resolution call so a hung
// the host degrades a fetch run instead of stalling it — a timeout takes the
// same path as an error return.
var resolveTrackedRefsTimeout = 10 * time.Second

// ResolveTrackedRefs asks the host for the org's glob-expanded tracked refs,
// branch tips, and history floor. On any failure (an error, a timeout, or a
// nil resolver) it returns (nil, false) after a warn log, so callers fall
// back to the default branch rather than fail the run.
func ResolveTrackedRefs(ctx context.Context, resolver sdk.TrackedRefsResolver, orgID, connectorID string) (*workqueuev1.ResolveTrackedRefsResponse, bool) {
	if resolver == nil {
		logger.WarnWithCtx(ctx, "tracked-ref resolution unavailable (no resolver); degrading to default-branch walk",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID))
		return nil, false
	}
	rctx, cancel := context.WithTimeout(ctx, resolveTrackedRefsTimeout)
	defer cancel()

	resp, err := resolver.ResolveTrackedRefs(rctx, &workqueuev1.ResolveTrackedRefsRequest{
		OrganizationId: orgID,
		ConnectorId:    connectorID,
	})
	if err != nil {
		logger.WarnWithCtx(ctx, "tracked-ref resolution failed; degrading to default-branch walk",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, false
	}
	return resp, true
}

// MaxRefsPerRepo bounds how many tracked refs one repository's commit walk visits.
//
// The bound is needed because no provider call covers several refs *and* attributes each
// commit to the ref it came from — GitHub takes one qualifiedName, GitLab one ref_name,
// Bitbucket one include — so cost is strictly refs × pages per repository, against an
// allowance Bitbucket shares with every other operation.
//
// Over the cap the refs are sorted, so the subset walked is stable rather than whatever
// order the resolver happened to return.
const MaxRefsPerRepo = 5

// IndexTrackedRefs keys the resolver's response by repo full name. A nil response yields an
// empty index, which CommitRefsForRepo reads as "no resolution" and degrades per repo.
func IndexTrackedRefs(resp *workqueuev1.ResolveTrackedRefsResponse) map[string]*workqueuev1.RepoTrackedRefs {
	if resp == nil {
		return nil
	}
	byRepo := make(map[string]*workqueuev1.RepoTrackedRefs, len(resp.GetRepos()))
	for _, r := range resp.GetRepos() {
		byRepo[r.GetRepoFullName()] = r
	}
	return byRepo
}

// CommitRefsForRepo picks the refs one repository's commit walk visits, and reports whether
// the repository should be skipped entirely.
//
// The degradations are deliberate and each is the safe direction:
//   - resolution unavailable, or the repo absent from it → the default branch. Never all: a
//     settings outage must not become a provider-budget incident.
//   - refs empty with all_branches false → MatchNone. The repo resolved to no countable refs,
//     so fetching nothing is the answer rather than falling back to a branch nobody asked for.
//   - all_branches → the default branch, for now. Honouring it would mean walking the
//     repository's live branch list, and the resolver carries tip SHAs but no tip dates, so
//     "the most recently touched N branches" is not computable here — an arbitrary N of a
//     hundred would silently pick a subset. Recorded rather than guessed.
//
// Over the cap, refs are sorted for a stable choice rather than relying on map order.
func CommitRefsForRepo(ctx context.Context, byRepo map[string]*workqueuev1.RepoTrackedRefs, resolved bool, repoFullName, defaultBranch string) (refs []string, skip bool) {
	if !resolved {
		return []string{defaultBranch}, false
	}
	entry, ok := byRepo[repoFullName]
	if !ok {
		return []string{defaultBranch}, false
	}
	if entry.GetAllBranches() {
		logger.Info(ctx, "fetch_commits: all-branches repo walks its default branch",
			zap.String("repo", repoFullName), zap.String("branch", defaultBranch))
		return []string{defaultBranch}, false
	}
	resolvedRefs := dedupeRefs(entry.GetRefs())
	if len(resolvedRefs) == 0 {
		logger.Info(ctx, "fetch_commits: repo resolved to no tracked refs; fetching nothing",
			zap.String("repo", repoFullName))
		return nil, true
	}
	if len(resolvedRefs) > MaxRefsPerRepo {
		// The default branch is always kept, whatever the cap drops. Sorting alone would
		// lose it on a perfectly ordinary set — "feature/a".."feature/e" plus "main" sorts
		// main out at a cap of five, silently abandoning the trunk the rest of the product
		// is built around.
		capped := make([]string, 0, MaxRefsPerRepo)
		if defaultBranch != "" && slices.Contains(resolvedRefs, defaultBranch) {
			capped = append(capped, defaultBranch)
		}
		rest := make([]string, 0, len(resolvedRefs))
		for _, r := range resolvedRefs {
			if r != defaultBranch {
				rest = append(rest, r)
			}
		}
		sort.Strings(rest)
		for _, r := range rest {
			if len(capped) == MaxRefsPerRepo {
				break
			}
			capped = append(capped, r)
		}
		logger.WarnWithCtx(ctx, "fetch_commits: tracked refs exceed the per-repo cap; walking a stable subset",
			zap.String("repo", repoFullName), zap.Int("resolved", len(resolvedRefs)),
			zap.Int("cap", MaxRefsPerRepo), zap.Strings("walked", capped))
		return capped, false
	}
	return resolvedRefs, false
}

// dedupeRefs drops repeated ref names, keeping first-seen order. Two globs can expand to
// the same branch, and walking it twice spends the provider budget twice — over the cap a
// repeat would also take the slot of a distinct ref. The input is never modified.
func dedupeRefs(refs []string) []string {
	seen := make(map[string]struct{}, len(refs))
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// MetaKeyPRCommitSHAs is the record-metadata key naming the commits a pull request
// contains, comma-separated. It is set by the connectors whose commit payload does not
// name its pull request — GitLab and Bitbucket — and read back by a consumer to write
// the link onto those commits' rows.
//
// GitHub does not set it. Its commit query already selects associatedPullRequests on
// every commit node, so the link arrives with the commit and needs no second channel.
const MetaKeyPRCommitSHAs = "pr_commit_shas"

// JoinCommitSHAs renders a pull request's commit list for MetaKeyPRCommitSHAs.
//
// SHAs are truncated to twelve lowercase characters, matching the canonical width every
// commit is stored and compared at. That is not only for matching: a full page of forty-
// character hashes is a ~4KB metadata blob per pull request, and twelve is the narrowest
// form any provider hands back, so storing wider would cost three times the space to
// carry precision no comparison uses.
//
// A shorter input is passed through rather than padded, so a provider that abbreviates
// below twelve fails a comparison loudly instead of matching the wrong commit.
func JoinCommitSHAs(shas []string) string {
	if len(shas) == 0 {
		return ""
	}
	const canonicalWidth = 12
	out := make([]string, 0, len(shas))
	seen := make(map[string]struct{}, len(shas))
	for _, raw := range shas {
		sha := strings.ToLower(strings.TrimSpace(raw))
		if len(sha) > canonicalWidth {
			sha = sha[:canonicalWidth]
		}
		if sha == "" {
			continue
		}
		if _, dup := seen[sha]; dup {
			continue
		}
		seen[sha] = struct{}{}
		out = append(out, sha)
	}
	return strings.Join(out, ",")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"

	"go.uber.org/zap"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The GitHub blame Source for the defect-lineage walk. The provider-agnostic hop logic lives in
// package lineageblame; this file supplies GitHub's answers to that port over the GraphQL API.
//
// KNOWN LIMITATION: GitHub's blame is path-scoped and does not follow renames, so a walk that
// crosses a rename or move sees the path vanish at that revision and fails closed (no candidate)
// rather than resolving across it — the same fail-closed contract lineageblame documents.

// WalkFix runs the GitHub blame walk for one claimed bug fix and assembles its outcome. It
// constructs a GitHub Source and delegates the hop logic to the host's lineage.Walker; the lineage drain
// calls this per claimed PendingLineageBlame and forwards the result to SubmitLineageBlame.
// The blame ranges come over GraphQL (gw); the commit→PR resolution is a REST call, so restGW is the
// REST-group gateway — metered against GitHub's separate REST budget rather than polluting the GraphQL
// limiter, the same split the scoring drain uses. orgID routes a simulator-configured org to its
// simulator base.
func WalkFix(ctx context.Context, walker lineage.Walker, gw, restGW sdk.VendorGateway, orgID string, fix *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error) {
	repoFullName := fix.GetRepoFullName()
	src := &githubBlameSource{gw: gw, repoFullName: repoFullName}
	state, results, err := walker.Walk(ctx, src, fix)
	// Enrich resolved culprits with the pull request that introduced them, best-effort. This runs even
	// on a partial walk (err != nil with some results): a resolved culprit is worth a PR link whether or
	// not a sibling anchor failed. A resolution failure never changes the walk's state or error.
	if len(results) > 0 {
		walker.ResolveCandidatePRs(ctx, results, func(rctx context.Context, sha string) (int32, bool) {
			return resolveGitHubCommitPR(rctx, restGW, orgID, repoFullName, sha)
		})
	}
	if err != nil {
		return state, results, fmt.Errorf("github blame walk for fix %s: %w", fix.GetBugFixPrId(), err)
	}
	return state, results, nil
}

// resolveGitHubCommitPR resolves the pull request that introduced a culprit commit via
// GET /repos/{owner}/{repo}/commits/{sha}/pulls — the REST endpoint that, unlike GraphQL's
// associatedPullRequests, resolves a squash-merged culprit's PR (associatedPullRequests is
// default-branch-only). It prefers a merged PR (the one that landed the culprit); absent one it takes
// the first returned. Best-effort: any transport error, non-200, or empty list yields (0, false),
// logged at warn, so PR resolution never fails the walk.
func resolveGitHubCommitPR(ctx context.Context, restGW sdk.VendorGateway, orgID, repoFullName, sha string) (int32, bool) {
	owner, name, ok := splitRepoFullName(repoFullName)
	if !ok {
		return 0, false
	}
	url := fmt.Sprintf("%s/repos/%s/%s/commits/%s/pulls", githubRESTBase(orgID), owner, name, sha)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.WarnWithCtx(ctx, "github lineage: building commit-pulls request failed; leaving candidate PR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := restGW.Do(ctx, req)
	if err != nil {
		logger.WarnWithCtx(ctx, "github lineage: commit-pulls request failed; leaving candidate PR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	defer resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only lookup
	if resp.StatusCode != http.StatusOK {
		logger.WarnWithCtx(ctx, "github lineage: commit-pulls unexpected status; leaving candidate PR unresolved",
			zap.String("culprit_sha", sha), zap.Int("status", resp.StatusCode))
		return 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, commitPullsBodyLimit))
	if err != nil {
		logger.WarnWithCtx(ctx, "github lineage: reading commit-pulls body failed; leaving candidate PR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	var pulls []struct {
		Number   int32  `json:"number"`
		MergedAt string `json:"merged_at"`
	}
	if err := json.Unmarshal(body, &pulls); err != nil {
		logger.WarnWithCtx(ctx, "github lineage: decoding commit-pulls body failed; leaving candidate PR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	// Prefer the merged PR — the one that actually landed the culprit — over an open/closed PR that
	// merely touched the commit. Fall back to the first with a positive number.
	for _, p := range pulls {
		if p.Number > 0 && p.MergedAt != "" {
			return p.Number, true
		}
	}
	for _, p := range pulls {
		if p.Number > 0 {
			return p.Number, true
		}
	}
	return 0, false
}

// commitPullsBodyLimit caps the commit→pulls response read. The list is a handful of PRs at most, so
// this is a generous ceiling that only guards a pathological body.
const commitPullsBodyLimit = 1 << 20 // 1 MiB

// githubBlameSource answers the walk from GitHub's GraphQL API through the VendorGateway.
type githubBlameSource struct {
	gw           sdk.VendorGateway
	repoFullName string // owner/name
}

var _ lineage.Source = (*githubBlameSource)(nil)

// ghBlameResponse models the blame-and-text query: blame ranges (with the owning commit's
// author) plus the file's text, both under repository so one request answers a revision. GitHub
// blame ranges carry no line text, so the blob text is fetched alongside and zipped by line
// number.
type ghBlameResponse struct {
	Repository struct {
		Object struct {
			Blame struct {
				Ranges []struct {
					StartingLine int `json:"startingLine"`
					EndingLine   int `json:"endingLine"`
					Commit       struct {
						OID           string    `json:"oid"`
						CommittedDate time.Time `json:"committedDate"`
						Author        struct {
							Name  string `json:"name"`
							Email string `json:"email"`
						} `json:"author"`
					} `json:"commit"`
				} `json:"ranges"`
			} `json:"blame"`
		} `json:"object"`
		Text struct {
			Text string `json:"text"`
		} `json:"text"`
	} `json:"repository"`
}

// Lines returns the file at path as of revision, each line tagged with its blame owner and
// author. Satisfies lineage.Source.
func (s *githubBlameSource) Lines(ctx context.Context, revision, path string) ([]lineage.BlamedLine, error) {
	owner, name, ok := splitRepoFullName(s.repoFullName)
	if !ok {
		return nil, fmt.Errorf("bad repo full name %q", s.repoFullName)
	}
	query := buildBlameQuery(owner, name, revision, path)
	raw, err := doGraphQL(ctx, s.gw, query, false)
	if err != nil {
		return nil, fmt.Errorf("blame query %s@%s: %w", path, revision, err)
	}
	var resp ghBlameResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decoding blame response: %w", err)
	}
	return zipBlame(resp), nil
}

// zipBlame turns blame ranges plus blob text into one owned line per file line. Empty text means
// the path does not exist at this revision (the blob object is null) — return no lines so the
// walk fails closed rather than reading a phantom empty line. A line the blame ranges do not
// cover keeps an empty owner and never matches a culprit.
func zipBlame(resp ghBlameResponse) []lineage.BlamedLine {
	if resp.Repository.Text.Text == "" {
		return nil
	}
	fileLines := strings.Split(resp.Repository.Text.Text, "\n")
	// GitHub blame numbers lines from 1; a range [start,end] owns lines start..end inclusive.
	type owner struct {
		sha         string
		at          time.Time
		authorName  string
		authorEmail string
	}
	owners := make([]owner, len(fileLines))
	for _, r := range resp.Repository.Object.Blame.Ranges {
		for ln := r.StartingLine; ln <= r.EndingLine && ln-1 < len(owners); ln++ {
			if ln-1 < 0 {
				continue
			}
			owners[ln-1] = owner{
				sha:         r.Commit.OID,
				at:          r.Commit.CommittedDate,
				authorName:  r.Commit.Author.Name,
				authorEmail: r.Commit.Author.Email,
			}
		}
	}
	out := make([]lineage.BlamedLine, len(fileLines))
	for i, text := range fileLines {
		out[i] = lineage.BlamedLine{
			Text:        text,
			OwnerSHA:    owners[i].sha,
			CommittedAt: owners[i].at,
			AuthorName:  owners[i].authorName,
			AuthorEmail: owners[i].authorEmail,
		}
	}
	return out
}

// ghParentResponse models the first-parent query.
type ghParentResponse struct {
	Repository struct {
		Object struct {
			Parents struct {
				Nodes []struct {
					OID string `json:"oid"`
				} `json:"nodes"`
			} `json:"parents"`
		} `json:"object"`
	} `json:"repository"`
}

// FirstParent returns sha's first-parent commit, or "" when sha is a root commit. Satisfies
// lineage.Source.
func (s *githubBlameSource) FirstParent(ctx context.Context, sha string) (string, error) {
	owner, name, ok := splitRepoFullName(s.repoFullName)
	if !ok {
		return "", fmt.Errorf("bad repo full name %q", s.repoFullName)
	}
	raw, err := doGraphQL(ctx, s.gw, buildFirstParentQuery(owner, name, sha), false)
	if err != nil {
		return "", fmt.Errorf("first-parent query %s: %w", sha, err)
	}
	var resp ghParentResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("decoding parent response: %w", err)
	}
	if len(resp.Repository.Object.Parents.Nodes) == 0 {
		return "", nil // root commit
	}
	return resp.Repository.Object.Parents.Nodes[0].OID, nil
}

// buildBlameQuery fetches the file's blame ranges (with each owning commit's author) and its text
// at a revision in one request. Arguments are quoted with %q; owner/name/revision/path have no
// exotic characters, and the query hits the gateway's rate limiter and circuit breaker like every
// other GitHub call. The author is the commit's author, not its committer.
//
// The commit is addressed by object(expression:), not object(oid:): a bare SHA is a valid revspec,
// and object(oid:) did not resolve the blame walk end-to-end against the live API, so do not
// "simplify" it back to oid.
func buildBlameQuery(owner, name, revision, path string) string {
	return fmt.Sprintf(`query {
  repository(owner: %q, name: %q) {
    object(expression: %q) {
      ... on Commit {
        blame(path: %q) {
          ranges { startingLine endingLine commit { oid committedDate author { name email } } }
        }
      }
    }
    text: object(expression: %q) { ... on Blob { text } }
  }
}`, owner, name, revision, path, revision+":"+path)
}

// buildFirstParentQuery asks for a commit's first parent oid. It addresses the commit by expression,
// not oid: oid requires a full 40-char GitObjectID, but the stored fix commit sha can be abbreviated
// (the scoring pass may hold a short sha), and expression resolves both
// an abbreviated and a full sha.
func buildFirstParentQuery(owner, name, sha string) string {
	return fmt.Sprintf(`query {
  repository(owner: %q, name: %q) {
    object(expression: %q) {
      ... on Commit { parents(first: 1) { nodes { oid } } }
    }
  }
}`, owner, name, sha)
}

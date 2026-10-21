// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"

	"go.uber.org/zap"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"
)

// The GitLab blame Source for the defect-lineage walk. The provider-agnostic hop logic lives in
// package lineageblame; this file supplies GitLab's answers to that port over the REST API. The
// GitLab project id is the fix's platform_repo_id (a numeric id rendered as a string).
//
// Unlike GitHub's GraphQL blame, GitLab's file-blame is one REST request per file per hop; the
// first parent comes from the commit's parent_ids (GitLab supplies them directly).

// blameBodyLimit caps one blame response read. One file's blame is small relative to this; the
// per-fix changed-file ceiling upstream already bounds how many files a fix reaches.
const blameBodyLimit = 16 << 20 // 16 MiB

// WalkFix runs the GitLab blame walk for one claimed bug fix and assembles its outcome. It
// constructs a GitLab Source and delegates the hop logic to the host's lineage.Walker; the drain calls
// this per claimed PendingLineageBlame and forwards the result to SubmitLineageBlame.
func WalkFix(ctx context.Context, walker lineage.Walker, gw sdk.VendorGateway, orgID string, fix *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error) {
	src := &gitlabBlameSource{gw: gw, orgID: orgID, projectID: fix.GetPlatformRepoId()}
	state, results, err := walker.Walk(ctx, src, fix)
	// Enrich resolved culprits with the merge request that introduced them, best-effort. The MR lookup
	// rides the same GitLab gateway as the blame, so it is metered against the same budget. A
	// resolution failure never changes the walk's state or error.
	if len(results) > 0 {
		walker.ResolveCandidatePRs(ctx, results, src.resolveCommitMR)
	}
	if err != nil {
		return state, results, fmt.Errorf("gitlab blame walk for fix %s: %w", fix.GetBugFixPrId(), err)
	}
	return state, results, nil
}

// gitlabBlameSource answers the walk from GitLab's REST API through the VendorGateway.
type gitlabBlameSource struct {
	gw        sdk.VendorGateway
	orgID     string // tenant org id, for simulator-aware REST base routing
	projectID string // GitLab numeric project id (platform_repo_id)
}

var _ lineage.Source = (*gitlabBlameSource)(nil)

// glBlameEntry is one GitLab blame group: a commit and the consecutive lines it owns. The full
// file's blame is the entries concatenated in order.
type glBlameEntry struct {
	Commit struct {
		ID            string    `json:"id"`
		CommittedDate time.Time `json:"committed_date"`
		AuthorName    string    `json:"author_name"`
		AuthorEmail   string    `json:"author_email"`
	} `json:"commit"`
	Lines []string `json:"lines"`
}

// Lines returns the file at path as of revision, each line tagged with its blame owner and
// author. Satisfies lineage.Source. A 404 means the path does not exist at that revision —
// return no lines so the walk fails closed rather than treating an absent path as authorship.
func (s *gitlabBlameSource) Lines(ctx context.Context, revision, path string) ([]lineage.BlamedLine, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gitlabBlameURL(s.orgID, s.projectID, path, revision), nil)
	if err != nil {
		return nil, fmt.Errorf("building blame request %s@%s: %w", path, revision, err)
	}
	resp, err := s.gw.Do(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("blame %s@%s: %w", path, revision, err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, blameBodyLimit))
	resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only blame request
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // path absent at this revision — the walk fails closed
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blame %s@%s: %w", path, revision, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header))
	}
	if readErr != nil {
		return nil, fmt.Errorf("reading blame body %s@%s: %w", path, revision, readErr)
	}
	var entries []glBlameEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("decoding blame response %s@%s: %w", path, revision, err)
	}
	return blameEntriesToLines(entries), nil
}

// blameEntriesToLines flattens GitLab's per-commit blame groups into one owned line per file
// line, in file order.
func blameEntriesToLines(entries []glBlameEntry) []lineage.BlamedLine {
	var out []lineage.BlamedLine
	for _, e := range entries {
		for _, line := range e.Lines {
			out = append(out, lineage.BlamedLine{
				Text:        line,
				OwnerSHA:    e.Commit.ID,
				CommittedAt: e.Commit.CommittedDate,
				AuthorName:  e.Commit.AuthorName,
				AuthorEmail: e.Commit.AuthorEmail,
			})
		}
	}
	return out
}

// glCommitResponse models the commit lookup, read only for its parents.
type glCommitResponse struct {
	ParentIDs []string `json:"parent_ids"`
}

// FirstParent returns sha's first-parent commit, or "" when sha is a root commit. Satisfies
// lineage.Source.
func (s *gitlabBlameSource) FirstParent(ctx context.Context, sha string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gitlabCommitURL(s.orgID, s.projectID, sha), nil)
	if err != nil {
		return "", fmt.Errorf("building commit request %s: %w", sha, err)
	}
	resp, err := s.gw.Do(ctx, req)
	if err != nil {
		return "", fmt.Errorf("commit lookup %s: %w", sha, err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, blameBodyLimit))
	resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only commit lookup
	if resp.StatusCode == http.StatusNotFound {
		// The commit is gone (force-push / GC). Signal it distinctly so scoring degrades the fix's
		// candidates while the blame walk still fails the fix (its caller maps the error terminal).
		return "", fmt.Errorf("commit lookup %s: %w", sha, lineage.ErrFixCommitGone)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("commit lookup %s: %w", sha, vendorapi.ClassifyHTTPErrorWithHeaders(resp.StatusCode, body, resp.Header))
	}
	if readErr != nil {
		return "", fmt.Errorf("reading commit body %s: %w", sha, readErr)
	}
	var c glCommitResponse
	if err := json.Unmarshal(body, &c); err != nil {
		return "", fmt.Errorf("decoding commit response %s: %w", sha, err)
	}
	if len(c.ParentIDs) == 0 {
		return "", nil // root commit
	}
	return c.ParentIDs[0], nil
}

// gitlabBlameURL builds the file-blame endpoint. The file path is URL-encoded with slashes as
// %2F, the form GitLab's path-parameter encoding requires. No line range is sent: the walk
// matches the removed line by content across the whole file, so it needs the full blame.
func gitlabBlameURL(orgID, projectID, filePath, ref string) string {
	encodedPath := strings.ReplaceAll(url.PathEscape(filePath), "/", "%2F")
	q := url.Values{}
	q.Set("ref", ref)
	return fmt.Sprintf("%s/projects/%s/repository/files/%s/blame?%s", gitlabRESTBase(orgID), projectID, encodedPath, q.Encode())
}

// gitlabCommitURL builds the single-commit endpoint (read for parent_ids).
func gitlabCommitURL(orgID, projectID, sha string) string {
	return fmt.Sprintf("%s/projects/%s/repository/commits/%s", gitlabRESTBase(orgID), projectID, url.PathEscape(sha))
}

// glCommitMR is one merge request the commit-MR endpoint associates with a commit. iid is the
// project-scoped MR number — the value a consumer's pull-request number stores for a GitLab MR (NOT the
// global id). state distinguishes the merged MR that landed the culprit from an open/closed one.
type glCommitMR struct {
	IID   int32  `json:"iid"`
	State string `json:"state"`
}

// resolveCommitMR resolves the merge request that introduced a culprit commit via
// GET /projects/{id}/repository/commits/{sha}/merge_requests. It prefers the merged MR (the one that
// landed the culprit); absent one it takes the first with a positive iid. Best-effort: any transport
// error, non-200, or empty list yields (0, false), logged at warn, so MR resolution never fails the
// walk. Satisfies lineage.CandidatePRResolver.
func (s *gitlabBlameSource) resolveCommitMR(ctx context.Context, sha string) (int32, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gitlabCommitMRsURL(s.orgID, s.projectID, sha), nil)
	if err != nil {
		logger.WarnWithCtx(ctx, "gitlab lineage: building commit-MRs request failed; leaving candidate MR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	resp, err := s.gw.Do(ctx, req)
	if err != nil {
		logger.WarnWithCtx(ctx, "gitlab lineage: commit-MRs request failed; leaving candidate MR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, blameBodyLimit))
	resp.Body.Close() //nolint:errcheck,gosec // best-effort close on a read-only lookup
	if resp.StatusCode != http.StatusOK {
		logger.WarnWithCtx(ctx, "gitlab lineage: commit-MRs unexpected status; leaving candidate MR unresolved",
			zap.String("culprit_sha", sha), zap.Int("status", resp.StatusCode))
		return 0, false
	}
	if readErr != nil {
		logger.WarnWithCtx(ctx, "gitlab lineage: reading commit-MRs body failed; leaving candidate MR unresolved",
			zap.String("culprit_sha", sha), zap.Error(readErr))
		return 0, false
	}
	var mrs []glCommitMR
	if err := json.Unmarshal(body, &mrs); err != nil {
		logger.WarnWithCtx(ctx, "gitlab lineage: decoding commit-MRs body failed; leaving candidate MR unresolved",
			zap.String("culprit_sha", sha), zap.Error(err))
		return 0, false
	}
	for _, mr := range mrs {
		if mr.IID > 0 && mr.State == "merged" {
			return mr.IID, true
		}
	}
	for _, mr := range mrs {
		if mr.IID > 0 {
			return mr.IID, true
		}
	}
	return 0, false
}

// gitlabCommitMRsURL builds the merge-requests-for-commit endpoint (the MRs a commit belongs to).
func gitlabCommitMRsURL(orgID, projectID, sha string) string {
	return fmt.Sprintf("%s/projects/%s/repository/commits/%s/merge_requests", gitlabRESTBase(orgID), projectID, url.PathEscape(sha))
}

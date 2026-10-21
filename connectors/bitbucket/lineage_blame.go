// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Bitbucket defect-lineage culprit reconstruction. Bitbucket Cloud publishes no blame/annotate
// endpoint (a permanent API limit recorded in the parity manifest), so the GitHub/GitLab blame
// walk cannot run here. Instead a culprit is reconstructed from two endpoints that do exist: the
// file's commit history and a per-commit diff. The result carries evidence type
// PATH_RECONSTRUCTION so a later scoring pass can weight it against the stronger blame evidence.
//
// Per anchor (a removed line), the walk lists the commits that touched the file up to the fix
// (newest first, renames-aware), then diffs each candidate newest→oldest and stops at the first
// one whose diff ADDED the removed line — that is the most recent commit to write the line before
// the fix, i.e. blame's answer. The diff is requested whitespace-insensitively, so a pure reformat
// is skipped and the substantive writer is found, mirroring the blame walk's reformat hop.
//
// It fails closed: no candidate whenever the line is not found within the bounded probe budget,
// the history is empty, or a provider call errors (the drain maps that to BLAME_UNAVAILABLE).
//
// KNOWN LIMITATION: without a native blame it cannot verify the removed line is UNIQUE in the file
// the way the GitHub/GitLab walk does, so a common line (a bare `return nil`, a lone brace) can be
// attributed to the newest commit that added that text anywhere in the file rather than the true
// ancestor. PATH_RECONSTRUCTION marks the evidence as the weaker kind for exactly this reason.

const (
	// reconstructDiffProbeCap bounds the diff calls one fix may spend across all its anchors — the
	// budget guard that keeps the walk affordable on Bitbucket's shared hourly allowance. Reaching
	// it yields no candidate rather than a guess.
	reconstructDiffProbeCap = 20
	// fileHistoryPageLen is the single page of file history fetched per file. The culprit is among
	// the newest commits to touch the file, so one page (newest first) is ample and paging further
	// would only add cost against the budget.
	fileHistoryPageLen = 100
)

// historyEntry is one commit that touched a file, with the path as it was at that revision
// (renames-aware) and the author the walk attributes.
type historyEntry struct {
	CommitSHA   string    `json:"commit_sha"`
	Path        string    `json:"path"`
	CommittedAt time.Time `json:"committed_at"`
	AuthorName  string    `json:"author_name"`
	AuthorEmail string    `json:"author_email"`
}

// reconstructSource is the walk's provider port, kept small so the hop logic is a pure function of
// it and testable against a fake without a live token.
type reconstructSource interface {
	// FileHistory returns the commits that modified path up to commit, newest first,
	// renames-aware. Empty means the file has no history the walk can read.
	FileHistory(ctx context.Context, commit, path string) ([]historyEntry, error)
	// Diff returns the lines commitSha ADDED for path (its diff against its first parent, filtered
	// to path, whitespace-insensitive).
	Diff(ctx context.Context, commitSha, path string) (addedLines []addedLine, err error)
}

// WalkFix runs the Bitbucket reconstruction walk for one claimed bug fix and assembles its
// outcome. The bitbucket drain op calls this per PendingLineageBlame and forwards the result to
// SubmitLineageBlame. Unlike the GitHub/GitLab WalkFix it also takes the fetch cache, a per-fix
// pacing budget, and the org/op identity, because the reconstruction makes many bounded provider
// calls that must be cached and paced against Bitbucket's shared budget.
func WalkFix(ctx context.Context, gw sdk.VendorGateway, fc sdk.FetchCache, budget *pacing.PacingBudget, orgID string, opID sdk.OperationID, fix *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error) {
	workspace, repoSlug, ok := splitRepoFullName(fix.GetRepoFullName())
	if !ok {
		return defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNSPECIFIED, nil,
			fmt.Errorf("bad repo full name %q", fix.GetRepoFullName())
	}
	src := &bbReconstructSource{
		gw: gw, fc: fc, budget: budget, orgID: orgID, opID: opID,
		workspace: workspace, repoSlug: repoSlug,
		histMemo: map[string][]historyEntry{}, diffMemo: map[string][]addedLine{},
	}
	state, results, err := reconstructWalk(ctx, src, fix)
	// No candidate_pr_number resolution here: unlike GitHub/GitLab, Bitbucket Cloud's commit→pull
	// request lookup is not wired through this connector's typed vendor-request factory, and the
	// endpoint is not verified against a live workspace. Bitbucket culprits therefore leave the field 0
	// and the host falls back to the commit's stored PR link.
	if err != nil {
		return state, results, fmt.Errorf("bitbucket reconstruction for fix %s: %w", fix.GetBugFixPrId(), err)
	}
	return state, results, nil
}

// probeBudget is the fix-wide diff-call allowance, shared across anchors so one anchor cannot
// exhaust the whole fix's budget while a later anchor goes unattributed.
type probeBudget struct{ remaining int }

// anchorOutcome wraps one anchor's result so "no candidate" is a value, not a nil-nil return —
// ambiguous, absent, and capped are all fail-closed answers, not errors.
type anchorOutcome struct {
	result *defectlineagev1.LineageBlameResult
}

// reconstructWalk runs the reconstruction for every anchor and folds the outcomes into a state:
// RESOLVED when at least one anchor names a culprit, UNATTRIBUTED when all were read cleanly but
// none did. A provider error surfaces for the drain to map to a terminal state.
func reconstructWalk(ctx context.Context, src reconstructSource, fix *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error) {
	probes := &probeBudget{remaining: reconstructDiffProbeCap}
	var results []*defectlineagev1.LineageBlameResult
	var firstAnchorErr error
	for _, a := range fix.GetLocationAnchors() {
		outcome, err := reconstructAnchor(ctx, src, fix.GetFixCommitSha(), a.GetPath(), a.GetRemovedText(), probes)
		if err != nil {
			// One anchor's provider error must not discard the culprits earlier anchors already named
			// — a fix resolves when ANY anchor names one. Record the first error and leave this anchor
			// unattributed; the loop moves on to the next anchor, and the error only decides the
			// outcome if NO anchor resolves (below).
			if firstAnchorErr == nil {
				firstAnchorErr = fmt.Errorf("anchor %s:%d: %w", a.GetPath(), a.GetStartLine(), err)
			}
			logger.WarnWithCtx(ctx, "bitbucket lineage reconstruct: anchor failed; skipping it and continuing the fix",
				zap.String("path", a.GetPath()), zap.Int32("start_line", a.GetStartLine()), zap.Error(err))
		} else if outcome.result != nil {
			outcome.result.StartLine = a.GetStartLine()
			results = append(results, outcome.result)
		}
	}
	if len(results) > 0 {
		// Surface firstAnchorErr alongside the results (nil when every anchor was clean): a partial
		// resolve must be distinguishable from a complete one so the drain re-offers on a transient
		// sibling error and keeps these culprits on a permanent one. See the walk's Walk for the full
		// rationale.
		return defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results, firstAnchorErr
	}
	if firstAnchorErr != nil {
		return defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNSPECIFIED, nil, firstAnchorErr
	}
	return defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, nil, nil
}

// reconstructAnchor traces one removed line to the newest commit before the fix that wrote it.
func reconstructAnchor(ctx context.Context, src reconstructSource, fixSha, path string, removedText []string, probes *probeBudget) (anchorOutcome, error) {
	target, ok := representativeLine(removedText)
	if !ok {
		return anchorOutcome{}, nil // nothing blameable in this anchor
	}
	wantWS := normalizeWS(target)

	hist, err := src.FileHistory(ctx, fixSha, path)
	if err != nil {
		return anchorOutcome{}, fmt.Errorf("file history %s@%s: %w", path, fixSha, err)
	}
	for _, e := range hist {
		if e.CommitSHA == fixSha {
			// The fix itself removed the line; it is never the culprit.
			continue
		}
		if probes.remaining <= 0 {
			// The fix's diff-call budget is spent — fail closed rather than keep spending against
			// Bitbucket's shared allowance.
			return anchorOutcome{}, nil
		}
		probes.remaining--
		added, err := src.Diff(ctx, e.CommitSHA, e.Path)
		if err != nil {
			return anchorOutcome{}, fmt.Errorf("diff %s@%s: %w", e.Path, e.CommitSHA, err)
		}
		if line, ok := matchedLineWS(added, wantWS); ok {
			// Newest commit before the fix that added the line = blame's answer. line is the culprit's
			// own-revision line, so the AST scorer needs no re-fetch on Bitbucket.
			return anchorOutcome{result: reconstructResult(e, path, line)}, nil
		}
	}
	return anchorOutcome{}, nil // not found within the file's recent history and the probe budget
}

// reconstructResult builds a PATH_RECONSTRUCTION candidate for the commit that wrote the line.
func reconstructResult(e historyEntry, anchorPath string, culpritLine int32) *defectlineagev1.LineageBlameResult {
	var committedAt *timestamppb.Timestamp
	if !e.CommittedAt.IsZero() {
		committedAt = timestamppb.New(e.CommittedAt)
	}
	var author *defectlineagev1.LineageBlameAuthor
	if e.AuthorName != "" || e.AuthorEmail != "" {
		author = &defectlineagev1.LineageBlameAuthor{Name: e.AuthorName, Email: e.AuthorEmail}
	}
	return &defectlineagev1.LineageBlameResult{
		Path:         anchorPath,
		CulpritSha:   e.CommitSHA,
		CommittedAt:  committedAt,
		EvidenceType: defectlineagev1.DefectLineageEvidenceType_DEFECT_LINEAGE_EVIDENCE_TYPE_PATH_RECONSTRUCTION,
		Author:       author,
		// Bitbucket's reconstruction resolves the culprit's OWN path (rename-aware, unlike the
		// path-scoped blame providers) and the exact line it wrote, from the reconstruction diff — so
		// the AST scorer locates the candidate node with no re-fetch.
		CulpritPath: e.Path,
		CulpritLine: culpritLine,
	}
}

// normalizeWS collapses a line to its whitespace-insensitive form — mirrors the blame walk's
// comparison so a re-indent is not read as a different line.
func normalizeWS(s string) string { return strings.Join(strings.Fields(s), " ") }

// representativeLine is the first non-blank removed line — the one line the walk reasons about per
// anchor, matching the blame walk.
func representativeLine(removedText []string) (string, bool) {
	for _, l := range removedText {
		if strings.TrimSpace(l) != "" {
			return l, true
		}
	}
	return "", false
}

// addedLine is an added line of a unified diff with its 1-based line number in the new (post-image)
// file, so a reconstructed culprit carries the exact line it wrote — the coordinate the AST scorer
// needs on the candidate side.
type addedLine struct {
	Text    string `json:"text"`
	NewLine int32  `json:"new_line"`
}

// matchedLineWS returns the new-file line number of the added line equal to wantWS under whitespace
// normalization, but only when EXACTLY ONE added line matches. A duplicated short/boilerplate line
// (a bare `}`, `return nil`, `break`) would otherwise mis-attribute CulpritLine to an arbitrary
// occurrence and flip the AST scorer's verdict; on a non-unique match it returns (0, false) so the
// scorer degrades to same-file coverage. Mirrors recoverLineByUniqueMatch's uniqueness guard.
func matchedLineWS(added []addedLine, wantWS string) (int32, bool) {
	var line int32
	count := 0
	for _, l := range added {
		if normalizeWS(l.Text) == wantWS {
			count++
			if count == 1 {
				line = l.NewLine
			}
		}
	}
	if count == 1 {
		return line, true
	}
	return 0, false
}

// addedLinesFromDiff extracts the added lines of a unified diff, each tagged with its 1-based line
// number in the new file. It tracks the new-file counter from each hunk header (@@ -a,b +c,d @@):
// added and context lines advance it, removed lines do not. The '+++' file header and the
// "\ No newline" marker are not content.
func addedLinesFromDiff(raw []byte) []addedLine {
	var out []addedLine
	var newLine int32
	inHunk := false
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.HasPrefix(line, "@@"):
			if n, ok := parseHunkNewStart(line); ok {
				newLine = n
				inHunk = true
			} else {
				inHunk = false
			}
		case strings.HasPrefix(line, "diff --git"):
			// A new file section (multi-file diff): its ---/+++/index headers follow and are not
			// hunk content, so leave the hunk before them.
			inHunk = false
		case !inHunk:
			// preamble / file headers (---, +++, index) before a hunk
		case strings.HasPrefix(line, "\\"):
			// "\ No newline at end of file" marker inside a hunk — not a line
		case strings.HasPrefix(line, "+"):
			// Added line. Matched by the single "+" prefix, NOT "+++": an added source line may
			// itself begin with "++" (e.g. `++count;`), and a real +++ file header only appears in the
			// preamble (handled by the !inHunk arm above) or after a diff --git separator.
			out = append(out, addedLine{Text: line[1:], NewLine: newLine})
			newLine++
		case strings.HasPrefix(line, "-"):
			// removed line: does not advance the new-file counter
		default:
			// context line (leading space, or blank): advances the new-file counter
			newLine++
		}
	}
	return out
}

// parseHunkNewStart reads the new-file start line c from a unified-diff hunk header "@@ -a,b +c,d @@".
// The digits after the '+' are accumulated directly (no strconv), since a diff line number is far
// within int32 range — a hunk header never carries an overflowing value.
func parseHunkNewStart(header string) (int32, bool) {
	plus := strings.IndexByte(header, '+')
	if plus < 0 {
		return 0, false
	}
	var n int32
	digits := 0
	for i := plus + 1; i < len(header) && header[i] >= '0' && header[i] <= '9'; i++ {
		n = n*10 + int32(header[i]-'0')
		digits++
	}
	if digits == 0 {
		return 0, false
	}
	return n, true
}

// parseAuthorRaw splits Bitbucket's "Name <email>" author string into its parts. Either may be
// empty; a raw with no angle brackets is treated as a name.
func parseAuthorRaw(raw string) (name, email string) {
	lt := strings.LastIndex(raw, "<")
	gt := strings.LastIndex(raw, ">")
	if lt >= 0 && gt > lt {
		email = strings.TrimSpace(raw[lt+1 : gt])
		name = strings.TrimSpace(raw[:lt])
		return name, email
	}
	return strings.TrimSpace(raw), ""
}

// bbReconstructSource answers the walk from Bitbucket's REST API through the VendorGateway, paced
// against the shared budget and memoised so anchors in the same file (and re-drives, via the fetch
// cache) do not re-fetch immutable history/diff content.
type bbReconstructSource struct {
	gw        sdk.VendorGateway
	fc        sdk.FetchCache
	budget    *pacing.PacingBudget
	orgID     string
	opID      sdk.OperationID
	workspace string
	repoSlug  string
	histMemo  map[string][]historyEntry // in-process memo, keyed by commit+"\x00"+path
	diffMemo  map[string][]addedLine
}

func (s *bbReconstructSource) FileHistory(ctx context.Context, commit, path string) ([]historyEntry, error) {
	key := commit + "\x00" + path
	if v, ok := s.histMemo[key]; ok {
		return v, nil
	}
	// Persistent cache: file history at a fixed commit is immutable.
	cached := map[string][]historyEntry{}
	primeFetchCache(ctx, s.fc, CacheTypeFileHistory, []string{key}, cached)
	if v, ok := cached[key]; ok {
		s.histMemo[key] = v
		return v, nil
	}

	protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, s.gw, &connectorpb.BitbucketGetFileHistoryRequest{
		Workspace: s.workspace,
		RepoSlug:  s.repoSlug,
		Commit:    commit,
		Path:      path,
		Renames:   true,
		Pagelen:   fileHistoryPageLen,
	}, s.budget, connectorID, s.opID, s.orgID)
	if err != nil {
		return nil, fmt.Errorf("list file history: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.BitbucketGetFileHistoryResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", protoResp)
	}
	entries := make([]historyEntry, 0, len(resp.GetEntries()))
	for _, rawEntry := range resp.GetEntries() {
		var e struct {
			Commit struct {
				Hash   string    `json:"hash"`
				Date   time.Time `json:"date"`
				Author struct {
					Raw string `json:"raw"`
				} `json:"author"`
			} `json:"commit"`
			Path string `json:"path"`
		}
		if err := json.Unmarshal(rawEntry, &e); err != nil {
			return nil, fmt.Errorf("unmarshal file-history entry: %w", err)
		}
		if e.Commit.Hash == "" {
			continue
		}
		name, email := parseAuthorRaw(e.Commit.Author.Raw)
		entries = append(entries, historyEntry{
			CommitSHA:   e.Commit.Hash,
			Path:        e.Path,
			CommittedAt: e.Commit.Date.UTC(),
			AuthorName:  name,
			AuthorEmail: email,
		})
	}
	s.histMemo[key] = entries
	putFetchCacheDirty(ctx, s.fc, CacheTypeFileHistory, map[string][]historyEntry{key: entries})
	return entries, nil
}

func (s *bbReconstructSource) Diff(ctx context.Context, commitSha, path string) ([]addedLine, error) {
	key := commitSha + "\x00" + path
	if v, ok := s.diffMemo[key]; ok {
		return v, nil
	}
	cached := map[string][]addedLine{}
	primeFetchCache(ctx, s.fc, CacheTypeCommitDiff, []string{key}, cached)
	if v, ok := cached[key]; ok {
		s.diffMemo[key] = v
		return v, nil
	}

	protoResp, err := pacing.ExecuteWaitingForLocalCapacity(ctx, s.gw, &connectorpb.BitbucketGetDiffRequest{
		Workspace:        s.workspace,
		RepoSlug:         s.repoSlug,
		Spec:             commitSha,
		Path:             path,
		IgnoreWhitespace: true,
	}, s.budget, connectorID, s.opID, s.orgID)
	if err != nil {
		return nil, fmt.Errorf("get diff: %w", err)
	}
	resp, ok := protoResp.(*connectorpb.BitbucketGetDiffResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", protoResp)
	}
	added := addedLinesFromDiff(resp.GetRawDiff())
	s.diffMemo[key] = added
	putFetchCacheDirty(ctx, s.fc, CacheTypeCommitDiff, map[string][]addedLine{key: added})
	return added, nil
}

var _ reconstructSource = (*bbReconstructSource)(nil)

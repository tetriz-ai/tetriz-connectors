// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
)

// fakeReconSource is a token-free reconstructSource: file history per path and the added lines per
// commit are supplied by the test, so the walk's hop/selection logic is exercised in isolation.
type fakeReconSource struct {
	history   map[string][]historyEntry // keyed by path
	added     map[string][]string       // added lines, keyed by commit sha
	diffCalls int
	diffPaths []string // records the path passed to each Diff call (rename-awareness)
}

func (f *fakeReconSource) FileHistory(_ context.Context, _ /*commit*/, path string) ([]historyEntry, error) {
	return f.history[path], nil
}

func (f *fakeReconSource) Diff(_ context.Context, commitSha, path string) ([]addedLine, error) {
	f.diffCalls++
	f.diffPaths = append(f.diffPaths, path)
	var out []addedLine
	for i, t := range f.added[commitSha] {
		out = append(out, addedLine{Text: t, NewLine: int32(i + 1)})
	}
	return out, nil
}

func oneAnchorFix(path string, removed ...string) *defectlineagev1.PendingLineageBlame {
	return &defectlineagev1.PendingLineageBlame{
		FixCommitSha: "fix",
		RepoFullName: "ws/repo",
		LocationAnchors: []*defectlineagev1.LineageBlameAnchor{
			{Path: path, RemovedText: removed, StartLine: 10},
		},
	}
}

// The newest commit before the fix that added the removed line is the culprit, recorded as
// PATH_RECONSTRUCTION with the author and date the history carried.
func TestReconstruct_NewestAdderIsCulprit(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeReconSource{
		history: map[string][]historyEntry{"a.go": {
			{CommitSHA: "fix", Path: "a.go"},
			{CommitSHA: "cNew", Path: "a.go", CommittedAt: at, AuthorName: "Ada", AuthorEmail: "ada@example.org"},
			{CommitSHA: "cOld", Path: "a.go"},
		}},
		added: map[string][]string{"cNew": {"x := foo()"}, "cOld": {"unrelated"}},
	}
	state, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "x := foo()"))
	require.NoError(t, err)
	require.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, state)
	require.Len(t, results, 1)
	assert.Equal(t, "cNew", results[0].GetCulpritSha())
	assert.Equal(t, defectlineagev1.DefectLineageEvidenceType_DEFECT_LINEAGE_EVIDENCE_TYPE_PATH_RECONSTRUCTION, results[0].GetEvidenceType())
	assert.Equal(t, int32(10), results[0].GetStartLine())
	assert.Equal(t, "Ada", results[0].GetAuthor().GetName())
	assert.Equal(t, at.Unix(), results[0].GetCommittedAt().GetSeconds())
	assert.Equal(t, 1, src.diffCalls, "stops at the first (newest) commit that added the line")
}

// The fix commit is never the culprit even though it heads its own file history — the fix removed
// the line, it did not write it.
func TestReconstruct_SkipsTheFixCommit(t *testing.T) {
	src := &fakeReconSource{
		history: map[string][]historyEntry{"a.go": {
			{CommitSHA: "fix", Path: "a.go"},
			{CommitSHA: "cWriter", Path: "a.go"},
		}},
		// If the fix were diffed and matched, this would wrongly resolve to "fix".
		added: map[string][]string{"fix": {"x := foo()"}, "cWriter": {"x := foo()"}},
	}
	_, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "x := foo()"))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "cWriter", results[0].GetCulpritSha())
}

// A pure reformat between the fix and the writer is skipped: the diff is requested
// whitespace-insensitively, so the reformatter's diff shows no added line and the walk continues to
// the substantive writer.
func TestReconstruct_WalksPastAReformat(t *testing.T) {
	src := &fakeReconSource{
		history: map[string][]historyEntry{"a.go": {
			{CommitSHA: "fix", Path: "a.go"},
			{CommitSHA: "cReformat", Path: "a.go"}, // whitespace-only → no added line survives
			{CommitSHA: "cWriter", Path: "a.go"},
		}},
		added: map[string][]string{"cReformat": {}, "cWriter": {"x := foo()"}},
	}
	_, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "x := foo()"))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "cWriter", results[0].GetCulpritSha())
	assert.Equal(t, 2, src.diffCalls, "diffed the reformatter then the writer")
}

// No commit in the history added the line → no candidate, and the fix settles UNATTRIBUTED (read
// cleanly, nothing survived) rather than erroring.
func TestReconstruct_NotFoundIsUnattributed(t *testing.T) {
	src := &fakeReconSource{
		history: map[string][]historyEntry{"a.go": {
			{CommitSHA: "fix", Path: "a.go"},
			{CommitSHA: "cA", Path: "a.go"},
			{CommitSHA: "cB", Path: "a.go"},
		}},
		added: map[string][]string{"cA": {"other"}, "cB": {"another"}},
	}
	state, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "x := foo()"))
	require.NoError(t, err)
	assert.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, state)
	assert.Empty(t, results)
}

// A blank-only removal has no representative line, so the anchor yields no candidate.
func TestReconstruct_BlankRemovalNoCandidate(t *testing.T) {
	src := &fakeReconSource{history: map[string][]historyEntry{"a.go": {{CommitSHA: "cA", Path: "a.go"}}}}
	state, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "", "   "))
	require.NoError(t, err)
	assert.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, state)
	assert.Empty(t, results)
	assert.Zero(t, src.diffCalls, "a blank anchor never diffs")
}

// The fix-wide diff-probe budget bounds cost: a history longer than the cap with the writer beyond
// it yields no candidate rather than diffing without limit.
func TestReconstruct_ProbeBudgetFailsClosed(t *testing.T) {
	hist := []historyEntry{{CommitSHA: "fix", Path: "a.go"}}
	added := map[string][]string{}
	for i := 0; i < reconstructDiffProbeCap; i++ {
		sha := "noise"
		sha += string(rune('a' + i%26))
		sha += string(rune('0' + i/26))
		hist = append(hist, historyEntry{CommitSHA: sha, Path: "a.go"})
		added[sha] = []string{"unrelated"}
	}
	// The real writer sits just past the probe budget.
	hist = append(hist, historyEntry{CommitSHA: "cWriter", Path: "a.go"})
	added["cWriter"] = []string{"x := foo()"}
	src := &fakeReconSource{history: map[string][]historyEntry{"a.go": hist}, added: added}

	state, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("a.go", "x := foo()"))
	require.NoError(t, err)
	assert.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, state)
	assert.Empty(t, results)
	assert.Equal(t, reconstructDiffProbeCap, src.diffCalls, "spends exactly the budget, then fails closed")
}

// Rename-awareness: the diff for a candidate is requested at that candidate's own path, not the
// fix's path, so a file renamed between the writer and the fix still resolves.
func TestReconstruct_DiffsAtHistoricalPath(t *testing.T) {
	src := &fakeReconSource{
		history: map[string][]historyEntry{"new.go": {
			{CommitSHA: "fix", Path: "new.go"},
			{CommitSHA: "cWriter", Path: "old.go"}, // path before a rename
		}},
		added: map[string][]string{"cWriter": {"x := foo()"}},
	}
	_, results, err := reconstructWalk(context.Background(), src, oneAnchorFix("new.go", "x := foo()"))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, []string{"old.go"}, src.diffPaths, "diffed the pre-rename path")
	assert.Equal(t, "new.go", results[0].GetPath(), "the candidate keeps the anchor's current path")
}

func TestAddedLinesFromDiff(t *testing.T) {
	raw := []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1,2 +1,2 @@\n-old line\n+new line\n context\n+another add\n")
	// +new line is line 1 in the new file; the context line advances the counter to 2; +another add
	// is line 3. Removed lines and file headers do not advance the new-file counter.
	assert.Equal(t, []addedLine{{Text: "new line", NewLine: 1}, {Text: "another add", NewLine: 3}}, addedLinesFromDiff(raw))
}

// An added source line whose content itself starts with "++" (e.g. a column-0 pre-increment) must be
// captured as content, not mistaken for a "+++" file header — otherwise it is dropped and every later
// added line in the hunk is numbered one too low.
func TestAddedLinesFromDiff_ContentStartingWithPlusPlus(t *testing.T) {
	raw := []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -9,1 +10,3 @@\n+first\n++count;\n+third\n")
	assert.Equal(t, []addedLine{
		{Text: "first", NewLine: 10},
		{Text: "+count;", NewLine: 11},
		{Text: "third", NewLine: 12},
	}, addedLinesFromDiff(raw))
}

// A multi-file diff must not carry the new-file counter across the diff --git separator.
func TestAddedLinesFromDiff_MultiFile(t *testing.T) {
	raw := []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1,1 +1,1 @@\n+a-add\n" +
		"diff --git a/b.go b/b.go\n--- a/b.go\n+++ b/b.go\n@@ -5,1 +5,1 @@\n+b-add\n")
	assert.Equal(t, []addedLine{{Text: "a-add", NewLine: 1}, {Text: "b-add", NewLine: 5}}, addedLinesFromDiff(raw))
}

func TestParseAuthorRaw(t *testing.T) {
	n, e := parseAuthorRaw("Ada Lovelace <ada@example.org>")
	assert.Equal(t, "Ada Lovelace", n)
	assert.Equal(t, "ada@example.org", e)
	n, e = parseAuthorRaw("just-a-name")
	assert.Equal(t, "just-a-name", n)
	assert.Empty(t, e)
}

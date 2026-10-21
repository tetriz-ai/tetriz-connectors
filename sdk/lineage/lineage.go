// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

//go:generate go tool mockgen -source=lineage.go -destination=../../internal/mocks/sdk/lineage/mock_lineage.go -package=mocks

// Package lineage declares the host services the VCS connectors' defect
// lineage operations need. Defect lineage finds the change that introduced a
// bug, SZZ-style: from a bug fix, blame the lines it changed back to the
// commits that last touched them. The services are a work list to claim
// fixes from and submit results to (BlameSink), a blame walk over a provider
// (Walker), and a scorer that rates each candidate against the fix (Scorer).
// The repository ships no Walker or Scorer; an operation that needs one
// fails when the host provides none (see sdk.HostServices).
package lineage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
)

// ErrFixCommitGone is returned by a FirstParentFn when the fix commit is
// permanently absent on the provider (a 404), as distinct from a transient
// lookup failure.
var ErrFixCommitGone = errors.New("fix commit not found")

// BlameSink is the host-owned defect-lineage work list.
type BlameSink interface {
	ListPendingLineageBlame(ctx context.Context, req *defectlineagev1.ListPendingLineageBlameRequest) (*defectlineagev1.ListPendingLineageBlameResponse, error)
	SubmitLineageBlame(ctx context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error)
	ListPendingStructuralScoring(ctx context.Context, req *defectlineagev1.ListPendingStructuralScoringRequest) (*defectlineagev1.ListPendingStructuralScoringResponse, error)
	SubmitStructuralVerdict(ctx context.Context, req *defectlineagev1.SubmitStructuralVerdictRequest) (*defectlineagev1.SubmitStructuralVerdictResponse, error)
}

// BlamedLine is one line of a file at a revision, tagged with the commit that
// owns it under blame and that commit's author.
type BlamedLine struct {
	Text        string
	OwnerSHA    string
	CommittedAt time.Time
	AuthorName  string
	AuthorEmail string
}

// Source is a provider's answer to a blame walk. Lines returns the file at
// path as of revision, each line with its blame owner; an empty slice means
// the path does not exist at that revision. FirstParent returns a commit's
// first parent, or "" for a root commit.
type Source interface {
	Lines(ctx context.Context, revision, path string) ([]BlamedLine, error)
	FirstParent(ctx context.Context, sha string) (string, error)
}

// CandidatePRResolver resolves the pull request that introduced a culprit
// commit. ok is false when none was resolved.
type CandidatePRResolver func(ctx context.Context, culpritSHA string) (prNumber int32, ok bool)

// Walker runs the blame walk for one claimed fix over a provider Source.
type Walker interface {
	Walk(ctx context.Context, src Source, fix *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error)
	// ResolveCandidatePRs fills each result's candidate pull request number,
	// best effort. A nil resolve is a no-op.
	ResolveCandidatePRs(ctx context.Context, results []*defectlineagev1.LineageBlameResult, resolve CandidatePRResolver)
}

// AddedLine is a line a unified diff added, with its 1-based line number in
// the new file.
type AddedLine struct {
	Text    string
	NewLine int
}

// Cache is the best-effort memo a Scorer keeps verdicts in. The operation
// passes its sdk.FetchCache, which has this method set.
type Cache interface {
	GetMany(ctx context.Context, cacheType string, entryKeys []string) (map[string]json.RawMessage, error)
	PutMany(ctx context.Context, cacheType string, entries map[string]json.RawMessage) error
}

// FileFetcher returns a file's content at a revision and a cache key for it.
type FileFetcher func(ctx context.Context, revision, path string) (content []byte, key string, err error)

// FirstParentFn resolves a commit's first parent; "" means a root commit.
type FirstParentFn func(ctx context.Context, sha string) (string, error)

// CulpritAddedLinesFn returns the lines a commit added to path. Nil means the
// provider already supplied the exact culprit line at blame time.
type CulpritAddedLinesFn func(ctx context.Context, commitSha, path string) ([]AddedLine, error)

// Scorer computes and submits the structural verdict for each candidate of
// one claimed fix, and parses the unified diffs the culprit-line recovery
// reads.
type Scorer interface {
	ScorePendingFix(ctx context.Context, sink BlameSink, cache Cache, provider, orgID string,
		fetch FileFetcher, firstParent FirstParentFn, culpritLines CulpritAddedLinesFn,
		fix *defectlineagev1.PendingStructuralScoring)
	// AddedLinesFromSingleFileDiff extracts the added lines of a diff scoped
	// to one file.
	AddedLinesFromSingleFileDiff(raw []byte) []AddedLine
	// AddedLinesForPath extracts the added lines of newPath from a
	// multi-file diff.
	AddedLinesForPath(raw []byte, newPath string) []AddedLine
}

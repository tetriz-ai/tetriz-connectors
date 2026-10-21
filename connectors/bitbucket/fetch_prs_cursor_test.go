// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- shared test infrastructure ---

// checkpointRecorder is a sdk.CheckpointFunc that records every call so
// tests can assert how often the per-repo checkpoint fired and what composite
// cursor was written at each point. failNthCall (1-indexed) injects a
// synthetic write failure on call N to exercise the best-effort path.
//
// NOT concurrency-safe: the slice append below is unsynchronised. The
// recorder is safe only because every caller in this package drives the
// CheckpointFunc from a single goroutine (FetchPRsOp.Execute walks repos
// sequentially). If a future change goroutine-izes the repo iteration or
// the page loop, swap the slice for a mutex-guarded append or a chan.
type checkpointRecorder struct {
	calls       []*sdk.Cursor
	failNthCall int
	callCount   int32
}

func (recorder *checkpointRecorder) fn(_ context.Context, cursor *sdk.Cursor) error {
	currentCall := atomic.AddInt32(&recorder.callCount, 1)
	recorder.calls = append(recorder.calls, cursor)
	if recorder.failNthCall > 0 && int(currentCall) == recorder.failNthCall {
		return errors.New("synthetic checkpoint failure")
	}
	return nil
}

func (recorder *checkpointRecorder) totalCalls() int {
	return int(atomic.LoadInt32(&recorder.callCount))
}

// decodedRepoEntries decodes a fetch_prs cursor into a map of repo→Timestamp
// for assertion.
func decodedRepoEntries(t *testing.T, cursor *sdk.Cursor) map[string]time.Time {
	t.Helper()
	if cursor == nil {
		return nil
	}
	states := existingCursorStates(context.Background(), cursor)
	timestamps := make(map[string]time.Time, len(states))
	for repoKey, state := range states {
		timestamps[repoKey] = state.Timestamp
	}
	return timestamps
}

// repoPRFixture lets a test declare what a repo's list-PR response looks like.
// Other request types (diffstat, PR commits) return zero values so test focus
// stays on the cursor + per-repo control flow.
type repoPRFixture struct {
	prs []bbPullRequest
	err error // when non-nil, the PR list call returns this error
}

// multiRepoExecutor builds an executeFunc that dispatches PR-list calls per
// repo using the provided fixtures. Diffstat and PR-commit calls fall through
// to empty-success so the loop's outer per-repo control flow is exercised
// without unnecessary fixture plumbing.
func multiRepoExecutor(repoFixtures map[string]repoPRFixture) func(context.Context, proto.Message) (proto.Message, error) {
	return func(_ context.Context, request proto.Message) (proto.Message, error) {
		switch typedRequest := request.(type) {
		case *connectorpb.BitbucketListPullRequestsRequest:
			fullName := typedRequest.GetWorkspace() + "/" + typedRequest.GetRepoSlug()
			fixture, found := repoFixtures[fullName]
			if !found {
				return nil, fmt.Errorf("unexpected repo in test: %s", fullName)
			}
			if fixture.err != nil {
				return nil, fixture.err
			}
			return bbPRsResponse(fixture.prs, false), nil
		case *connectorpb.BitbucketGetPRDiffstatRequest:
			return bbDiffstatResponse(0, 0), nil
		case *connectorpb.BitbucketListPRCommitsRequest:
			return bbPRCommitsResponse(nil, false), nil
		default:
			return nil, fmt.Errorf("unexpected request type: %T", request)
		}
	}
}

// testParamsMultiRepo builds an ExecutionParams with multiple repos and an
// optional Checkpoint. Replaces single-repo testParams for tests that exercise
// cross-repo behaviour.
func testParamsMultiRepo(sink sdk.DataSink, gateway sdk.VendorGateway, cursor *sdk.Cursor, repos []string, checkpoint sdk.CheckpointFunc) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
			OrganizationID: "org-test",
			Credentials: mustJSON(BitbucketCredentials{
				OAuthAccessToken: "test-oauth-token",
			}),
			Settings: mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: repos,
			}),
			Enabled: true,
		},
		Cursor:        cursor,
		Sink:          sink,
		VendorGateway: gateway,
		Checkpoint:    checkpoint,
	}
}

// buildCorruptCursor crafts a composite cursor whose payload is well-formed
// JSON but whose per-repo values don't decode as time.Time. It asserts
// that seedCursorTimestamps treats corrupt input as empty (no crash).
func buildCorruptCursor() *sdk.Cursor {
	rawParts := map[string]json.RawMessage{
		"myworkspace/repo": json.RawMessage(`"not-a-timestamp"`),
	}
	return sdk.NewCompositeCursor(rawParts)
}

// --- TestSeedCursorTimestamps ---

// Verifies the cursor-seeding helper across every meaningful input shape. The
// helper is the entry point for partial-run cursor preservation: a wrong
// answer here means lost progress across runs.
func TestSeedCursorTimestamps(t *testing.T) {
	t.Parallel()

	canonicalTime := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	earlierTime := canonicalTime.Add(-time.Hour)

	testCases := []struct {
		name     string
		cursor   *sdk.Cursor
		expected map[string]time.Time
	}{
		{
			name:     "nil cursor returns empty map (first sync)",
			cursor:   nil,
			expected: map[string]time.Time{},
		},
		{
			name:     "empty composite returns empty map",
			cursor:   buildCompositeCursor(map[string]time.Time{}),
			expected: map[string]time.Time{},
		},
		{
			name:     "single repo entry decodes correctly",
			cursor:   buildCompositeCursor(map[string]time.Time{"ws/repo1": canonicalTime}),
			expected: map[string]time.Time{"ws/repo1": canonicalTime},
		},
		{
			name: "multiple repo entries decode correctly",
			cursor: buildCompositeCursor(map[string]time.Time{
				"ws/repo1": canonicalTime,
				"ws/repo2": earlierTime,
			}),
			expected: map[string]time.Time{
				"ws/repo1": canonicalTime,
				"ws/repo2": earlierTime,
			},
		},
		{
			name:     "corrupt per-entry value is silently skipped",
			cursor:   buildCorruptCursor(),
			expected: map[string]time.Time{},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase // capture for parallel subtests
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			gotEntries := seedCursorTimestamps(context.Background(), testCase.cursor)
			require.Len(t, gotEntries, len(testCase.expected))
			for repoFullName, expectedTime := range testCase.expected {
				assert.True(t, gotEntries[repoFullName].Equal(expectedTime),
					"key %q: got %v, want %v", repoFullName, gotEntries[repoFullName], expectedTime)
			}
		})
	}
}

// --- TestFirstTypedSyncError ---

// Verifies the all-failed wrap prefers the first *sdk.SyncError over
// iteration order, so the orchestrator's errors.As classification recovers the
// right errKind (rate_limit, sink_failure, …) even when a generic error
// appears earlier in the slice.
func TestFirstTypedSyncError(t *testing.T) {
	t.Parallel()

	syncErrRateLimit := &sdk.SyncError{
		ConnectorID: "bitbucket",
		OperationID: "fetch_prs",
		Kind:        sdk.ErrKindRateLimit,
		Cause:       errors.New("upstream 429"),
	}
	syncErrSink := &sdk.SyncError{
		ConnectorID: "bitbucket",
		OperationID: "fetch_prs",
		Kind:        sdk.ErrKindSinkFailure,
		Cause:       errors.New("sqs publish failed"),
	}
	genericErr := errors.New("plain repo failure")

	testCases := []struct {
		name      string
		errs      []error
		expectErr error
	}{
		{name: "empty slice returns nil", errs: nil, expectErr: nil},
		{name: "single generic returns it", errs: []error{genericErr}, expectErr: genericErr},
		{name: "single SyncError returns it", errs: []error{syncErrRateLimit}, expectErr: syncErrRateLimit},
		{
			name:      "SyncError preferred over earlier generic",
			errs:      []error{genericErr, syncErrRateLimit},
			expectErr: syncErrRateLimit,
		},
		{
			name:      "first SyncError wins when multiple SyncErrors present",
			errs:      []error{genericErr, syncErrRateLimit, syncErrSink},
			expectErr: syncErrRateLimit,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			gotErr := firstTypedSyncError(testCase.errs)
			assert.Equal(t, testCase.expectErr, gotErr)
		})
	}
}

// --- TestFetchPRsOp_Execute_CursorAndErrorPaths ---

// Single table-driven test covering every cursor + error-handling path through
// FetchPRsOp.Execute. Cases:
//   - cursor seeded from input
//   - per-repo continue on error (loop does not bail out on the first failure)
//   - ANY repo failure surfaces as a run-level error — not just the all-failed
//     case. A partial failure that quietly returned nil would mark the sync_run
//     "completed" in the table, hiding silent degradation from monitoring.
//   - all-attempted-repos-failed wraps the first typed SyncError so the
//     orchestrator's errors.As classification recovers the correct error_kind
//   - failed-run still returns a cursor reflecting per-repo Checkpoint progress;
//     the partial cursor must survive the run-level failure return so the next
//     run resumes from the cursored repos rather than starting from epoch
//   - per-repo checkpoint fires once per success regardless of overall outcome
//   - a failing checkpoint does NOT kill the run (best-effort)
func TestFetchPRsOp_Execute_CursorAndErrorPaths(t *testing.T) {
	t.Parallel()

	canonicalTime := time.Now().UTC().Truncate(time.Second)
	earlierTime := canonicalTime.Add(-time.Hour)
	priorCursorTime := canonicalTime.Add(-24 * time.Hour)

	successPR := func(prID int) bbPullRequest {
		return bbPullRequest{
			ID: prID, State: "OPEN",
			CreatedOn: earlierTime, UpdatedOn: canonicalTime,
		}
	}

	rateLimitSyncErr := &sdk.SyncError{
		ConnectorID: "bitbucket",
		OperationID: "fetch_prs",
		Kind:        sdk.ErrKindRateLimit,
		Cause:       errors.New("upstream 429"),
	}
	rateLimitKindRef := sdk.ErrKindRateLimit

	testCases := []struct {
		name                  string
		repos                 []string
		repoFixtures          map[string]repoPRFixture
		inputCursor           *sdk.Cursor
		checkpointFailNthCall int
		expectErr             bool
		expectErrSubstr       string
		expectSyncErrKind     *sdk.SyncErrorKind // pointer so we can distinguish "not asserted" from "expect zero kind"
		expectCursorRepos     []string
		expectCheckpoints     int // exact total Checkpoint call count. Each successful single-page repo causes TWO writes: the per-page checkpoint (onPage, fired by fetchPRsForRepo after page 1 completes) plus the per-repo checkpoint (fired by Execute after fetchPRsForRepo returns). Failed repos contribute zero because the error path returns before any page completes.
	}{
		{
			name:  "all repos succeed: cursor contains every iterated repo, per-page + per-repo checkpoint fires per success",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {prs: []bbPullRequest{successPR(1)}},
				"myworkspace/repoB": {prs: []bbPullRequest{successPR(2)}},
			},
			expectCursorRepos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			expectCheckpoints: 4, // 2 repos × (1 onPage + 1 per-repo) each
		},
		{
			name:  "one repo fails, others succeed: loop continues, returns partial-failure error",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB", "myworkspace/repoC"},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {prs: []bbPullRequest{successPR(1)}},
				"myworkspace/repoB": {err: errors.New("simulated repoB failure")},
				"myworkspace/repoC": {prs: []bbPullRequest{successPR(2)}},
			},
			expectErr:         true,
			expectErrSubstr:   "1 of 3 repos failed",
			expectCursorRepos: []string{"myworkspace/repoA", "myworkspace/repoC"},
			expectCheckpoints: 4, // 2 successful repos × 2 checkpoint calls each; repoB contributes 0
		},
		{
			name:  "partial failure with typed SyncError preserves error_kind for orchestrator",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {prs: []bbPullRequest{successPR(1)}},
				"myworkspace/repoB": {err: rateLimitSyncErr},
			},
			expectErr:         true,
			expectErrSubstr:   "1 of 2 repos failed",
			expectSyncErrKind: &rateLimitKindRef,
			expectCursorRepos: []string{"myworkspace/repoA"},
			expectCheckpoints: 2, // 1 successful repo × 2 checkpoint calls
		},
		{
			name:  "all repos fail: returns run-level error (no silent completion)",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {err: errors.New("rate_limit on repoA")},
				"myworkspace/repoB": {err: errors.New("rate_limit on repoB")},
			},
			expectErr:         true,
			expectErrSubstr:   "all 2 attempted repos failed",
			expectCheckpoints: 0,
		},
		{
			name:  "all repos fail wraps first SyncError so orchestrator errKind classifies correctly",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			repoFixtures: map[string]repoPRFixture{
				// repoA returns a generic error FIRST; repoB returns the SyncError.
				// The wrap must skip past the generic error and pick up repoB's
				// SyncError so the orchestrator's errors.As classification works.
				"myworkspace/repoA": {err: errors.New("plain error first")},
				"myworkspace/repoB": {err: rateLimitSyncErr},
			},
			expectErr:         true,
			expectErrSubstr:   "all 2 attempted repos failed",
			expectSyncErrKind: &rateLimitKindRef,
		},
		{
			name: "partial run preserves prior cursor entries for unreached repos",
			repos: []string{
				"myworkspace/repoA",
				"myworkspace/repoB",
				"myworkspace/repoC",
			},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {prs: []bbPullRequest{successPR(1)}},
				"myworkspace/repoB": {prs: []bbPullRequest{successPR(2)}},
				"myworkspace/repoC": {prs: []bbPullRequest{successPR(3)}},
			},
			inputCursor: prsCursor(map[string]time.Time{
				"myworkspace/repoA":      priorCursorTime,
				"myworkspace/repoB":      priorCursorTime,
				"myworkspace/repoC":      priorCursorTime,
				"myworkspace/historical": priorCursorTime,
			}),
			// historical was not in this run's repos list, but its prior entry
			// must survive (seed-from-input-cursor invariant).
			expectCursorRepos: []string{
				"myworkspace/repoA",
				"myworkspace/repoB",
				"myworkspace/repoC",
				"myworkspace/historical",
			},
			expectCheckpoints: 6, // 3 successful repos × (1 onPage + 1 per-repo)
		},
		{
			name:  "checkpoint write failure does NOT kill the run",
			repos: []string{"myworkspace/repoA", "myworkspace/repoB"},
			repoFixtures: map[string]repoPRFixture{
				"myworkspace/repoA": {prs: []bbPullRequest{successPR(1)}},
				"myworkspace/repoB": {prs: []bbPullRequest{successPR(2)}},
			},
			checkpointFailNthCall: 1,
			expectCursorRepos:     []string{"myworkspace/repoA", "myworkspace/repoB"},
			expectCheckpoints:     4, // the injected failure on call #1 (onPage for repoA) still counts; subsequent calls keep firing
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			gateway := &mockVendorGateway{executeFunc: multiRepoExecutor(testCase.repoFixtures)}
			sink := testkit.NewChannelSink()
			recorder := &checkpointRecorder{failNthCall: testCase.checkpointFailNthCall}
			params := testParamsMultiRepo(sink, gateway, testCase.inputCursor, testCase.repos, recorder.fn)

			op := &FetchPRsOp{}
			gotCursor, gotErr := op.Execute(context.Background(), params)

			if testCase.expectErr {
				require.Error(t, gotErr)
				if testCase.expectErrSubstr != "" {
					assert.Contains(t, gotErr.Error(), testCase.expectErrSubstr)
				}
				if testCase.expectSyncErrKind != nil {
					var wrappedSyncErr *sdk.SyncError
					require.ErrorAs(t, gotErr, &wrappedSyncErr,
						"failure wrap must preserve sdk.SyncError for orchestrator classification")
					assert.Equal(t, *testCase.expectSyncErrKind, wrappedSyncErr.Kind)
				}
			} else {
				require.NoError(t, gotErr)
			}

			// Cursor + checkpoint assertions apply to both success and
			// partial-failure paths — per-repo Checkpoint writes during the
			// loop guarantee the partial cursor survives the run-level error
			// return. When all repos failed and no input cursor was seeded,
			// gotCursor is legitimately nil (no progress to preserve).
			if len(testCase.expectCursorRepos) > 0 {
				require.NotNil(t, gotCursor, "cursor must be non-nil whenever any repo was cursored or an input cursor was seeded")
				gotEntries := decodedRepoEntries(t, gotCursor)
				for _, expectedRepo := range testCase.expectCursorRepos {
					assert.Contains(t, gotEntries, expectedRepo,
						"cursor should contain entry for %q", expectedRepo)
				}
			}
			assert.Equal(t, testCase.expectCheckpoints, recorder.totalCalls(),
				"checkpoint must fire exactly once per successful repo (regardless of overall outcome)")
		})
	}
}

// --- TestFetchPRsOp_Execute_ContextCancellation ---

// Verifies ctx-cancel behaviour across the two distinct timing windows:
//  1. pre-canceled ctx (cancel before Execute is called) — the loop's ctx.Err()
//     guard fires on the first iteration; no vendor calls happen, no repos are
//     processed, and the run is reported as failed so monitoring doesn't see
//     a "completed" sync with zero records emitted.
//  2. ctx-cancel mid-iter (cancel triggered AFTER one repo finishes) — the
//     loop bails out and the run is reported as failed because subsequent repos
//     were never attempted. The first repo's cursor write (via per-repo
//     Checkpoint) and prior cursor entries for unreached repos still survive.
//
// Both cases assert the partial cursor is returned so the next run picks up
// from progress even though the orchestrator marks the run failed.
func TestFetchPRsOp_Execute_ContextCancellation(t *testing.T) {
	t.Parallel()

	priorTime := time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC)

	testCases := []struct {
		name              string
		preCancelCtx      bool
		cancelAfterRepoA  bool
		inputCursor       *sdk.Cursor
		expectErrSubstr   string
		expectCursorRepos []string
	}{
		{
			name:         "ctx canceled before Execute: run marked failed, input cursor preserved",
			preCancelCtx: true,
			inputCursor: prsCursor(map[string]time.Time{
				"myworkspace/repoA": priorTime,
				"myworkspace/repoB": priorTime.Add(time.Hour),
			}),
			expectErrSubstr:   "ctx exhausted after 0/2 repos",
			expectCursorRepos: []string{"myworkspace/repoA", "myworkspace/repoB"},
		},
		{
			name:             "ctx canceled mid-iter: run marked failed, repoA cursor preserved",
			cancelAfterRepoA: true,
			inputCursor: prsCursor(map[string]time.Time{
				"myworkspace/repoB": priorTime,
				"myworkspace/repoC": priorTime.Add(time.Hour),
			}),
			expectErrSubstr: "ctx exhausted after 1/3 repos",
			expectCursorRepos: []string{
				"myworkspace/repoA", // freshly cursored before ctx break
				"myworkspace/repoB", // preserved from input
				"myworkspace/repoC", // preserved from input
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()

			if testCase.preCancelCtx {
				cancelCtx() // pre-cancel before Execute

				gateway := &mockVendorGateway{
					executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
						t.Fatal("vendor gateway must NOT be invoked when ctx is already canceled")
						return nil, nil
					},
				}
				sink := testkit.NewChannelSink()
				params := testParamsMultiRepo(sink, gateway, testCase.inputCursor,
					[]string{"myworkspace/repoA", "myworkspace/repoB"}, nil)

				op := &FetchPRsOp{}
				gotCursor, gotErr := op.Execute(ctx, params)

				require.Error(t, gotErr,
					"ctx-cancel before any work must surface as a run-level error so the sync_run is marked failed")
				assert.Contains(t, gotErr.Error(), testCase.expectErrSubstr)
				require.ErrorIs(t, gotErr, context.Canceled,
					"wrapped error must unwrap to context.Canceled for orchestrator classification")
				require.NotNil(t, gotCursor, "input cursor must still be returned so the next run picks up the seed")
				gotEntries := decodedRepoEntries(t, gotCursor)
				for _, expectedRepo := range testCase.expectCursorRepos {
					assert.Contains(t, gotEntries, expectedRepo)
				}
				return
			}

			// cancelAfterRepoA: trigger ctx cancellation right after repoA's
			// PR-list call returns, so the next iteration's ctx.Err() guard
			// fires before repoB/repoC are touched.
			gateway := &mockVendorGateway{
				executeFunc: func(_ context.Context, request proto.Message) (proto.Message, error) {
					switch typedRequest := request.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						if typedRequest.GetRepoSlug() == "repoA" {
							defer cancelCtx()
							return bbPRsResponse([]bbPullRequest{{
								ID: 1, State: "OPEN",
								CreatedOn: time.Now().Add(-time.Hour),
								UpdatedOn: time.Now(),
							}}, false), nil
						}
						return bbPRsResponse(nil, false), nil
					case *connectorpb.BitbucketGetPRDiffstatRequest:
						return bbDiffstatResponse(0, 0), nil
					case *connectorpb.BitbucketListPRCommitsRequest:
						return bbPRCommitsResponse(nil, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", request)
					}
				},
			}
			sink := testkit.NewChannelSink()
			params := testParamsMultiRepo(sink, gateway, testCase.inputCursor,
				[]string{"myworkspace/repoA", "myworkspace/repoB", "myworkspace/repoC"}, nil)

			op := &FetchPRsOp{}
			gotCursor, gotErr := op.Execute(ctx, params)

			require.Error(t, gotErr,
				"mid-iter ctx exhaustion must surface as a run-level error so unattempted repos aren't hidden behind a 'completed' status")
			assert.Contains(t, gotErr.Error(), testCase.expectErrSubstr)
			require.ErrorIs(t, gotErr, context.Canceled,
				"wrapped error must unwrap to context.Canceled")
			require.NotNil(t, gotCursor)
			gotEntries := decodedRepoEntries(t, gotCursor)
			for _, expectedRepo := range testCase.expectCursorRepos {
				assert.Contains(t, gotEntries, expectedRepo,
					"partial cursor must preserve or add entry for %q", expectedRepo)
			}
		})
	}
}

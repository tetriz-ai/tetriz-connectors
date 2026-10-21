// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func mkSettings(trailing, firstRun int) Settings {
	return Settings{
		Bucket: "b", Prefix: "p", Region: "r", AccountID: "a",
		TrailingOverlapDays: trailing, FirstRunWindowDays: firstRun,
	}
}

// The window decides exactly which days a run walks, and both of its branches are
// load-bearing: the first-run cap bounds an unbounded history, and the steady-state
// re-read is the only way a corrected day is ever collected.
func TestWindowFirstRunUsesTheCapAndEndsYesterday(t *testing.T) {
	t.Parallel()

	op := &FetchUserActivityReportOp{}
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)

	days := op.window(syncState{}, mkSettings(0, 5), now)

	assert.Len(t, days, 5)
	assert.Equal(t, day(2026, 8, 26), days[0])
	assert.Equal(t, day(2026, 8, 30), days[len(days)-1],
		"the newest day walked is yesterday: today is not yet published")
}

func TestWindowSteadyStateReReadsBehindTheMark(t *testing.T) {
	t.Parallel()

	op := &FetchUserActivityReportOp{}
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)

	var s syncState
	s.raiseCoveredThrough(day(2026, 8, 29))

	days := op.window(s, mkSettings(3, 0), now)

	assert.Equal(t, day(2026, 8, 26), days[0], "the walk starts the overlap before the mark")
	assert.Equal(t, day(2026, 8, 30), days[len(days)-1])
	assert.Contains(t, days, day(2026, 8, 29), "the mark's own day is re-read, not skipped")
}

// A mark in the future must not produce a walk that runs backwards or never terminates.
func TestWindowWithAMarkAheadOfYesterdayIsEmptyNotInfinite(t *testing.T) {
	t.Parallel()

	op := &FetchUserActivityReportOp{}
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)

	var s syncState
	s.raiseCoveredThrough(day(2026, 12, 25))

	days := op.window(s, mkSettings(3, 0), now)
	assert.Empty(t, days, "a mark past yesterday walks nothing rather than looping")
}

func TestWindowHonoursTheSettingsOverrides(t *testing.T) {
	t.Parallel()

	op := &FetchUserActivityReportOp{}
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)

	assert.Len(t, op.window(syncState{}, mkSettings(0, 1), now), 1)
	assert.Len(t, op.window(syncState{}, mkSettings(0, 10), now), 10)

	var s syncState
	s.raiseCoveredThrough(day(2026, 8, 30))
	assert.Equal(t, day(2026, 8, 29), op.window(s, mkSettings(1, 0), now)[0])
}

// ---- Execute, end to end over a faked bucket ----------------------------------------
//
// These exercise the seam the day-walk and the cursor's settling logic meet at. That seam
// is where a healthy report can be lost without any single piece looking wrong: the walk
// decides what to attempt, the cursor decides what is abandoned, and only together do they
// decide whether a file is ever collected.

// fakeBucket is the customer's bucket. It satisfies the framework's gateway interface
// because the operation asserts for the narrow object source on that value; the five HTTP
// methods are unreachable here for the same reason they are unreachable in real use —
// this connector makes no HTTP call.
type fakeBucket struct {
	objects map[string][]SourceObject // day prefix -> what listing returns
	unread  map[string]bool           // object key -> the object itself cannot be read
	gets    map[string]int            // object key -> how many times it was fetched
	listErr error                     // when set, every listing fails with it
	// onList fires before each listing, with the count of listings already served. It is
	// how a test interrupts a walk partway rather than at a boundary the code chose.
	onList func(served int)
	listed int
}

func (f *fakeBucket) ListDay(ctx context.Context, _, prefix string) ([]SourceObject, error) {
	if f.onList != nil {
		f.onList(f.listed)
	}
	f.listed++
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("listing %s: %w", prefix, err)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.objects[prefix], nil
}

func (f *fakeBucket) Get(_ context.Context, _, key string) ([]byte, error) {
	if f.gets == nil {
		f.gets = map[string]int{}
	}
	f.gets[key]++
	if f.unread[key] {
		return nil, fmt.Errorf("fetching %s: %w", key, ErrObjectUnreadable)
	}
	return []byte("Date,UserId,Client_Type\n2026-08-30,u1,KIRO_IDE\n"), nil
}

func (f *fakeBucket) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("not an http gateway")
}

func (f *fakeBucket) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("not an http gateway")
}
func (f *fakeBucket) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return f }
func (f *fakeBucket) ForGroup(string) sdk.VendorGateway                      { return f }
func (f *fakeBucket) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, errors.New("not an http gateway")
}

// recordingSink keeps the record identity of everything emitted.
type recordingSink struct{ emitted []string }

func (s *recordingSink) Emit(_ context.Context, r *sdk.RawRecord) error {
	s.emitted = append(s.emitted, r.Metadata["day"]+"/"+r.ExternalID)
	return nil
}

func (s *recordingSink) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (s *recordingSink) Flush(context.Context) error { return nil }

const execTestOrg = "11111111-2222-3333-4444-555555555555"

func execSettings(t *testing.T) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(Settings{
		Bucket: "acme-kiro-reports", Prefix: testPrefix,
		Region: testRegion, AccountID: testAccount,
		// One day, so the window is exactly the day under test on a first run.
		FirstRunWindowDays: 1,
	})
	require.NoError(t, err)
	return b
}

// runOnce executes the operation against the fake bucket, carrying the cursor in and the
// last checkpointed cursor out — which is how the real orchestrator behaves on a failed
// run, and the only reason a failure count survives to reach its threshold.
func runOnce(t *testing.T, bucket *fakeBucket, sink *recordingSink, in *sdk.Cursor) (*sdk.Cursor, error) {
	t.Helper()
	return runOnceFor(t, execTestOrg, bucket, sink, in)
}

// runOnceFor is runOnce for a named organisation, so a test reading process-wide counters
// can have deltas of its own.
func runOnceFor(
	t *testing.T, orgID string, bucket *fakeBucket, sink *recordingSink, in *sdk.Cursor,
) (*sdk.Cursor, error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	s3mock.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	checkpointed := in
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: connectorID, OrganizationID: orgID,
			Settings: execSettings(t),
		},
		Cursor:        in,
		Sink:          sink,
		VendorGateway: bucket,
		BlobStore:     s3mock,
		Checkpoint: func(_ context.Context, c *sdk.Cursor) error {
			checkpointed = c
			return nil
		},
	}

	op := &FetchUserActivityReportOp{}
	out, err := op.Execute(context.Background(), params)
	if out != nil {
		return out, err
	}
	return checkpointed, err
}

// yesterdaysPrefix is the one day a first-run window covers.
func yesterdaysPrefix(t *testing.T) (string, time.Time) {
	t.Helper()
	d := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	return dayPrefix(testPrefix, testAccount, testRegion, d), d
}

// A day holds one file per client type; when one of them
// cannot be read, the other must still be collected — on the first run and every run after,
// including after the broken one is abandoned for good. Stopping the day at the first
// failure loses the healthy file, and because Go randomises map order, loses a different
// one each time.
func TestAnUnreadableObjectDoesNotStrandItsSibling(t *testing.T) {
	t.Parallel()

	prefix, d := yesterdaysPrefix(t)
	healthy := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
	broken := prefix + "KIRO_CHAT_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: healthy, LastModified: time.Now().UTC(), Size: 10},
			{Key: broken, LastModified: time.Now().UTC(), Size: 10},
		}},
		unread: map[string]bool{broken: true},
	}
	sink := &recordingSink{}
	want := d.Format(dayLayout) + "/KIRO_IDE"

	var cursor *sdk.Cursor
	for run := 1; run <= maxConsecutiveFailures; run++ {
		var err error
		cursor, err = runOnce(t, bucket, sink, cursor)
		require.Error(t, err, "a run that could not read an object must not report success (run %d)", run)
		require.Len(t, sink.emitted, run,
			"the healthy sibling must be collected on every run, not abandoned with the broken one (run %d)", run)
		assert.Equal(t, want, sink.emitted[run-1])
	}

	// The broken object is now settled. The day must keep collecting, and must stop
	// re-fetching what it has given up on.
	state, err := loadState(cursor)
	require.NoError(t, err)
	require.True(t, state.isObjectTerminal(d, broken), "the broken object must be settled after the threshold")
	require.False(t, state.isObjectTerminal(d, healthy), "the healthy object must never be settled")

	fetchesBefore := bucket.gets[broken]
	cursor, err = runOnce(t, bucket, sink, cursor)
	require.NoError(t, err, "with the broken object abandoned, the day now completes")
	assert.Len(t, sink.emitted, maxConsecutiveFailures+1, "the healthy sibling is still collected")
	assert.Equal(t, fetchesBefore, bucket.gets[broken], "a settled object must not be fetched again")

	state, err = loadState(cursor)
	require.NoError(t, err)
	mark, ok := state.coveredThroughDay()
	require.True(t, ok, "the mark must advance once the day owes nothing collectable")
	assert.Equal(t, d.Format(dayLayout), mark.Format(dayLayout))
}

// The mark is what keeps an incomplete day inside the trailing re-read window. If it moved
// past a day still owing an object, that day would fall out of the window before the object
// either succeeded or settled — never collected, never abandoned, and never reported.
func TestTheMarkDoesNotPassADayThatStillOwesAnObject(t *testing.T) {
	t.Parallel()

	prefix, d := yesterdaysPrefix(t)
	broken := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: broken, LastModified: time.Now().UTC(), Size: 10},
		}},
		unread: map[string]bool{broken: true},
	}

	cursor, err := runOnce(t, bucket, &recordingSink{}, nil)
	require.Error(t, err)

	state, err := loadState(cursor)
	require.NoError(t, err)
	_, ok := state.coveredThroughDay()
	assert.False(t, ok, "an incomplete day must leave the mark unset so the day stays in the window")
	assert.Equal(t, 1, state.FailureCounts[failureKey(d, broken)],
		"the count must survive the failed run, or it can never reach the threshold")
}

// An empty day is not a failure: before reporting was switched on, and on any day the
// vendor did not publish, the prefix is legitimately empty. The walk must treat it as
// covered and move on, or every organisation's pre-enablement history looks broken.
func TestAnEmptyDayIsCoveredRatherThanFailed(t *testing.T) {
	t.Parallel()

	_, d := yesterdaysPrefix(t)
	sink := &recordingSink{}

	cursor, err := runOnce(t, &fakeBucket{objects: map[string][]SourceObject{}}, sink, nil)
	require.NoError(t, err, "a day the vendor never wrote to is not an error")
	assert.Empty(t, sink.emitted)

	state, err := loadState(cursor)
	require.NoError(t, err)
	mark, ok := state.coveredThroughDay()
	require.True(t, ok)
	assert.Equal(t, d.Format(dayLayout), mark.Format(dayLayout))
}

// A day's objects must each be attempted, and this asserts it without depending on the
// order they come in. Go randomises map iteration, and the skew is fixed per process, so a
// test that needs the broken object to be drawn first is decided by the hash seed rather
// than by the code — it would pass or fail per run and guard nothing.
//
// Making BOTH objects unreadable removes the ordering from the question: a walk that stops
// at its first failure leaves the other one unfetched and uncounted whichever it drew
// first, so the assertion holds the invariant rather than a coin toss.
func TestEveryObjectInADayIsAttemptedWhateverOrderTheyCome(t *testing.T) {
	t.Parallel()

	prefix, d := yesterdaysPrefix(t)
	ide := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
	chat := prefix + "KIRO_CHAT_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: ide, LastModified: time.Now().UTC(), Size: 10},
			{Key: chat, LastModified: time.Now().UTC(), Size: 10},
		}},
		unread: map[string]bool{ide: true, chat: true},
	}

	cursor, err := runOnce(t, bucket, &recordingSink{}, nil)
	require.Error(t, err, "a day whose objects cannot be read must not report success")

	assert.Equal(t, 1, bucket.gets[ide], "every object in the day must be attempted")
	assert.Equal(t, 1, bucket.gets[chat], "every object in the day must be attempted")

	state, err := loadState(cursor)
	require.NoError(t, err)
	assert.Equal(t, 1, state.FailureCounts[failureKey(d, ide)],
		"an object that was never attempted can never accrue toward being settled")
	assert.Equal(t, 1, state.FailureCounts[failureKey(d, chat)],
		"an object that was never attempted can never accrue toward being settled")
}

// The threshold counts consecutive failures, and a day holding one persistently broken
// file is where that claim is easiest to break: the day never completes, so anything
// waiting on the day to forget a failure waits forever. A file recovering beside it would
// then carry a stale count through its own successes and be abandoned on its neighbour's
// evidence, having never failed the threshold number of times in a row.
func TestARecoveringObjectIsNotAbandonedOnItsNeighboursFailures(t *testing.T) {
	t.Parallel()

	prefix, d := yesterdaysPrefix(t)
	intermittent := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
	broken := prefix + "KIRO_CHAT_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: intermittent, LastModified: time.Now().UTC(), Size: 10},
			{Key: broken, LastModified: time.Now().UTC(), Size: 10},
		}},
		unread: map[string]bool{intermittent: true, broken: true},
	}
	sink := &recordingSink{}

	// It fails once, recovers, then fails again. Two failures either side of a success are
	// not two in a row.
	cursor, err := runOnce(t, bucket, sink, nil)
	require.Error(t, err)

	bucket.unread[intermittent] = false
	cursor, err = runOnce(t, bucket, sink, cursor)
	require.Error(t, err, "the persistently broken file still fails the run")
	require.Len(t, sink.emitted, 1, "the recovered file must be collected")

	state, err := loadState(cursor)
	require.NoError(t, err)
	assert.NotContains(t, state.FailureCounts, failureKey(d, intermittent),
		"a collected object must forget its own failures without waiting for the day")

	bucket.unread[intermittent] = true
	cursor, err = runOnce(t, bucket, sink, cursor)
	require.Error(t, err)

	state, err = loadState(cursor)
	require.NoError(t, err)
	assert.Equal(t, 1, state.FailureCounts[failureKey(d, intermittent)],
		"the count must have restarted at the success, so this is its first failure and not its second")

	// The neighbour, meanwhile, has failed every run and is accruing properly.
	assert.Equal(t, 3, state.FailureCounts[failureKey(d, broken)],
		"the persistently broken file accrues on every run")
	assert.False(t, state.isObjectTerminal(d, intermittent),
		"a file that keeps recovering must never be settled")
}

// ---- the three counters ---------------------------------------------------------------

func copiedCount(org, clientType string) float64 {
	return prom_testutil.ToFloat64(ReportsCopiedTotal.WithLabelValues(tenantlabel.OrgID(org), clientType))
}

func emptyDayCount(org string) float64 {
	return prom_testutil.ToFloat64(EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(org)))
}

func abandonedCount(org string) float64 {
	return prom_testutil.ToFloat64(ReportsAbandonedTotal.WithLabelValues(tenantlabel.OrgID(org)))
}

// Each counter exists because the run-level series cannot say what it says, so each is
// asserted to move on the case it was added for — a counter nobody has watched increment
// is indistinguishable from one wired to nothing.
//
// Not parallel: these read process-wide counters, and a sibling test collecting for the
// same organisation would make the deltas somebody else's.
func TestTheThreeCountersMoveOnTheirOwnCases(t *testing.T) {
	prefix, d := yesterdaysPrefix(t)

	t.Run("a copied report counts once, by client type", func(t *testing.T) {
		const org = "org-metrics-copied"
		key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
		before := copiedCount(org, "KIRO_IDE")

		bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
			{Key: key, LastModified: time.Now().UTC(), Size: 10},
		}}}
		_, err := runOnceFor(t, org, bucket, &recordingSink{}, nil)
		require.NoError(t, err)

		assert.Equal(t, before+1, copiedCount(org, "KIRO_IDE"),
			"the copied rate is the only liveness signal this connector has: every run succeeds whether or not anything arrived")
	})

	t.Run("a day the vendor did not publish counts as empty", func(t *testing.T) {
		const org = "org-metrics-empty"
		before := emptyDayCount(org)

		_, err := runOnceFor(t, org, &fakeBucket{objects: map[string][]SourceObject{}}, &recordingSink{}, nil)
		require.NoError(t, err)

		assert.Equal(t, before+1, emptyDayCount(org),
			"without this, a revoked grant and a quiet vendor are the same observation from outside")
	})

	t.Run("a settled report counts as abandoned", func(t *testing.T) {
		const org = "org-metrics-abandoned"
		key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
		before := abandonedCount(org)

		bucket := &fakeBucket{
			objects: map[string][]SourceObject{prefix: {{Key: key, LastModified: time.Now().UTC(), Size: 10}}},
			unread:  map[string]bool{key: true},
		}

		var cursor *sdk.Cursor
		for run := 0; run < maxConsecutiveFailures; run++ {
			var err error
			cursor, err = runOnceFor(t, org, bucket, &recordingSink{}, cursor)
			require.Error(t, err)
		}

		state, err := loadState(cursor)
		require.NoError(t, err)
		require.True(t, state.isObjectTerminal(d, key), "the threshold must have been reached")

		assert.Equal(t, before+1, abandonedCount(org),
			"collection continues past a settled report, so no run fails and this is the only record that one will never arrive")
	})
}

// ---- the repair path -------------------------------------------------------------------

// repairRange runs the repair for a day range, carrying the cursor in and out.
func repairRange(
	t *testing.T, orgID string, bucket *fakeBucket, sink *recordingSink, in *sdk.Cursor,
	from, to time.Time,
) (*CollectRangeResult, *sdk.Cursor, error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	s3mock.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	checkpointed := in
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: connectorID, OrganizationID: orgID, Settings: execSettings(t),
		},
		Cursor:        in,
		Sink:          sink,
		VendorGateway: bucket,
		BlobStore:     s3mock,
		Checkpoint: func(_ context.Context, c *sdk.Cursor) error {
			checkpointed = c
			return nil
		},
	}
	result, err := (&FetchUserActivityReportOp{}).CollectRange(context.Background(), params, from, to)
	return result, checkpointed, err
}

// The two properties that make this the answer to deepening history rather than a second
// cursor axis: it collects the days it was given, and it leaves the mark alone.
func TestTheRepairCollectsARangeWithoutMovingTheMark(t *testing.T) {
	t.Parallel()

	const org = "org-repair"
	end := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	from, to := end.AddDate(0, 0, -2), end

	objects := map[string][]SourceObject{}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		prefix := dayPrefix(testPrefix, testAccount, testRegion, d)
		objects[prefix] = []SourceObject{{
			Key:          prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv",
			LastModified: time.Now().UTC(), Size: 10,
		}}
	}
	bucket := &fakeBucket{objects: objects}

	// A mark already well ahead of the range, which is the situation a repair happens in:
	// the schedule has moved on and an old day needs filling.
	var ahead syncState
	ahead.raiseCoveredThrough(end)
	cursorIn, err := ahead.toCursor()
	require.NoError(t, err)

	sink := &recordingSink{}
	result, cursorOut, err := repairRange(t, org, bucket, sink, cursorIn, from, to)
	require.NoError(t, err)

	assert.Equal(t, 3, result.Days)
	assert.Equal(t, 3, result.Copied, "each day in the range is collected")
	assert.Len(t, sink.emitted, 3)

	after, err := loadState(cursorOut)
	require.NoError(t, err)
	mark, ok := after.coveredThroughDay()
	require.True(t, ok)
	assert.Equal(t, end.Format(dayLayout), mark.Format(dayLayout),
		"the mark says how far collection has REACHED; repairing an old day says nothing about that")
}

// Run twice, and the second pass must change nothing that accumulates. The destination key
// is deterministic and the record identity is the client type, so a repeat overwrites and
// updates rather than duplicating — which is what makes an interrupted repair safe to rerun.
func TestASecondRepairPassChangesNoCounts(t *testing.T) {
	t.Parallel()

	const org = "org-repair-idempotent"
	end := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	from, to := end.AddDate(0, 0, -2), end

	objects := map[string][]SourceObject{}
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		prefix := dayPrefix(testPrefix, testAccount, testRegion, d)
		objects[prefix] = []SourceObject{{
			Key:          prefix + "KIRO_CLI_" + testAccount + "_user_report_1717171717.csv",
			LastModified: time.Now().UTC(), Size: 10,
		}}
	}
	bucket := &fakeBucket{objects: objects}

	sink := &recordingSink{}
	first, cursor, err := repairRange(t, org, bucket, sink, nil, from, to)
	require.NoError(t, err)

	second, _, err := repairRange(t, org, bucket, sink, cursor, from, to)
	require.NoError(t, err)

	assert.Equal(t, first.Days, second.Days)
	assert.Equal(t, first.Copied, second.Copied, "the same days copy the same number of files")
	assert.Equal(t, 0, second.Failed)

	// The identities repeat rather than multiply, which is the property that makes a
	// re-run safe: six emissions across two passes, three distinct identities.
	require.Len(t, sink.emitted, 6)
	distinct := map[string]struct{}{}
	for _, e := range sink.emitted {
		distinct[e] = struct{}{}
	}
	assert.Len(t, distinct, 3, "a second pass updates the same records rather than adding new ones")
}

// The behaviour clearTerminal's doc explains, asserted end to end.
func TestTheRepairClearsASettledVerdictAndRetriesTheObject(t *testing.T) {
	t.Parallel()

	const org = "org-repair-settled"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	// Settled earlier, and the cause has since been fixed — so the bucket now reads.
	var settled syncState
	for i := 0; i < maxConsecutiveFailures; i++ {
		settled.recordFailure(day, key, "kms key we may not use")
	}
	require.True(t, settled.isObjectTerminal(day, key))
	cursorIn, err := settled.toCursor()
	require.NoError(t, err)

	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: key, LastModified: time.Now().UTC(), Size: 10},
	}}}
	sink := &recordingSink{}

	result, cursorOut, err := repairRange(t, org, bucket, sink, cursorIn, day, day)
	require.NoError(t, err)

	assert.Equal(t, 1, result.ClearedTerminal, "the verdict must be cleared before the day is attempted")
	assert.Equal(t, 1, result.Copied, "and the object must then actually be collected")
	assert.Zero(t, result.Skipped, "nothing may be skipped as settled once the verdict is cleared")

	after, err := loadState(cursorOut)
	require.NoError(t, err)
	assert.False(t, after.isObjectTerminal(day, key), "the repaired object is no longer settled")
}

// A repair against a cause nobody fixed must settle again rather than report success.
func TestARepairOfAStillBrokenObjectSettlesItAgain(t *testing.T) {
	t.Parallel()

	const org = "org-repair-still-broken"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {{Key: key, LastModified: time.Now().UTC(), Size: 10}}},
		unread:  map[string]bool{key: true},
	}

	abandonedBefore := prom_testutil.ToFloat64(
		ReportsAbandonedTotal.WithLabelValues(tenantlabel.OrgID(org)))

	var cursor *sdk.Cursor
	for pass := 0; pass < maxConsecutiveFailures; pass++ {
		var result *CollectRangeResult
		var err error
		result, cursor, err = repairRange(t, org, bucket, &recordingSink{}, cursor, day, day)
		require.NoError(t, err, "a day that lists but cannot be read is reported, not fatal")
		assert.Equal(t, 1, result.Failed)
	}

	after, err := loadState(cursor)
	require.NoError(t, err)
	assert.True(t, after.isObjectTerminal(day, key),
		"repairing against an unfixed cause must settle again rather than retry forever")

	// The settling must be visible in the metric, not only in the log, so
	// ReportsAbandonedTotal counts the settled object.
	assert.Equal(t, abandonedBefore+1,
		prom_testutil.ToFloat64(ReportsAbandonedTotal.WithLabelValues(tenantlabel.OrgID(org))),
		"a repair that settles an object must count it exactly once")
}

func TestARepairBanksEachDayRatherThanOnlyTheRange(t *testing.T) {
	t.Parallel()

	// The claim an operator acts on: interrupting a long repair costs the day in flight,
	// not the whole range. That is only true if the cursor is written per day, so a single
	// end-of-range checkpoint is what this test catches — it would still pass every
	// other repair test in this file.
	const org = "org-repair-per-day"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {{Key: key, LastModified: time.Now().UTC(), Size: 10}}},
	}

	ctrl := gomock.NewController(t)
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	s3mock.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	saves := 0
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: connectorID, OrganizationID: org, Settings: execSettings(t),
		},
		Sink:          &recordingSink{},
		VendorGateway: bucket,
		BlobStore:     s3mock,
		Checkpoint: func(_ context.Context, _ *sdk.Cursor) error {
			saves++
			return nil
		},
	}

	// Three days: one holding a report, two empty. Every one of them is a resume point.
	result, err := (&FetchUserActivityReportOp{}).CollectRange(
		context.Background(), params, day.AddDate(0, 0, -1), day.AddDate(0, 0, 1))
	require.NoError(t, err)
	require.Equal(t, 3, result.Days)
	assert.Equal(t, 3, saves, "the cursor must be banked once per day walked, not once per range")
}

func TestARepairBanksADayItCouldNotList(t *testing.T) {
	t.Parallel()

	// A day whose listing failed still had its terminal verdicts cleared before the
	// attempt. Skipping the checkpoint on that path would leave a re-run finding them
	// settled — the day would be silently skipped by the very repair meant to fix it.
	const org = "org-repair-unlistable-day"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	bucket := &fakeBucket{objects: map[string][]SourceObject{}, listErr: errors.New("access denied")}

	ctrl := gomock.NewController(t)
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	s3mock.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	saves := 0
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: connectorID, OrganizationID: org, Settings: execSettings(t),
		},
		Sink:          &recordingSink{},
		VendorGateway: bucket,
		BlobStore:     s3mock,
		Checkpoint: func(_ context.Context, _ *sdk.Cursor) error {
			saves++
			return nil
		},
	}

	// Two days, both unlistable. A single-day range would not discriminate: one
	// end-of-range checkpoint also reads as one save, so the count has to exceed one
	// before it can distinguish per-day banking from banking at the end.
	result, err := (&FetchUserActivityReportOp{}).CollectRange(
		context.Background(), params, day.AddDate(0, 0, -1), day)
	require.NoError(t, err, "an unlistable day is reported, not fatal")
	require.Equal(t, 2, result.Failed)
	assert.Equal(t, 2, saves, "a day that could not be listed must still bank its cleared verdicts")
}

func TestARepairOfAnEmptyRangeCountsTheDaysItWalked(t *testing.T) {
	t.Parallel()

	const org = "org-repair-empty-days"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	// Lists cleanly and holds nothing, which is what every pre-enablement day looks like.
	bucket := &fakeBucket{objects: map[string][]SourceObject{}}

	before := prom_testutil.ToFloat64(EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(org)))

	result, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day.AddDate(0, 0, 2))
	require.NoError(t, err)
	require.Equal(t, 3, result.Days)
	require.Zero(t, result.Copied)

	// The copied counter already fires from the shared day walk, so a repair's copies are
	// counted. An uncounted empty day would leave the two sides of the copied-versus-empty
	// ratio describing different populations.
	assert.Equal(t, before+3,
		prom_testutil.ToFloat64(EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(org))),
		"every empty day a repair walked must be counted")
}

func TestAnOversizedReportIsRefusedBeforeItIsFetched(t *testing.T) {
	t.Parallel()

	// The bucket belongs to the customer, so its object sizes are not ours to trust. The
	// listing already reports each size; spending the read first and discovering the size
	// afterwards is how one object in someone else's bucket takes the process down.
	const org = "org-oversized-report"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: key, LastModified: time.Now().UTC(), Size: MaxReportBytes + 1},
		}},
	}

	result, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err, "an oversized object is a settled failure, not a fatal one")
	assert.Equal(t, 1, result.Failed)
	assert.Zero(t, result.Copied)
	assert.Empty(t, bucket.gets, "it must never be fetched — the listing's size was enough")
}

func TestAReportAtTheCeilingIsStillCollected(t *testing.T) {
	t.Parallel()

	// The boundary in the direction that matters: a cap that refused a legitimate report
	// would lose data to protect memory, which is the wrong trade in this direction.
	const org = "org-report-at-ceiling"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {
			{Key: key, LastModified: time.Now().UTC(), Size: MaxReportBytes},
		}},
	}

	result, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Copied)
	assert.Zero(t, result.Failed)
}

func TestTwoObjectsRepublishedInTheSameSecondPickTheLaterOne(t *testing.T) {
	t.Parallel()

	// S3 reports LastModified to the second, so a republication inside one second is a
	// real tie — and an unbroken tie falls to listing order, which S3 gives
	// lexicographically by key. That is selection by filename, the ordering this design
	// rejects, arrived at silently.
	const org = "org-same-second"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	same := time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC)

	earlier := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T020000Z.csv"
	later := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T030000Z.csv"

	// Listed EARLIER-first, which is the order that discriminates: whichever object is
	// seen first becomes the incumbent, and an unbroken tie leaves it there. So without a
	// tie-break this listing keeps the earlier report — and with one it does not. Listed
	// the other way round the test passes either way and proves nothing, which is how it
	// was first written.
	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: earlier, LastModified: same, Size: 10},
		{Key: later, LastModified: same, Size: 10},
	}}}

	sink := &recordingSink{}
	result, _, err := repairRange(t, org, bucket, sink, nil, day, day)
	require.NoError(t, err)
	require.Equal(t, 1, result.Copied, "one client type on the day means one report")

	_, fetched := bucket.gets[later]
	assert.True(t, fetched, "the later of two same-second objects must be the one collected")
	assert.NotContains(t, bucket.gets, earlier, "the earlier one must not be fetched")
}

func TestAnUnreadableObjectIsNotReportedAsAConfigurationFault(t *testing.T) {
	t.Parallel()

	// The class decides what an operator is told. A configuration fault says the stored
	// settings no longer match the vendor — true of a revoked grant, false of one report
	// encrypted with a key we may not use. An object that settles has its own counter; one
	// still accruing warns on every attempt.
	const org = "org-unreadable-not-config"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)
	key := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{prefix: {{Key: key, LastModified: time.Now().UTC(), Size: 10}}},
		unread:  map[string]bool{key: true},
	}

	_, err := runOnceFor(t, org, bucket, &recordingSink{}, nil)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) {
		assert.NotEqual(t, sdk.ErrKindConfigInvalid, syncErr.Kind,
			"an object we cannot read is not the configuration's fault")
	}
}

func TestABucketWeCannotReachIsStillAConfigurationFault(t *testing.T) {
	t.Parallel()

	// The other side of the same line, so neither can be changed without the other being
	// considered: a revoked grant IS the configuration's fault and must keep that class.
	const org = "org-bucket-is-config"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)

	bucket := &fakeBucket{
		objects: map[string][]SourceObject{},
		listErr: fmt.Errorf("listing %s: %w", prefix, ErrBucketUnreachable),
	}

	_, err := runOnceFor(t, org, bucket, &recordingSink{}, nil)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "a bucket-level failure must be a classified sync error")
	assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
}

// --- the guards the testing strategy promises ---

func runOnceWithCtxSettings(
	t *testing.T, ctx context.Context, orgID string, bucket *fakeBucket,
	sink *recordingSink, in *sdk.Cursor, settings json.RawMessage,
) (*sdk.Cursor, error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	s3mock := sdkmocks.NewMockBlobStore(ctrl)
	s3mock.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	checkpointed := in
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID: connectorID, OrganizationID: orgID, Settings: settings,
		},
		Cursor:        in,
		Sink:          sink,
		VendorGateway: bucket,
		BlobStore:     s3mock,
		Checkpoint: func(_ context.Context, c *sdk.Cursor) error {
			checkpointed = c
			return nil
		},
	}
	_, err := (&FetchUserActivityReportOp{}).Execute(ctx, params)
	return checkpointed, err
}

func TestTheLaterObjectWinsWhenTheTimestampsDiffer(t *testing.T) {
	t.Parallel()

	// The primary ordering: newest last-modified wins, whatever the vendor named the files.
	const org = "org-later-wins"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)

	stale := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T010000Z.csv"
	fresh := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T020000Z.csv"

	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: fresh, LastModified: time.Date(2026, 6, 2, 4, 0, 0, 0, time.UTC), Size: 10},
		{Key: stale, LastModified: time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC), Size: 10},
	}}}

	_, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err)
	assert.Contains(t, bucket.gets, fresh, "the newest last-modified object must win")
	assert.NotContains(t, bucket.gets, stale)
}

func TestFilenameOrderingCannotWinOverTheTimestamp(t *testing.T) {
	t.Parallel()

	// The companion the strategy names: the STALE object is given the lexicographically
	// greater key, so a refactor that quietly reverts to filename ordering fails here
	// rather than passing. Without this pair the rule is only asserted where the two
	// orderings agree.
	const org = "org-filename-loses"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)

	// Greater key, older object.
	staleGreaterKey := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T090000Z.csv"
	// Lesser key, newer object.
	freshLesserKey := prefix + "KIRO_IDE_" + testAccount + "_user_report_20260602T010000Z.csv"

	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: staleGreaterKey, LastModified: time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC), Size: 10},
		{Key: freshLesserKey, LastModified: time.Date(2026, 6, 2, 5, 0, 0, 0, time.UTC), Size: 10},
	}}}

	_, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err)
	assert.Contains(t, bucket.gets, freshLesserKey,
		"the newer object must win even though its key sorts lower")
	assert.NotContains(t, bucket.gets, staleGreaterKey,
		"selection by filename is the ordering this design rejects")
}

// execSettingsWindow is execSettings with a first-run window wide enough that a walk has
// several days to be interrupted between.
func execSettingsWindow(t *testing.T, days int) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(Settings{
		Bucket: "acme-kiro-reports", Prefix: testPrefix,
		Region: testRegion, AccountID: testAccount,
		FirstRunWindowDays: days,
	})
	require.NoError(t, err)
	return b
}

func TestARunCancelledPartwayBanksTheDaysItFinished(t *testing.T) {
	t.Parallel()

	// The property first-run convergence rests on: a first run over a long history will be
	// interrupted, and if that discarded its progress the organisation would never
	// converge — every attempt would start from the same place and be cut off at the same
	// place. The mark must sit at the last completed day, not at the start.
	const org = "org-cancelled-partway"
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	// Five days, each holding one report, walked oldest first.
	objects := map[string][]SourceObject{}
	days := make([]time.Time, 0, 5)
	for i := 5; i >= 1; i-- {
		d := yesterday.AddDate(0, 0, -i+1)
		days = append(days, d)
		p := dayPrefix(testPrefix, testAccount, testRegion, d)
		objects[p] = []SourceObject{{
			Key:          p + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv",
			LastModified: time.Now().UTC(), Size: 10,
		}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	bucket := &fakeBucket{objects: objects}
	// Cancelled once two days have been listed, so the third is the one interrupted.
	bucket.onList = func(served int) {
		if served == 2 {
			cancel()
		}
	}

	cursor, err := runOnceWithCtxSettings(t, ctx, org, bucket, &recordingSink{}, nil,
		execSettingsWindow(t, 5))
	require.Error(t, err, "a cancelled run reports the interruption")
	require.NotNil(t, cursor, "and still banks what it finished")

	state, lerr := loadState(cursor)
	require.NoError(t, lerr)
	assert.Equal(t, days[1].Format(dayLayout), state.CoveredThrough,
		"the mark must sit at the second day — the last one completed — not at the start")
	cancel()
}

func TestTheCursorDoesNotReconcileTwoConcurrentRuns(t *testing.T) {
	t.Parallel()

	// An explicit limit, asserted so nobody builds on a reconciliation that is not there.
	//
	// The framework's cursor save replaces the whole blob, and the only field carried
	// across is the high-water mark — which the repair path does not own. Failure counts
	// and settled verdicts are NOT merged: two runs starting from one cursor will have the
	// later save discard the earlier's bookkeeping.
	//
	// That is why the overlap is refused at the trigger rather than reconciled here, and
	// why the design accepts the residual: a trigger check is not a distributed lock, and
	// an interleaving heals on the next scheduled run. This test exists to fail if someone
	// later assumes the cursor reconciles, and to be the place that says it does not.
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	shared := &syncState{CoveredThrough: day.AddDate(0, 0, -2).Format(dayLayout)}
	start, err := shared.toCursor()
	require.NoError(t, err)

	// Run A accrues a failure against one object.
	runA, err := loadState(start)
	require.NoError(t, err)
	runA.recordFailure(day, "obj-a", "unreadable")
	savedA, err := runA.toCursor()
	require.NoError(t, err)

	// Run B starts from the same cursor, knows nothing of A, and advances the mark.
	runB, err := loadState(start)
	require.NoError(t, err)
	runB.raiseCoveredThrough(day)

	merged, err := PreserveHighWaterMark(context.Background(), savedA, mustCursor(t, runB))
	require.NoError(t, err)
	final, err := loadState(merged)
	require.NoError(t, err)

	// The mark that A held is carried across — that much IS protected.
	assert.Equal(t, runA.CoveredThrough, final.CoveredThrough,
		"the mark belongs to whoever last advanced it, not to the writer")

	// A's count is gone, and that is the documented limit rather than a defect to fix
	// here. If this ever starts passing, the cursor has gained a merge and the trigger
	// refusal is no longer the only thing standing between two runs — which is worth
	// knowing, so it should fail loudly rather than quietly become true.
	assert.Zero(t, final.FailureCounts[failureKey(day, "obj-a")],
		"the cursor does not reconcile: the later save replaces the blob, refusal is the mechanism")
}

func mustCursor(t *testing.T, s syncState) *sdk.Cursor {
	t.Helper()
	c, err := s.toCursor()
	require.NoError(t, err)
	return c
}

func TestAnObjectWeCannotPlaceIsCountedRatherThanOnlyLogged(t *testing.T) {
	t.Parallel()

	// The second way a report goes missing, and until this counter the only one with no
	// signal. An object whose key does not match the layout cannot be filed under a day,
	// so it is neither collected nor settled — and the day completes regardless, because
	// holding it open would pin the organisation on any stray vendor file. That trade is
	// deliberate; being unable to see it was not.
	const org = "org-unplaceable"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)

	good := prefix + "KIRO_IDE_" + testAccount + "_user_report_1717171717.csv"
	odd := prefix + "manifest.json"

	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: good, LastModified: time.Now().UTC(), Size: 10},
		{Key: odd, LastModified: time.Now().UTC(), Size: 10},
	}}}

	before := prom_testutil.ToFloat64(
		ReportsUnplaceableTotal.WithLabelValues(tenantlabel.OrgID(org)))

	result, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Copied, "the placeable report is still collected")

	assert.Equal(t, before+1,
		prom_testutil.ToFloat64(ReportsUnplaceableTotal.WithLabelValues(tenantlabel.OrgID(org))),
		"an object we cannot place must reach a counter, not only a log line")
}

func TestADayOfOnlyUnplaceableObjectsIsNotAnEmptyDay(t *testing.T) {
	t.Parallel()

	// It held files. Counting it empty would have the copied-versus-empty comparison read
	// a vendor whose layout we cannot parse as a vendor that published nothing — which is
	// the one conclusion that would stop anyone looking.
	const org = "org-unplaceable-only"
	day := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	prefix := dayPrefix(testPrefix, testAccount, testRegion, day)

	bucket := &fakeBucket{objects: map[string][]SourceObject{prefix: {
		{Key: prefix + "manifest.json", LastModified: time.Now().UTC(), Size: 10},
	}}}

	emptyBefore := prom_testutil.ToFloat64(EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(org)))

	_, _, err := repairRange(t, org, bucket, &recordingSink{}, nil, day, day)
	require.NoError(t, err)

	assert.Equal(t, emptyBefore,
		prom_testutil.ToFloat64(EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(org))),
		"a day holding files we could not place is not an empty day")
}

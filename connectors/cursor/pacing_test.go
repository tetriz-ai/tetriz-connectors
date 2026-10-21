// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	vgcursorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// localRateLimit is the error the gateway raises when OUR OWN limiter has no
// capacity left in the window — a *SyncError whose cause wraps the sentinel and
// whose RetryAfterSeconds sizes the wait. Built here in the shape the gateway
// actually produces, so these tests fail if that shape changes rather than passing
// against a hand-rolled stand-in.
func localRateLimit(retryAfter int) error {
	return &sdk.SyncError{
		Kind:              sdk.ErrKindRateLimit,
		Cause:             fmt.Errorf("rate limit exceeded for group cursor-usage: %w", sdk.ErrLocalRateLimit),
		Retryable:         true,
		RetryAfterSeconds: retryAfter,
	}
}

// throttlingGateway rejects the first rejectFirst calls with our limiter's error,
// then serves pages normally. It records how long the caller waited between calls,
// which is what separates "paced through it" from "got lucky".
type throttlingGateway struct {
	rejectFirst int
	pages       [][]json.RawMessage
	retryAfter  int

	calls    int
	served   int
	lastCall time.Time
	gaps     []time.Duration
}

func (g *throttlingGateway) Execute(_ context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest); !ok {
		return nil, fmt.Errorf("unexpected request %T", req)
	}
	now := time.Now()
	if !g.lastCall.IsZero() {
		g.gaps = append(g.gaps, now.Sub(g.lastCall))
	}
	g.lastCall = now

	g.calls++
	if g.calls <= g.rejectFirst {
		return nil, localRateLimit(g.retryAfter)
	}
	idx := g.served
	g.served++
	if idx >= len(g.pages) {
		return &vgcursorpb.CursorFilteredUsageEventsResponse{}, nil
	}
	out := make([][]byte, 0, len(g.pages[idx]))
	for _, e := range g.pages[idx] {
		out = append(out, e)
	}
	return &vgcursorpb.CursorFilteredUsageEventsResponse{UsageEvents: out}, nil
}

func (g *throttlingGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *throttlingGateway) ForGroup(string) sdk.VendorGateway { return g }

// The operation speaks proto through Execute and never raw HTTP; these exist to
// satisfy the interface and fail loudly if that ever stops being true.
func (g *throttlingGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("the operation must not make raw HTTP calls")
}

func (g *throttlingGateway) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("the operation must not make raw HTTP calls")
}

// TestExecute_PacesThroughOurOwnRateLimit guards the pacing fix.
//
// Before it, the FIRST rejection from our own limiter failed the whole run. A chunk
// needing more requests than one window allows could therefore never finish — and
// because nothing is checkpointed until a chunk completes, the position never moved
// past it and every future run repeated the same failure. Waiting turns that hard
// ceiling into a soft one: the run has thirty minutes and the window is a minute, so
// the deadline is the real budget, not the window.
func TestExecute_PacesThroughOurOwnRateLimit(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	gw := &throttlingGateway{
		rejectFirst: 1,
		retryAfter:  1, // a real window is a minute; one second keeps the test honest and quick
		pages:       [][]json.RawMessage{{event("conv-a", "dev@example.com", at, "")}},
	}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	next, err := (&FetchUsageEventsOp{}).Execute(ctx, newParams(t, gw, sink, cur))
	require.NoError(t, err, "a rejection from our own limiter must be waited out, not failed on")

	assert.Len(t, sink.Records(), 1, "the page behind the rejection must still be fetched and stored")
	assert.Equal(t, 2, gw.calls, "the rejected page is retried, not skipped")
	assert.NotNil(t, next, "the run completed, so the position must advance")
	require.NotEmpty(t, gw.gaps)
	assert.GreaterOrEqual(t, gw.gaps[0], time.Second,
		"the retry must come after the window's wait; a shorter gap means it never paced")
}

// TestExecute_NoDeadlineDoesNotWait keeps the pacing bounded by something real. The
// wait is only affordable against a known budget; with no deadline there is nothing
// stopping an unattended run sitting for hours, so it fails fast instead.
func TestExecute_NoDeadlineDoesNotWait(t *testing.T) {
	t.Parallel()

	gw := &throttlingGateway{rejectFirst: 1, retryAfter: 60}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	start := time.Now()
	next, err := (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))

	require.Error(t, err, "no chunk was covered, so this cannot report success")
	assert.Nil(t, next, "nothing was covered; advancing the position would record coverage never achieved")
	assert.Less(t, time.Since(start), 30*time.Second, "it must fail fast rather than sleep out the window")
	assert.Equal(t, 1, gw.calls, "without a deadline there is no budget to pace against, so no retry")
}

// TestExecute_NoProgressAtAllFails is the guard against trading a loud stall for a
// silent one. Yielding is only defensible once something was covered: if the very
// first chunk cannot be covered even with the whole run spent pacing, the floor can
// never move and every later run repeats it. Reporting success there would make an
// unfixable stall read as a healthy sync.
func TestExecute_NoProgressAtAllFails(t *testing.T) {
	t.Parallel()

	// Rejects forever, and the deadline is far too short to afford even one wait.
	gw := &throttlingGateway{rejectFirst: 1000, retryAfter: 60}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	next, err := (&FetchUsageEventsOp{}).Execute(ctx, newParams(t, gw, sink, cur))

	require.Error(t, err, "no progress with the budget gone is not a clean stop")
	assert.Nil(t, next)
	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr, "the orchestrator classifies on SyncError")
	// rate_limit, not vendor_error, and the sentinel must survive: together they are
	// what routes this to the self_throttled status. As vendor_error it would be
	// counted as an unreadable vendor response.
	assert.Equal(t, sdk.ErrKindRateLimit, syncErr.Kind)
	// Terminal maps to asynq.SkipRetry. Without it the scheduler's default backoff
	// retries three times inside a minute, each meeting the same budget wall — the
	// re-hammering the retry mechanism exists to prevent, on the one path that can
	// least afford it.
	assert.True(t, syncErr.Terminal,
		"a chunk that outruns a whole run's budget cannot be fixed by an immediate retry")
	require.ErrorIs(t, err, sdk.ErrLocalRateLimit,
		"the orchestrator reaches self_throttled only through this marker")
	assert.Empty(t, sink.Records())
}

// TestCanWaitWithin pins the arithmetic the yield decision rests on. The margin is
// not decoration: a wait that fits the deadline "exactly" returns to a dead context
// and loses the chunk it just spent a window paying for.
func TestCanWaitWithin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deadline   time.Duration
		noDeadline bool
		wait       time.Duration
		want       bool
	}{
		{name: "no deadline never waits", noDeadline: true, wait: time.Minute, want: false},
		{name: "ample budget waits", deadline: 30 * time.Minute, wait: time.Minute, want: true},
		{name: "wait plus margin exactly at the deadline does not", deadline: time.Minute + pacingSafetyMargin, wait: time.Minute, want: false},
		{name: "wait alone fits but the margin does not", deadline: 70 * time.Second, wait: time.Minute, want: false},
		{name: "past the deadline never waits", deadline: -time.Second, wait: time.Minute, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if !tt.noDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(tt.deadline))
				defer cancel()
			}
			assert.Equal(t, tt.want, canWaitWithin(ctx, tt.wait))
		})
	}
}

// TestLocalLimiterWait takes the wait from the gateway's own hint, which already
// carries jitter — that jitter is what stops several organisations rejected by one
// window from all returning in the same instant.
func TestLocalLimiterWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want time.Duration
	}{
		{name: "the gateway's sized hint is used", err: localRateLimit(54), want: 54 * time.Second},
		{name: "no hint falls back to the endpoint's window", err: localRateLimit(0), want: time.Minute},
		{name: "a non-SyncError falls back too", err: errors.New("plain error"), want: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, localLimiterWait(tt.err))
		})
	}
}

// deadlineEaterGateway serves the first chunk normally, then stalls until the run's
// deadline passes — a vendor slow enough that the pages after a wait spend what the
// margin left, which is the shape that arrives as a context error from inside the
// walk rather than as the pacing sentinel.
type deadlineEaterGateway struct {
	serveFirst int
	page       []json.RawMessage
	calls      int
}

func (g *deadlineEaterGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest); !ok {
		return nil, fmt.Errorf("unexpected request %T", req)
	}
	g.calls++
	if g.calls <= g.serveFirst {
		out := make([][]byte, 0, len(g.page))
		for _, e := range g.page {
			out = append(out, e)
		}
		return &vgcursorpb.CursorFilteredUsageEventsResponse{UsageEvents: out}, nil
	}
	<-ctx.Done()
	return nil, fmt.Errorf("cursor usage: vendor call outlived the run: %w", ctx.Err())
}

func (g *deadlineEaterGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *deadlineEaterGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *deadlineEaterGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("the operation must not make raw HTTP calls")
}

func (g *deadlineEaterGateway) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("the operation must not make raw HTTP calls")
}

// TestExecute_DeadlineAfterProgressYieldsCleanly covers the way a paced run most
// often ends, which is NOT the pacing sentinel.
//
// A wait is only taken when it fits inside the deadline plus a margin, so the walk
// resumes holding little more than that margin. Slow vendor pages then spend it
// mid-request, and the walk returns a wrapped context error rather than the
// sentinel. Chunks already covered are checkpointed, so failing the run there buys
// nothing and costs a spurious failed run plus its retries, for a backfill that is
// working exactly as designed.
func TestExecute_DeadlineAfterProgressYieldsCleanly(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	gw := &deadlineEaterGateway{
		serveFirst: 1, // the first chunk completes and checkpoints; the next one stalls
		page:       []json.RawMessage{event("conv-a", "dev@example.com", at, "")},
	}
	sink := testkit.NewChannelSink()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// No stored position, so this is a first run: a lookback window sliced into
	// several chunks, which is what lets one complete before the deadline goes.
	next, err := (&FetchUsageEventsOp{}).Execute(ctx, newParams(t, gw, sink, nil))

	require.NoError(t, err,
		"progress was made and checkpointed; a failed run here is a false alarm, not a fault")
	require.NotNil(t, next, "the covered chunk must be recorded so the next run resumes past it")
	assert.Len(t, sink.Records(), 1, "the chunk that completed before the deadline must still land")
}

// slowFirstChunkGateway serves one chunk, then sleeps past the run's deadline while
// serving it — so the context is already dead when the walk reaches the NEXT chunk
// and tests it before making any request. That is a different exit point from a
// deadline dying inside a vendor call, and it is where the margin most often lands:
// the margin is sized to finish the chunk being resumed, not to leave slack for
// starting another.
type slowFirstChunkGateway struct {
	page  []json.RawMessage
	sleep time.Duration
	calls int
}

func (g *slowFirstChunkGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	if _, ok := req.(*vgcursorpb.CursorFilteredUsageEventsRequest); !ok {
		return nil, fmt.Errorf("unexpected request %T", req)
	}
	g.calls++
	out := make([][]byte, 0, len(g.page))
	for _, e := range g.page {
		out = append(out, e)
	}
	resp := &vgcursorpb.CursorFilteredUsageEventsResponse{UsageEvents: out}
	if g.calls == 1 {
		// Serve the chunk, then burn the rest of the deadline before returning, so the
		// walk checkpoints this chunk and finds the context gone at the next one.
		timer := time.NewTimer(g.sleep)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	return resp, nil
}

func (g *slowFirstChunkGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway {
	return g
}
func (g *slowFirstChunkGateway) ForGroup(string) sdk.VendorGateway { return g }
func (g *slowFirstChunkGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errors.New("the operation must not make raw HTTP calls")
}

func (g *slowFirstChunkGateway) DoJSON(context.Context, string, string, any, any) error {
	return errors.New("the operation must not make raw HTTP calls")
}

// TestExecute_DeadlineBetweenChunksYieldsCleanly guards the exit point the walk hits
// most often, where a context check could swallow the yield.
//
// A deadline can expire between one chunk's checkpoint and the next chunk's first
// request just as easily as inside a vendor call — more easily, since the wait margin
// is sized to finish the chunk being resumed rather than to start another. Both mean
// the same thing and must reach the same decision; a check that returned before the
// walk could classify the error turned a working backfill into a failed run.
func TestExecute_DeadlineBetweenChunksYieldsCleanly(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	gw := &slowFirstChunkGateway{
		page:  []json.RawMessage{event("conv-a", "dev@example.com", at, "")},
		sleep: 3 * time.Second, // outlives the deadline below
	}
	sink := testkit.NewChannelSink()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	next, err := (&FetchUsageEventsOp{}).Execute(ctx, newParams(t, gw, sink, nil))

	require.NoError(t, err, "the first chunk was covered and checkpointed; failing here is a false alarm")
	require.NotNil(t, next, "the covered chunk must be recorded so the next run resumes past it")
	assert.Len(t, sink.Records(), 1)
}

// TestCanAffordWait pins the two independent limits on a pacing wait. The deadline
// asks whether anything useful can happen afterwards; the per-run cap asks whether
// the shared worker slot has been held long enough. A busy organisation reaches the
// cap first — its deadline is ample, and the slot is what costs everyone else.
func TestCanAffordWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		deadline time.Duration
		wait     time.Duration
		spent    time.Duration
		want     bool
	}{
		{name: "fresh run with ample deadline waits", deadline: 30 * time.Minute, wait: time.Minute, spent: 0, want: true},
		{name: "under the cap still waits", deadline: 30 * time.Minute, wait: time.Minute, spent: 3 * time.Minute, want: true},
		{name: "landing exactly on the cap is allowed", deadline: 30 * time.Minute, wait: time.Minute, spent: maxPacingWaitPerRun - time.Minute, want: true},
		{name: "one second past the cap is refused", deadline: 30 * time.Minute, wait: time.Minute, spent: maxPacingWaitPerRun - time.Minute + time.Second, want: false},
		{name: "cap refuses even with the whole deadline free", deadline: 30 * time.Minute, wait: time.Minute, spent: maxPacingWaitPerRun, want: false},
		{name: "deadline refuses even with the cap untouched", deadline: 70 * time.Second, wait: time.Minute, spent: 0, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(tt.deadline))
			defer cancel()
			got, _ := canAffordWait(ctx, tt.wait, tt.spent)
			assert.Equal(t, tt.want, got)
		})
	}
}

// failingSink refuses records after the first acceptFirst of them. The shared
// ChannelSink cannot fail at all, so without this the sink-outage path has no way to
// be exercised.
//
// Accepting some first is what makes it discriminating: with NO progress both the
// excluded and unexcluded paths return an error, so only a run that already covered a
// chunk can show the difference between failing and reporting success.
type failingSink struct {
	err         error
	acceptFirst int
	accepted    int
}

func (s *failingSink) Emit(context.Context, *sdk.RawRecord) error {
	if s.accepted < s.acceptFirst {
		s.accepted++
		return nil
	}
	return s.err
}

func (s *failingSink) EmitBatch(context.Context, []*sdk.RawRecord) error {
	return s.err
}
func (s *failingSink) Flush(context.Context) error { return nil }

// TestExecute_SinkFailureIsNotAPacingYield guards the distinction the pacing branch
// depends on and could not otherwise assert.
//
// The sink shares the run's context, so a queue path degrading under a near-expired
// deadline surfaces as context.DeadlineExceeded — the same error the walk produces
// when it runs out of time to pace. Treating them alike would launder a sink outage
// into a healthy short run: the operation would report success, the position would
// advance over chunks whose records never reached the queue, and nothing would say so.
func TestExecute_SinkFailureIsNotAPacingYield(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		err  error
	}{
		{name: "a plain sink error fails the run", err: errors.New("sqs unavailable")},
		{
			// The case the branch exists for: indistinguishable from running out of
			// pacing budget unless the emit path is marked.
			name: "a sink error that IS a deadline still fails the run",
			err:  fmt.Errorf("publishing: %w", context.DeadlineExceeded),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			// Per subtest: the gateway serves its pages once, so a shared one would
			// leave the second case with an empty response, nothing to emit, and a
			// clean run that passes for entirely the wrong reason.
			gw := &throttlingGateway{
				pages: [][]json.RawMessage{
					{event("conv-a", "dev@example.com", at, "")},
					{event("conv-b", "dev@example.com", at.AddDate(0, 0, -4), "")},
				},
			}
			// One record lands, so a chunk completes and is checkpointed. Only then
			// does the sink start refusing — the state where treating a sink failure
			// as a pacing yield would report SUCCESS for a run whose later records
			// never reached the queue.
			// No stored position, so the window is the first-run lookback and slices
			// into many chunks — the only shape where one can complete before the
			// sink starts refusing.
			next, execErr := (&FetchUsageEventsOp{}).Execute(
				ctx, newParams(t, gw, &failingSink{err: tt.err, acceptFirst: 1}, nil))

			require.Error(t, execErr, "a sink that refuses records must never read as a clean yield")
			assert.Nil(t, next,
				"advancing the position would record coverage for records the queue never took")
			assert.ErrorIs(t, execErr, errEmitFailed,
				"the emit path must stay identifiable, or the deadline branch swallows it")
		})
	}
}

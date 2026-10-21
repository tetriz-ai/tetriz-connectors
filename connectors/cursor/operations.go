// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgcursorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// opFetchUsageEvents reads the vendor's billed-events endpoint.
const opFetchUsageEvents sdk.OperationID = "fetch_usage_events"

// usagePageSize is the vendor maximum. The endpoint carries a tighter per-minute
// budget than the rest of the Admin API, so the fewest calls that cover a window is
// the cheapest way through it.
const usagePageSize = 1000

// maxPagesPerChunk bounds the walk. Nothing in the response can be trusted to
// terminate a loop — the pagination fields are named differently than documented —
// so the walk stops on a short page and this only catches a vendor that never returns
// one, rather than spinning until the deadline.
const maxPagesPerChunk = 500

// errRunOutOfBudget marks a wait that was refused because it would not fit inside
// the run's remaining deadline (see canWaitWithin). The chunks already covered stay
// checkpointed.
var errRunOutOfBudget = errors.New("cursor usage: run deadline reached mid-window")

// errEmitFailed marks a failure to hand a record to the sink, so it cannot be
// mistaken for the walk running out of time. Both can surface as a context
// deadline — the sink call is bound to the same context — but they are different
// incidents: one is this operation pacing itself, the other is the queue path
// degrading. Without the distinction a sink outage reads as a healthy short run.
var errEmitFailed = errors.New("cursor usage: handing a record to the sink failed")

// pacingSafetyMargin is held back from the deadline so a wait never consumes the
// time the walk needs to finish the page it resumes, emit its records and
// checkpoint. Without it a wait that fits "exactly" returns to a dead context and
// loses the chunk it just paid a minute for.
const pacingSafetyMargin = 90 * time.Second

// maxPacingWaitPerRun bounds how long ONE run may spend asleep waiting for its own
// rate-limit window, and exists for the scheduler rather than for this connector.
//
// A paced run holds a slot in the worker pool that every connector and every
// organisation shares, and this codebase already moves long-running work OFF that
// pool onto dedicated lanes for exactly that reason. Spending the whole deadline
// asleep would hold one slot for close to half an hour while doing nothing — cheap
// while a single organisation syncs, and a way to delay unrelated connectors once
// several onboard, which is the moment pacing is needed most.
//
// Reaching the cap is not a failure. The chunks already covered are checkpointed,
// the walk stops, and the next run resumes — a large backfill lands over several
// runs instead of one long one.
const maxPacingWaitPerRun = 5 * time.Minute

// FetchUsageEventsOp stores one record per billed model call.
type FetchUsageEventsOp struct{}

func (o *FetchUsageEventsOp) OperationID() sdk.OperationID { return opFetchUsageEvents }

// usageEvent is the three fields lifted out of a vendor event, and only three, each
// for routing rather than interpretation.
//
// Nothing else is parsed, and that is deliberate rather than lazy: several fields
// actively punish eager parsing. One identifier arrives as the literal text "null"
// rather than an absent value, one label reads as a human-facing phrase rather than a
// stable code, the model field mixes internal identifiers with display names, and the
// token breakdown is absent on a small fraction of events. Storing the payload
// untouched means none of those is an ingestion failure.
type usageEvent struct {
	// Timestamp is epoch milliseconds encoded as a string. The vendor's own
	// documentation says it is a number; it is not.
	Timestamp string `json:"timestamp"`
	// ConversationID is what the deferred attribution work joins on, and half of the
	// record's identity.
	ConversationID string `json:"conversationId"`
	// UserEmail is the fallback the same attribution work uses when a conversation
	// cannot be matched.
	UserEmail string `json:"userEmail"`
}

// Execute walks the window's chunks, emitting one record per billed event.
func (o *FetchUsageEventsOp) Execute(
	ctx context.Context, params *sdk.ExecutionParams,
) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID
	_, settings, err := ParseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    o.OperationID(),
			OrganizationID: orgID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          fmt.Errorf("parsing config: %w", err),
		}
	}

	pos, present, err := parseUsagePosition(params.Cursor)
	if err != nil {
		// An unreadable position is treated as absent rather than fatal: re-reading the
		// lookback window is safe (the write is an update in place) where refusing to
		// run leaves the organisation stalled with no way out but an operator.
		logger.WarnWithCtx(ctx, "cursor usage: unreadable sync position, restarting from the lookback window",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		pos, present = resolvedPosition{}, false
	}

	window := resolveUsageWindow(time.Now(), pos, present, settings.GetLookbackDays(), settings.GetTrailingOverlapDays())
	// One line per run, carrying the reason rather than a pair of booleans. Two of the
	// shapes are otherwise indistinguishable from a healthy sync — a resume reads as a
	// first run, and a floor left behind by a pause reads as an ordinary trailing read —
	// and both advance the floor, so neither shows up as a stalled cursor either.
	logger.Info(ctx, "cursor usage: resolved window",
		zap.String(logger.OrgID, orgID),
		zap.String("window_reason", string(window.Reason)),
		zap.Time("window_start", window.Start),
		zap.Time("window_end", window.End))

	gw := params.VendorGateway.ForGroup(constants.GroupCursorUsage)

	// The identity set is scoped to the RUN, never to a page or a chunk. A first sync
	// flushes progressively, so a per-chunk set would let a duplicate through at a
	// flush boundary — which is the case this check exists for.
	seen := make(map[string]struct{})
	// One warn line per distinct shape, not per event. A vendor field rename makes
	// every event unreadable, and at a thousand events a page that is a log flood
	// precisely when the connector is already broken — every line after the first
	// saying the same thing.
	loggedShapes := make(map[string]struct{})

	// The wait budget is the RUN's, not the chunk's: a run that paced through an early
	// chunk has less to spend on a later one, which is what keeps the slot-occupancy
	// bound a per-run promise rather than a per-chunk one.
	var paceSpent time.Duration

	var lastCovered usageWindow
	var yielded bool
	// Empty unless a wait was refused; a deadline landing outside the pacing path
	// leaves it so, which the yield branch reads as an ordinary deadline.
	var yieldReason string

	// The days an earlier run failed, read first when the window does not reach them, and
	// the days that failed in THIS run, for the error the run returns.
	// Listed days older than the lookback can no longer be read: dropped, each logged.
	failedDays := keepRecentDays(ctx, orgID, pos.FailedDays, startOfUTCDay(time.Now()).AddDate(0, 0, -settings.GetLookbackDays()))
	retries := retryChunks(failedDays, window)
	var failedThisRun []string
	var firstFailure error
	// position is what a checkpoint stores now: the stored one until a window chunk is
	// covered, with the failed days as they stand.
	position := func() usagePosition {
		var p usagePosition
		if lastCovered.End.IsZero() {
			p = pos.stored()
		} else {
			p = coveredPosition(lastCovered, window)
		}
		p.FailedDays = failedDays
		return p
	}
	checkpoint := func() error {
		// Checkpoint is nil outside the orchestrator.
		if params.Checkpoint == nil {
			return nil
		}
		cur, mErr := newUsagePositionCursor(position())
		if mErr != nil {
			return fmt.Errorf("cursor usage: %w", mErr)
		}
		if cpErr := params.Checkpoint(ctx, cur); cpErr != nil {
			logger.WarnWithCtx(ctx, "cursor usage: checkpoint failed, continuing",
				zap.String(logger.OrgID, orgID), zap.Error(cpErr))
		}
		return nil
	}

	for i, chunk := range append(retries, window.chunks()...) {
		retrying := i < len(retries)
		// No context check here on purpose. fetchChunk tests the context before its
		// first request, so an expired deadline still stops the walk immediately — but
		// it arrives as an error the branch below can classify. Checking here as well
		// returned unconditionally and skipped that classification, which mattered
		// because this is a LIKELY place to land, not an edge: the wait margin is sized
		// to finish the chunk being resumed, so the deadline often falls in the gap
		// between one chunk's checkpoint and the next chunk's first request.
		err := o.fetchChunk(ctx, params, gw, chunk, seen, loggedShapes, &paceSpent, &yieldReason)
		// Two ways a run ends because its time is gone rather than because anything
		// broke. The sentinel is the tidy one: a wait was refused because it would not
		// fit. The deadline can also expire mid-request, since a wait leaves only the
		// margin and a few slow pages spend it — that arrives as a context error from
		// inside the walk, not as the sentinel, and it means exactly the same thing.
		// An emit failure is excluded even when it surfaces as a deadline: the sink
		// shares this context, so a degrading queue path would otherwise be laundered
		// into a clean short run.
		if !errors.Is(err, errEmitFailed) &&
			(errors.Is(err, errRunOutOfBudget) || errors.Is(err, context.DeadlineExceeded)) {
			// Out of deadline, not broken. Stop here so the chunks already covered are
			// kept and the next run resumes from this one.
			//
			// Whether that is acceptable turns on having covered something. With
			// progress, this is a long backfill advancing a run at a time. With none,
			// the FIRST chunk could not be covered even with the whole run spent pacing
			// — the floor cannot move, every later run repeats it, and reporting success
			// would make an unfixable stall look like a healthy sync. That case fails.
			if lastCovered.End.IsZero() {
				// Nothing covered. Only the sentinel diagnoses that precisely — the
				// budget could not carry one chunk — so a bare deadline falls through
				// to the generic path below, where the orchestrator classifies it as
				// ctx_exhausted rather than being mislabelled a vendor problem.
				if !errors.Is(err, errRunOutOfBudget) {
					return nil, fmt.Errorf("cursor usage: covering %s: %w", chunk.Start.Format(dateLayout), err)
				}
				// Kind and sentinel are load-bearing, not decoration. This IS a
				// self-throttle — our own budget could not carry one chunk — and the
				// orchestrator only reaches that label through rate_limit plus the
				// ErrLocalRateLimit marker. As vendor_error it would be counted as an
				// unreadable vendor response.
				return nil, &sdk.SyncError{
					ConnectorID:    connectorID,
					OperationID:    o.OperationID(),
					OrganizationID: orgID,
					Kind:           sdk.ErrKindRateLimit,
					// Terminal, not merely retryable. An immediate retry cannot help:
					// this chunk needed more than a WHOLE run's pacing budget, so a
					// fresh attempt seconds later meets the same wall with the same
					// cap. Without this the scheduler's default backoff fires three
					// retries inside a minute — the exact re-hammering the retry hint
					// exists to prevent — burning shared worker slots to learn the
					// same thing three more times. The next scheduled run still tries;
					// only the futile immediate retries are skipped.
					Terminal: true,
					Cause: fmt.Errorf(
						"the window starting %s needs more requests than one run's budget can pace through; the position cannot advance: %w",
						chunk.Start.Format(dateLayout), sdk.ErrLocalRateLimit),
				}
			}
			if yieldReason == "" {
				yieldReason = yieldReasonDeadline
			}
			yielded = true
			usageRunYielded.WithLabelValues(tenantlabel.OrgID(orgID), yieldReason).Inc()
			logger.Info(ctx, "cursor usage: stopping short of the window, progress checkpointed",
				zap.String(logger.OrgID, orgID),
				zap.String("yield_reason", yieldReason),
				zap.Duration("paced_for", paceSpent),
				zap.String("resumes_from", chunk.Start.Format(dateLayout)),
				zap.String("covered_through", lastCovered.End.Format(dateLayout)))
			break
		}
		days := chunkDayList(chunk)
		if err != nil && retrying && !errors.Is(err, errEmitFailed) && ctx.Err() == nil {
			// Listed days that fail again, for any reason (a refused 400 included), stay
			// listed and the walk goes on with the window: a listed day that keeps
			// failing must not stall every later run.
			failedThisRun = append(failedThisRun, chunk.Start.Format(dateLayout)+".."+startOfUTCDay(chunk.End).Format(dateLayout))
			if firstFailure == nil {
				firstFailure = err
			}
			logger.WarnWithCtx(ctx, "cursor usage: listed days failed again; they stay listed",
				zap.String(logger.OrgID, orgID), zap.Time("chunk_start", chunk.Start), zap.Error(err))
			if err := checkpoint(); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			// A failure of this chunk alone is listed and walked past: the floor moves past
			// it and every later run reads its days again. Anything that would fail every
			// later chunk too stops the run here, as does a full list, and the position is
			// then left untouched, so the next run re-reads this chunk — the same mechanism
			// the trailing overlap already relies on.
			listed, ok := listFailedDays(failedDays, days)
			if errors.Is(err, errEmitFailed) || !chunkLocal(ctx, err) || !ok {
				return nil, fmt.Errorf("cursor usage: covering %s: %w", chunk.Start.Format(dateLayout), err)
			}
			failedDays = listed
			failedThisRun = append(failedThisRun, chunk.Start.Format(dateLayout)+".."+startOfUTCDay(chunk.End).Format(dateLayout))
			if firstFailure == nil {
				firstFailure = err
			}
			logger.WarnWithCtx(ctx, "cursor usage: chunk failed, walking past it; the next run reads it again",
				zap.String(logger.OrgID, orgID), zap.Time("chunk_start", chunk.Start), zap.Error(err))
		} else {
			// Days read in full leave the failed list, whichever run listed them.
			failedDays = withoutDays(failedDays, days)
		}
		if !retrying {
			lastCovered = chunk
		}

		// Flush per chunk so a hard kill re-reads one chunk rather than the whole
		// window.
		if err := checkpoint(); err != nil {
			return nil, err
		}
	}

	if len(failedThisRun) > 0 {
		// Every listed day is already checkpointed; the orchestrator keeps no cursor from a
		// failed run. The run fails, retryably, so a chunk that keeps failing keeps the run
		// failing rather than passing as a healthy sync.
		kind := sdk.ErrKindVendorError
		var syncErr *sdk.SyncError
		if errors.As(firstFailure, &syncErr) {
			kind = syncErr.Kind
		}
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    o.OperationID(),
			OrganizationID: orgID,
			Kind:           kind,
			Retryable:      true,
			Cause: fmt.Errorf("cursor usage: %d chunk(s) failed and were walked past, to be read again next run (%s): %w",
				len(failedThisRun), strings.Join(failedThisRun, ", "), firstFailure),
		}
	}

	if lastCovered.End.IsZero() {
		if len(retries) > 0 {
			// Only listed days were read; the floor stays, the list shrinks.
			cur, err := newUsagePositionCursor(position())
			if err != nil {
				return nil, fmt.Errorf("cursor usage: %w", err)
			}
			return cur, nil
		}
		// No chunk completed, which for a well-formed window means there was nothing to
		// cover. Leave the position alone rather than inventing one.
		return nil, nil //nolint:nilnil // nothing covered: no position to advance, and not an error
	}
	// The denominator that makes a yield readable: yields are ordinary during a large
	// backfill and only mean a stall when no run ever finishes a window.
	if !yielded {
		usageWindowCompleted.WithLabelValues(tenantlabel.OrgID(orgID)).Inc()
	}

	cur, err := newUsagePositionCursor(position())
	if err != nil {
		return nil, fmt.Errorf("cursor usage: %w", err)
	}
	return cur, nil
}

// chunkLocal reports whether err is a failure of one chunk alone, which the walk lists and
// walks past: a vendor error, a missing resource or a timeout for that chunk. A rate
// limit, an auth failure, a refused request (our own query, a 4xx other than 404), a sink
// failure, an ended context or a plain error would fail the next chunk too, so the run
// stops on it.
func chunkLocal(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return false
	}
	if st := syncErr.HTTPStatus; st >= 400 && st < 500 && st != 404 {
		return false
	}
	switch syncErr.Kind {
	case sdk.ErrKindVendorError, sdk.ErrKindNotFound, sdk.ErrKindTimeout:
		return true
	default:
		return false
	}
}

// keepRecentDays returns failed without the days before oldest, which the walk can no
// longer read; each drop is logged. A day exactly at oldest is kept.
func keepRecentDays(ctx context.Context, orgID string, failed []string, oldest time.Time) []string {
	var kept []string
	for _, f := range failed {
		d, err := time.ParseInLocation(dateLayout, f, time.UTC)
		if err == nil && d.Before(oldest) {
			logger.WarnWithCtx(ctx, "cursor usage: failed day is older than the lookback; dropping it",
				zap.String(logger.OrgID, orgID), zap.String("date", f))
			continue
		}
		kept = append(kept, f)
	}
	return kept
}

// listFailedDays returns failed with days added, sorted. ok is false, and failed
// unchanged, when the days would take the list past maxFailedDays.
func listFailedDays(failed, days []string) ([]string, bool) {
	have := make(map[string]bool, len(failed))
	for _, d := range failed {
		have[d] = true
	}
	out := append([]string(nil), failed...)
	for _, d := range days {
		if !have[d] {
			out = append(out, d)
		}
	}
	if len(out) > maxFailedDays {
		return failed, false
	}
	sort.Strings(out)
	return out, true
}

// withoutDays returns failed without days. It never aliases a list it changes.
func withoutDays(failed, days []string) []string {
	drop := make(map[string]bool, len(days))
	for _, d := range days {
		drop[d] = true
	}
	var out []string
	for _, d := range failed {
		if !drop[d] {
			out = append(out, d)
		}
	}
	return out
}

// fetchPagePaced fetches one page, waiting out our OWN rate limiter rather than
// failing on it.
//
// The same wait-out-local-limit-vs-abort policy also lives in
// pacing.ExecuteWaitingForLocalCapacity and bitbucket/fetch_repo_tree.go
// getJSONWaitingForCapacity; the three are deliberately not merged and must stay
// in sync.
//
// The budget is a count per window, and the run holds a deadline far longer than
// that window — thirty minutes against a minute for this endpoint. Failing on the
// first rejection therefore threw away almost the whole run: one chunk got a single
// window's worth of requests and no more, and a chunk needing more than that could
// never complete, on this run or any future one, because nothing is checkpointed
// until a chunk finishes. Waiting converts that hard ceiling into a soft one.
//
// Only OUR limiter is waited on. A vendor 429 is the vendor pacing us and belongs to
// the gateway's own retry hint; sleeping on it here would stack two waits for one
// rejection.
func fetchPagePaced(
	ctx context.Context,
	gw sdk.VendorGateway,
	orgID string,
	chunk usageWindow,
	page int,
	spent *time.Duration,
	yieldReason *string,
) (proto.Message, error) {
	for {
		resp, err := gw.Execute(ctx, &vgcursorpb.CursorFilteredUsageEventsRequest{
			StartDate: chunk.Start.UnixMilli(),
			EndDate:   chunk.End.UnixMilli(),
			Page:      int32(page), //nolint:gosec // G115: bounded by maxPagesPerChunk
			PageSize:  usagePageSize,
		})
		if err == nil {
			return resp, nil
		}
		if !errors.Is(err, sdk.ErrLocalRateLimit) {
			return nil, fmt.Errorf("fetching page %d: %w", page, err)
		}

		wait := localLimiterWait(err)
		if ok, reason := canAffordWait(ctx, wait, *spent); !ok {
			*yieldReason = reason
			return nil, errRunOutOfBudget
		}
		*spent += wait

		logger.Info(ctx, "cursor usage: budget spent, waiting for the window to reset",
			zap.String(logger.OrgID, orgID),
			zap.String("chunk_start", chunk.Start.Format(dateLayout)),
			zap.Int("page", page),
			zap.Duration("wait", wait),
			zap.Duration("waited_this_run", *spent))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("cursor usage: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

// localLimiterWait reads the wait the gateway sized for the window that rejected
// us. It already carries jitter, which is what keeps several organisations rejected
// by the same window from returning in lockstep. An in-run wait (LocalWait) wins
// over the scheduler's retry delay, as in pacing.ExecuteWaitingForLocalCapacity.
func localLimiterWait(err error) time.Duration {
	var syncErr *sdk.SyncError
	if errors.As(err, &syncErr) && syncErr.LocalWait > 0 {
		return syncErr.LocalWait
	}
	if errors.As(err, &syncErr) && syncErr.RetryAfterSeconds > 0 {
		return sdk.SecondsToDuration(syncErr.RetryAfterSeconds)
	}
	// No hint means the limiter did not size one. A minute is this endpoint's window
	// and the smallest wait that can free capacity, so it is the safe floor rather
	// than a guess at the right answer.
	return time.Minute
}

// canAffordWait reports whether this run may sleep for wait, having already slept
// for spent. Two independent limits, and either one refusing ends the walk: the
// deadline says whether there is time left to do anything afterwards, and the
// per-run cap says whether the worker slot has been held long enough. The cap is
// the one a busy organisation hits first — its deadline is ample, and holding a
// shared slot is what costs everyone else.
func canAffordWait(ctx context.Context, wait, spent time.Duration) (bool, string) {
	if spent+wait > maxPacingWaitPerRun {
		return false, yieldReasonCap
	}
	if !canWaitWithin(ctx, wait) {
		return false, yieldReasonDeadline
	}
	return true, ""
}

// The two reasons a walk stops short. They are reported separately because they call
// for different responses: the cap is this connector pacing itself and is expected
// during a large backfill, while the deadline means the run had no time left for the
// work it still had.
const (
	yieldReasonCap      = "cap"
	yieldReasonDeadline = "deadline"
)

// canWaitWithin reports whether the run can afford to sleep and still do something
// useful afterwards.
//
// A run with no deadline is treated as unable to wait. That is deliberate: the
// pacing exists to spend a KNOWN budget, and without a deadline there is nothing
// bounding the sleep — an unattended run could sit for hours. The scheduled path
// always carries one.
func canWaitWithin(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return time.Until(deadline) > wait+pacingSafetyMargin
}

// fetchChunk walks one chunk's pages, emitting one record per billed event. An event
// it cannot route is skipped and the walk continues, so the chunk completes and the
// floor moves past it — one unreadable event costs that event alone.
func (o *FetchUsageEventsOp) fetchChunk(
	ctx context.Context,
	params *sdk.ExecutionParams,
	gw sdk.VendorGateway,
	chunk usageWindow,
	seen map[string]struct{},
	loggedShapes map[string]struct{},
	paceSpent *time.Duration,
	yieldReason *string,
) error {
	orgID := params.Config.OrganizationID
	var emitted, skipped int
	for page := 1; page <= maxPagesPerChunk; page++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cursor usage: %w", err)
		}

		protoResp, err := fetchPagePaced(ctx, gw, orgID, chunk, page, paceSpent, yieldReason)
		if err != nil {
			// Wrapped, including the pacing sentinel: errors.Is reaches through the
			// frame, so Execute's classification is unaffected by the added context.
			return fmt.Errorf("cursor usage: covering %s: %w", chunk.Start.Format(dateLayout), err)
		}
		resp, ok := protoResp.(*vgcursorpb.CursorFilteredUsageEventsResponse)
		if !ok {
			return &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    o.OperationID(),
				OrganizationID: orgID,
				Kind:           sdk.ErrKindInternal,
				Cause:          fmt.Errorf("unexpected response type %T", protoResp),
			}
		}

		events := resp.GetUsageEvents()
		for _, raw := range events {
			billedAt, identity, meta, ok := routeEvent(ctx, orgID, raw)
			if !ok {
				// One event this connector cannot read costs that event, and nothing
				// else. It is not emitted, because the consumer rejects an empty
				// identity as a permanent contract violation; the run carries on,
				// because stopping costs every later event in the page, every later
				// page and every later day, on every run, for as long as the vendor
				// keeps returning it. The loss here is one event, bounded and counted;
				// a chunk with no readable event at all is a different thing and fails
				// below.
				//
				// The event's SHAPE goes to the log, never its contents: a billed event
				// carries the developer's email, and a log is the wrong place for it.
				// The field set is what actually diagnoses a vendor change, and the
				// digest separates one recurring bad shape from many distinct ones.
				skipped++
				if shape := payloadFingerprint(raw); !isKnownShape(loggedShapes, shape) {
					logger.WarnWithCtx(ctx, "cursor usage: skipping an event that cannot be routed",
						zap.String(logger.OrgID, orgID),
						zap.Strings("payload_fields", payloadFieldNames(raw)),
						zap.String("payload_fingerprint", shape))
				}
				continue
			}

			// A duplicate identity fails the run rather than emitting. The write is an
			// update in place, so the second of a colliding pair would silently replace
			// the first and storage would report success — nothing below this can notice.
			if _, dup := seen[identity]; dup {
				return &sdk.SyncError{
					ConnectorID:    connectorID,
					OperationID:    o.OperationID(),
					OrganizationID: orgID,
					Kind:           sdk.ErrKindVendorError,
					Cause: fmt.Errorf(
						"two billed events share the identity %q in one run; emitting would silently lose one", identity),
				}
			}
			seen[identity] = struct{}{}

			emitted++
			if err := emitRecord(ctx, params.Sink, o.OperationID(), orgID, identity, raw, meta, billedAt); err != nil {
				return fmt.Errorf("emitting event %q: %w: %w", identity, errEmitFailed, err)
			}
		}

		// A short page ends the walk. The response's own pagination fields are named
		// differently than the vendor documents, so page length is the signal that does
		// not depend on reading them correctly.
		if len(events) < usagePageSize {
			// Every event unreadable is the vendor's shape moving, not one blemished
			// record. Completing here would advance the floor past days whose events are
			// then gone for good, with the run reporting success — the loud stall this
			// change removed, traded for a silent one. Failing leaves the position
			// untouched, so the days stay recoverable once the parser is fixed.
			if emitted == 0 && skipped > 0 {
				return &sdk.SyncError{
					ConnectorID:    connectorID,
					OperationID:    o.OperationID(),
					OrganizationID: orgID,
					Kind:           sdk.ErrKindVendorError,
					Cause: fmt.Errorf("all %d events covering %s were unreadable; the response shape moved",
						skipped, chunk.Start.Format(dateLayout)),
				}
			}
			// Published only here, on the path that advances the floor. A skip in a
			// chunk that then fails costs nothing — the position stays put and the
			// events are re-read — so counting it would call for a backfill that has
			// nothing to recover.
			usageEventsSkipped.WithLabelValues(tenantlabel.OrgID(orgID)).Add(float64(skipped))
			return nil
		}
	}
	return &sdk.SyncError{
		ConnectorID:    connectorID,
		OperationID:    o.OperationID(),
		OrganizationID: orgID,
		Kind:           sdk.ErrKindVendorError,
		Cause:          fmt.Errorf("chunk did not terminate within %d pages", maxPagesPerChunk),
	}
}

// routeEvent lifts the three routing fields out of a vendor event.
//
// It reports false when the event cannot be routed, which the caller turns into a skip
// rather than an emission. The identity combines the conversation the call belonged to
// with the call's own timestamp: the vendor issues no identifier for an individual
// event, so one has to be constructed.
func routeEvent(ctx context.Context, orgID string, raw json.RawMessage) (
	billedAt time.Time, identity string, meta map[string]string, ok bool,
) {
	var ev usageEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		logger.WarnWithCtx(ctx, "cursor usage: unparseable event",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return time.Time{}, "", nil, false
	}
	// Parsed as a number despite arriving as text.
	millis, err := strconv.ParseInt(ev.Timestamp, 10, 64)
	if err != nil {
		logger.WarnWithCtx(ctx, "cursor usage: event timestamp is not a number",
			zap.String(logger.OrgID, orgID), zap.String("timestamp", ev.Timestamp), zap.Error(err))
		return time.Time{}, "", nil, false
	}
	if millis <= 0 {
		logger.WarnWithCtx(ctx, "cursor usage: event timestamp is not a usable instant",
			zap.String(logger.OrgID, orgID), zap.Int64("timestamp_millis", millis))
		return time.Time{}, "", nil, false
	}
	if ev.ConversationID == "" {
		logger.WarnWithCtx(ctx, "cursor usage: event carries no conversation identifier",
			zap.String(logger.OrgID, orgID))
		return time.Time{}, "", nil, false
	}

	at := time.UnixMilli(millis).UTC()
	// The business date is what storage partitions on and it participates in the
	// record's uniqueness, so it is derived identically on every run and set
	// explicitly. Leaving it unset does not fail — it silently falls back to the fetch
	// day, which for a ninety-day first sync would file every historical event under
	// today.
	return startOfUTCDay(at),
		ev.ConversationID + "#" + ev.Timestamp,
		map[string]string{"conversation_id": ev.ConversationID, "user_email": ev.UserEmail},
		true
}

// emitRecord hands one record to the sink, following the shape every connector uses.
// Extracting the shared version is a cross-connector change, not part of this one.
func emitRecord(
	ctx context.Context,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, externalID string,
	payload json.RawMessage,
	metadata map[string]string,
	dataDate time.Time,
) error {
	record := sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(opID),
		OrganizationID: orgID,
		ExternalID:     externalID,
		Payload:        payload,
		FetchedAt:      time.Now().UTC(),
		DataDate:       dataDate,
		Metadata:       metadata,
	}
	if err := sink.Emit(ctx, &record); err != nil {
		logger.Error(ctx, "cursor usage: failed to emit record",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    opID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindSinkFailure,
			Cause:          fmt.Errorf("emitting record: %w", err),
		}
	}
	return nil
}

// safeFieldName matches a name that cannot itself be a value. A vendor is free to key
// an object BY an identifier — an email, a user id — and that shape is precisely the
// one that leaves conversationId empty, so it is precisely the one that reaches this
// log. A key is only printable if it looks like a field name.
var safeFieldName = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

// maxLoggedFields bounds one log line. A vendor that keys by identifier produces as
// many "fields" as it has identifiers.
const maxLoggedFields = 32

// payloadFieldNames lists the event's top-level keys, sorted, with anything that does
// not look like a field name replaced — a key can carry a value, and the values are
// the developer's business.
//
// A payload that is not an object yields no names, which the fingerprint beside it
// still tells apart.
func payloadFieldNames(raw json.RawMessage) []string {
	var keyed map[string]json.RawMessage
	// A failure here IS the answer — the payload is not an object, so it has no field
	// names — and the fingerprint logged beside it still tells the shapes apart.
	if err := json.Unmarshal(raw, &keyed); err != nil { // the error is the signal, not a fault
		return nil
	}
	names := make([]string, 0, len(keyed))
	redacted := false
	for name := range keyed {
		if safeFieldName.MatchString(name) {
			names = append(names, name)
			continue
		}
		redacted = true
	}
	sort.Strings(names)
	if redacted {
		names = append(names, "<unprintable-key>")
	}
	if len(names) > maxLoggedFields {
		names = append(names[:maxLoggedFields:maxLoggedFields], "<truncated>")
	}
	return names
}

// payloadFingerprint identifies the event's SHAPE — its field names and the JSON type
// of each value — and never its contents.
//
// Hashing the bytes would have grouped nothing: every event that omits the same field
// still carries a different timestamp, developer and charge, so one recurring vendor
// change would have produced a fresh fingerprint per event rather than one recognisable
// class. Hashing the shape groups them, and it removes the reason to protect the value
// at all — there is nothing left in the input to guess.
func payloadFingerprint(raw json.RawMessage) string {
	var keyed map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keyed); err != nil { // not an object; the shape is its own answer
		return "shape:non-object"
	}
	pairs := make([]string, 0, len(keyed))
	for name, value := range keyed {
		// The same allowlist the log uses, for a sharper reason: a fingerprint over a
		// raw key is a confirmation oracle. A vendor keying by email would let anyone
		// holding a candidate address recompute the digest and confirm it, and would
		// give every developer a distinct "shape", which is the opposite of grouping.
		if !safeFieldName.MatchString(name) {
			name = "<unprintable-key>"
		}
		pairs = append(pairs, name+":"+jsonTypeOf(value))
	}
	sort.Strings(pairs)
	sum := sha256.Sum256([]byte(strings.Join(pairs, ",")))
	return "shape:" + hex.EncodeToString(sum[:6])
}

// jsonTypeOf names a value's JSON type without reading it.
func jsonTypeOf(value json.RawMessage) string {
	trimmed := strings.TrimSpace(string(value))
	if trimmed == "" {
		return "empty"
	}
	switch trimmed[0] {
	case '{':
		return "object"
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "bool"
	case 'n':
		return "null"
	default:
		return "number"
	}
}

// isKnownShape reports whether this shape has already been logged in this run, and
// records it if not. The counter carries how many; the log carries what, once each.
func isKnownShape(logged map[string]struct{}, shape string) bool {
	if _, known := logged[shape]; known {
		return true
	}
	logged[shape] = struct{}{}
	return false
}

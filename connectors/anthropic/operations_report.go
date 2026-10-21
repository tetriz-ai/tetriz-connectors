// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// reportPageSize is the vendor maximum. Note the response can exceed it: cost_type and
// token_type fan-out rows do not count toward the limit, so the page length is not a
// reliable end condition and has_more is what terminates the walk.
const reportPageSize = 1000

// maxPagesPerChunk bounds one chunk's walk, catching a vendor that never clears has_more
// rather than spinning until the run's deadline.
const maxPagesPerChunk = 500

// reportRow is what is lifted out of a report row, and only this: the actor, the bucket
// start and the product. All three are un-renameable, which is the whole basis of the
// record's identity — the model is deliberately absent, because a model in a uniqueness
// key means the day the vendor returns a display name instead of a slug, one
// actor-bucket-product silently becomes two rows and its cost double-counts.
type reportRow struct {
	Actor struct {
		UserID string `json:"user_id"`
	} `json:"actor"`
	StartingAt string `json:"starting_at"`
	Product    string `json:"product"`
}

// unknownProduct is the ExternalID's product part for a row that carries no product, so
// the key never ends in an empty segment.
const unknownProduct = "unknown"

// FetchAnalyticsUserCostOp stores one record per actor per bucket per product, holding the
// per-model rows for that product as an array.
type FetchAnalyticsUserCostOp struct{}

func (o *FetchAnalyticsUserCostOp) OperationID() sdk.OperationID {
	return opFetchAnalyticsUserCost
}

func (o *FetchAnalyticsUserCostOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	cur, err := runReportWalk(ctx, params, o.OperationID(), reportRequestCost)
	if err != nil {
		return nil, fmt.Errorf("walking the per-user cost report: %w", err)
	}
	return cur, nil
}

// FetchAnalyticsUserUsageOp is the token-usage twin of the cost report, with the same
// shape and the same identity.
type FetchAnalyticsUserUsageOp struct{}

func (o *FetchAnalyticsUserUsageOp) OperationID() sdk.OperationID {
	return opFetchAnalyticsUserUsage
}

func (o *FetchAnalyticsUserUsageOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	cur, err := runReportWalk(ctx, params, o.OperationID(), reportRequestUsage)
	if err != nil {
		return nil, fmt.Errorf("walking the per-user usage report: %w", err)
	}
	return cur, nil
}

// reportRequestFn builds one page request for a chunk.
type reportRequestFn func(startingAt, endingAt time.Time, page string) proto.Message

func reportRequestCost(startingAt, endingAt time.Time, page string) proto.Message {
	return &vgpb.AnthropicAnalyticsUserCostReportRequest{
		StartingAt: timestamppb.New(startingAt),
		EndingAt:   timestamppb.New(endingAt),
		Limit:      reportPageSize,
		Page:       page,
	}
}

func reportRequestUsage(startingAt, endingAt time.Time, page string) proto.Message {
	return &vgpb.AnthropicAnalyticsUserUsageReportRequest{
		StartingAt: timestamppb.New(startingAt),
		EndingAt:   timestamppb.New(endingAt),
		Limit:      reportPageSize,
		Page:       page,
	}
}

// reportPageData is the shape both report responses share once the concrete type is known.
type reportPageData struct {
	rows     [][]byte
	nextPage string
	hasMore  bool
	// refreshedAt is the vendor's statement of its own currency. Carried as a time, not as
	// the string it is eventually stored as, so nothing downstream re-parses a rendering
	// this package produced.
	refreshedAt time.Time
	// organizationID is which Anthropic organisation answered. Empty when the response omitted
	// it; only the report endpoints carry it at all.
	organizationID string
}

func readReportPage(resp proto.Message) (reportPageData, bool) {
	switch r := resp.(type) {
	case *vgpb.AnthropicAnalyticsUserCostReportResponse:
		return reportPageData{
			rows: r.GetData(), nextPage: r.GetNextPage(), hasMore: r.GetHasMore(),
			refreshedAt:    refreshedAtTime(r.GetDataRefreshedAt()),
			organizationID: r.GetOrganizationId(),
		}, true
	case *vgpb.AnthropicAnalyticsUserUsageReportResponse:
		return reportPageData{
			rows: r.GetData(), nextPage: r.GetNextPage(), hasMore: r.GetHasMore(),
			refreshedAt:    refreshedAtTime(r.GetDataRefreshedAt()),
			organizationID: r.GetOrganizationId(),
		}, true
	}
	return reportPageData{}, false
}

func refreshedAtTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime().UTC()
}

// refreshedAtString is the stored rendering of a watermark. The stored value and the
// observed value must be formatted identically or the planner's comparison would see every
// run as a moved watermark and re-read the trailing window unconditionally.
func refreshedAtString(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}

// runReportWalk walks the window in chunks no wider than the vendor's maximum, emitting one
// record per actor per bucket per product and checkpointing after each chunk.
//
// Chunking is a hard requirement rather than pacing: thirty-one days is the widest range one
// request accepts, and a ninety-day window sent whole returns 400 before ingesting anything.
//
// The walk plans twice, and that is what makes the watermark work. It cannot know whether
// anything needs re-reading until the vendor tells it — data_refreshed_at arrives on a
// response, not before one — so the first plan covers unread history only, and once the
// vendor's currency is known the trailing re-read is planned against it. Comparing the
// stored value with itself, which is what a single-pass plan is forced to do, means the
// re-read never fires and a re-priced amount is never collected.
func runReportWalk(
	ctx context.Context, params *sdk.ExecutionParams,
	opID sdk.OperationID, buildRequest reportRequestFn,
) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	settings, err := ParseSettings(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Cause: fmt.Errorf("parsing settings: %w", err),
		}
	}
	opts, err := parseOperationOptions(params.Config, opID)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Cause: err,
		}
	}

	pos, err := parseReportPosition(params.Cursor)
	if err != nil {
		logger.WarnWithCtx(ctx, "claude-code report: unreadable position, restarting from the lookback", zap.Error(err))
		positionUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
		pos = resolvedReportPosition{}
	}

	notePositionFarBehind(opID, orgID, pos.Covered)

	// One clock read for the whole run. Two would let a run straddling UTC midnight plan its
	// two passes against different days, and judge finality against a third.
	now := time.Now()

	state := &reportWalkState{
		opID: opID, orgID: orgID, covered: pos.Stored, present: pos.Present, coveredDay: pos.Covered,
		storedRefreshedDay: pos.RefreshedDay,
		clockDay:           now.UTC().Truncate(24 * time.Hour),
		wantOrganizationID: settings.AnthropicOrganizationID,
	}

	if settings.AnthropicOrganizationID == "" {
		logger.WarnWithCtx(ctx, "claude-code report: no stored Anthropic organisation id, so the cross-organisation check is inactive for this run",
			zap.String("operation", string(opID)))
	}

	// Pass one: unread history only. rereadDays is zero because nothing observed yet can
	// justify a re-read — see resolveReportChunks for what the observed value must be.
	// The observed watermark is empty here, not the stored one: with rereadDays at zero the
	// planner never reads it, and passing the stored value would be the self-comparison its
	// own doc calls a defect.
	forward, unreadableFrom, unreadableTo := resolveReportChunks(now, pos,
		settings.GetLookbackDays(), 0, opts.chunkDays(), maxForwardChunksPerRun, "")

	logger.Info(ctx, "claude-code report: planned unread history",
		zap.Int("chunks", len(forward)), zap.Bool("first_run", !pos.Present))

	// A span the vendor will not serve is stated, because the position advances past it either
	// way. Silence here would present as a healthy sync over days that were never read.
	//
	// Only for a RESUMED position: the vendor-floor clamp bites a first sync alone, where
	// nothing was dropped because no position ever covered those days and the vendor holds no
	// data before the floor. Reporting that as loss would fire a never-self-healing alarm on
	// every first sync with a deep lookback.
	if pos.Present && !unreadableFrom.IsZero() {
		logger.WarnWithCtx(ctx, "claude-code report: a span before the vendor's limits cannot be requested and is being skipped",
			zap.Time("unreadable_from", unreadableFrom), zap.Time("unreadable_to", unreadableTo))
	}

	// The chunks an earlier run failed are read first. Each lies behind the position, so
	// reading one never moves it.
	// A listed chunk older than the lookback or the vendor's report range can no longer be
	// read: dropped, each logged. A listed chunk that fails again stays listed and the walk
	// goes on.
	listedChunks := keepRecentChunks(ctx, opID, state, pos.FailedChunks,
		oldestReadable(now.UTC().Truncate(24*time.Hour), settings.GetLookbackDays(), maxReportLookbackDays))
	state.retrying = true
	if err := walkReportChunks(ctx, params, state, listedChunks, buildRequest); err != nil {
		return nil, fmt.Errorf("re-reading failed %s chunks: %w", opID, err)
	}
	state.retrying = false
	if err := walkReportChunks(ctx, params, state, forward, buildRequest); err != nil {
		return nil, fmt.Errorf("reading unread %s history: %w", opID, err)
	}

	// Pass two: the trailing re-read, planned against what the vendor actually said this run.
	//
	// Whether it is warranted is the planner's decision alone — gating it here on a non-empty
	// observed watermark would cancel the case the planner deliberately treats as re-read
	// worthy: a vendor that omits data_refreshed_at tells us nothing, and assuming nothing
	// changed would make the whole restatement mechanism dead for that vendor, silently.
	trailing, _, _ := resolveReportChunks(now, pos,
		settings.GetLookbackDays(), opts.rereadDays(), opts.chunkDays(), 0, state.observedRefreshedAt)
	trailing = chunksBefore(trailing, pos.Covered, pos.Present)
	if len(trailing) > 0 {
		logger.Info(ctx, "claude-code report: re-reading the trailing window",
			zap.Int("chunks", len(trailing)),
			zap.String("stored_watermark", pos.Stored.DataRefreshedAt),
			zap.String("observed_watermark", state.observedRefreshedAt))
		if err := walkReportChunks(ctx, params, state, trailing, buildRequest); err != nil {
			return nil, fmt.Errorf("re-reading the %s trailing window: %w", opID, err)
		}
	}

	if len(state.failed) > 0 {
		// Every listed chunk is already checkpointed; the orchestrator keeps no cursor from a
		// failed run, and the watermark is not stored, so the next run re-reads the trailing
		// window too. The run fails, retryably, so a chunk that keeps failing keeps the run
		// failing rather than passing as a healthy sync.
		return nil, failedUnitsError(opID, orgID, "chunk", state.failed, state.firstFailure)
	}

	// Whether this run reached the furthest point it could legitimately claim — the clock, or
	// the vendor's currency when that is nearer.
	//
	// Measured on the CLAIM, not on the plan: a plan can reach the clock and claim nothing.
	// But measured against the clock ALONE it inverts for the most ordinary configuration
	// there is — a vendor one day behind means the claim is capped one day behind by design,
	// so a perfectly healthy sync would report "yields with no covers" forever, which is the
	// signature this pair exists to alarm on. A run held short by the vendor's own currency
	// has done everything available to it.
	//
	// An empty plan means already caught up, which is covered, not a yield.
	claimable := state.clockDay
	if ceiling, ok := finalityCeiling(state.finalityDay(), state.clockDay); ok && ceiling.Before(claimable) {
		claimable = ceiling
	}
	reachedPresent := len(forward) == 0 || !state.coveredDay.Before(claimable)

	// A plan that ran and claimed nothing is a window that sits entirely after the vendor's
	// currency. It repeats identically forever, because the window and the ceiling both
	// re-anchor to the clock on every run — so it fails here, naming the two numbers whose
	// relation is the cause, rather than surfacing as "covered nothing" and pointing at a
	// window that was in fact read.
	if len(forward) > 0 && !state.claimedAny && !pos.Present {
		// A non-claimable chunk on a first sync implies the vendor reported a watermark this
		// run: with no stored position there is no other source for the ceiling.
		lag := int(state.clockDay.Sub(state.observedRefreshedDay).Hours() / 24)
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid,
			Cause: fmt.Errorf(
				"%s read its whole window but none of it is final yet: the vendor is %d days behind "+
					"and lookback_days is %d — widen lookback_days past the vendor's lag",
				opID, lag, settings.GetLookbackDays()),
		}
	}

	// Counted only now, after both passes: a run that failed partway has not covered its
	// window, and counting it at planning time would put failures in the denominator that is
	// supposed to separate an ordinary backfill from a stall.
	// Counted after the walks, not at planning time, so a run that failed partway is not in
	// the denominator meant to separate a backfill from a stall.
	if pos.Present && !unreadableFrom.IsZero() {
		runYielded.WithLabelValues(tenantlabel.OrgID(orgID), string(opID), yieldUnreadable).Inc()
	}

	if reachedPresent {
		runCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
	} else {
		runYielded.WithLabelValues(tenantlabel.OrgID(orgID), string(opID), yieldForwardCap).Inc()

		// The per-run reach against the gap still open — see maxForwardChunksPerRun for the
		// invariant. Reported from the first occurrence, because the alternative is finding it
		// once the position has fallen past the vendor's year limit. Guarded on the reach
		// actually being the binding constraint, so the advice names a lever that would help.
		reach := maxForwardChunksPerRun * opts.chunkDays()
		if behind := int(claimable.Sub(pos.Covered).Hours() / 24); pos.Present && behind > reach {
			logger.WarnWithCtx(ctx, "claude-code report: one run cannot reach the present; widen chunk_days or run more often or the position falls further behind each run",
				zap.Int("days_behind_before_this_run", behind), zap.Int("days_reachable_per_run", reach))
		}
	}

	// The observed watermark is stored only HERE, once both passes are done. Storing it
	// per chunk during pass one would mean a run killed between the passes has already
	// recorded the new value, so the next run sees an unchanged watermark, skips the
	// re-read, and never collects the restatements that watermark announced.
	// Only forward. A vendor that regresses its own watermark would otherwise have the older
	// value written back, which then reads as "unchanged" and retires the re-read at the lower
	// value.
	if state.observedRefreshedAt != "" && state.observedRefreshed.After(pos.RefreshedAt) {
		state.covered.DataRefreshedAt = state.observedRefreshedAt
	}

	cur, err := storeCoveredPosition(opID, orgID, state.covered, state.coveredDay)
	if err != nil {
		return nil, fmt.Errorf("storing the position after the %s walk: %w", opID, err)
	}
	return cur, nil
}

// reportWalkState carries what the walk learns across its two passes.
type reportWalkState struct {
	opID    sdk.OperationID
	orgID   string
	covered reportPosition
	present bool
	// coveredDay is covered.CoveredThrough as a time, written with it so the forward-only
	// guard compares times rather than re-parsing this walk's own output.
	coveredDay time.Time
	// wantOrganizationID is the Anthropic organisation the stored key resolved to when it was
	// saved. Empty disables the cross-organisation check, so the walk says so rather than
	// letting it lapse quietly: the connect path refuses a key whose report response carries
	// no organisation id, so nothing the product writes can be empty here.
	wantOrganizationID string
	// claimedAny records whether ANY chunk was claimable this run. It is what separates a walk
	// that made progress from one whose whole window sat after the vendor's currency — the
	// second is a window too shallow to contain final data, and it repeats identically on
	// every run because both the window and the ceiling re-anchor to the clock.
	claimedAny bool
	// retrying is set while the walk re-reads the chunks an earlier run listed: one that
	// fails again, for any reason, stays listed and the walk goes on.
	retrying bool
	// staleReported keeps the unbounded-coverage count to once per run. It is a property of the
	// vendor's currency, not of each chunk, and the counter it feeds is per run.
	staleReported bool
	// clockDay is the run's single reading of today, shared by the planner and the finality
	// judgement so the two cannot disagree about when "now" is.
	clockDay time.Time
	// observedRefreshed is the highest watermark any page reported THIS run, at full
	// precision — the comparison has to be at full precision, because two watermarks on one
	// calendar day are ordered only by their times, and letting an older one win would store
	// it and force a spurious re-read on every subsequent run.
	//
	// observedRefreshedAt is its stored rendering, and observedRefreshedDay is the day grain
	// the coverage ceiling uses.
	observedRefreshed time.Time
	//
	// Both stay EMPTY until a response actually carries a watermark, because emptiness is what
	// the re-read reads as "we have not learned it yet". Seeding them from the stored value
	// would make an unchanged watermark indistinguishable from an unobserved one, and the
	// re-read would then fire on every quiet day. storedRefreshedDay carries the seed instead,
	// so a response that omits the field still bounds coverage by the last finality point
	// known.
	observedRefreshedAt  string
	observedRefreshedDay time.Time
	storedRefreshedDay   time.Time
	// failed are the chunks that failed THIS run, and firstFailure the first of their
	// errors, for the error the run returns.
	failed       []string
	firstFailure error
}

// finalityDay is the vendor's currency as this run knows it: what a response reported, or the
// last value known when every response omitted the field.
func (s *reportWalkState) finalityDay() time.Time {
	if !s.observedRefreshedDay.IsZero() {
		return s.observedRefreshedDay
	}
	return s.storedRefreshedDay
}

// chunksBefore narrows a trailing plan to the history the forward pass did not read.
//
// It TRUNCATES at the covered point rather than dropping chunks that cross it. The trailing
// window is wider than the gap the forward pass read, so the covered point sits INSIDE one
// chunk — dropping that chunk whole leaves the pass planning nothing at all.
func chunksBefore(chunks []reportChunk, covered time.Time, present bool) []reportChunk {
	if !present {
		return nil
	}
	var out []reportChunk
	for _, c := range chunks {
		if !c.StartingAt.Before(covered) {
			continue // entirely in the forward pass's territory
		}
		if c.EndingAt.After(covered) {
			c.EndingAt = covered
		}
		if c.StartingAt.Before(c.EndingAt) {
			out = append(out, c)
		}
	}
	return out
}

// walkReportChunks reads a planned set of chunks, emitting one unit per chunk.
func walkReportChunks(
	ctx context.Context, params *sdk.ExecutionParams,
	state *reportWalkState, chunks []reportChunk, buildRequest reportRequestFn,
) error {
	if len(chunks) == 0 {
		return nil
	}
	gw := params.VendorGateway.ForGroup(constants.GroupAnthropicAnalytics)

	// readChunk reads one whole chunk and emits it, or emits nothing.
	readChunk := func(chunk reportChunk) error {
		pageCursor := ""
		// The accumulator's lifetime is one chunk — the unit this walk checkpoints (see
		// recordAccumulator). Here the per-model fan-out rows the vendor excludes from the
		// page limit make a straddled identity likelier, not rarer.
		acc := newRecordAccumulator(state.opID, state.orgID)
		chunkComplete := false
		restarts := 0

		for page := 0; page < maxPagesPerChunk; page++ {
			pagesRead.WithLabelValues(tenantlabel.OrgID(state.orgID), string(state.opID)).Inc()
			resp, err := gw.Execute(ctx, buildRequest(chunk.StartingAt, chunk.EndingAt, pageCursor))
			if err != nil {
				// A 400 on a report range is always our own query: these endpoints have no
				// not-yet-aggregated answer, unlike the activity ones.
				if isBadRequest(err) {
					vendorBadRequest.WithLabelValues(tenantlabel.OrgID(state.orgID),
						string(state.opID), badRequestQueryDefect).Inc()
				}
				// Bounded separately from the page budget, so repeated restarts cannot
				// exhaust the pages and then blame the page cap.
				if isExpiredCursor(err) && pageCursor != "" {
					restarts++
					if restarts > maxCursorRestarts {
						return &sdk.SyncError{
							ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
							Kind: sdk.ErrKindVendorError,
							Cause: fmt.Errorf("%s cursor for %s..%s expired %d times; narrow chunk_days so the range fits one cursor lifetime",
								state.opID, chunk.StartingAt.Format(dateLayout),
								chunk.EndingAt.Format(dateLayout), restarts),
						}
					}
					logger.WarnWithCtx(ctx, "claude-code report: page cursor expired, restarting the chunk",
						zap.Time("chunk_start", chunk.StartingAt), zap.Int("restarts", restarts))
					pageCursor = ""
					acc = newRecordAccumulator(state.opID, state.orgID)
					page = 0
					continue
				}
				// Returned with context but not re-classified: the gateway's SyncError
				// carries the kind, the terminality and the vendor's Retry-After.
				return fmt.Errorf("reading %s for %s..%s: %w", state.opID,
					chunk.StartingAt.Format(dateLayout), chunk.EndingAt.Format(dateLayout), err)
			}

			pageData, ok := readReportPage(resp)
			if !ok {
				return &sdk.SyncError{
					ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
					Kind:  sdk.ErrKindInternal,
					Cause: fmt.Errorf("unexpected response type %T for %s", resp, state.opID),
				}
			}
			// The organisation that answered must be the one this connector was configured
			// for. A key pointing at a different Anthropic organisation otherwise ingests
			// THAT organisation's per-user data under this one's id, and every record looks
			// well-formed — there is nothing downstream that could notice.
			//
			// Terminal AUTH, not config-invalid, and the kind is what makes the guard work at
			// all. Only a Terminal auth error reaches the orchestrator's auth-revoked
			// transition, which pauses EVERY operation on this connector until a human writes
			// a fresh credential. A config-invalid error only skips asynq's retries: the three
			// operations whose endpoints carry no organisation id — both activity walks and
			// the roster — would keep running on the next tick and keep emitting.
			//
			// It is also the honest classification. The credential authenticates fine; it
			// authenticates as somebody else, and the remedy is a new key rather than a
			// changed setting.
			// An ABSENT id fails too, and separately, because the two cases are not the same
			// claim. A mismatch proves we are reading another organisation's data; an absence
			// means we cannot tell — and the setup probe already refuses a response with no
			// organisation id, so a scheduled page that accepts one leaves the two paths
			// disagreeing about the same shape. Not terminal: unlike a mismatched key, a
			// missing field is as likely a transient vendor regression as a wrong credential,
			// and pausing every operation on the connector is not proportionate to "cannot
			// verify". The run fails, the position holds, nothing unattributable is stored,
			// and a persistent case surfaces through the run status and the coverage series.
			if state.wantOrganizationID != "" && pageData.organizationID == "" {
				return &sdk.SyncError{
					ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
					Kind: sdk.ErrKindVendorError,
					Cause: fmt.Errorf(
						"the %s response carries no organisation id, so it cannot be checked against the %q this credential was saved against; "+
							"refusing to store data this connector cannot attribute",
						state.opID, state.wantOrganizationID),
				}
			}
			if want := state.wantOrganizationID; want != "" &&
				pageData.organizationID != "" && pageData.organizationID != want {
				return &sdk.SyncError{
					ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
					Kind:     sdk.ErrKindAuth,
					Terminal: true,
					Cause: fmt.Errorf(
						"the stored key now answers for Anthropic organisation %q, not the %q it was saved against; "+
							"refusing to ingest another organisation's data — reconnect the integration to re-probe "+
							"and re-save the organisation, or restore the original key",
						pageData.organizationID, want),
				}
			}

			// The highest watermark seen, not the last. Pages are read across both passes and
			// the trailing pass reads older ranges, so taking the last would let an older
			// value regress what is stored and force an unnecessary re-read next run.
			if pageData.refreshedAt.After(state.observedRefreshed) {
				state.observedRefreshed = pageData.refreshedAt
				state.observedRefreshedAt = refreshedAtString(pageData.refreshedAt)
				state.observedRefreshedDay = pageData.refreshedAt.Truncate(24 * time.Hour)
			}

			if err := collectReportRows(acc, state.opID, state.orgID, pageData.rows); err != nil {
				return fmt.Errorf("reading %s rows for %s..%s: %w", state.opID,
					chunk.StartingAt.Format(dateLayout), chunk.EndingAt.Format(dateLayout), err)
			}

			// has_more is the end condition, not the page length: cost_type and token_type
			// fan-out rows do not count toward the limit, so a full-looking page can be the
			// last and a short one need not be.
			if !pageData.hasMore {
				chunkComplete = true
				break
			}
			if pageData.nextPage == "" {
				// The vendor asserts there is more and hands back nothing to resume from.
				// Treating that as the end would drop data it has just told us exists.
				return &sdk.SyncError{
					ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
					Kind: sdk.ErrKindVendorError,
					Cause: fmt.Errorf("%s reported more pages for %s..%s but returned no cursor",
						state.opID, chunk.StartingAt.Format(dateLayout), chunk.EndingAt.Format(dateLayout)),
				}
			}
			pageCursor = pageData.nextPage
		}

		if !chunkComplete {
			// The cap was exhausted with pages pending: emitting and advancing would declare
			// the chunk covered and drop the rest permanently.
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: state.opID, OrganizationID: state.orgID,
				Kind: sdk.ErrKindVendorError,
				Cause: fmt.Errorf("%s did not finish %s..%s within %d pages; refusing to advance past unread pages",
					state.opID, chunk.StartingAt.Format(dateLayout), chunk.EndingAt.Format(dateLayout), maxPagesPerChunk),
			}
		}

		if err := acc.emitAll(ctx, params.Sink, false); err != nil {
			return fmt.Errorf("emitting %s records for %s..%s: %w", state.opID,
				chunk.StartingAt.Format(dateLayout), chunk.EndingAt.Format(dateLayout), err)
		}
		return nil
	}

	for _, chunk := range chunks {
		unit := chunkUnit(chunk)
		readErr := readChunk(chunk)
		if readErr != nil && state.retrying && ctx.Err() == nil {
			// A listed chunk that fails again, a refused one included, stays listed: a
			// listed chunk that keeps failing must not stall every later run.
			state.failed = append(state.failed, unit)
			if state.firstFailure == nil {
				state.firstFailure = readErr
			}
			logger.WarnWithCtx(ctx, "claude-code report: listed chunk failed again; it stays listed",
				zap.String("chunk", unit), zap.Error(readErr))
			continue
		}
		// A failure of this chunk alone is walked past below. Anything that would fail every
		// later chunk too stops the run here.
		if readErr != nil && !unitLocal(ctx, readErr) {
			return readErr
		}

		// Forward only, and never past what the vendor is still revising. The trailing pass
		// re-reads history, so an unguarded write would move the position backwards and the
		// next run would re-read the same span again.
		advanceTo := chunk.EndingAt
		finalityDay := state.finalityDay()
		// Staleness is judged against the CLOCK, not against this chunk's end. Judging it
		// against the chunk made the release unreachable whenever the per-run plan was
		// narrower than the staleness bound: no planned chunk could get far enough past a
		// frozen watermark, so coverage collapsed back to it on every chunk of every run and
		// the walk never advanced again — a permanent stall reporting success.
		claimable := true
		if ceiling, ok := finalityCeiling(finalityDay, state.clockDay); ok && ceiling.Before(advanceTo) {
			// Behind the chunk entirely: none of it is final, so none of it may be claimed.
			// Clamping up to the chunk's start instead would name a day the vendor is still
			// revising, which is the whole thing the ceiling prevents.
			claimable = !ceiling.Before(chunk.StartingAt)
			advanceTo = ceiling
		} else if !finalityDay.IsZero() && finalityDay.Before(chunk.EndingAt) {
			// The watermark is too far behind to bound anything, so the read stands. Said out
			// loud, and counted, because it means the vendor has stopped reporting finality and
			// a restatement inside the unbounded span will not be collected.
			logger.WarnWithCtx(ctx, "claude-code report: watermark too stale to bound coverage, recording the read as covered",
				zap.Time("watermark", finalityDay), zap.Time("read_through", chunk.EndingAt))
			if !state.staleReported {
				coverageUnbounded.WithLabelValues(tenantlabel.OrgID(state.orgID), string(state.opID)).Inc()
				state.staleReported = true
			}
		}
		advances := claimable && (!state.present || advanceTo.After(state.coveredDay))
		if readErr != nil {
			// A failed chunk the position passes, or has passed, is listed so every later run
			// reads it again; one the position stays before is read again anyway. A full list
			// stops the run here, before the position moves.
			coveredAfter := state.coveredDay
			if advances {
				coveredAfter = advanceTo
			}
			if (state.present || advances) && coveredAfter.After(chunk.StartingAt) {
				listed, ok := listFailedUnit(state.covered.FailedChunks, unit)
				if !ok {
					return readErr
				}
				state.covered.FailedChunks = listed
			}
			state.failed = append(state.failed, unit)
			if state.firstFailure == nil {
				state.firstFailure = readErr
			}
			logger.WarnWithCtx(ctx, "claude-code report: chunk failed, walking past it; the next run reads it again",
				zap.String("chunk", unit), zap.Error(readErr))
		} else {
			// A chunk read in full leaves the failed list, whichever run listed it.
			state.covered.FailedChunks = removeUnit(state.covered.FailedChunks, unit)
		}
		if advances {
			state.covered.CoveredThrough = advanceTo.Format(dateLayout)
			state.coveredDay = advanceTo
			state.present = true
			state.claimedAny = true
			// Counted on the CLAIM, not on having walked the chunk: a chunk the finality
			// ceiling sits entirely behind is read and then not claimed, and counting it
			// would erase the distinction this series exists to draw.
			periodsCovered.WithLabelValues(tenantlabel.OrgID(state.orgID), string(state.opID)).Inc()
		}

		checkpointPosition(ctx, params, state.opID, state.orgID, state.covered,
			zap.String("covered_through", state.covered.CoveredThrough))
	}
	return nil
}

// keepRecentChunks returns the chunks that still hold a day the vendor serves, oldest
// being the first such day. A chunk's EndingAt is exclusive, so one ending at or before
// oldest holds none and is dropped. One starting before oldest is clipped to start at it:
// the vendor refuses a request reaching past its range, so the unclipped chunk would fail
// every run until its end aged out. Each drop and clip changes the stored list and is
// logged.
func keepRecentChunks(ctx context.Context, opID sdk.OperationID, state *reportWalkState, chunks []reportChunk, oldest time.Time) []reportChunk {
	var kept []reportChunk
	for _, c := range chunks {
		unit := chunkUnit(c)
		if !c.EndingAt.After(oldest) {
			state.covered.FailedChunks = removeUnit(state.covered.FailedChunks, unit)
			logger.WarnWithCtx(ctx, "claude-code report: failed chunk is older than the lookback; dropping it",
				zap.String("operation", string(opID)), zap.String("chunk", unit))
			continue
		}
		if c.StartingAt.Before(oldest) {
			c = reportChunk{StartingAt: oldest, EndingAt: c.EndingAt}
			state.covered.FailedChunks = replaceUnit(state.covered.FailedChunks, unit, chunkUnit(c))
			logger.WarnWithCtx(ctx, "claude-code report: failed chunk starts before the oldest readable day; reading it from that day",
				zap.String("operation", string(opID)), zap.String("chunk", unit), zap.String("clipped", chunkUnit(c)))
		}
		kept = append(kept, c)
	}
	return kept
}

// collectReportRows files each row under its identity — actor, bucket and product, all
// un-renameable; the walk emits once the chunk is whole.
func collectReportRows(
	acc *recordAccumulator, opID sdk.OperationID, orgID string, rows [][]byte,
) error {
	// A row this loop cannot key fails the run rather than being skipped, for the reason
	// collectActivityRows states.
	for _, row := range rows {
		var parsed reportRow
		if err := json.Unmarshal(row, &parsed); err != nil {
			rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("unreadable %s row, refusing to checkpoint past it: %w", opID, err),
			}
		}
		if parsed.Actor.UserID == "" || parsed.StartingAt == "" {
			rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("%s row carries no actor or bucket, so it has no identity to store under", opID),
			}
		}
		bucket, err := time.Parse(time.RFC3339, parsed.StartingAt)
		if err != nil {
			rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("unparseable bucket start %q on a %s row: %w", parsed.StartingAt, opID, err),
			}
		}
		// The ExternalID's date and the DataDate are one UTC day: a bucket start sent with an
		// offset or off midnight still files under, and is dated, midnight UTC of its date.
		u := bucket.UTC()
		day := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
		// The product is part of the identity and is a documented closed set. An absent one
		// groups under unknownProduct rather than being dropped: the rows are real spend.
		product := parsed.Product
		if product == "" {
			product = unknownProduct
		}
		if err := acc.add(parsed.Actor.UserID+"|"+day.Format(dateLayout)+"|"+product,
			day, row); err != nil {
			return fmt.Errorf("holding %s rows for %s: %w", opID, day.Format(dateLayout), err)
		}
	}
	return nil
}

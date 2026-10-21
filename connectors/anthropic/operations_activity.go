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

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// activityPageSize is the vendor maximum, so a day usually resolves in one request.
const activityPageSize = 1000

// maxPagesPerDay bounds one day's walk. The activity endpoints carry no has_more, so an
// absent cursor is the only end condition — this catches a vendor that never stops
// returning one, rather than spinning until the run's deadline.
const maxPagesPerDay = 500

// activityActor is the one field lifted out of an activity row, and only one: the actor's
// identifier, which is half the record's identity. Everything else stays in the payload,
// because several fields punish eager parsing — amounts are decimal strings and several
// metrics are nullable in a way that separates zero from not-applicable.
type activityActor struct {
	User struct {
		ID string `json:"id"`
	} `json:"user"`
}

// FetchAnalyticsUsersOp stores one record per actor per calendar day.
type FetchAnalyticsUsersOp struct{}

func (o *FetchAnalyticsUsersOp) OperationID() sdk.OperationID { return opFetchAnalyticsUsers }

func (o *FetchAnalyticsUsersOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	cur, err := runActivityWalk(ctx, params, o.OperationID(), activityRequestUsers, false)
	if err != nil {
		return nil, fmt.Errorf("walking per-user activity: %w", err)
	}
	return cur, nil
}

// FetchAnalyticsSkillsOp stores one record per actor per calendar day, holding that
// actor's skill rows as an array.
//
// The roll-up is what keeps the identity stable: a skill name is renameable by the
// customer, so a skill in a uniqueness key means a rename silently splits one
// actor-day into two rows. As an array it simply leaves the next day's payload, which
// needs no tombstone.
type FetchAnalyticsSkillsOp struct{}

func (o *FetchAnalyticsSkillsOp) OperationID() sdk.OperationID { return opFetchAnalyticsSkills }

func (o *FetchAnalyticsSkillsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	cur, err := runActivityWalk(ctx, params, o.OperationID(), activityRequestSkills, true)
	if err != nil {
		return nil, fmt.Errorf("walking per-user skill usage: %w", err)
	}
	return cur, nil
}

// activityRequestFn builds one page request. The two activity operations differ only in
// which endpoint they address, so the walk is shared and this is the difference.
type activityRequestFn func(date string, page string) proto.Message

func activityRequestUsers(date, page string) proto.Message {
	return &vgpb.AnthropicAnalyticsUsersRequest{Date: date, Limit: activityPageSize, Page: page}
}

func activityRequestSkills(date, page string) proto.Message {
	return &vgpb.AnthropicAnalyticsSkillsRequest{Date: date, Limit: activityPageSize, Page: page}
}

// activityPage is the shape both activity responses share once the concrete type is known.
type activityPage struct {
	rows     [][]byte
	nextPage string
}

func readActivityPage(resp proto.Message) (activityPage, bool) {
	switch r := resp.(type) {
	case *vgpb.AnthropicAnalyticsUsersResponse:
		return activityPage{rows: r.GetData(), nextPage: r.GetNextPage()}, true
	case *vgpb.AnthropicAnalyticsSkillsResponse:
		return activityPage{rows: r.GetData(), nextPage: r.GetNextPage()}, true
	}
	return activityPage{}, false
}

// runActivityWalk walks whole calendar days oldest first, emitting one record per actor per
// day and checkpointing after each day.
//
// Oldest-first is not a preference: a run that stops partway must leave a CONTIGUOUS
// covered span behind it, and only ascending order does that.
//
// aggregate says whether the endpoint's grain is finer than the record's identity. /users
// returns one row per actor per day, so its row is stored as-is. /skills returns one row per
// actor per SKILL, so those rows are grouped into one array per actor-day — a skill name is
// renameable, so keying on it would let a rename split one actor-day into
// two rows.
func runActivityWalk(
	ctx context.Context, params *sdk.ExecutionParams,
	opID sdk.OperationID, buildRequest activityRequestFn, aggregate bool,
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

	stored, coveredDay, present, err := parseActivityPosition(params.Cursor)
	if err != nil {
		// Treated as absent rather than fatal: re-reading a covered day is safe (see
		// recordAccumulator on the update-in-place write), while refusing to run leaves the
		// organisation stalled until an operator intervenes.
		logger.WarnWithCtx(ctx, "claude-code activity: unreadable position, restarting from the lookback", zap.Error(err))
		positionUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
		stored, coveredDay, present = activityPosition{}, time.Time{}, false
	}

	notePositionFarBehind(opID, orgID, coveredDay)

	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	window := resolveActivityWindow(coveredDay, present, latest, settings.GetLookbackDays(), opts.overlapDays())

	logger.Info(ctx, "claude-code activity: resolved window",
		zap.Bool("first_run", window.FirstRun),
		zap.Time("from", window.From), zap.Time("through", window.Through))

	if !window.HasWork() && len(stored.FailedDays) == 0 {
		return params.Cursor, nil
	}

	gw := params.VendorGateway.ForGroup(constants.GroupAnthropicAnalytics)
	// Seeded from the stored position, not empty. The window re-enters already-covered days
	// for late arrivals, so the first days this loop completes are EARLIER than the stored
	// point; writing them unguarded moves the position backwards, and a run killed just after
	// that first checkpoint leaves the cursor behind where it started — ratcheting back a day
	// per run with the window widening each time.
	covered := activityPosition{}
	if present {
		covered = stored
	}

	// readDay reads one whole day and emits it, or emits nothing. notReady reports the
	// vendor's answer that the day is not aggregated yet, naming the latest day it has.
	readDay := func(dayToRead time.Time) (named time.Time, notReady bool, err error) {
		date := dayToRead.Format(dateLayout)
		pageCursor := ""

		// The accumulator's lifetime is one day — the unit this walk checkpoints. See
		// recordAccumulator for why a per-page lifetime is silently wrong.
		acc := newRecordAccumulator(opID, orgID)
		dayComplete := false
		restarts := 0

		for page := 0; page < maxPagesPerDay; page++ {
			pagesRead.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			resp, err := gw.Execute(ctx, buildRequest(date, pageCursor))
			if err != nil {
				// The vendor telling us a day is not aggregated yet is a normal condition,
				// not a failure: stop at the day it names and keep the position where it
				// is, so the next run picks the day up rather than skipping it forever.
				available, notAggregated := latestAvailableDay(ctx, err, dayToRead, pageCursor != "")
				if isBadRequest(err) {
					// Classified by MEANING before anything else: the not-ready answer and the
					// changed-parameter defect are the same status code, and counted together
					// the defect hides inside the normal condition.
					kind := badRequestQueryDefect
					if notAggregated {
						kind = badRequestNotAggregated
					}
					vendorBadRequest.WithLabelValues(tenantlabel.OrgID(orgID), string(opID), kind).Inc()
				}
				if notAggregated {
					logger.Info(ctx, "claude-code activity: vendor has not aggregated this day yet, stopping",
						zap.String("requested", date), zap.Time("latest_available", available))
					return available, true, nil
				}
				// An expired cursor loses the page position, not the data: restart this day
				// from its first page. Safe because the accumulator has not emitted yet.
				//
				// Bounded separately from the page budget. Sharing it would let repeated
				// restarts exhaust the pages and then report "did not finish within N pages",
				// which names the wrong cause.
				if isExpiredCursor(err) && pageCursor != "" {
					restarts++
					if restarts > maxCursorRestarts {
						return time.Time{}, false, &sdk.SyncError{
							ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
							Kind: sdk.ErrKindVendorError,
							Cause: fmt.Errorf("%s cursor for %s expired %d times; the day cannot be read within one cursor lifetime",
								opID, date, restarts),
						}
					}
					logger.WarnWithCtx(ctx, "claude-code activity: page cursor expired, restarting the day",
						zap.String("date", date), zap.Int("restarts", restarts))
					pageCursor = ""
					acc = newRecordAccumulator(opID, orgID)
					page = 0
					continue
				}
				// Returned with context but WITHOUT a new SyncError wrapper: the gateway's
				// own carries the kind, the terminality and the vendor's Retry-After, and
				// re-classifying it as a generic vendor error is what stops a revoked
				// credential from ever reaching auth_revoked and loses the retry hint.
				return time.Time{}, false, fmt.Errorf("reading %s for %s: %w", opID, date, err)
			}

			pageData, ok := readActivityPage(resp)
			if !ok {
				return time.Time{}, false, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
					Kind:  sdk.ErrKindInternal,
					Cause: fmt.Errorf("unexpected response type %T for %s", resp, opID),
				}
			}

			if err := collectActivityRows(acc, opID, orgID, dayToRead, pageData.rows); err != nil {
				return time.Time{}, false, fmt.Errorf("reading %s rows for %s: %w", opID, date, err)
			}

			// No has_more on these endpoints, so an absent cursor is the only end condition
			// the vendor offers.
			if pageData.nextPage == "" {
				dayComplete = true
				break
			}
			pageCursor = pageData.nextPage
		}

		if !dayComplete {
			// The cap was exhausted with pages still pending. Emitting what was read and
			// advancing would declare the day covered and drop the rest permanently — the
			// position would move past it and no retry would come back. So nothing is
			// emitted for this day, the position stays where the last COMPLETE day left it,
			// and the run fails so it is visible rather than reported as a healthy sync.
			return time.Time{}, false, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind: sdk.ErrKindVendorError,
				Cause: fmt.Errorf("%s did not finish %s within %d pages; refusing to advance past unread pages",
					opID, date, maxPagesPerDay),
			}
		}

		// The day is whole: emit it as one unit.
		if err := acc.emitAll(ctx, params.Sink, !aggregate); err != nil {
			return time.Time{}, false, fmt.Errorf("emitting %s records for %s: %w", opID, date, err)
		}
		return time.Time{}, false, nil
	}

	// The days an earlier run failed come first, then the window; a failed day inside the
	// window is read once.
	days := window.Days()
	if present {
		// Listed days older than the lookback can no longer be read: dropped, each logged.
		covered.FailedDays = keepRecentDays(ctx, opID, covered.FailedDays, oldestReadable(latest, settings.GetLookbackDays(), 0))
		days = withFailedDays(covered.FailedDays, days)
	}
	// The days that failed in THIS run, for the error the run returns.
	var failed []string
	var firstFailure error
	var notReadyFrom time.Time
	notReady := false

	for _, dayToRead := range days {
		date := dayToRead.Format(dateLayout)
		named, stop, err := readDay(dayToRead)
		if stop {
			notReady, notReadyFrom = true, named
			break
		}
		if err != nil && ctx.Err() == nil && isUnit(covered.FailedDays, date) {
			// A listed day that fails again, a refused one included, stays listed and the
			// walk goes on: a listed day that keeps failing must not stall every later run.
			failed = append(failed, date)
			if firstFailure == nil {
				firstFailure = err
			}
			logger.WarnWithCtx(ctx, "claude-code activity: listed day failed again; it stays listed",
				zap.String("date", date), zap.Error(err))
			continue
		}
		if err != nil {
			// A failure of this day alone is listed and walked past; the next run reads the day
			// again. Anything that would fail every later day too stops the run here, as does a
			// full list: the position then stays on the day before this one.
			listed, ok := listFailedUnit(covered.FailedDays, date)
			if !unitLocal(ctx, err) || !ok {
				return nil, err
			}
			covered.FailedDays = listed
			failed = append(failed, date)
			if firstFailure == nil {
				firstFailure = err
			}
			logger.WarnWithCtx(ctx, "claude-code activity: day failed, walking past it; the next run reads it again",
				zap.String("date", date), zap.Error(err))
			if !present || dayToRead.After(coveredDay) {
				covered.CoveredThrough = date
				coveredDay = dayToRead
				present = true
			}
			checkpointPosition(ctx, params, opID, orgID, covered, zap.String("date", date))
			continue
		}
		// A day read in full leaves the failed list, whichever run listed it.
		covered.FailedDays = removeUnit(covered.FailedDays, date)

		// Advance past the day — forward only, for the reason the seeding comment above gives.
		if !present || dayToRead.After(coveredDay) {
			covered.CoveredThrough = date
			coveredDay = dayToRead
			present = true
			// Counted on the ADVANCE. The window re-enters already-covered days for late
			// arrivals, so counting every day walked would report progress on a run that
			// advanced nothing — which is the distinction this series exists to draw.
			periodsCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
		}

		// Persist the day before starting the next one — see checkpointPosition.
		checkpointPosition(ctx, params, opID, orgID, covered, zap.String("date", date))
	}

	if len(failed) > 0 {
		// Every listed day is already checkpointed; the orchestrator keeps no cursor from a
		// failed run. The run fails, retryably, so a day that keeps failing keeps the run
		// failing rather than passing as a healthy sync.
		return nil, failedUnitsError(opID, orgID, "day", failed, firstFailure)
	}

	if notReady {
		// A run that covered NOTHING reached nothing, so it yields — and that run also fails,
		// at storeCoveredPosition. Counting it as a cover would hide, in the coverage series,
		// an organisation whose sync never started, which is what makes this the branch worth
		// writing down.
		//
		// Once anything is covered, the cover branch is the invariant one: the days are walked
		// in ascending order and each completion advances coveredDay, so the request is at
		// most a day past the coverage, and a not-ready answer names a day strictly earlier
		// than the request. The comparison survives for a stored position older than the
		// vendor floor, whose window starts at the floor instead. See runCovered for what a
		// cover means and why it is judged against the vendor rather than the clock.
		org := tenantlabel.OrgID(orgID)
		if present && !coveredDay.Before(notReadyFrom) {
			runCovered.WithLabelValues(org, string(opID)).Inc()
		} else {
			runYielded.WithLabelValues(org, string(opID), yieldNotAggregated).Inc()
		}
		cur, storeErr := storeCoveredPosition(opID, orgID, covered, coveredDay)
		if storeErr != nil {
			return nil, fmt.Errorf("storing the position after a not-yet-aggregated day: %w", storeErr)
		}
		return cur, nil
	}

	runCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()

	cur, err := storeCoveredPosition(opID, orgID, covered, coveredDay)
	if err != nil {
		return nil, fmt.Errorf("storing the position after the %s walk: %w", opID, err)
	}
	return cur, nil
}

// collectActivityRows files each row under its identity; the walk emits once the day is whole.
func collectActivityRows(
	acc *recordAccumulator, opID sdk.OperationID, orgID string,
	dayRead time.Time, rows [][]byte,
) error {
	// A row without a readable actor is a hard failure rather than a skip, because
	// this day checkpoints once it completes, so a skipped row is permanently
	// lost — the position moves past it and no retry comes back.
	//
	// These rows are per-actor because /users is actor-grained and /skills is requested with
	// group_by[]=user_id, so a row without an actor says the response shape changed. That
	// deserves one loud stop, not thousands of warnings.
	for _, row := range rows {
		var actor activityActor
		if err := json.Unmarshal(row, &actor); err != nil {
			rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("unreadable %s row, refusing to checkpoint past it: %w", opID, err),
			}
		}
		if actor.User.ID == "" {
			rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("%s row carries no actor id, so it has no identity to store under", opID),
			}
		}
		// Identity: the actor and the period, both un-renameable.
		if err := acc.add(actor.User.ID+"|"+dayRead.Format(dateLayout), dayRead, row); err != nil {
			return fmt.Errorf("holding %s rows for %s: %w", opID, dayRead.Format(dateLayout), err)
		}
	}
	return nil
}

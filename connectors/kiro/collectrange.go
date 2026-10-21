// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// CollectRangeResult is what one repair pass did.
type CollectRangeResult struct {
	Days            int
	Copied          int
	Skipped         int
	Failed          int
	ClearedTerminal int
	// SettledTerminal counts reports this pass settled as permanently unreadable. Reported
	// separately from Failed because the two need different responses: a failure is retried,
	// a settled report never is, so it is a permanent gap the moment this number is non-zero.
	SettledTerminal int
}

// CollectRange collects a given day range for one organisation and leaves the high-water
// mark exactly where it was.
//
// This is the repair path, and the two things it does differently from a scheduled run are
// the whole reason it exists.
//
// It does not move the mark. The mark says how far forward collection has reached, and
// repairing an old day says nothing about that — moving it would either lie about progress
// or, if the range is behind the mark, do nothing at all. Both are wrong, so the mark is
// simply not touched, which is also what makes a capped first run followed by this the
// answer to deepening history rather than a second cursor axis.
//
// And it clears terminal markers for the days it is asked about, before attempting them —
// see clearTerminal for why that is what makes a repair mean anything. Clearing first means
// a still-broken object simply settles again, which is the honest outcome.
//
// It takes the operation's own execution parameters rather than building anything, so
// whatever drives it, such as the collection script's execute mode, supplies the same
// gateway, sink and object store a scheduled run uses. Nothing about the
// repair is a second implementation of collection.
//
// Idempotent for the same reasons the scheduled operation is: the destination key is
// deterministic in the organisation, day and client type, and the record identity is the
// client type — so a second pass overwrites the same object and updates the same record
// rather than accumulating either.
func (op *FetchUserActivityReportOp) CollectRange(
	ctx context.Context, params *sdk.ExecutionParams, from, to time.Time,
) (*CollectRangeResult, error) {
	orgID := params.Config.OrganizationID

	_, settings, err := ParseConfig(params.Config)
	if err != nil {
		return nil, fmt.Errorf("parsing the configuration for %s: %w", orgID, err)
	}
	if err := validateSettings(settings); err != nil {
		return nil, fmt.Errorf("the stored configuration for %s cannot address a bucket: %w", orgID, err)
	}
	src, ok := params.VendorGateway.(ObjectSource)
	if !ok {
		return nil, fmt.Errorf("gateway %T does not provide the kiro object source", params.VendorGateway)
	}
	if params.BlobStore == nil {
		return nil, fmt.Errorf("no destination object store was provided")
	}

	state, err := loadState(params.Cursor)
	if err != nil {
		return nil, fmt.Errorf("loading the cursor for %s: %w", orgID, err)
	}

	first := from.UTC().Truncate(24 * time.Hour)
	last := to.UTC().Truncate(24 * time.Hour)
	if last.Before(first) {
		return nil, fmt.Errorf("the range ends before it starts: %s to %s",
			first.Format(dayLayout), last.Format(dayLayout))
	}

	result := &CollectRangeResult{}
	markBefore := state.CoveredThrough

	for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
		result.Days++
		// The settled verdict is cleared so the object is attempted; its failure COUNT is
		// not. A repair attempt is itself an attempt, so it must accrue — clearing the
		// count each pass would let an operator repair the same unfixable object forever
		// while it never settled again and nothing said so.
		result.ClearedTerminal += state.clearTerminal(d)

		outcome, dayErr := op.collectDay(ctx, params, src, settings, state, d)
		if dayErr != nil {
			// Reported and the walk continues: the whole point of a range is that the
			// days are independent, so one unreadable day must not cost the others.
			logger.WarnWithCtx(ctx, "kiro repair: a day could not be listed",
				zap.String(logger.OrgID, orgID), zap.String("day", d.Format(dayLayout)), zap.Error(dayErr))
			result.Failed++
			// Banked even though the day failed: its verdicts were cleared above, and a
			// re-run must not find them still settled.
			if cerr := checkpointRepair(ctx, params, state, markBefore); cerr != nil {
				return result, fmt.Errorf("banking %s after it could not be listed: %w",
					d.Format(dayLayout), cerr)
			}
			continue
		}
		result.Copied += len(outcome.Collected)
		result.Skipped += outcome.Skipped
		result.Failed += len(outcome.Failed)

		if len(outcome.Collected) == 0 && outcome.Skipped == 0 &&
			len(outcome.Failed) == 0 && outcome.Unplaceable == 0 {
			// The copied counter already fires from the shared day walk, so it counts a
			// repair's copies. Leaving the empty days uncounted would make the two sides
			// of the copied-versus-empty ratio describe different populations, which is
			// the one thing that ratio must not do.
			EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(orgID)).Inc()
		}

		// A collected object forgets its own history, exactly as it does on the scheduled
		// path: what a repair fixed should not count against it later.
		for _, key := range outcome.Collected {
			state.clearObjectFailure(d, key)
		}

		for _, f := range outcome.Failed {
			if errors.Is(f.Err, ErrObjectUnreadable) {
				// Re-accrues from zero, because the markers were cleared above. A repair
				// attempted against a cause nobody fixed settles again, which is the
				// truthful result rather than a silent success.
				if state.recordFailure(d, f.Key, f.Err.Error()) {
					result.SettledTerminal++
					// Counted here as well as on the scheduled path, so the counter means the
					// same thing wherever this runs. A caller without a metrics endpoint reads
					// SettledTerminal from the result.
					ReportsAbandonedTotal.WithLabelValues(tenantlabel.OrgID(orgID)).Inc()
					logger.Error(ctx, "kiro repair: object settled again as permanently unreadable",
						zap.String(logger.OrgID, orgID), zap.String("object", f.Key), zap.Error(f.Err))
				}
			}
		}

		// Per day, not once per range, so an interruption keeps the bookkeeping every
		// completed day produced — the verdicts it cleared and the counts it accrued.
		//
		// It does NOT make the range resumable, and the distinction matters to whoever
		// re-runs one. The cursor holds no repair position — this path leaves the high-water
		// mark alone by contract, and the report-ingestion design refuses a second cursor
		// axis — so a re-run starts at the original first day and walks the lot again. Safe
		// rather than cheap: the destination keys and record identities are deterministic,
		// so the re-walk overwrites rather than duplicates, and bounding the cost of it is
		// what the range cap is for.
		if cerr := checkpointRepair(ctx, params, state, markBefore); cerr != nil {
			return result, fmt.Errorf("banking %s: %w", d.Format(dayLayout), cerr)
		}
	}

	logger.Info(ctx, "kiro repair: range collected",
		zap.String(logger.OrgID, orgID),
		zap.String("from", first.Format(dayLayout)), zap.String("to", last.Format(dayLayout)),
		zap.Int("days", result.Days), zap.Int("copied", result.Copied),
		zap.Int("skipped", result.Skipped), zap.Int("failed", result.Failed),
		zap.Int("cleared_terminal", result.ClearedTerminal))
	return result, nil
}

// checkpointRepair persists the cursor, asserting first that the high-water mark has not
// moved.
//
// The assertion is here rather than at the end of the range because this is the only place
// the cursor is written: a future edit that raised the mark would otherwise reach storage
// before anything checked it.
func checkpointRepair(
	ctx context.Context, params *sdk.ExecutionParams, state syncState, markBefore string,
) error {
	if state.CoveredThrough != markBefore {
		return fmt.Errorf("the repair moved the high-water mark from %q to %q, which it must never do",
			markBefore, state.CoveredThrough)
	}
	if params.Checkpoint == nil {
		return nil
	}
	cursor, err := state.toCursor()
	if err != nil {
		return fmt.Errorf("serialising the cursor during the repair: %w", err)
	}
	if err := params.Checkpoint(ctx, cursor); err != nil {
		return fmt.Errorf("persisting the cursor during the repair: %w", err)
	}
	return nil
}

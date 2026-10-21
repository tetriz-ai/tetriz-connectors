// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// notePositionFarBehind counts a stored position further behind the clock than a converging
// backfill would leave it.
//
// Called where the position is LOADED, not where it is stored, and that is the whole point: a
// position is most stuck exactly when runs stop succeeding, and a store-time measurement is
// silent on every run that fails before it. Loading happens first, whatever the run then does.
//
// See positionFarBehind for why this is a counter rather than a gauge of the age.
func notePositionFarBehind(opID sdk.OperationID, orgID string, covered time.Time) {
	// The zero time is the guard that matters, not the absence of a position: every parse path
	// that reports no position also returns a zero day, while a zero day reaching time.Since
	// would read as two millennia behind and count every run.
	if covered.IsZero() {
		return
	}
	if time.Since(covered) > maxHealthyPositionAge {
		positionFarBehind.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
	}
}

// checkpointPosition persists progress mid-run, so a run killed partway does not repeat
// everything it already read. Without it the returned cursor is the only progress record
// and the orchestrator discards that on any error — a first sync killed on day 87 of 90
// would repeat days 1-86 on every attempt and never converge.
//
// A failure here is logged, not returned: the walk's own progress is still sound and the
// returned cursor still carries it. The two failures are distinguished because they mean
// different things — a failed write may be transient, while a position that will not encode
// will fail identically on every later checkpoint, making the whole run unresumable.
func checkpointPosition(
	ctx context.Context, params *sdk.ExecutionParams,
	opID sdk.OperationID, orgID string, pos any, extra ...zap.Field,
) {
	if params.Checkpoint == nil {
		return
	}

	fields := append([]zap.Field{}, extra...)

	cur, encErr := encodeCursor(pos)
	if encErr != nil {
		logger.WarnWithCtx(ctx, "claude-code: could not encode a checkpoint position, run is unresumable",
			append(fields, zap.Error(encErr))...)
		return
	}
	if cpErr := params.Checkpoint(ctx, cur); cpErr != nil {
		logger.WarnWithCtx(ctx, "claude-code: checkpoint failed, continuing",
			append(fields, zap.Error(cpErr))...)
	}
}

// storeCoveredPosition turns a walk's final position into the cursor to return, refusing to
// report an empty one as success.
//
// A zero coveredDay means the walk completed no unit at all, which a zero or negative
// window is the way to reach — a genuinely empty organisation still advances, since every
// completed unit records coverage whatever its row count. Returning an empty cursor for that
// would read back as absent, so the run would look like a healthy first sync and repeat
// identically forever.
//
// It also implies no stored position, because both walks seed from the stored one, so a
// present position always carries a day.
//
// The day is passed already parsed rather than re-derived from the position's string form: this
// walk wrote that string, so re-parsing it here could only fail on our own bug and would have
// nowhere to report it.
func storeCoveredPosition(
	opID sdk.OperationID, orgID string, pos any, coveredDay time.Time,
) (*sdk.Cursor, error) {
	if coveredDay.IsZero() {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
			Kind: sdk.ErrKindVendorError,
			Cause: fmt.Errorf(
				"%s covered nothing and has no stored position; reporting success would hide a stall", opID),
		}
	}

	cur, err := encodeCursor(pos)
	if err != nil {
		return nil, fmt.Errorf("encoding the %s position: %w", opID, err)
	}

	return cur, nil
}

// maxFailedUnits bounds the failed days or chunks a position lists. At the bound the walk
// stops advancing, as it did before failed units were tracked: a further unit that fails
// ends the run there, so the position stays before it and no listed unit is ever dropped.
const maxFailedUnits = 31

// unitLocal reports whether err is a failure of one unit (a day or a chunk) alone, which a
// walk lists and walks past: a vendor error, a missing resource or a timeout for that unit.
// A rate limit, an auth failure, a refused request (our own query), a sink failure, an
// ended context or a plain error would fail the next unit too, so the run stops on it.
func unitLocal(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return false
	}
	// A 4xx other than 404 is our own request refused, whatever kind it was classified as:
	// the same request shape fails every unit.
	if s := syncErr.HTTPStatus; s >= 400 && s < 500 && s != 404 {
		return false
	}
	switch syncErr.Kind {
	case sdk.ErrKindVendorError, sdk.ErrKindNotFound, sdk.ErrKindTimeout:
		return true
	default:
		return false
	}
}

// listFailedUnit returns units with unit added, sorted. ok is false, and units unchanged,
// when the list is full and unit is not already on it.
func listFailedUnit(units []string, unit string) ([]string, bool) {
	for _, u := range units {
		if u == unit {
			return units, true
		}
	}
	if len(units) >= maxFailedUnits {
		return units, false
	}
	out := append(append([]string(nil), units...), unit)
	sort.Strings(out)
	return out, true
}

// replaceUnit returns units with unit replaced by with, in place of it. It never aliases a
// list it changes.
func replaceUnit(units []string, unit, with string) []string {
	for i, u := range units {
		if u == unit {
			out := append([]string(nil), units...)
			out[i] = with
			return out
		}
	}
	return units
}

// removeUnit returns units without unit. It never aliases a list it changes.
func removeUnit(units []string, unit string) []string {
	for i, u := range units {
		if u == unit {
			return append(append([]string(nil), units[:i]...), units[i+1:]...)
		}
	}
	return units
}

// oldestReadable is the earliest day a listed unit may still be read: the later of the
// vendor floor, the lookback before latest and, when vendorRange is positive, the vendor's
// own range before latest.
func oldestReadable(latest time.Time, lookbackDays, vendorRange int) time.Time {
	oldest := latest.AddDate(0, 0, -(lookbackDays - 1))
	if vendorRange > 0 {
		if r := latest.AddDate(0, 0, -vendorRange); r.After(oldest) {
			oldest = r
		}
	}
	if oldest.Before(vendorFloorDate) {
		oldest = vendorFloorDate
	}
	return oldest
}

// keepRecentDays returns days without those before oldest, which can no longer be read;
// each drop is logged. oldest is the first readable day, so a day exactly at it is kept.
func keepRecentDays(ctx context.Context, opID sdk.OperationID, days []string, oldest time.Time) []string {
	var kept []string
	for _, d := range days {
		day, err := time.ParseInLocation(dateLayout, d, time.UTC)
		if err == nil && day.Before(oldest) {
			logger.WarnWithCtx(ctx, "claude-code activity: failed day is older than the lookback; dropping it",
				zap.String("operation", string(opID)), zap.String("date", d))
			continue
		}
		kept = append(kept, d)
	}
	return kept
}

// isUnit reports whether unit is in units.
func isUnit(units []string, unit string) bool {
	for _, u := range units {
		if u == unit {
			return true
		}
	}
	return false
}

// withFailedDays returns the failed days, then the window's days, each once.
func withFailedDays(failed []string, window []time.Time) []time.Time {
	inWindow := make(map[string]bool, len(window))
	for _, d := range window {
		inWindow[d.Format(dateLayout)] = true
	}
	var out []time.Time
	for _, f := range failed {
		if inWindow[f] {
			continue
		}
		// Already validated when the position was parsed.
		d, err := time.ParseInLocation(dateLayout, f, time.UTC)
		if err != nil {
			continue
		}
		out = append(out, d)
	}
	return append(out, window...)
}

// failedUnitsError is what a walk that listed failed units returns: retryable, of the first
// failure's kind, naming every unit.
func failedUnitsError(opID sdk.OperationID, orgID, unit string, units []string, first error) error {
	kind := sdk.ErrKindVendorError
	var syncErr *sdk.SyncError
	if errors.As(first, &syncErr) {
		kind = syncErr.Kind
	}
	return &sdk.SyncError{
		ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
		Kind: kind, Retryable: true,
		Cause: fmt.Errorf("%s: %d %s(s) failed and were walked past, to be read again next run (%s): %w",
			opID, len(units), unit, strings.Join(units, ", "), first),
	}
}

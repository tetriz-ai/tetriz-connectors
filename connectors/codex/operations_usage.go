// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// maxPagesPerDay bounds one day's walk, and it is load-bearing rather than defensive.
//
// Nothing in the response can be trusted to terminate the loop: the envelope is modelled on
// a third-party contract rather than a published schema, so both a `has_more` that never
// flips false and a `has_more` of true beside an unchanged cursor are live possibilities.
// Each is an unbounded loop. Reaching this cap ends the day with a distinct failure and NOT
// a silent stop — a day that ended silently would be recorded as covered and never re-read.
const maxPagesPerDay = 500

// maxRowsPerDay bounds what one day may hold before anything is emitted.
//
// A whole day is accumulated before the first record is emitted — deliberately, since a day
// that emitted part of itself and then failed would be indistinguishable from a complete one
// — so this is the bound on resident memory. The page cap does not provide it: that bounds
// REQUESTS, and `limit` is a hint the vendor may ignore, so five hundred pages of ten
// thousand rows is a bound in name only.
const maxRowsPerDay = 200_000

// usageRow is the sliver of a usage row this walk reads. Four values, and no counter among
// them: the schema grades every counter nullable in a way that separates absent from zero, so
// a parser flattening those would understate every later aggregate, and the credit figure must
// never become a currency. Everything not named here stays inside the payload verbatim.
type usageRow struct {
	// UserID is half the record's identity.
	UserID string `json:"user_id"`
	// BucketStartTime is the day the vendor says this row belongs to, in Unix seconds. Read
	// so it can be checked against the day requested — see checkBucket.
	BucketStartTime int64 `json:"bucket_start_time"`
	Actor           struct {
		// Email is nullable, so it is copied into metadata rather than trusted as identity.
		Email string `json:"email"`
	} `json:"actor"`
}

// FetchUsageOp walks whole UTC days oldest first, storing one record per developer per day.
type FetchUsageOp struct{}

func (o *FetchUsageOp) OperationID() sdk.OperationID { return OpFetchUsage }

// Execute walks the resolved window a day at a time, checkpointing after each whole day.
//
// Oldest-first is not a preference: a run that stops partway must leave a CONTIGUOUS covered
// span behind it, and only ascending order does that.
func (o *FetchUsageOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	settings, err := ParseSettings(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Cause: fmt.Errorf("parsing settings: %w", err),
		}
	}

	opts, err := parseOperationOptions(params.Config, OpFetchUsage)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Cause: err,
		}
	}

	stored, coveredDay, present, err := positionFor(params.Cursor, settings.WorkspaceID)
	if err != nil {
		// Treated as absent rather than fatal: re-reading a covered day is safe, while
		// refusing to run leaves the organisation stalled until an operator intervenes.
		logger.WarnWithCtx(ctx, "codex usage: unreadable position, restarting from the lookback",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		positionUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
		stored, coveredDay, present = position{}, time.Time{}, false
	}

	// Yesterday, never today. OpenAI publishes no aggregation-lag statement, and a
	// partially-aggregated day read as complete is a wrong number no later run revisits —
	// the day would be marked covered and never asked for again.
	latest := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	days := resolveWindow(coveredDay, present, latest, settings.GetLookbackDays(), opts.overlapDays())
	if len(days) == 0 {
		// The only way a resumed walk plans nothing is a stored day AHEAD of yesterday — a
		// restored backup, a clock-skewed writer, a hand edit that typed the wrong year.
		// Returning the cursor with no error would report a healthy sync that ingests
		// nothing, every night, indefinitely: a frozen position reported as success is
		// indistinguishable from a working one in every metric.
		runYielded.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage), yieldPositionAhead).Inc()
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid,
			Cause: fmt.Errorf("stored position %s is ahead of the newest readable day %s, so no day can be read; the position needs repair",
				coveredDay.Format(dateLayout), latest.Format(dateLayout)),
		}
	}

	logger.Info(ctx, "codex usage: resolved window",
		zap.String(logger.OrgID, orgID), zap.Bool("first_run", !present),
		zap.String("from", days[0].Format(dateLayout)),
		zap.String("through", days[len(days)-1].Format(dateLayout)),
		zap.Strings("retrying", stored.FailedDays))
	// Listed days older than the lookback cannot be read any more: dropped, each logged.
	stored.FailedDays = keepRecentDays(ctx, orgID, stored.FailedDays, latest.AddDate(0, 0, -(settings.GetLookbackDays()-1)))
	// The days an earlier run failed come first, then the window. A failed day inside the
	// window is read once.
	days = withFailedDays(stored.FailedDays, days)

	gw := params.VendorGateway.ForGroup(constants.GroupCodexAnalytics)

	// Seeded from the stored position, not empty. The window re-enters already-covered days
	// for late arrivals, so the first days this loop completes are EARLIER than the stored
	// point; writing them unguarded would move the position backwards, and a run killed just
	// after that first checkpoint would leave the cursor behind where it started —
	// ratcheting back a day per run with the window widening each time.
	// Seeded from the STORED struct, not rebuilt field by field from the parsed day. The two
	// are equivalent while the position has one field, and stop being equivalent the moment a
	// second is added — a rebuild would silently drop it on every run, and the JSON contract
	// test would not notice.
	covered := stored
	// Stamped every run.
	covered.WorkspaceID = settings.WorkspaceID
	// The days that failed in THIS run, for the error the run returns.
	var failed []string
	var firstFailure error

	for _, day := range days {
		// The day is computed ONCE and used for both halves of the record's identity: the
		// trailing component of the key and the DataDate column. Deriving them separately is
		// the one way this encoding can hurt itself — the day appears twice in the uniqueness
		// tuple, so two derivations that ever disagreed would split a single developer-day
		// into two rows, the precise failure the identity exists to prevent.
		date := day.Format(dateLayout)

		emitted, pages, err := o.readDay(ctx, params, gw, opts, settings.WorkspaceID, day, date)
		if err != nil {
			// A run that ran out of vendor requests mid-window is the ordinary backfill
			// shape, not a defect — counted so the covered/yielded ratio can show whether it
			// is converging. Any other failure is already loud on its own error path.
			var syncErr *sdk.SyncError
			if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindRateLimit {
				runYielded.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage), yieldBudget).Inc()
			}
			// A day already listed that fails again, for any reason, stays listed and the walk
			// goes on: a listed day the vendor refuses must not stall every later run. A
			// failure of an unlisted day alone is listed and walked past; the next run reads
			// the day again. Anything that would fail every later day too (the budget, the
			// credential, the run's context, the sink) stops the run here, as does a full
			// list: the position then stays on the day before this one.
			listedAlready := ctx.Err() == nil && isFailedDay(covered.FailedDays, date)
			if !listedAlready && (!dayLocal(ctx, err) || !listFailedDay(&covered, date)) {
				// Returned unwrapped: every error readDay produces already names the day, and its
				// SyncErrors carry the connector, operation and organisation. A wrap here would
				// say the day twice and add nothing a reader can use.
				return nil, err //nolint:wrapcheck // see above
			}
			failed = append(failed, date)
			if firstFailure == nil {
				firstFailure = err
			}
			logger.WarnWithCtx(ctx, "codex usage: day failed, walking past it; the next run reads it again",
				zap.String(logger.OrgID, orgID), zap.String("date", date), zap.Error(err))
			if !present || day.After(coveredDay) {
				covered.CoveredThrough = date
			}
			checkpoint(ctx, params, orgID, covered, zap.String("date", date))
			continue
		}
		// A day read in full leaves the failed list, whichever run listed it.
		covered.FailedDays = removeDay(covered.FailedDays, date)
		// A day that fetched several pages and stored nothing means a page was read and
		// dropped. Counted here rather than derived in Prometheus, which cannot tell a
		// multi-page day from several single-page days at the aggregate level.
		if pages > 1 && emitted == 0 {
			daysWithoutRecords.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
		}

		// Forward only, for the reason the seeding comment above gives — and the counter goes
		// inside this branch on purpose: a day re-read from the trailing overlap completes
		// without gaining ground, so counting completions would show movement through a stall.
		if !present || day.After(coveredDay) {
			covered.CoveredThrough = date
			daysCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
		}
		// Checkpointed per DAY, not once at the end, and this is what makes a first sync
		// possible at all. The per-organisation request budget is small enough that a
		// ninety-day first sync spans several runs; a walk that only returned its cursor on
		// completion would have every one of those runs die mid-window, discard the days it
		// had finished, and start again from day one — reading the same early days forever
		// and never reaching the later ones.
		checkpoint(ctx, params, orgID, covered, zap.String("date", date))
		logger.Info(ctx, "codex usage: day covered",
			zap.String(logger.OrgID, orgID), zap.String("date", date), zap.Int("records", emitted))
	}

	if len(failed) > 0 {
		// The position, failed days included, is already checkpointed; the orchestrator
		// keeps no cursor from a failed run. The run fails, not terminally, so the days are
		// read again by a retry or the next run, and a day that keeps failing keeps the run
		// failing rather than passing as a healthy sync.
		return nil, failedDaysError(orgID, failed, firstFailure)
	}

	runCovered.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()

	cur, err := storePosition(covered)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
			Kind: sdk.ErrKindInternal, Cause: err,
		}
	}
	return cur, nil
}

// readDay pages one whole UTC day and emits its records, or fails without emitting any.
//
// Nothing is emitted until the day is whole. A day that emitted what it had and then failed
// would be indistinguishable from a complete one to the caller, which would advance past it.
func (o *FetchUsageOp) readDay(
	ctx context.Context, params *sdk.ExecutionParams, gw sdk.VendorGateway,
	opts operationOptions, workspaceID string, day time.Time, date string,
) (emitted int, pages int, err error) {
	orgID := params.Config.OrganizationID

	type held struct {
		externalID string
		payload    json.RawMessage
		// email travels to metadata, never to identity: it is nullable, and a nullable value
		// in a dedup key forks a row the day the vendor fills it in.
		email string
	}
	var (
		rows       []held
		seen       = map[string]struct{}{}
		seenCursor = map[string]struct{}{}
		pageCursor string
		complete   bool
		pagesRead  int
	)

	// A day is [00:00:00, 24:00:00) in UTC, sent as Unix SECONDS because that is what the
	// vendor takes. The end is the last second of the day rather than the next day's
	// midnight, so two consecutive days cannot both claim the same boundary second.
	start := day.Unix()
	end := day.Add(24 * time.Hour).Add(-time.Second).Unix()

	for page := 0; page < maxPagesPerDay; page++ {
		pagesRead++
		pagesReadTotal.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
		resp, err := gw.Execute(ctx, &vgpb.CodexUsageRequest{
			WorkspaceId: workspaceID,
			StartTime:   start,
			EndTime:     end,
			Limit:       opts.pageSize(),
			Page:        pageCursor,
		})
		if err != nil {
			// Returned with context but WITHOUT a new SyncError wrapper: the gateway's own
			// carries the kind, the terminality and the vendor's Retry-After, and
			// re-classifying it as a generic vendor error is what stops a revoked credential
			// from ever reaching auth_revoked and loses the retry hint.
			return 0, pagesRead, fmt.Errorf("reading codex usage for %s: %w", date, err)
		}
		usage, ok := resp.(*vgpb.CodexUsageResponse)
		if !ok {
			return 0, pagesRead, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
				Kind:  sdk.ErrKindInternal,
				Cause: fmt.Errorf("unexpected response type %T for %s", resp, OpFetchUsage),
			}
		}

		for _, raw := range usage.GetUsageRows() {
			var row usageRow
			if err := json.Unmarshal(raw, &row); err != nil {
				// A hard failure rather than a skip: this day checkpoints once it completes,
				// so a skipped row is permanently lost — the position moves past it and no
				// retry comes back.
				rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
				return 0, pagesRead, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
					Kind:  sdk.ErrKindVendorError,
					Cause: fmt.Errorf("unreadable codex usage row for %s, refusing to checkpoint past it: %w", date, err),
				}
			}
			// The bucket the vendor filed this row under, checked against the day asked for.
			// A vendor that honours start_time but rounds or ignores end_time would otherwise
			// have tomorrow's row stored under today — and tomorrow's own request would store
			// it again under its real day, so one developer-day becomes two records, one of
			// them holding the other's counters. Neither the count invariant nor the
			// duplicate guard can see it, because both days look plausible.
			if err := checkBucket(row.BucketStartTime, day, date); err != nil {
				rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
				return 0, pagesRead, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
					Kind: sdk.ErrKindVendorError, Cause: err,
				}
			}
			externalID, err := recordKey(workspaceID, row.UserID, date)
			if err != nil {
				rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
				return 0, pagesRead, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
					Kind: sdk.ErrKindVendorError, Cause: err,
				}
			}
			// The endpoint's grain is one row per developer per day, so a second row under
			// one identity means the grain changed. Emitting both would silently overwrite
			// the first with the second, since the write is an update in place — so this
			// stops the run rather than picking a winner.
			if _, dup := seen[externalID]; dup {
				return 0, pagesRead, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
					Kind:  sdk.ErrKindVendorError,
					Cause: fmt.Errorf("codex returned two usage rows for %q on an endpoint whose grain is one row per developer per day", externalID),
				}
			}
			seen[externalID] = struct{}{}
			rows = append(rows, held{externalID: externalID, payload: raw, email: row.Actor.Email})
			if len(rows) > maxRowsPerDay {
				return 0, pagesRead, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
					Kind: sdk.ErrKindVendorError,
					Cause: fmt.Errorf("codex returned more than %d rows for %s, past what one day may hold before emitting",
						maxRowsPerDay, date),
				}
			}
		}

		if !usage.GetHasMore() {
			complete = true
			break
		}
		// A has_more of true beside an absent or unchanged cursor is an unbounded loop the
		// page budget alone would only end after five hundred wasted requests. Named here so
		// the failure says what the vendor did rather than "did not finish in N pages".
		next := usage.GetNextPage()
		// Every cursor is remembered, not just the previous one. An alternating cursor
		// (A→B→A→B) advances on every comparison against its predecessor, so a check against
		// only the last page would let the walk burn the whole budget and then blame the page
		// cap — naming the wrong cause, which is the failure this guard exists to prevent.
		if _, cycled := seenCursor[next]; next == "" || cycled {
			return 0, pagesRead, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("codex reported more pages for %s but returned a cursor it had already given, so the walk cannot progress", date),
			}
		}
		seenCursor[next] = struct{}{}
		pageCursor = next
	}

	if !complete {
		// The cap was exhausted with pages still pending. Emitting what was read and
		// advancing would declare the day covered and drop the rest permanently — the
		// position would move past it and no retry would come back.
		return 0, pagesRead, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
			Kind: sdk.ErrKindVendorError,
			Cause: fmt.Errorf("codex usage did not finish %s within %d pages; refusing to advance past unread pages",
				date, maxPagesPerDay),
		}
	}

	for _, r := range rows {
		record := &sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    string(OpFetchUsage),
			OrganizationID: orgID,
			ExternalID:     r.externalID,
			// Stored exactly as the vendor returned it. Nothing is parsed out beyond the
			// identity above, because no counter may be flattened and no credit figure may
			// become a currency.
			Payload:   r.payload,
			FetchedAt: time.Now().UTC(),
			DataDate:  day,
			// The four values a downstream reader needs without parsing the payload. The
			// upsert replaces the payload on a re-read, so a value not lifted here cannot be
			// recovered later from history — there is none.
			Metadata: map[string]string{
				"workspace_id":    workspaceID,
				"developer_id":    developerIDOf(r.externalID, workspaceID),
				"developer_email": r.email,
				"data_date":       date,
			},
		}
		if err := params.Sink.Emit(ctx, record); err != nil {
			// A sink failure, not a connector bug: a queue outage would otherwise read as
			// "bug in connector code" and be triaged at the wrong layer.
			return 0, pagesRead, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
				Kind:  sdk.ErrKindSinkFailure,
				Cause: fmt.Errorf("emitting record %q: %w", r.externalID, err),
			}
		}
		recordsEmitted.WithLabelValues(tenantlabel.OrgID(orgID), string(OpFetchUsage)).Inc()
	}
	return len(rows), pagesRead, nil
}

// recordKey builds the record's external identifier: the workspace, the developer and the
// day, joined by the delimiter the sibling connectors already use.
//
// All three are values the vendor cannot rename — two opaque identifiers and a day this
// design chose when it made the request. An identity must survive every field the vendor may
// rename, which is why everything the vendor could restate stays inside the payload.
//
// The workspace is in the key for a reason neither sibling faced. A developer's identifier
// is an OpenAI ACCOUNT identifier rather than a workspace-local one, so one person in two
// workspaces carries one identifier. Without the workspace, an organisation that re-pointed
// its connector would have the second workspace's rows overwrite the first's for the same
// developer and day, inside one tenant, silently.
//
// A component carrying the delimiter is refused rather than escaped: escaping would make the
// key unreadable to the operator tooling that splits on it, and the workspace half is
// already refused at connect time, so this is the developer identifier's guard.
func recordKey(workspaceID, developerID, date string) (string, error) {
	if developerID == "" {
		return "", fmt.Errorf("codex usage row for %s carries no developer id, so it has no identity to store under", date)
	}
	// A slice, not a map: with both components bad, a map's iteration order would make the
	// error name whichever the runtime picked.
	for _, c := range []struct{ name, value string }{{"workspace", workspaceID}, {"developer", developerID}} {
		name, part := c.name, c.value
		if containsDelimiter(part) {
			return "", fmt.Errorf("codex %s identifier contains the record key delimiter %q, so the row cannot be keyed", name, RecordKeyDelimiter)
		}
	}
	return workspaceID + RecordKeyDelimiter + developerID + RecordKeyDelimiter + date, nil
}

// resolveWindow returns the days to read, oldest first.
//
// The lookback bounds a FIRST sync only. A resumed walk starts at the covered point and reads
// the whole gap however wide, because narrowing it would skip the days in between while the
// position still advanced past them — the gap would be permanent and silent. That is the case
// an operator's shortened lookback would otherwise create the moment a connector is paused
// longer than it.
func resolveWindow(coveredDay time.Time, present bool, latest time.Time, lookbackDays, overlapDays int) []time.Time {
	var from time.Time
	if present {
		// The day after the covered one, less the overlap. Re-reading is safe and cheap; not
		// re-reading loses a row that arrived after its day closed.
		from = coveredDay.AddDate(0, 0, 1-overlapDays)
	} else {
		from = latest.AddDate(0, 0, -(lookbackDays - 1))
	}
	var days []time.Time
	for d := from; !d.After(latest); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	return days
}

// checkBucket rejects a row the vendor filed under a day other than the one requested.
//
// Compared as a DAY rather than against the request's exact bounds: the boundary second is a
// contract we cannot verify against a live workspace, so a row landing on the last second of
// the day belongs to that day either way.
func checkBucket(bucketStart int64, day time.Time, date string) error {
	if bucketStart == 0 {
		// Absent rather than wrong. The field is not part of the identity, so a row without
		// it is still storable under the day it was requested for — but it cannot corroborate
		// that day, which is worth saying rather than silently accepting.
		return nil
	}
	got := time.Unix(bucketStart, 0).UTC().Truncate(24 * time.Hour)
	if !got.Equal(day) {
		return fmt.Errorf("codex returned a row bucketed on %s while %s was requested, so it would be filed under a day it did not happen on",
			got.Format(dateLayout), date)
	}
	return nil
}

// developerIDOf recovers the developer component from a record key. The key is built here, so
// this reads back what recordKey wrote rather than re-deriving it from the row — one place
// decides what the identity is.
func developerIDOf(externalID, workspaceID string) string {
	rest := strings.TrimPrefix(externalID, workspaceID+RecordKeyDelimiter)
	if i := strings.LastIndex(rest, RecordKeyDelimiter); i >= 0 {
		return rest[:i]
	}
	return ""
}

// dayLocal reports whether err is a failure of one day alone, which the walk lists and
// walks past: a vendor error, a missing resource or a timeout for that day. A rate limit,
// an auth failure, a refused request (our own query), a sink failure, an ended context or
// a plain error would fail the next day too, so the run stops on it.
func dayLocal(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) {
		return false
	}
	// A 4xx other than 404 is our own request refused, whatever kind it was classified as:
	// the same request shape fails every day.
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

// listFailedDay adds date to the position's failed days, oldest first. It reports false,
// changing nothing, when the list is full and date is not already on it.
func listFailedDay(p *position, date string) bool {
	for _, d := range p.FailedDays {
		if d == date {
			return true
		}
	}
	if len(p.FailedDays) >= maxFailedDays {
		return false
	}
	p.FailedDays = append(p.FailedDays, date)
	sort.Strings(p.FailedDays)
	return true
}

// isFailedDay reports whether date is in days.
func isFailedDay(days []string, date string) bool {
	for _, d := range days {
		if d == date {
			return true
		}
	}
	return false
}

// keepRecentDays returns days without those before oldest, which the walk can no longer
// read; each drop is logged. oldest is the window's first day, so a day exactly at it is
// kept.
func keepRecentDays(ctx context.Context, orgID string, days []string, oldest time.Time) []string {
	var kept []string
	for _, d := range days {
		day, err := time.Parse(dateLayout, d)
		if err == nil && day.Before(oldest) {
			logger.WarnWithCtx(ctx, "codex usage: failed day is older than the lookback; dropping it",
				zap.String(logger.OrgID, orgID), zap.String("date", d))
			continue
		}
		kept = append(kept, d)
	}
	return kept
}

// removeDay returns days without date. It never aliases a list it changes.
func removeDay(days []string, date string) []string {
	for i, d := range days {
		if d == date {
			return append(append([]string(nil), days[:i]...), days[i+1:]...)
		}
	}
	return days
}

// withFailedDays returns the failed days, then the window's days, oldest first and each
// once.
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
		// Already validated by parsePosition.
		d, err := time.Parse(dateLayout, f)
		if err != nil {
			continue
		}
		out = append(out, d.UTC())
	}
	return append(out, window...)
}

// failedDaysError is what a walk that listed failed days returns: retryable, of the first
// failure's kind, naming every day.
func failedDaysError(orgID string, days []string, first error) error {
	kind := sdk.ErrKindVendorError
	var syncErr *sdk.SyncError
	if errors.As(first, &syncErr) {
		kind = syncErr.Kind
	}
	return &sdk.SyncError{
		ConnectorID: connectorID, OperationID: OpFetchUsage, OrganizationID: orgID,
		Kind: kind, Retryable: true,
		Cause: fmt.Errorf("codex usage: %d day(s) failed and were walked past, to be read again next run (%s): %w",
			len(days), strings.Join(days, ", "), first),
	}
}

// checkpoint persists progress mid-run, best effort. A checkpoint that cannot be written is
// worth a warning and not a failed run: the day's records are already emitted, so the cost is
// re-reading it next time rather than losing it.
func checkpoint(ctx context.Context, params *sdk.ExecutionParams, orgID string, pos position, extra ...zap.Field) {
	if params.Checkpoint == nil {
		return
	}
	cur, err := storePosition(pos)
	if err != nil {
		logger.WarnWithCtx(ctx, "codex usage: could not encode a checkpoint, run is unresumable",
			append(extra, zap.String(logger.OrgID, orgID), zap.Error(err))...)
		return
	}
	if err := params.Checkpoint(ctx, cur); err != nil {
		logger.WarnWithCtx(ctx, "codex usage: checkpoint failed, continuing",
			append(extra, zap.String(logger.OrgID, orgID), zap.Error(err))...)
	}
}

// The repeated-cursor guard: an alternating cursor (A→B→A→B) passes a comparison against only the previous
// page, so the walk would burn the whole page budget and then blame the page cap. Remembering
// every cursor names the real cause at the moment it repeats.

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// linearBackfillSentinel is the `$since` value used on the first run of a
// per-team operation (no prior cursor). Linear's issues filter treats a null
// `updatedAt: { gt: ... }` as "match nothing", so a far-past sentinel is passed
// to fetch the full history.
const linearBackfillSentinel = "2000-01-01T00:00:00.000Z"

// linearTimeFormat is Linear's ISO-8601 DateTime layout (milliseconds, UTC).
const linearTimeFormat = "2006-01-02T15:04:05.000Z"

// linearCursorOverlap is subtracted from a per-team high-water mark when building
// the incremental `updatedAt > since` boundary. It guards against missing issues
// that share the exact high-water millisecond (e.g. a bulk update) and against
// minor clock skew. Re-fetched records are absorbed by the idempotent upsert at
// the consumer. Mirrors the Jira connector's CursorOverlapWindow.
const linearCursorOverlap = 5 * time.Minute

// emitRecord emits a single raw JSON record to the sink. Every Linear record is
// the node's current state, so none carries a DataDate (sdk.RawRecord): the
// node's updatedAt moves on every edit, and dating by it would give a
// date-keyed sink a new row per edit.
func emitRecord(ctx context.Context, sink sdk.DataSink, opID sdk.OperationID, orgID, externalID string, payload json.RawMessage) error {
	now := time.Now().UTC()
	if err := sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(opID),
		OrganizationID: orgID,
		ExternalID:     externalID,
		Payload:        payload,
		FetchedAt:      now,
	}); err != nil {
		return fmt.Errorf("emitting record: %w", err)
	}
	return nil
}

// teamCursorState is the per-team checkpoint stored inside the composite cursor
// map (keyed by team ID). Since is the high-water `updatedAt` used as the
// incremental `updatedAt > since` boundary on the next run. fetch_sprints stores
// it for bookkeeping/observability even though the cycles query is not filtered
// incrementally (cycles are low-volume and re-emitted idempotently each run).
type teamCursorState struct {
	Since time.Time `json:"since"`
}

// loadTeamCursors decodes the composite per-team cursor map. A nil/empty/corrupt
// cursor yields an empty map so the caller backfills from the sentinel — losing a
// checkpoint costs a re-fetch (idempotent upsert downstream), never data. A
// corrupt blob or unparseable team entry is logged at warn so the (expensive)
// re-backfill it triggers is diagnosable rather than silent.
func loadTeamCursors(ctx context.Context, c *sdk.Cursor) map[string]teamCursorState {
	out := map[string]teamCursorState{}
	// A nil/empty cursor (first run) is normal; only a non-empty-but-undecodable
	// blob is worth a warning.
	parts, err := sdk.CompositeFromCursor(c)
	if err != nil {
		if c != nil && len(c.Data) > 0 {
			logger.WarnWithCtx(ctx, "linear: corrupt composite cursor; backfilling all teams from sentinel",
				zap.Error(err))
		}
		return out
	}
	for teamID, raw := range parts {
		var st teamCursorState
		if err := json.Unmarshal(raw, &st); err != nil {
			logger.WarnWithCtx(ctx, "linear: corrupt per-team cursor entry; backfilling that team from sentinel",
				zap.String(logger.TeamID, teamID), zap.Error(err))
			continue
		}
		out[teamID] = st
	}
	return out
}

// pruneToDiscovered returns a copy of states containing only currently-discovered
// teams, carrying each team's prior checkpoint forward. This bounds the composite
// cursor blob: without it, teams deleted/recreated in Linear (a fresh UUID each
// time) would accumulate dead entries for the connector's lifetime.
func pruneToDiscovered(states map[string]teamCursorState, teamIDs []string) map[string]teamCursorState {
	out := make(map[string]teamCursorState, len(teamIDs))
	for _, id := range teamIDs {
		out[id] = states[id] // zero-value teamCursorState when absent — a fresh backfill
	}
	return out
}

// advancePage interprets a GraphQL PageInfo for cursor pagination. It returns the
// next cursor to request, or done=true when the connection is fully walked. It
// errors on a degraded page — hasNextPage with an empty or non-advancing
// endCursor — so the caller fails the scope loudly instead of silently treating a
// truncated walk as complete and advancing its checkpoint past unfetched data.
func advancePage(pi PageInfo, prev *string) (next *string, done bool, err error) {
	if !pi.HasNextPage {
		return nil, true, nil
	}
	if pi.EndCursor == "" {
		return nil, false, fmt.Errorf("hasNextPage=true but endCursor is empty")
	}
	if prev != nil && *prev == pi.EndCursor {
		return nil, false, fmt.Errorf("pagination cursor did not advance (%q)", pi.EndCursor)
	}
	cur := pi.EndCursor
	return &cur, false, nil
}

// pageWalk extends advancePage to a whole connection walk: it also fails a
// cursor the walk already followed earlier, so a connection that cycles
// (A -> B -> A) fails loudly instead of re-emitting the same nodes until the
// deadline. The zero value is ready to use.
type pageWalk struct {
	seen map[string]struct{}
}

func (w *pageWalk) advance(pi PageInfo, prev *string) (*string, bool, error) {
	next, done, err := advancePage(pi, prev)
	if err != nil || done {
		return next, done, err
	}
	if _, dup := w.seen[*next]; dup {
		return nil, false, &sdk.SyncError{
			ConnectorID: connectorID,
			Kind:        sdk.ErrKindVendorError,
			Cause:       fmt.Errorf("pagination cursor %q repeated after %d pages; the walk would not terminate", *next, len(w.seen)),
		}
	}
	if w.seen == nil {
		w.seen = map[string]struct{}{}
	}
	w.seen[*next] = struct{}{}
	return next, false, nil
}

// buildTeamCursor encodes the per-team checkpoint map into a composite cursor.
func buildTeamCursor(states map[string]teamCursorState) *sdk.Cursor {
	parts := make(map[string]json.RawMessage, len(states))
	for teamID, st := range states {
		raw, err := json.Marshal(st) // pure helper (no ctx); teamCursorState is all primitives so marshal cannot fail; skipping a corrupt entry is the safe fallback
		if err != nil {
			continue
		}
		parts[teamID] = raw
	}
	return sdk.NewCompositeCursor(parts)
}

// sinceArg formats a high-water timestamp into Linear's `$since` query argument,
// falling back to the backfill sentinel when no prior checkpoint exists. For an
// incremental run it rewinds by linearCursorOverlap so boundary-millisecond ties
// are not skipped (see the constant's rationale).
func sinceArg(highWater time.Time) string {
	if highWater.IsZero() {
		return linearBackfillSentinel
	}
	return highWater.Add(-linearCursorOverlap).UTC().Format(linearTimeFormat)
}

// discoverTeamIDs enumerates current Linear team IDs via the lightweight
// DiscoverTeams query. Archived teams are intentionally included so their
// historical issues and cycles are still backfilled. The returned IDs are
// ephemeral inputs to the per-team loop and are not persisted. gw must already
// be rate-group-scoped by the caller. A degraded pagination page fails the call
// (via advancePage) rather than silently truncating the team set — a truncated
// list would be treated as authoritative and drop every issue/cycle for the
// missing teams on every subsequent run.
func discoverTeamIDs(ctx context.Context, gw sdk.VendorGateway) ([]string, error) {
	var ids []string
	var cursor *string
	var walk pageWalk
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled while discovering teams: %w", err)
		}
		vars := map[string]any{"first": linearPageSize}
		if cursor != nil {
			vars["after"] = *cursor
		}
		var resp graphqlResponse[teamsData]
		if err := execGraphQL(ctx, gw, queryDiscoverTeams, vars, &resp); err != nil {
			return nil, fmt.Errorf("discover teams: %w", err)
		}
		if len(resp.Errors) > 0 {
			return nil, graphQLErr("discover teams graphql error", resp.Errors)
		}
		for _, node := range resp.Data.Teams.Nodes {
			if node.ID != "" {
				ids = append(ids, node.ID)
			}
		}
		next, done, err := walk.advance(resp.Data.Teams.PageInfo, cursor)
		if err != nil {
			return nil, fmt.Errorf("discover teams: %w", err)
		}
		if done {
			break
		}
		cursor = next
	}
	return ids, nil
}

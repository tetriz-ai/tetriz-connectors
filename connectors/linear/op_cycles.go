// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchCyclesOp fetches Linear cycles (sprint equivalents) per team.
// Registered as operation ID "fetch_sprints", the id other connectors' sprint
// records use, so a consumer handles cycles as sprints.
//
// Cycles are low-volume and have no incremental filter, so each run re-fetches
// every cycle for every team and re-emits it; the downstream upsert (keyed on
// cycle.id) makes this idempotent. The per-team composite cursor records the
// high-water updatedAt for observability/consistency with fetch_issues, not as a
// query filter. ExternalID is cycle.id (UUID); there is no DataDate (current state).
type FetchCyclesOp struct{}

func (op *FetchCyclesOp) OperationID() sdk.OperationID { return "fetch_sprints" }

func (op *FetchCyclesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (cur *sdk.Cursor, err error) {
	start := time.Now()
	orgID := params.Config.OrganizationID
	opID := string(op.OperationID())
	emitted := 0
	// Record run duration (status from the returned err) and total records emitted.
	defer func() {
		recordOpDuration(opID, orgID, start, err)
		recordFetched(opID, orgID, emitted)
	}()

	if _, _, parseErr := ParseConfig(params.Config); parseErr != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    op.OperationID(),
			OrganizationID: orgID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          parseErr,
		}
	}

	gw := params.VendorGateway.ForGroup(constants.GroupLinearGraphQL)

	teamIDs, err := discoverTeamIDs(ctx, gw)
	if err != nil {
		return nil, fmt.Errorf("fetch_sprints: discover teams for org %s: %w", orgID, err)
	}

	// Prune to currently-discovered teams (carrying prior high-water marks) so the
	// composite cursor blob does not accumulate dead entries for deleted teams.
	cursors := pruneToDiscovered(loadTeamCursors(ctx, params.Cursor), teamIDs)

	var teamErrors []error
	var ctxBreakErr error
	successful := 0

	for _, teamID := range teamIDs {
		if ctxErr := ctx.Err(); ctxErr != nil {
			ctxBreakErr = ctxErr
			logger.Error(ctx, "fetch_sprints: ctx done, breaking out of team loop",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.TeamID, teamID),
				zap.Error(ctxErr))
			break
		}

		latest, n, fetchErr := op.fetchCyclesForTeam(ctx, gw, params.Sink, orgID, teamID)
		emitted += n // count records emitted before any per-team failure too
		if fetchErr != nil {
			logger.Error(ctx, "fetch_sprints: team failed; continuing with remaining teams",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.TeamID, teamID),
				zap.Error(fetchErr))
			teamErrors = append(teamErrors, fmt.Errorf("team %s: %w", teamID, fetchErr))
			continue
		}

		// Preserve a prior high-water if this run saw nothing newer, so the
		// recorded mark never regresses.
		if prior := cursors[teamID].Since; prior.After(latest) {
			latest = prior
		}
		cursors[teamID] = teamCursorState{Since: latest}
		if params.Checkpoint != nil {
			if cpErr := params.Checkpoint(ctx, buildTeamCursor(cursors)); cpErr != nil {
				logger.Error(ctx, "fetch_sprints: per-team checkpoint failed (continuing)",
					zap.String(logger.OrgID, orgID),
					zap.String(logger.TeamID, teamID),
					zap.Error(cpErr))
			}
		}
		successful++
	}

	cursorOut := buildTeamCursor(cursors)
	// A failed team is that team's failure: it keeps its cursor and the
	// others' records stand. The run fails only when every attempted team
	// failed, and pauses only when every failure was a credential failure.
	if err := repotally.AllFailed(successful, teamErrors); err != nil && ctxBreakErr == nil {
		return cursorOut, fmt.Errorf("fetch_sprints: all %d teams failed for org %s: %w",
			len(teamErrors), orgID, err)
	}
	if ctxBreakErr != nil {
		return cursorOut, fmt.Errorf("fetch_sprints: ctx exhausted after %d/%d teams for org %s: %w",
			successful+len(teamErrors), len(teamIDs), orgID, ctxBreakErr)
	}
	if len(teamErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_sprints: skipped teams that failed",
			zap.String(logger.OrgID, orgID),
			zap.Int("failed_teams", len(teamErrors)),
			zap.Int("successful_teams", successful),
			zap.Error(teamErrors[0]))
	}

	logger.Info(ctx, "fetch_sprints: done",
		zap.String(logger.OrgID, orgID),
		zap.Int("teams", len(teamIDs)))
	return cursorOut, nil
}

// fetchCyclesForTeam pages through every cycle for one team, emits each, and
// returns the high-water updatedAt observed across them.
func (op *FetchCyclesOp) fetchCyclesForTeam(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, orgID, teamID string) (time.Time, int, error) {
	var latest time.Time
	count := 0
	var cursor *string
	var walk pageWalk

	for {
		if err := ctx.Err(); err != nil {
			return latest, count, fmt.Errorf("context cancelled while fetching cycles: %w", err)
		}

		vars := map[string]any{
			"teamId": teamID,
			"first":  linearPageSize,
		}
		if cursor != nil {
			vars["after"] = *cursor
		}

		var resp graphqlResponse[cyclesData]
		if err := execGraphQL(ctx, gw, queryFetchCycles, vars, &resp); err != nil {
			return latest, count, fmt.Errorf("fetch cycles page (team %s): %w", teamID, err)
		}
		if len(resp.Errors) > 0 {
			return latest, count, graphQLErr(fmt.Sprintf("fetch cycles graphql error (team %s)", teamID), resp.Errors)
		}

		for i := range resp.Data.Cycles.Nodes {
			node := resp.Data.Cycles.Nodes[i]
			payload, err := json.Marshal(node)
			if err != nil {
				logger.Error(ctx, "fetch_sprints: marshal cycle node",
					zap.String(logger.OrgID, orgID),
					zap.String(logger.TeamID, teamID),
					zap.String("cycle_id", node.ID),
					zap.Error(err))
				continue
			}
			if emitErr := emitRecord(ctx, sink, op.OperationID(), orgID, node.ID, payload); emitErr != nil {
				return latest, count, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       fmt.Errorf("emit cycle %s: %w", node.ID, emitErr),
				}
			}
			count++
			if node.UpdatedAt.After(latest) {
				latest = node.UpdatedAt
			}
		}

		next, done, err := walk.advance(resp.Data.Cycles.PageInfo, cursor)
		if err != nil {
			return latest, count, fmt.Errorf("fetch cycles page (team %s): %w", teamID, err)
		}
		if done {
			break
		}
		cursor = next
	}

	return latest, count, nil
}

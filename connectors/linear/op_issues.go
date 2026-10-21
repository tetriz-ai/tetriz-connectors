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

// FetchIssuesOp fetches Linear issues per team, incrementally by updatedAt.
// It discovers current team IDs at run start, then pages through each team's
// issues filtered by updatedAt > since, where `since` is that team's high-water
// mark held in a composite (per-team) cursor. The first run uses a far-past
// sentinel to backfill full history (see sinceArg / linearBackfillSentinel).
//
// ExternalID is the issue's immutable id (UUID). The human identifier (e.g. "ENG-42")
// stays in the payload but is not the key: it changes when the issue moves team, which
// would fork one issue into two records. There is no DataDate: the record is the
// issue's current state, and dating it by updatedAt would fork it per edit.
type FetchIssuesOp struct{}

func (op *FetchIssuesOp) OperationID() sdk.OperationID { return "fetch_issues" }

func (op *FetchIssuesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (cur *sdk.Cursor, err error) {
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
		return nil, fmt.Errorf("fetch_issues: discover teams for org %s: %w", orgID, err)
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
			logger.Error(ctx, "fetch_issues: ctx done, breaking out of team loop",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.TeamID, teamID),
				zap.Error(ctxErr))
			break
		}

		highWater := cursors[teamID].Since
		latest, n, fetchErr := op.fetchIssuesForTeam(ctx, gw, params.Sink, orgID, teamID, highWater)
		emitted += n // count records emitted before any per-team failure too
		if fetchErr != nil {
			logger.Error(ctx, "fetch_issues: team failed; continuing with remaining teams",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.TeamID, teamID),
				zap.Error(fetchErr))
			teamErrors = append(teamErrors, fmt.Errorf("team %s: %w", teamID, fetchErr))
			continue
		}

		// Never regress the high-water if this run observed nothing newer.
		if prior := cursors[teamID].Since; prior.After(latest) {
			latest = prior
		}
		cursors[teamID] = teamCursorState{Since: latest}
		if params.Checkpoint != nil {
			if cpErr := params.Checkpoint(ctx, buildTeamCursor(cursors)); cpErr != nil {
				logger.Error(ctx, "fetch_issues: per-team checkpoint failed (continuing)",
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
		return cursorOut, fmt.Errorf("fetch_issues: all %d teams failed for org %s: %w",
			len(teamErrors), orgID, err)
	}
	if ctxBreakErr != nil {
		return cursorOut, fmt.Errorf("fetch_issues: ctx exhausted after %d/%d teams for org %s: %w",
			successful+len(teamErrors), len(teamIDs), orgID, ctxBreakErr)
	}
	if len(teamErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_issues: skipped teams that failed",
			zap.String(logger.OrgID, orgID),
			zap.Int("failed_teams", len(teamErrors)),
			zap.Int("successful_teams", successful),
			zap.Error(teamErrors[0]))
	}

	logger.Info(ctx, "fetch_issues: done",
		zap.String(logger.OrgID, orgID),
		zap.Int("teams", len(teamIDs)))
	return cursorOut, nil
}

// fetchIssuesForTeam pages through one team's issues with updatedAt > since
// (since derived from highWater via sinceArg), emits each issue, and returns the
// high-water updatedAt observed (never earlier than the incoming highWater so a
// page with no newer issues does not regress the cursor).
func (op *FetchIssuesOp) fetchIssuesForTeam(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, orgID, teamID string, highWater time.Time) (time.Time, int, error) {
	latest := highWater
	count := 0
	// since is the incremental boundary for the whole team walk; it is fixed for
	// every page (deriving from the incoming highWater, not the running latest).
	since := sinceArg(highWater)
	var cursor *string
	var walk pageWalk

	for {
		if err := ctx.Err(); err != nil {
			return latest, count, fmt.Errorf("context cancelled while fetching issues: %w", err)
		}

		vars := map[string]any{
			"teamId": teamID,
			"first":  linearIssuesPageSize,
			"since":  since,
		}
		if cursor != nil {
			vars["after"] = *cursor
		}

		var resp graphqlResponse[issuesData]
		if err := execGraphQL(ctx, gw, queryFetchIssues, vars, &resp); err != nil {
			return latest, count, fmt.Errorf("fetch issues page (team %s): %w", teamID, err)
		}
		if len(resp.Errors) > 0 {
			return latest, count, graphQLErr(fmt.Sprintf("fetch issues graphql error (team %s)", teamID), resp.Errors)
		}

		for i := range resp.Data.Issues.Nodes {
			node := resp.Data.Issues.Nodes[i]
			payload, err := json.Marshal(node)
			if err != nil {
				logger.Error(ctx, "fetch_issues: marshal issue node",
					zap.String(logger.OrgID, orgID),
					zap.String(logger.TeamID, teamID),
					zap.String("issue", node.Identifier),
					zap.Error(err))
				continue
			}
			if emitErr := emitRecord(ctx, sink, op.OperationID(), orgID, node.ID, payload); emitErr != nil {
				return latest, count, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       fmt.Errorf("emit issue %s: %w", node.Identifier, emitErr),
				}
			}
			count++
			if node.UpdatedAt.After(latest) {
				latest = node.UpdatedAt
			}
		}

		next, done, err := walk.advance(resp.Data.Issues.PageInfo, cursor)
		if err != nil {
			return latest, count, fmt.Errorf("fetch issues page (team %s): %w", teamID, err)
		}
		if done {
			break
		}
		cursor = next
	}

	return latest, count, nil
}

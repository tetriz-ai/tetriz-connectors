// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchTeamsOp fetches all Linear teams in the connected workspace.
type FetchTeamsOp struct{}

func (op *FetchTeamsOp) OperationID() sdk.OperationID { return "fetch_teams" }

func (op *FetchTeamsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (cur *sdk.Cursor, err error) {
	start := time.Now()
	orgID := params.Config.OrganizationID
	opID := string(op.OperationID())
	count := 0
	// Record run duration (status from the returned err) and total records emitted.
	defer func() {
		recordOpDuration(opID, orgID, start, err)
		recordFetched(opID, orgID, count)
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

	var cursor *string

	var walk pageWalk

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("fetch_teams: context cancelled: %w", ctxErr)
		}

		vars := map[string]any{
			"first": linearPageSize,
		}
		if cursor != nil {
			vars["after"] = *cursor
		}

		var resp graphqlResponse[teamsData]
		if execErr := execGraphQL(ctx, gw, queryFetchTeams, vars, &resp); execErr != nil {
			return nil, fmt.Errorf("fetch_teams: %w", execErr)
		}
		if len(resp.Errors) > 0 {
			return nil, graphQLErr("fetch_teams graphql error", resp.Errors)
		}

		for i := range resp.Data.Teams.Nodes {
			node := resp.Data.Teams.Nodes[i]
			payload, marshalErr := json.Marshal(node)
			if marshalErr != nil {
				logger.Error(ctx, "fetch_teams: marshal team node",
					zap.String(logger.OrgID, orgID),
					zap.String(logger.TeamID, node.ID),
					zap.Error(marshalErr))
				continue
			}
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), orgID, node.ID, payload); emitErr != nil {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       fmt.Errorf("emit team %s: %w", node.ID, emitErr),
				}
			}
			count++
		}

		next, done, advErr := walk.advance(resp.Data.Teams.PageInfo, cursor)
		if advErr != nil {
			return nil, fmt.Errorf("fetch_teams: %w", advErr)
		}
		if done {
			break
		}
		cursor = next
	}

	logger.Info(ctx, "fetch_teams: done",
		zap.String(logger.OrgID, orgID),
		zap.Int("count", count))
	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

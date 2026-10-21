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

// FetchLabelsOp fetches all Linear issue labels (workspace and team scoped).
type FetchLabelsOp struct{}

func (op *FetchLabelsOp) OperationID() sdk.OperationID { return "fetch_labels" }

func (op *FetchLabelsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (cur *sdk.Cursor, err error) {
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
			return nil, fmt.Errorf("fetch_labels: context cancelled: %w", ctxErr)
		}

		vars := map[string]any{"first": linearPageSize}
		if cursor != nil {
			vars["after"] = *cursor
		}

		var resp graphqlResponse[labelsData]
		if execErr := execGraphQL(ctx, gw, queryFetchLabels, vars, &resp); execErr != nil {
			return nil, fmt.Errorf("fetch_labels: %w", execErr)
		}
		if len(resp.Errors) > 0 {
			return nil, graphQLErr("fetch_labels graphql error", resp.Errors)
		}

		for i := range resp.Data.IssueLabels.Nodes {
			node := resp.Data.IssueLabels.Nodes[i]
			payload, marshalErr := json.Marshal(node)
			if marshalErr != nil {
				logger.Error(ctx, "fetch_labels: marshal label node",
					zap.String(logger.OrgID, orgID),
					zap.String("label_id", node.ID),
					zap.Error(marshalErr))
				continue
			}
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), orgID, node.ID, payload); emitErr != nil {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       fmt.Errorf("emit label %s: %w", node.ID, emitErr),
				}
			}
			count++
		}

		next, done, advErr := walk.advance(resp.Data.IssueLabels.PageInfo, cursor)
		if advErr != nil {
			return nil, fmt.Errorf("fetch_labels: %w", advErr)
		}
		if done {
			break
		}
		cursor = next
	}

	logger.Info(ctx, "fetch_labels: done",
		zap.String(logger.OrgID, orgID),
		zap.Int("count", count))
	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

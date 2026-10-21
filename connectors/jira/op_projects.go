// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchProjectsOp fetches all projects from a Jira Cloud site.
type FetchProjectsOp struct{}

func (op *FetchProjectsOp) OperationID() sdk.OperationID { return "fetch_projects" }

func (op *FetchProjectsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupJiraREST)
	startAt := int32(0)
	count := 0

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.JiraListProjectsRequest{
			CloudId:    settings.CloudID,
			StartAt:    startAt,
			MaxResults: projectsPageSize,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch projects: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.JiraListProjectsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T", protoResp)
		}

		for _, projectRaw := range resp.GetProjects() {
			var project jiraProject
			if err := json.Unmarshal(projectRaw, &project); err != nil {
				logger.Error(ctx, "fetch_projects: skipping unparseable project payload",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("cloud_id", settings.CloudID),
					zap.ByteString("raw", projectRaw),
					zap.Error(err))
				continue
			}

			// The numeric id, not the key: an admin can rename the key, the id
			// never changes. A project with no id has no identity.
			if project.ID == "" {
				logger.WarnWithCtx(ctx, "fetch_projects: skipping project without an id",
					zap.String("cloud_id", settings.CloudID), zap.String("project_key", project.Key))
				continue
			}
			externalID := fmt.Sprintf("project-%s", project.ID)
			// A project is a current-state snapshot: no DataDate (sdk.RawRecord).
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, projectRaw, map[string]string{
				"cloud_id":    settings.CloudID,
				"project_key": project.Key,
			}, time.Time{}); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit project %s: %w", project.Key, emitErr)}
			}
			count++
		}

		if resp.GetIsLast() {
			break
		}
		if len(resp.GetProjects()) == 0 {
			// Defensive: a stuck pager (isLast=false with an empty page) would
			// otherwise spin forever advancing startAt by zero.
			break
		}
		startAt += safePageLen(len(resp.GetProjects()))
	}

	logger.Info(ctx, "fetched projects", zap.String("cloud_id", settings.CloudID), zap.Int("count", count))
	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

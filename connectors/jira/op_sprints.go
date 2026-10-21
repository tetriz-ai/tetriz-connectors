// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// boardsTypeFilter restricts board listing to scrum boards — the board type
// that supports sprints. Agility boards with sprints are uncommon and can be
// added later by broadening this filter.
const boardsTypeFilter = "scrum"

// Sprint record metadata keys. These are the producer side of the contract
// with any downstream consumer (the consumer) reading sprint raw
// records; the consumer must read the same keys, so both sides should import
// from here rather than inlining strings.
const (
	MetaKeyCloudID    = "cloud_id"
	MetaKeyBoardID    = "board_id"
	MetaKeyBoardName  = "board_name"
	MetaKeyBoardType  = "board_type"
	MetaKeyProjectID  = "project_id"
	MetaKeyProjectKey = "project_key"
	// MetaKeyIssueKey is the Jira issue key (e.g. "PROJ-1") that
	// fetch_comments and fetch_worklogs stamp on each emitted record so the
	// downstream consumer can route the sub-resource to its parent issue
	// without a separate lookup. Downstream consumers read it
	// under this key; changing it breaks them.
	MetaKeyIssueKey = "issue_key"
)

// FetchSprintsOp fetches sprints across all scrum boards in the connected
// Jira Cloud site. Sprints are low volume and have no reliable
// `updated-since` filter on the Agile API, so each run re-fetches every
// scrum board and emits a record per sprint. Downstream upserts on
// `sprint-<id>` keep re-emission idempotent.
//
// Each sprint emits a single RawRecord with board and project metadata
// attached so the downstream consumer can resolve the sprint to its owning
// Jira project without a separate board sync.
type FetchSprintsOp struct{}

func (op *FetchSprintsOp) OperationID() sdk.OperationID { return "fetch_sprints" }

// boardMeta holds per-board fields copied into each sprint's record metadata.
type boardMeta struct {
	id         int64
	name       string
	boardType  string
	projectID  int64
	projectKey string
}

func (op *FetchSprintsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupJiraREST)

	boards, err := op.listBoards(ctx, restGW, params.Config.OrganizationID, settings.CloudID)
	if err != nil {
		return nil, fmt.Errorf("list boards: %w", err)
	}

	// A board whose sprints cannot be read is that board's failure: the other
	// boards still sync. The run fails only when every board failed.
	totalSprints := 0
	var boardErrors []error
	succeeded := 0
	for _, board := range boards {
		if err := ctx.Err(); err != nil {
			logger.Error(ctx, "fetch_sprints: ctx done, breaking out of board loop",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.Int64("next_board_id", board.id),
				zap.Error(err))
			return nil, fmt.Errorf("fetch_sprints: ctx exhausted for org %s: %w", params.Config.OrganizationID, err)
		}

		count, err := op.fetchSprintsForBoard(ctx, restGW, params, settings.CloudID, board)
		if err != nil {
			if isBoardDoesNotSupportSprints(err) {
				logger.Info(ctx, "skipping board that does not support sprints",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String(logger.ConnectorID, string(connectorID)),
					zap.Int64(MetaKeyBoardID, board.id),
					zap.String(MetaKeyBoardType, board.boardType))
				continue
			}
			if repotally.StopsTheRun(err) {
				return nil, fmt.Errorf("fetch sprints for board %d: %w", board.id, err)
			}
			logger.WarnWithCtx(ctx, "fetch_sprints: skipping a board that failed",
				zap.String(logger.OrgID, params.Config.OrganizationID), zap.Int64(MetaKeyBoardID, board.id), zap.Error(err))
			boardErrors = append(boardErrors, fmt.Errorf("fetch sprints for board %d: %w", board.id, err))
			continue
		}
		succeeded++
		totalSprints += count
	}
	if err := repotally.AllFailed(succeeded, boardErrors); err != nil {
		return nil, fmt.Errorf("fetch_sprints: all %d boards failed: %w", len(boardErrors), err)
	}

	logger.Info(ctx, "fetched sprints",
		zap.String(logger.OrgID, params.Config.OrganizationID),
		zap.String(logger.ConnectorID, string(connectorID)),
		zap.String(MetaKeyCloudID, settings.CloudID),
		zap.Int("board_count", len(boards)),
		zap.Int("sprint_count", totalSprints),
	)

	return sdk.NewTimestampCursor(time.Now().UTC()), nil
}

// listBoards pages through the Agile board-listing API and returns metadata
// for each scrum board.
func (op *FetchSprintsOp) listBoards(ctx context.Context, gw sdk.VendorGateway, orgID, cloudID string) ([]boardMeta, error) {
	var boards []boardMeta
	startAt := int32(0)

	for {
		protoResp, err := gw.Execute(ctx, &connectorpb.JiraListBoardsRequest{
			CloudId:    cloudID,
			StartAt:    startAt,
			MaxResults: boardsPageSize,
			Type:       boardsTypeFilter,
		})
		if err != nil {
			return nil, fmt.Errorf("executing: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.JiraListBoardsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *JiraListBoardsResponse", protoResp)
		}

		for _, raw := range resp.GetBoards() {
			var b jiraBoard
			if err := json.Unmarshal(raw, &b); err != nil {
				logger.Error(ctx, "failed to parse board, skipping",
					zap.Error(err),
					zap.String(logger.OrgID, orgID),
					zap.String(logger.ConnectorID, string(connectorID)),
					zap.ByteString("raw", raw))
				continue
			}
			meta := boardMeta{id: b.ID, name: b.Name, boardType: b.Type}
			if b.Location != nil {
				meta.projectID = b.Location.ProjectID
				meta.projectKey = b.Location.ProjectKey
			}
			boards = append(boards, meta)
		}

		if resp.GetIsLast() {
			break
		}
		n := len(resp.GetBoards())
		if n == 0 {
			break
		}
		if n > math.MaxInt32 {
			n = math.MaxInt32
		}
		startAt += int32(n) //#nosec G115 -- bounded above by math.MaxInt32 check.
	}

	return boards, nil
}

// fetchSprintsForBoard pages through sprints for a single board, emitting a
// record per sprint. Returns the number of sprints emitted.
func (op *FetchSprintsOp) fetchSprintsForBoard(
	ctx context.Context,
	gw sdk.VendorGateway,
	params *sdk.ExecutionParams,
	cloudID string,
	board boardMeta,
) (int, error) {
	startAt := int32(0)
	count := 0

	for {
		protoResp, err := gw.Execute(ctx, &connectorpb.JiraListSprintsRequest{
			CloudId:    cloudID,
			BoardId:    board.id,
			StartAt:    startAt,
			MaxResults: sprintsPageSize,
		})
		if err != nil {
			return count, fmt.Errorf("executing: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.JiraListSprintsResponse)
		if !ok {
			return count, fmt.Errorf("unexpected response type %T, expected *JiraListSprintsResponse", protoResp)
		}

		for _, raw := range resp.GetSprints() {
			var s jiraSprint
			if err := json.Unmarshal(raw, &s); err != nil {
				logger.Error(ctx, "failed to parse sprint, skipping",
					zap.Error(err),
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String(logger.ConnectorID, string(connectorID)),
					zap.Int64(MetaKeyBoardID, board.id),
					zap.ByteString("raw", raw))
				continue
			}

			externalID := fmt.Sprintf("sprint-%d", s.ID)
			meta := buildSprintMetadata(cloudID, board)
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, raw, meta, time.Time{}); emitErr != nil { // a sprint is current state: no DataDate
				return count, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
			count++
		}

		if resp.GetIsLast() {
			break
		}
		n := len(resp.GetSprints())
		if n == 0 {
			break
		}
		if n > math.MaxInt32 {
			n = math.MaxInt32
		}
		startAt += int32(n) //#nosec G115 -- bounded above by math.MaxInt32 check.
	}

	return count, nil
}

// buildSprintMetadata assembles the metadata map copied onto each sprint
// record. board_id / project_id are stringified so all metadata values are
// strings (matching the framework's map[string]string contract).
func buildSprintMetadata(cloudID string, b boardMeta) map[string]string {
	m := map[string]string{
		MetaKeyCloudID:   cloudID,
		MetaKeyBoardID:   strconv.FormatInt(b.id, 10),
		MetaKeyBoardName: b.name,
		MetaKeyBoardType: b.boardType,
	}
	if b.projectID != 0 {
		m[MetaKeyProjectID] = strconv.FormatInt(b.projectID, 10)
	}
	if b.projectKey != "" {
		m[MetaKeyProjectKey] = b.projectKey
	}
	return m
}

// boardDoesNotSupportSprintsMarkers are the body substrings Jira returns in
// HTTP 400 responses from the sprint list endpoint when the board's type
// doesn't support sprints (e.g. a kanban or agility board). Matching on body
// content — not just on status 400 — avoids silently swallowing genuine
// client-side bugs (bad cloud_id, URL-builder regression) that would also
// surface as 400.
var boardDoesNotSupportSprintsMarkers = []string{
	"does not support sprints",
	"not a scrum board",
}

// isBoardDoesNotSupportSprints reports whether an error from the sprint list
// endpoint is Jira's "this board does not support sprints" response.
func isBoardDoesNotSupportSprints(err error) bool {
	var syncErr *sdk.SyncError
	if !errors.As(err, &syncErr) || syncErr.HTTPStatus != http.StatusBadRequest || syncErr.Cause == nil {
		return false
	}
	msg := strings.ToLower(syncErr.Cause.Error())
	for _, marker := range boardDoesNotSupportSprintsMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

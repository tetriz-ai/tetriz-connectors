// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchCommentsOp fetches comments from recently updated Jira issues.
//
// Iteration model: per project, list issues updated since the cursor, then for
// each issue fetch its full comment list. The cursor is a composite keyed by
// project; each project entry carries (Timestamp, LastIssueID) — Timestamp is
// the high-water mark of issue.fields.updated, LastIssueID is the numeric Jira
// ID of the last issue we successfully emitted comments for. The cursor
// advances after each issue completes so a ctx-cancel mid-iter leaves a usable
// resume point.
//
// cursorOverlap is resolved at connector-construction time from
// config.JiraConfig.CursorOverlapWindow (5-minute default) and applied when
// building the JQL `updated >= ?` filter that feeds fetchUpdatedIssueKeys.
type FetchCommentsOp struct {
	cursorOverlap time.Duration
}

func (op *FetchCommentsOp) OperationID() sdk.OperationID { return "fetch_comments" }

func (op *FetchCommentsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projectKeys, err := fetchAllProjectKeys(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_comments: list project keys for org %s: %w", params.Config.OrganizationID, err)
	}

	cursorStates := existingCursorStates(ctx, params.Cursor)
	var projectErrors []error
	var ctxBreakErr error
	successfulProjects := 0

	for _, projectKey := range projectKeys {
		if err := ctx.Err(); err != nil {
			ctxBreakErr = err
			logger.Error(ctx, "fetch_comments: ctx done, breaking out of project loop",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("next_project", projectKey),
				zap.Error(ctxBreakErr))
			break
		}

		state := cursorStates[projectKey]
		since := effectiveSince(state.Timestamp, op.cursorOverlap)

		issueRefs, err := fetchUpdatedIssueKeys(ctx, params.VendorGateway, settings.CloudID, projectKey, since)
		if err != nil {
			logger.Error(ctx, "fetch_comments: skipping project after issue-keys fetch failure",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("project", projectKey),
				zap.Error(err))
			projectErrors = append(projectErrors, fmt.Errorf("fetch_comments: issue keys for project %s in org %s: %w", projectKey, params.Config.OrganizationID, err))
			continue
		}

		var issueErrors []error
		ctxBreakInIssueLoop := false
		commentCount := 0
		issuesProcessed := 0

		for _, ref := range issueRefs {
			if err := ctx.Err(); err != nil {
				ctxBreakErr = err
				ctxBreakInIssueLoop = true
				logger.Error(ctx, "fetch_comments: ctx done, breaking out of issue loop",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("project", projectKey),
					zap.String("next_issue", ref.Key),
					zap.Error(ctxBreakErr))
				break
			}

			if isAlreadyProcessed(ref, state.Timestamp, state.LastIssueID) {
				continue
			}

			emitted, err := fetchCommentsForIssue(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, settings.CloudID, projectKey, ref.Key)
			if err != nil {
				logger.Error(ctx, "fetch_comments: stopping project iteration after issue fetch failure to prevent cursor advancing past failed issue",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("project", projectKey),
					zap.String("failed_issue", ref.Key),
					zap.Error(err))
				issueErrors = append(issueErrors, fmt.Errorf("fetch_comments: comments for issue %s in project %s: %w", ref.Key, projectKey, err))
				// Stop iterating issues in this project. Continuing would
				// update the cursor on later successes and `isAlreadyProcessed`
				// would then permanently skip this failed issue on retry
				// (its UpdatedOn < advanced cursor.Timestamp). Stay at the
				// last successful issue's (T, ID); next run resumes from there.
				break
			}

			commentCount += emitted
			issuesProcessed++
			cursorStates[projectKey] = projectCursorState{
				Timestamp:   ref.UpdatedOn,
				LastIssueID: ref.ID,
			}
			if params.Checkpoint != nil {
				if checkpointErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); checkpointErr != nil {
					logger.Error(ctx, "fetch_comments: per-issue checkpoint failed (continuing)",
						zap.String(logger.OrgID, params.Config.OrganizationID),
						zap.String("project", projectKey),
						zap.String("issue", ref.Key),
						zap.Error(checkpointErr))
				}
			}
		}

		if ctxBreakInIssueLoop {
			break
		}
		if len(issueErrors) > 0 {
			projectErrors = append(projectErrors, issueErrors...)
			continue
		}
		successfulProjects++
		logger.Info(ctx, "fetched comments",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.String("project", projectKey),
			zap.Int("issues_seen", len(issueRefs)),
			zap.Int("issues_processed", issuesProcessed),
			zap.Int("comments", commentCount),
			zap.String("last_issue_id", cursorStates[projectKey].LastIssueID))
	}

	cursorOut := params.Cursor
	if len(cursorStates) > 0 {
		cursorOut = buildStateCursor(ctx, cursorStates)
	}

	// A failed project is that project's failure: it keeps its cursor and the
	// others' records stand. The run fails only when every attempted project
	// failed, and pauses only when every failure was a credential failure.
	if err := repotally.AllFailed(successfulProjects, projectErrors); err != nil && ctxBreakErr == nil {
		return cursorOut, fmt.Errorf("fetch_comments: all %d attempted projects failed for org %s: %w",
			len(projectErrors), params.Config.OrganizationID, err)
	}
	if len(projectErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_comments: skipped projects that failed",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Int("failed_projects", len(projectErrors)),
			zap.Int("successful_projects", successfulProjects),
			zap.Error(projectErrors[0]))
	}
	if ctxBreakErr != nil {
		return cursorOut, fmt.Errorf("fetch_comments: ctx exhausted after %d/%d projects for org %s: %w",
			successfulProjects, len(projectKeys), params.Config.OrganizationID, ctxBreakErr)
	}
	return cursorOut, nil
}

// fetchCommentsForIssue fetches all comments for a single issue and emits each as a raw record.
func fetchCommentsForIssue(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, cloudID, projectKey, issueKey string) (int, error) {
	restGW := gw.ForGroup(constants.GroupJiraREST)
	startAt := int32(0)
	emitted := 0

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.JiraListIssueCommentsRequest{
			CloudId:      cloudID,
			IssueIdOrKey: issueKey,
			StartAt:      startAt,
			MaxResults:   commentsPageSize,
			OrderBy:      "created",
		})
		if err != nil {
			return emitted, fmt.Errorf("list comments for issue %s: %w", issueKey, err)
		}

		resp, ok := protoResp.(*connectorpb.JiraListIssueCommentsResponse)
		if !ok {
			return emitted, fmt.Errorf("list comments for issue %s: unexpected response type %T", issueKey, protoResp)
		}

		for _, commentRaw := range resp.GetComments() {
			var comment jiraComment
			if err := json.Unmarshal(commentRaw, &comment); err != nil {
				logger.Error(ctx, "fetch_comments: skipping unparseable comment payload",
					zap.String(logger.OrgID, orgID),
					zap.String("project", projectKey),
					zap.String("issue", issueKey),
					zap.ByteString("raw", commentRaw),
					zap.Error(err))
				continue
			}

			if comment.ID == "" {
				// No id, no ExternalID of its own: every such comment would share
				// "comment-" and collapse into one record.
				logger.WarnWithCtx(ctx, "fetch_comments: skipping comment without id",
					zap.String(logger.OrgID, orgID),
					zap.String("issue", issueKey))
				continue
			}
			externalID := fmt.Sprintf("comment-%s", comment.ID)
			// Dated by its creation. Without one it has no event date, and the
			// fetch time would fork a row per run, so DataDate stays empty.
			dataDate := comment.Created.Time
			if emitErr := emitRecord(ctx, sink, opID, orgID, externalID, commentRaw, map[string]string{
				"project":       projectKey,
				MetaKeyIssueKey: issueKey,
				MetaKeyCloudID:  cloudID,
			}, dataDate); emitErr != nil {
				return emitted, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit comment %s for issue %s: %w", comment.ID, issueKey, emitErr)}
			}
			emitted++
		}

		fetched := resp.GetStartAt() + safePageLen(len(resp.GetComments()))
		if fetched >= resp.GetTotal() || len(resp.GetComments()) == 0 {
			break
		}
		startAt = fetched
	}

	return emitted, nil
}

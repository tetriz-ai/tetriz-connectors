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
	connectordescriptions "github.com/tetriz-ai/tetriz-connectors/internal/descriptions"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// jiraIssueDescription is the narrow per-issue projection.
// fields.Description carries raw ADF JSON.
type jiraIssueDescription struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Fields struct {
		Updated     jiraTime        `json:"updated"`
		Description json.RawMessage `json:"description,omitempty"`
		Project     *struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		} `json:"project,omitempty"`
	} `json:"fields"`
}

type FetchIssuesDataOp struct{}

func (op *FetchIssuesDataOp) OperationID() sdk.OperationID { return "fetch_issues_data" }

func (op *FetchIssuesDataOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}
	if params.BlobStore == nil {
		logger.WarnWithCtx(ctx, "fetch_issues_data: BlobStore missing, skipping tick",
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.String(logger.OrgID, params.Config.OrganizationID))
		return params.Cursor, nil
	}

	projectKeys, err := fetchAllProjectKeys(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching all project keys: %w", err)
	}

	cursorStates := existingCursorStates(ctx, params.Cursor)
	var projectErrors []error
	var ctxBreakErr error
	successfulProjects := 0

	for _, projectKey := range projectKeys {
		if err := ctx.Err(); err != nil {
			ctxBreakErr = err
			break
		}
		state := cursorStates[projectKey]
		// No overlap window here, but the time-zone widening still applies: the
		// JQL literal is read in the user's zone (see maxJQLZoneOffset).
		since := effectiveSince(state.Timestamp, 0)
		resumePageToken := ""
		if state.PageToken != "" {
			since = state.Since
			resumePageToken = state.PageToken
		}

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, projectKey, since)
		latestUpdated, err := fetchIssuesDataForProject(ctx, params.VendorGateway, params.BlobStore, params.Sink, op.OperationID(), params.Config.OrganizationID, settings.CloudID, projectKey, since, resumePageToken, state.Timestamp, onPage)
		if err != nil {
			logger.Error(ctx, "fetch_issues_data: project failed; continuing with remaining projects",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("project", projectKey), zap.Error(err))
			projectErrors = append(projectErrors, fmt.Errorf("fetch_issues_data: project %s for org %s: %w", projectKey, params.Config.OrganizationID, err))

			// Clear a possibly-stale resume token so the next run does a fresh
			// JQL from cursor.Timestamp instead of replaying the same expired
			// PageToken forever. Persistence via params.Checkpoint is REQUIRED:
			// the orchestrator's failRun path does NOT save the returned cursor
			// when Execute errors, so without an explicit write here the cleared
			// PageToken stays in-memory only and the next run reloads the stale
			// token from the saved sync state (stateDAO.GetCursor over
			// connector_sync_states), recreating the loop. Mirrors op_issues.go's
			// resume-token rescue.
			if resumePageToken != "" {
				if existing, ok := cursorStates[projectKey]; ok {
					existing.PageToken = ""
					existing.Since = time.Time{}
					cursorStates[projectKey] = existing
				}
				if params.Checkpoint != nil {
					if checkpointErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); checkpointErr != nil {
						logger.Error(ctx, "fetch_issues_data: failed to persist cleared resume token; next run may still see stale state",
							zap.String(logger.OrgID, params.Config.OrganizationID),
							zap.String("project", projectKey), zap.Error(checkpointErr))
					}
				}
			}
			continue
		}

		cursorStates[projectKey] = projectCursorState{Timestamp: latestUpdated}
		// Persist each project's advanced high-water mark as it completes. The
		// tail's returned cursor is NOT a substitute: once any project in this
		// run errors, Execute returns non-nil and the orchestrator's failRun
		// path drops the returned cursor — without this per-project write, every
		// already-succeeded project's progress would be lost and re-fetched next
		// run for as long as one sibling project keeps failing. Mirrors
		// op_issues.go.
		if params.Checkpoint != nil {
			if checkpointErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); checkpointErr != nil {
				logger.Error(ctx, "fetch_issues_data: per-project checkpoint failed (continuing)",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("project", projectKey), zap.Error(checkpointErr))
			}
		}
		successfulProjects++
		logger.Info(ctx, "fetch_issues_data: fetched issue descriptions",
			zap.String("project", projectKey), zap.Time("cursor", latestUpdated))
	}

	cursorOut := params.Cursor
	if len(cursorStates) > 0 {
		cursorOut = buildStateCursor(ctx, cursorStates)
	}

	// A failed project is that project's failure: it keeps its cursor and the
	// others' records stand. The run fails only when every attempted project
	// failed, and pauses only when every failure was a credential failure.
	if ctxBreakErr != nil {
		return cursorOut, fmt.Errorf("fetch_issues_data: ctx exhausted after %d/%d projects for org %s: %w",
			successfulProjects+len(projectErrors), len(projectKeys), params.Config.OrganizationID, ctxBreakErr)
	}
	if err := repotally.AllFailed(successfulProjects, projectErrors); err != nil {
		return cursorOut, fmt.Errorf("fetch_issues_data: all %d attempted projects failed for org %s: %w",
			len(projectErrors), params.Config.OrganizationID, err)
	}
	if len(projectErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_issues_data: skipped projects that failed",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Int("failed_projects", len(projectErrors)),
			zap.Int("successful_projects", successfulProjects),
			zap.Error(projectErrors[0]))
	}
	return cursorOut, nil
}

func fetchIssuesDataForProject(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client connectordescriptions.S3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, cloudID, projectKey string,
	since time.Time,
	resumePageToken string,
	highWater time.Time,
	onPage pageCheckpointFn,
) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupJiraREST)
	latestUpdated := highWater
	nextPageToken := resumePageToken

	jql := fmt.Sprintf("project = %q ORDER BY updated ASC", projectKey)
	if !since.IsZero() {
		jql = fmt.Sprintf("project = %q AND updated >= %q ORDER BY updated ASC",
			projectKey, since.UTC().Format(jiraTimeFormat))
	}

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.JiraSearchIssuesRequest{
			CloudId:       cloudID,
			Jql:           jql,
			MaxResults:    maxResultsPerPage,
			Fields:        "description,updated,project",
			Expand:        "renderedFields",
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch_issues_data for %s: %w", projectKey, err)
		}
		resp, ok := protoResp.(*connectorpb.JiraSearchIssuesResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("fetch_issues_data %s: unexpected response type %T", projectKey, protoResp)
		}
		if err := checkPageTokenAdvances(nextPageToken, resp); err != nil {
			return time.Time{}, fmt.Errorf("fetch_issues_data %s: %w", projectKey, err)
		}

		for _, raw := range resp.GetIssues() {
			var iss jiraIssueDescription
			if err := json.Unmarshal(raw, &iss); err != nil {
				return time.Time{}, fmt.Errorf("unmarshal issue for %s: %w", projectKey, err)
			}
			if err := emitJiraIssueDescription(ctx, s3Client, sink, opID, orgID, cloudID, projectKey, iss); err != nil {
				// Type the S3/sink failure so it survives error aggregation as a
				// sink failure (retry-classifiable) rather than being flattened
				// to an internal error at the orchestrator. Mirrors op_issues.go.
				return latestUpdated, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit issue %s in %s: %w", iss.Key, projectKey, err)}
			}
			if iss.Fields.Updated.After(latestUpdated) {
				latestUpdated = iss.Fields.Updated.Time
			}
		}

		if resp.GetNextPageToken() == "" || len(resp.GetIssues()) == 0 {
			break
		}
		nextPageToken = resp.GetNextPageToken()

		if onPage != nil {
			if err := onPage(ctx, latestUpdated, nextPageToken); err != nil {
				return time.Time{}, fmt.Errorf("checkpoint for %s: %w", projectKey, err)
			}
		}
	}

	return latestUpdated, nil
}

// emitJiraIssueDescription delegates the S3 → sha → payload → emit pipeline to
// connectordescriptions. project.id is the PlatformProjectID — empty short-circuits
// the helper to warn-skip.
func emitJiraIssueDescription(
	ctx context.Context,
	s3Client connectordescriptions.S3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, cloudID, projectKey string,
	iss jiraIssueDescription,
) error {
	desc := iss.Fields.Description
	if len(desc) == 0 || string(desc) == "null" {
		return nil
	}
	if iss.ID == "" {
		logger.WarnWithCtx(ctx, "fetch_issues_data: skipping issue without an id",
			zap.String("project", projectKey), zap.String("issue_key", iss.Key))
		return nil
	}

	projectID := ""
	if iss.Fields.Project != nil {
		projectID = iss.Fields.Project.ID
	}
	meta := map[string]string{
		"project":  projectKey,
		"cloud_id": cloudID,
	}
	if projectID != "" {
		meta["project_id"] = projectID
	}

	if err := connectordescriptions.EmitIssueDescription(ctx, s3Client, sink, connectordescriptions.EmitIssueDescriptionInput{
		ConnectorID: connectorID,
		OperationID: opID,
		OrgID:       orgID,
		IssueKey:    iss.Key,
		// The numeric id survives a move to another project; the key does not.
		ExternalID:        iss.ID,
		PlatformProjectID: projectID,
		// A description pointer is current state: no DataDate (sdk.RawRecord).
		Raw:   []byte(desc),
		Meta:  meta,
		S3Key: fmt.Sprintf("descriptions/%s/%s/issues/%s/%s", orgID, connectorID, projectID, iss.Key),
	}); err != nil {
		return fmt.Errorf("emitting jira issue description: %w", err)
	}
	return nil
}

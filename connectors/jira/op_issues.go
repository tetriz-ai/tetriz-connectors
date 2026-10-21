// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchIssuesOp fetches issues from Jira Cloud projects using JQL search with
// an incremental updated-date cursor. It uses a composite cursor to track the
// last updated timestamp per project, and persists `nextPageToken` mid-run so
// large projects can resume after a ctx-cancel without re-walking from the top.
//
// cursorOverlap is resolved at connector-construction time from
// config.JiraConfig.CursorOverlapWindow (5-minute default) and applied when
// building the JQL `updated >= ?` filter.
type FetchIssuesOp struct {
	cursorOverlap time.Duration
}

func (op *FetchIssuesOp) OperationID() sdk.OperationID { return "fetch_issues" }

func (op *FetchIssuesOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	projectKeys, err := fetchAllProjectKeys(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_issues: list project keys for org %s: %w", params.Config.OrganizationID, err)
	}

	cursorStates := existingCursorStates(ctx, params.Cursor)
	var projectErrors []error
	var ctxBreakErr error
	successfulProjects := 0

	for _, projectKey := range projectKeys {
		if err := ctx.Err(); err != nil {
			ctxBreakErr = err
			logger.Error(ctx, "fetch_issues: ctx done, breaking out of project loop",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("next_project", projectKey),
				zap.Error(ctxBreakErr))
			break
		}

		state := cursorStates[projectKey]

		// Default to a fresh JQL boundary derived from the cursor's high-water
		// mark with the overlap-window safety margin applied. If we're
		// mid-pagination (PageToken set with a pinned Since), the resume path
		// below overrides this — page tokens are server-stateful and depend on
		// the original JQL boundary, so we MUST NOT re-apply the overlap there.
		since := effectiveSince(state.Timestamp, op.cursorOverlap)
		resumePageToken := ""
		if state.PageToken != "" {
			if state.Since.IsZero() {
				logger.Error(ctx, "fetch_issues: malformed cursor (PageToken set, Since zero); falling back to fresh iteration",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("project", projectKey),
					zap.String("page_token", state.PageToken))
			} else {
				since = state.Since
				resumePageToken = state.PageToken
			}
		}

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, projectKey, since)
		latestUpdated, err := fetchIssuesForProject(ctx, params.VendorGateway, params.Sink, op.OperationID(), params.Config.OrganizationID, settings.CloudID, projectKey, since, resumePageToken, state.Timestamp, onPage)
		if err != nil {
			logger.Error(ctx, "fetch_issues: project failed; continuing with remaining projects",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("project", projectKey),
				zap.Error(err))
			projectErrors = append(projectErrors, fmt.Errorf("fetch_issues: project %s for org %s: %w", projectKey, params.Config.OrganizationID, err))

			// Clear a possibly-stale resume token so the next run does a fresh
			// JQL from cursor.Timestamp instead of replaying the same expired
			// PageToken forever. Atlassian doesn't pin server-side token TTL,
			// so an opaque API error on resume must not pin us into a loop.
			// Cost: small re-fetch on next run (covered by the overlap window
			// and idempotent UPSERT at the consumer).
			//
			// Persistence via params.Checkpoint is REQUIRED here: the
			// orchestrator's failRun (see runner/orchestrator.go)
			// does NOT save the returned
			// cursor when Execute errors — it only updates the sync_runs
			// row. Without an explicit Checkpoint write here, the cleared
			// PageToken stays in-memory only and the next run reloads the
			// stale token from the stored cursor, recreating the loop.
			if resumePageToken != "" {
				if existing, ok := cursorStates[projectKey]; ok {
					existing.PageToken = ""
					existing.Since = time.Time{}
					cursorStates[projectKey] = existing
				}
				if params.Checkpoint != nil {
					if checkpointErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); checkpointErr != nil {
						logger.Error(ctx, "fetch_issues: failed to persist cleared resume token; next run may still see stale state",
							zap.String(logger.OrgID, params.Config.OrganizationID),
							zap.String("project", projectKey),
							zap.Error(checkpointErr))
					}
				}
			}
			continue
		}

		cursorStates[projectKey] = projectCursorState{Timestamp: latestUpdated}
		if params.Checkpoint != nil {
			if checkpointErr := params.Checkpoint(ctx, buildStateCursor(ctx, cursorStates)); checkpointErr != nil {
				logger.Error(ctx, "fetch_issues: per-project checkpoint failed (continuing)",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.String("project", projectKey),
					zap.Error(checkpointErr))
			}
		}
		successfulProjects++
		logger.Info(ctx, "fetched issues",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.String("project", projectKey),
			zap.Time("cursor", latestUpdated))
	}

	cursorOut := params.Cursor
	if len(cursorStates) > 0 {
		cursorOut = buildStateCursor(ctx, cursorStates)
	}

	// A failed project is that project's failure: it keeps its cursor and the
	// others' records stand. The run fails only when every attempted project
	// failed, and pauses only when every failure was a credential failure.
	if err := repotally.AllFailed(successfulProjects, projectErrors); err != nil && ctxBreakErr == nil {
		return cursorOut, fmt.Errorf("fetch_issues: all %d attempted projects failed for org %s: %w",
			len(projectErrors), params.Config.OrganizationID, err)
	}
	if len(projectErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_issues: skipped projects that failed",
			zap.String(logger.OrgID, params.Config.OrganizationID),
			zap.Int("failed_projects", len(projectErrors)),
			zap.Int("successful_projects", successfulProjects),
			zap.Error(projectErrors[0]))
	}
	if ctxBreakErr != nil {
		return cursorOut, fmt.Errorf("fetch_issues: ctx exhausted after %d/%d projects for org %s: %w",
			successfulProjects, len(projectKeys), params.Config.OrganizationID, ctxBreakErr)
	}
	return cursorOut, nil
}

// fetchIssuesForProject fetches all issues for a single project since the given
// timestamp. resumePageToken allows resuming from a checkpointed
// page token; highWater is the recorded high-water mark (may differ
// from since when resuming).
func fetchIssuesForProject(ctx context.Context, gw sdk.VendorGateway, sink sdk.DataSink, opID sdk.OperationID, orgID, cloudID, projectKey string, since time.Time, resumePageToken string, highWater time.Time, onPage pageCheckpointFn) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupJiraREST)
	latestUpdated := highWater
	nextPageToken := resumePageToken

	// %q quotes the project key so a key that is a reserved JQL word (e.g.
	// "DO", "AND", "ORDER") does not trip Atlassian's HTTP 400 parser. Matches
	// op_issues_data.go. The timestamp is quoted the same way.
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
			Expand:        issueExpandFields,
			Fields:        issueFields,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return latestUpdated, fmt.Errorf("search issues for project %s: %w", projectKey, err)
		}

		resp, ok := protoResp.(*connectorpb.JiraSearchIssuesResponse)
		if !ok {
			return latestUpdated, fmt.Errorf("search issues for project %s: unexpected response type %T", projectKey, protoResp)
		}
		if err := checkPageTokenAdvances(nextPageToken, resp); err != nil {
			return latestUpdated, fmt.Errorf("search issues for project %s: %w", projectKey, err)
		}

		for _, issueRaw := range resp.GetIssues() {
			var issue jiraIssue
			if err := json.Unmarshal(issueRaw, &issue); err != nil {
				// Fail-fast on unparseable payload. Skipping here would let
				// later issues in the same page advance latestUpdated past
				// the bad issue's position, and onPage would then checkpoint
				// the cursor past it. On retry the cursor's `since` filter
				// (even with the overlap window) might exclude the issue,
				// causing permanent loss of its created/snapshot/resolved
				// records. Returning the error keeps the cursor at the
				// last successfully-checkpointed page so retries re-fetch.
				return latestUpdated, fmt.Errorf("unmarshal issue in project %s: %w", projectKey, err)
			}

			// The ExternalID is the numeric issue id: the key changes when the
			// issue moves to another project, the id never does. An issue
			// without an id has no identity, so it is skipped rather than
			// emitted under a colliding "#created".
			if issue.ID == "" {
				logger.WarnWithCtx(ctx, "fetch_issues: skipping issue without an id",
					zap.String("project", projectKey), zap.String("issue_key", issue.Key))
				if issue.Fields.Updated.After(latestUpdated) {
					latestUpdated = issue.Fields.Updated.Time
				}
				continue
			}

			baseMeta := map[string]string{
				"project":       projectKey,
				"cloud_id":      cloudID,
				MetaKeyIssueKey: issue.Key,
			}

			// Lifecycle event records (ADR-0003, mirrors GitHub PR pattern).
			// "created" — emitted for every issue. DataDate = fields.created so
			// the creation count lands on the actual creation date.
			createdMeta := copyMeta(baseMeta)
			createdMeta["event_type"] = "created"
			if emitErr := emitRecord(ctx, sink, opID, orgID, issue.ID+"#created", issueRaw, createdMeta, issue.Fields.Created.Time); emitErr != nil {
				return latestUpdated, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit created record for %s: %w", issue.Key, emitErr)}
			}

			// "snapshot" — captures current state for identity resolution and
			// mutable counters. A current-state record has no DataDate
			// (sdk.RawRecord), so it upserts one row across runs.
			snapshotMeta := copyMeta(baseMeta)
			snapshotMeta["event_type"] = "snapshot"
			if emitErr := emitRecord(ctx, sink, opID, orgID, issue.ID+"#snapshot", issueRaw, snapshotMeta, time.Time{}); emitErr != nil {
				return latestUpdated, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit snapshot record for %s: %w", issue.Key, emitErr)}
			}

			// "resolved" — emitted only when resolutiondate is set. DataDate =
			// resolutiondate so the resolution count lands on the actual
			// resolution date.
			if issue.Fields.ResolutionDate != nil && !issue.Fields.ResolutionDate.IsZero() {
				resolvedMeta := copyMeta(baseMeta)
				resolvedMeta["event_type"] = "resolved"
				if emitErr := emitRecord(ctx, sink, opID, orgID, issue.ID+"#resolved", issueRaw, resolvedMeta, issue.Fields.ResolutionDate.Time); emitErr != nil {
					return latestUpdated, &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, Kind: sdk.ErrKindSinkFailure, Cause: fmt.Errorf("emit resolved record for %s: %w", issue.Key, emitErr)}
				}
			}

			if issue.Fields.Updated.After(latestUpdated) {
				latestUpdated = issue.Fields.Updated.Time
			}
		}

		// Order matters: compute finished before deciding nextTokenForCheckpoint
		// so the final page's checkpoint correctly clears the resume token. If
		// `break` preceded the onPage call, the last page's progress would
		// never be persisted via the per-page hook.
		finished := resp.GetNextPageToken() == "" || len(resp.GetIssues()) == 0
		nextTokenForCheckpoint := ""
		if !finished {
			nextTokenForCheckpoint = resp.GetNextPageToken()
		}
		if onPage != nil {
			if err := onPage(ctx, latestUpdated, nextTokenForCheckpoint); err != nil {
				logger.Error(ctx, "fetch_issues: per-page checkpoint failed (continuing)",
					zap.String(logger.OrgID, orgID),
					zap.String("project", projectKey),
					zap.Error(err))
			}
		}
		if finished {
			break
		}
		nextPageToken = resp.GetNextPageToken()
	}

	return latestUpdated, nil
}

// fetchUpdatedIssueKeys returns issueRefs for recently-updated issues in a
// project. It uses the same JQL pattern as fetchIssuesForProject but only
// extracts (ID, Key, UpdatedOn) for use by fetch_comments and fetch_worklogs,
// which iterate one issue at a time and need both UpdatedOn (for high-water
// advancement) and ID (for the resume tiebreaker — see isAlreadyProcessed).
func fetchUpdatedIssueKeys(ctx context.Context, gw sdk.VendorGateway, cloudID, projectKey string, since time.Time) ([]issueRef, error) {
	restGW := gw.ForGroup(constants.GroupJiraREST)
	var nextPageToken string
	var issueRefs []issueRef

	// %q quotes the project key so a key that is a reserved JQL word (e.g.
	// "DO", "AND", "ORDER") does not trip Atlassian's HTTP 400 parser. Matches
	// op_issues_data.go. The timestamp is quoted the same way.
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
			Fields:        "updated",
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("search issue keys for project %s: %w", projectKey, err)
		}

		resp, ok := protoResp.(*connectorpb.JiraSearchIssuesResponse)
		if !ok {
			return nil, fmt.Errorf("search issue keys for project %s: unexpected response type %T", projectKey, protoResp)
		}
		if err := checkPageTokenAdvances(nextPageToken, resp); err != nil {
			return nil, fmt.Errorf("search issue keys for project %s: %w", projectKey, err)
		}

		for _, issueRaw := range resp.GetIssues() {
			var issue jiraIssue
			if err := json.Unmarshal(issueRaw, &issue); err != nil {
				// Fail-fast: skipping here would omit this issue from
				// issueRefs, so fetch_worklogs / fetch_comments would never
				// fetch its sub-resources. Later issues in the page would
				// still process and advance the cursor past the bad issue's
				// UpdatedOn, making `isAlreadyProcessed` permanently skip
				// it on retry. Returning the error keeps the cursor where
				// it is and prompts a clean re-fetch on the next run.
				return nil, fmt.Errorf("unmarshal issue key in project %s: %w", projectKey, err)
			}
			issueRefs = append(issueRefs, issueRef{
				ID:        issue.ID,
				Key:       issue.Key,
				UpdatedOn: issue.Fields.Updated.Time,
			})
		}

		if resp.GetNextPageToken() == "" || len(resp.GetIssues()) == 0 {
			break
		}
		nextPageToken = resp.GetNextPageToken()
	}

	// Sort by (UpdatedOn, ID) so the resume-skip predicate has a stable order
	// across runs. Jira's secondary sort within ORDER BY updated ASC is not
	// pinned, so we sort Go-side to make the tiebreaker behaviour deterministic.
	sort.Slice(issueRefs, func(left, right int) bool {
		if issueRefs[left].UpdatedOn.Equal(issueRefs[right].UpdatedOn) {
			return compareIssueIDs(issueRefs[left].ID, issueRefs[right].ID) < 0
		}
		return issueRefs[left].UpdatedOn.Before(issueRefs[right].UpdatedOn)
	})

	return issueRefs, nil
}

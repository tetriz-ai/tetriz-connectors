// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectordescriptions "github.com/tetriz-ai/tetriz-connectors/internal/descriptions"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ghIssueDescription is the narrow GraphQL projection.
type ghIssueDescription struct {
	Number    int       `json:"number"`
	UpdatedAt time.Time `json:"updatedAt"`
	Body      string    `json:"body"`
}

type fetchIssuesDataResponse struct {
	Repository struct {
		DatabaseID int64 `json:"databaseId"`
		Issues     *struct {
			Nodes    []ghIssueDescription `json:"nodes"`
			PageInfo ghPageInfo           `json:"pageInfo"`
		} `json:"issues"`
	} `json:"repository"`
}

type FetchIssuesDataOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchIssuesDataOp) OperationID() sdk.OperationID { return opFetchIssuesData }

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

	repoInfos, err := fetchAllRepos(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching repos: %w", err)
	}

	cursorStates := existingCursorStates(params.Cursor)

	var repoErrors []error
	var tally repotally.Tally
	for _, ri := range repoInfos {
		if !hasTimeRemaining(ctx, op.checkpointReserve) {
			logger.WarnWithCtx(ctx, "fetch_issues_data: deadline approaching, returning partial cursor",
				zap.String("operation", string(op.OperationID())))
			break
		}
		repoFullName := ri.FullName
		owner, repo := resolveRepoOwner(repoFullName, settings.Organization)

		state := getRepoCursorState(params.Cursor, repoFullName)
		since := firstSyncSince(state.Timestamp, settings.GetLookbackDays())
		resumePageCursor := ""
		if state.PageCursor != "" {
			since = state.Since
			resumePageCursor = state.PageCursor
		}

		pageCfg := opPageConfig(op.paginationCfg, opFetchIssuesData)
		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)

		tally.Attempt()
		latestUpdated, err := fetchIssuesDataForRepo(ctx, params.VendorGateway, params.BlobStore, params.Sink, op.OperationID(), params.Config.OrganizationID, owner, repo, repoFullName, since, resumePageCursor, state.Timestamp, onPage, pageCfg)
		if err != nil {
			logger.Error(ctx, "fetch_issues_data: skipping repo after fetch failure",
				zap.String(logger.RepoFullName, repoFullName), zap.Error(err))
			repoErrors = append(repoErrors, fmt.Errorf("%s: %w", repoFullName, err))
			tally.Fail(repoFullName, err)
			continue
		}
		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetch_issues_data: fetched issue descriptions",
			zap.String(logger.RepoFullName, repoFullName), zap.Time("cursor", latestUpdated))
	}

	if err := tally.Err(len(repoInfos)); err != nil {
		return nil, fmt.Errorf("fetch_issues_data: %w", err)
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	if len(repoErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_issues_data: completed with repo-level failures",
			zap.Int("failed_repos", len(repoErrors)),
			zap.Int("total_repos", len(repoInfos)))
	}
	return buildStateCursor(cursorStates), nil
}

func fetchIssuesDataForRepo(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client s3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, owner, repo, repoFullName string,
	since time.Time,
	resumePageCursor string,
	highWater time.Time,
	onPage pageCheckpointFn,
	pageCfg *config.OperationPaginationConfig,
) (time.Time, error) {
	latestUpdated := highWater
	graphQLCursor := resumePageCursor

	for {
		body, err := doGraphQLAdaptive(ctx, gw, pageCfg, func(ps int) string {
			return buildIssuesDataQuery(owner, repo, graphQLCursor, ps)
		}, repoFullName, false)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch_issues_data for %s: %w", repoFullName, err)
		}

		var data fetchIssuesDataResponse
		if err := json.Unmarshal(body, &data); err != nil {
			return time.Time{}, fmt.Errorf("parse fetch_issues_data response for %s: %w", repoFullName, err)
		}

		issues := data.Repository.Issues
		if issues == nil {
			// A null connection is never normal EOF (an accessible repo returns a
			// non-null connection with nodes:[]); nil means forbidden/truncated —
			// fail loud so the repo is retried, never silently recorded empty.
			return time.Time{}, fmt.Errorf("fetch_issues_data for %s: null issues connection (forbidden or truncated response)", repoFullName)
		}
		repoDatabaseID := data.Repository.DatabaseID

		done := false
		for _, iss := range issues.Nodes {
			if !since.IsZero() && iss.UpdatedAt.Before(since) {
				done = true
				break
			}
			if err := emitIssueDescription(ctx, s3Client, sink, opID, orgID, repoFullName, repoDatabaseID, iss); err != nil {
				return latestUpdated, fmt.Errorf("emit issue %s#%d in %s: %w", repoFullName, iss.Number, repoFullName, err)
			}
			if iss.UpdatedAt.After(latestUpdated) {
				latestUpdated = iss.UpdatedAt
			}
		}

		if done || !issues.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, issues.PageInfo.EndCursor); err != nil {
			return time.Time{}, fmt.Errorf("fetch_issues_data for %s: %w", repoFullName, err)
		}

		if onPage != nil {
			if err := onPage(ctx, latestUpdated, graphQLCursor); err != nil {
				return time.Time{}, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
			}
		}
	}

	return latestUpdated, nil
}

// emitIssueDescription uses a repo-qualified external_id — GitHub issue numbers
// are unique per-repo, so two repos with the same issue number would otherwise
// share an external ID.
func emitIssueDescription(
	ctx context.Context,
	s3Client s3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, repoFullName string,
	repoDatabaseID int64,
	iss ghIssueDescription,
) error {
	platformRepoID := ""
	if repoDatabaseID != 0 {
		platformRepoID = strconv.FormatInt(repoDatabaseID, 10)
	}
	meta := map[string]string{"repo": repoFullName}
	if platformRepoID != "" {
		meta["platform_repo_id"] = platformRepoID
	}

	if err := connectordescriptions.EmitIssueDescription(ctx, s3Client, sink, connectordescriptions.EmitIssueDescriptionInput{
		ConnectorID:       connectorID,
		OperationID:       opID,
		OrgID:             orgID,
		IssueKey:          fmt.Sprintf("%s#%d", repoFullName, iss.Number),
		ExternalID:        fmt.Sprintf("%s#I-%d", repoFullName, iss.Number),
		PlatformProjectID: platformRepoID,
		RepoFullName:      repoFullName,
		Raw:               []byte(iss.Body),
		Meta:              meta,
		S3Key:             fmt.Sprintf("descriptions/%s/%s/issues/%s/%d", orgID, connectorID, platformRepoID, iss.Number),
	}); err != nil {
		return fmt.Errorf("emitting issue description: %w", err)
	}
	return nil
}

func buildIssuesDataQuery(owner, repo, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}
	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    issues(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        updatedAt
        body
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, owner, repo, pageSize, afterClause)
}

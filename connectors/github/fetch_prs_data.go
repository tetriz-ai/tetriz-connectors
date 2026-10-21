// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

type ghPRDescription struct {
	Number    int       `json:"number"`
	UpdatedAt time.Time `json:"updatedAt"`
	Body      string    `json:"body"`
}

type fetchPRsDataResponse struct {
	Repository struct {
		DatabaseID   int64 `json:"databaseId"`
		PullRequests *struct {
			Nodes    []ghPRDescription `json:"nodes"`
			PageInfo ghPageInfo        `json:"pageInfo"`
		} `json:"pullRequests"`
	} `json:"repository"`
}

type FetchPRsDataOp struct {
	paginationCfg     *config.GitHubPaginationConfig
	checkpointReserve time.Duration
}

func (op *FetchPRsDataOp) OperationID() sdk.OperationID { return opFetchPRsData }

func (op *FetchPRsDataOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}
	if params.BlobStore == nil {
		// A host that wired no blob store: log and return the input cursor so the op doesn't enter back-off.
		logger.WarnWithCtx(ctx, "fetch_prs_data: BlobStore missing, skipping tick",
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
			logger.WarnWithCtx(ctx, "fetch_prs_data: deadline approaching, returning partial cursor",
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

		pageCfg := opPageConfig(op.paginationCfg, opFetchPRsData)
		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)

		tally.Attempt()
		latestUpdated, err := fetchPRsDataForRepo(ctx, params.VendorGateway, params.BlobStore, params.Sink, op.OperationID(), params.Config.OrganizationID, owner, repo, repoFullName, since, resumePageCursor, state.Timestamp, onPage, pageCfg)
		if err != nil {
			logger.Error(ctx, "fetch_prs_data: skipping repo after fetch failure",
				zap.String("repo", repoFullName), zap.Error(err))
			repoErrors = append(repoErrors, fmt.Errorf("%s: %w", repoFullName, err))
			tally.Fail(repoFullName, err)
			continue
		}

		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetch_prs_data: fetched PR descriptions",
			zap.String("repo", repoFullName), zap.Time("cursor", latestUpdated))
	}

	if err := tally.Err(len(repoInfos)); err != nil {
		return nil, fmt.Errorf("fetch_prs_data: %w", err)
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	if len(repoErrors) > 0 {
		logger.WarnWithCtx(ctx, "fetch_prs_data: completed with repo-level failures",
			zap.Int("failed_repos", len(repoErrors)),
			zap.Int("total_repos", len(repoInfos)))
	}
	return buildStateCursor(cursorStates), nil
}

func fetchPRsDataForRepo(
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
			return buildPRsDataQuery(owner, repo, graphQLCursor, ps)
		}, repoFullName, false)
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch_prs_data for %s: %w", repoFullName, err)
		}

		var data fetchPRsDataResponse
		if err := json.Unmarshal(body, &data); err != nil {
			return time.Time{}, fmt.Errorf("parse fetch_prs_data response for %s: %w", repoFullName, err)
		}

		prs := data.Repository.PullRequests
		if prs == nil {
			// A null connection is never normal EOF (an accessible repo returns a
			// non-null connection with nodes:[]); nil means forbidden/truncated —
			// fail loud so the repo is retried, never silently recorded empty.
			return time.Time{}, fmt.Errorf("fetch_prs_data for %s: null pullRequests connection (forbidden or truncated response)", repoFullName)
		}
		repoDatabaseID := data.Repository.DatabaseID

		done := false
		for _, pr := range prs.Nodes {
			if !since.IsZero() && pr.UpdatedAt.Before(since) {
				done = true
				break
			}
			if err := emitPRDescription(ctx, s3Client, sink, opID, orgID, owner, repoFullName, repoDatabaseID, pr); err != nil {
				// Abort repo loop: PRs are DESC by UpdatedAt, so advancing past a failed
				// emit would never re-fetch it. SHA-skip absorbs the dup on retry.
				return latestUpdated, fmt.Errorf("emit PR %d in %s: %w", pr.Number, repoFullName, err)
			}
			if pr.UpdatedAt.After(latestUpdated) {
				latestUpdated = pr.UpdatedAt
			}
		}

		if done || !prs.PageInfo.HasNextPage {
			break
		}
		if graphQLCursor, err = nextGraphQLCursor(graphQLCursor, prs.PageInfo.EndCursor); err != nil {
			return time.Time{}, fmt.Errorf("fetch_prs_data for %s: %w", repoFullName, err)
		}

		if onPage != nil {
			if err := onPage(ctx, latestUpdated, graphQLCursor); err != nil {
				return time.Time{}, fmt.Errorf("checkpoint for %s: %w", repoFullName, err)
			}
		}
	}

	return latestUpdated, nil
}

func emitPRDescription(
	ctx context.Context,
	s3Client s3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, owner, repoFullName string,
	repoDatabaseID int64,
	pr ghPRDescription,
) error {
	externalID := fmt.Sprintf("%s#PR-%d", repoFullName, pr.Number)

	platformRepoID := ""
	if repoDatabaseID != 0 {
		platformRepoID = strconv.FormatInt(repoDatabaseID, 10)
	}

	meta := map[string]string{
		"repo":  repoFullName,
		"owner": owner,
	}
	if platformRepoID != "" {
		meta["platform_repo_id"] = platformRepoID
	}

	body := pr.Body
	if strings.TrimSpace(body) == "" {
		return nil
	}

	if platformRepoID == "" {
		// No stable repo ID yet — skip until a future fetch resolves it.
		logger.WarnWithCtx(ctx, "fetch_prs_data: skipping PR without resolved platform_repo_id",
			zap.String("repo", repoFullName),
			zap.Int("pr", pr.Number))
		return nil
	}

	raw := []byte(body)
	s3Key := fmt.Sprintf("descriptions/%s/%s/pull_requests/%s/%d", orgID, connectorID, platformRepoID, pr.Number)
	if err := s3Client.Write(ctx, s3Key, raw, ""); err != nil {
		return fmt.Errorf("s3 write %s: %w", s3Key, err)
	}

	digest := sha256.Sum256(raw)
	sha := "sha256:" + hex.EncodeToString(digest[:])

	payload, err := json.Marshal(map[string]any{
		"s3_key":           s3Key,
		"pr_number":        pr.Number,
		"platform_repo_id": platformRepoID,
		"repo_full_name":   repoFullName,
	})
	if err != nil {
		return fmt.Errorf("marshal description payload: %w", err)
	}

	meta["description_sha"] = sha
	// No DataDate: the pointer is the description's current state, and
	// updatedAt moves on every edit, which would fork the row per edit.
	if err := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, time.Time{}); err != nil {
		return fmt.Errorf("emitting pr description record: %w", err)
	}
	return nil
}

func buildPRsDataQuery(owner, repo, cursor string, pageSize int) string {
	afterClause := graphQLNullCursor
	if cursor != "" {
		afterClause = fmt.Sprintf("%q", cursor)
	}
	return fmt.Sprintf(`{
  repository(owner: %q, name: %q) {
    databaseId
    pullRequests(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: DESC}) {
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

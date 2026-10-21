// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// destination.repository.uuid drives the S3 key because Bitbucket repos can be renamed.
type bbPRDescription struct {
	ID          int       `json:"id"`
	UpdatedOn   time.Time `json:"updated_on"`
	Description string    `json:"description"`
	Destination *struct {
		Repository struct {
			UUID string `json:"uuid"`
		} `json:"repository"`
	} `json:"destination,omitempty"`
}

type FetchPRsDataOp struct {
	paginationCfg *config.BitbucketPaginationConfig
}

func (op *FetchPRsDataOp) OperationID() sdk.OperationID { return opFetchPRsData }

func (op *FetchPRsDataOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}
	if params.BlobStore == nil {
		logger.WarnWithCtx(ctx, "fetch_prs_data: BlobStore missing, skipping tick",
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.String(logger.OrgID, params.Config.OrganizationID))
		return params.Cursor, nil
	}

	// Discovery-only pacing budget (this op does not pace its own loop). See
	// FetchCommitsOp.Execute for why repo discovery is still paced.
	repos, err := fetchAllRepos(ctx, params.VendorGateway, settings, op.OperationID(), params.Config.OrganizationID, &pacing.PacingBudget{})
	if err != nil {
		return nil, fmt.Errorf("fetching all repos: %w", err)
	}

	cursorStates, droppedPinned := loadCursorStates(ctx, params.Cursor)

	var tally repotally.Tally
	for _, repoFullName := range repos {
		if err := ctx.Err(); err != nil {
			logger.WarnWithCtx(ctx, "fetch_prs_data: context done, breaking out of repo loop",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("next_repo", repoFullName), zap.Error(err))
			break
		}
		workspace, repoSlug, splitErr := splitFullName(repoFullName)
		if splitErr != nil {
			logger.Error(ctx, "fetch_prs_data: skipping invalid repo full name",
				zap.String("repo", repoFullName), zap.Error(splitErr))
			continue
		}

		state := cursorStates[repoFullName]
		since := firstSyncSince(state.Timestamp)
		resumePage := ""
		if state.PageCursor != "" && !state.Since.IsZero() {
			since = state.Since
			resumePage = state.PageCursor
		}

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, repoFullName, since)
		tally.Attempt()
		latestUpdated, err := fetchPRsDataForRepo(ctx, params.VendorGateway, params.BlobStore, params.Sink, op.OperationID(), params.Config.OrganizationID, workspace, repoSlug, repoFullName, since, state.Timestamp, resumePage, onPage)
		if err != nil {
			logger.Error(ctx, "fetch_prs_data: skipping repo after fetch failure",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.String("repo", repoFullName), zap.Error(err))
			tally.Fail(repoFullName, err)
			continue
		}
		// A first sync that observed nothing records nothing (see
		// watermarkIsPinnedBeyondFloor): latestUpdated still equals the synthetic `since` floor, and persisting it would publish
		// a query bound as observed progress — the pin the cursor loaders drop.
		if state.Timestamp.IsZero() && latestUpdated.Equal(since) && resumePage == "" {
			delete(cursorStates, repoFullName)
			logger.Info(ctx, "fetch_prs_data: nothing observed, watermark left unset",
				zap.String("repo", repoFullName))
			continue
		}
		cursorStates[repoFullName] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetch_prs_data: fetched PR descriptions",
			zap.String("repo", repoFullName), zap.Time("cursor", latestUpdated))
	}

	if err := tally.Err(len(repos)); err != nil {
		return nil, fmt.Errorf("fetch_prs_data: %w", err)
	}
	// droppedPinned defeats the nothing-to-write fallback: a cursor whose only
	// entries were dropped as pinned must persist as empty, not be resurrected.
	if len(cursorStates) == 0 && !droppedPinned {
		return params.Cursor, nil
	}
	return buildStateCursor(ctx, cursorStates), nil
}

func fetchPRsDataForRepo(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client bbS3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, workspace, repoSlug, repoFullName string,
	since time.Time,
	priorWatermark time.Time,
	resumePage string,
	onPage pageCheckpointFn,
) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupBitbucketREST)
	latestUpdated := since
	if priorWatermark.After(latestUpdated) {
		latestUpdated = priorWatermark
	}
	page := int32(1)
	if resumePage != "" {
		parsed, parseErr := strconv.ParseInt(resumePage, 10, 32)
		switch {
		case parseErr != nil:
			logger.WarnWithCtx(ctx, "fetch_prs_data: unparseable resume page cursor; restarting repo at page 1",
				zap.String("repo", repoFullName), zap.String("resume_page", resumePage), zap.Error(parseErr))
		case parsed > 0:
			page = int32(parsed)
		}
	}

	for {
		if page > maxListPages {
			return time.Time{}, errListPageCap("fetch_prs_data for " + repoFullName)
		}
		protoResp, err := restGW.Execute(ctx, &connectorpb.BitbucketListPullRequestsRequest{
			Workspace: workspace,
			RepoSlug:  repoSlug,
			Page:      page,
			Pagelen:   50,
		})
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch_prs_data for %s: %w", repoFullName, err)
		}
		resp, ok := protoResp.(*connectorpb.BitbucketListPullRequestsResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("fetch_prs_data %s: unexpected response type %T", repoFullName, protoResp)
		}

		done := false
		for _, raw := range resp.GetPullRequests() {
			var pr bbPRDescription
			if err := json.Unmarshal(raw, &pr); err != nil {
				return time.Time{}, fmt.Errorf("fetch_prs_data %s: unmarshal PR: %w", repoFullName, err)
			}
			if !since.IsZero() && pr.UpdatedOn.Before(since) {
				done = true
				break
			}
			if err := emitPRDescription(ctx, s3Client, sink, opID, orgID, workspace, repoFullName, pr); err != nil {
				// See github sibling for the cursor-advance trap rationale.
				return latestUpdated, fmt.Errorf("emit PR %d in %s: %w", pr.ID, repoFullName, err)
			}
			if pr.UpdatedOn.After(latestUpdated) {
				latestUpdated = pr.UpdatedOn
			}
		}

		// Decide whether another page follows BEFORE invoking onPage so the
		// checkpoint fires on the final page too — an empty nextPageCursor tells
		// the caller the iteration finished cleanly (clear PageCursor).
		finished := done || resp.GetNextPageUrl() == ""
		nextPageCursor := ""
		if !finished {
			nextPageCursor = strconv.Itoa(int(page) + 1)
		}
		if onPage != nil {
			if cpErr := onPage(ctx, latestUpdated, nextPageCursor); cpErr != nil {
				// Best-effort progress, not the success contract — log and keep
				// going so a checkpoint blip doesn't abort an otherwise-good run.
				logger.Error(ctx, "fetch_prs_data: per-page checkpoint failed (continuing)",
					zap.String("repo", repoFullName),
					zap.Int32("page", page),
					zap.Error(cpErr))
			}
		}

		if finished {
			break
		}
		page++
	}

	return latestUpdated, nil
}

func emitPRDescription(
	ctx context.Context,
	s3Client bbS3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, workspace, repoFullName string,
	pr bbPRDescription,
) error {
	externalID := fmt.Sprintf("%s#PR-%d", repoFullName, pr.ID)

	platformRepoID := ""
	if pr.Destination != nil {
		platformRepoID = pr.Destination.Repository.UUID
	}

	meta := map[string]string{
		"repo":      repoFullName,
		"workspace": workspace,
	}
	if platformRepoID != "" {
		meta["platform_repo_id"] = platformRepoID
	}

	body := pr.Description
	if strings.TrimSpace(body) == "" {
		return nil
	}

	if platformRepoID == "" {
		// destination.repository.uuid is the rename-proof key — skip + retry next tick.
		logger.WarnWithCtx(ctx, "fetch_prs_data: skipping PR without destination.repository.uuid",
			zap.String("repo", repoFullName), zap.Int("pr", pr.ID))
		return nil
	}

	raw := []byte(body)
	s3Key := fmt.Sprintf("descriptions/%s/%s/pull_requests/%s/%d", orgID, connectorID, platformRepoID, pr.ID)
	if err := s3Client.Write(ctx, s3Key, raw, ""); err != nil {
		return fmt.Errorf("s3 write %s: %w", s3Key, err)
	}

	digest := sha256.Sum256(raw)
	sha := "sha256:" + hex.EncodeToString(digest[:])

	payload, err := json.Marshal(map[string]any{
		"s3_key":           s3Key,
		"pr_number":        pr.ID,
		"platform_repo_id": platformRepoID,
		"repo_full_name":   repoFullName,
	})
	if err != nil {
		return fmt.Errorf("marshal description payload: %w", err)
	}

	meta["description_sha"] = sha
	// A description pointer is current state: no DataDate. updated_on moves on
	// every edit and would fork the pointer into a row per edit.
	if err := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, time.Time{}); err != nil {
		return fmt.Errorf("emitting PR description record: %w", err)
	}
	return nil
}

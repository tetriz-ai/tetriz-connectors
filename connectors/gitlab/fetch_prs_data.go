// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

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

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/repotally"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const opFetchPRsData = "fetch_prs_data"

// glPRDescription is the subset of a merge-request list entry fetch_prs_data
// needs: the IID (project-scoped MR number), the update timestamp (the
// cursor advance; the pointer record has no DataDate), and the inline
// description body that becomes the S3 blob.
type glPRDescription struct {
	IID         int       `json:"iid"`
	UpdatedAt   time.Time `json:"updated_at"`
	Description string    `json:"description"`
}

// glS3Writer is the narrow S3 surface this op needs, matching
// blobstore.Store.Write — declared locally so tests can stub it.
type glS3Writer interface {
	Write(ctx context.Context, dst string, content []byte, aclString string) error
}

// FetchPRsDataOp ships each MR's description to S3 and emits a description-ref
// record carrying the S3 key + a sha256 of the body, mirroring the GitHub
// and Bitbucket fetch_prs_data siblings. The description is inline on the MR
// list payload, so no extra vendor call is needed — it reuses the same
// paginated GitLabListProjectMergeRequestsRequest fetch_prs walks.
type FetchPRsDataOp struct{}

func (op *FetchPRsDataOp) OperationID() sdk.OperationID { return opFetchPRsData }

func (op *FetchPRsDataOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}
	if params.BlobStore == nil {
		// A host that wired no blob store: log and return the input cursor so
		// the op does not enter back-off.
		logger.WarnWithCtx(ctx, "fetch_prs_data: BlobStore missing, skipping tick",
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.String(logger.OrgID, params.Config.OrganizationID))
		return params.Cursor, nil
	}

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetching projects: %w", err)
	}

	cursorStates := existingCursorStates(ctx, params.Cursor)

	var tally repotally.Tally
	for _, project := range projects {
		if err := ctx.Err(); err != nil {
			logger.WarnWithCtx(ctx, "fetch_prs_data: context done, breaking out of project loop",
				zap.String(logger.OrgID, params.Config.OrganizationID),
				zap.Int("next_project", project.ID), zap.Error(err))
			break
		}
		projectKey := strconv.Itoa(project.ID)

		// Every run — clean or resuming after a mid-pagination kill — starts at
		// page 1 with updated_after = the last checkpointed high-water mark. See
		// repoCursorState's doc comment (operations.go) for why resuming by a
		// saved page NUMBER is unsafe on this endpoint.
		state := getRepoCursorState(ctx, params.Cursor, projectKey)
		since := firstSyncSince(state.Timestamp)

		onPage := makePageCheckpoint(params.Checkpoint, cursorStates, projectKey)
		tally.Attempt()
		latestUpdated, err := fetchPRsDataForProject(ctx, params.VendorGateway, params.BlobStore, params.Sink, op.OperationID(), params.Config.OrganizationID, settings.GroupName, project, since, state.Timestamp, onPage)
		if err != nil {
			logger.Error(ctx, "fetch_prs_data: skipping project after fetch failure",
				zap.String("project", project.PathWithNamespace), zap.Error(err))
			tally.Fail(project.PathWithNamespace, err)
			continue
		}
		cursorStates[projectKey] = repoCursorState{Timestamp: latestUpdated}
		logger.Info(ctx, "fetch_prs_data: fetched MR descriptions",
			zap.String("project", project.PathWithNamespace), zap.Time("cursor", latestUpdated))
	}

	if err := tally.Err(len(projects)); err != nil {
		return nil, fmt.Errorf("fetch_prs_data: %w", err)
	}
	if len(cursorStates) == 0 {
		return params.Cursor, nil
	}
	return buildStateCursor(ctx, cursorStates), nil
}

func fetchPRsDataForProject(
	ctx context.Context,
	gw sdk.VendorGateway,
	s3Client glS3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, groupName string,
	project glProject,
	since time.Time,
	priorWatermark time.Time,
	onPage pageCheckpointFn,
) (time.Time, error) {
	restGW := gw.ForGroup(constants.GroupGitLabREST)
	// since == priorWatermark today (firstSyncSince is the identity for a
	// non-zero input), but this guard keeps that monotonic even if
	// firstSyncSince ever grows a clock-skew overlap buffer — without it, a
	// tick with no new MRs would persist a high-water mark BELOW the prior
	// one, regressing the cursor.
	latestUpdated := since
	if priorWatermark.After(latestUpdated) {
		latestUpdated = priorWatermark
	}
	page := int32(1)
	updatedAfter := since.UTC().Format(time.RFC3339)
	// stuckAt pins the high-water mark at the first malformed entry so it is
	// never silently skipped forever: pages are walked ascending by updated_at
	// (see internal/gateway/gitlab/requests.go's order_by=updated_at&sort=asc), so once one entry fails
	// to parse, every later entry in this run — however well-formed — is newer
	// and would otherwise leapfrog the cursor past it. Once stuckAt is set,
	// latestUpdated no longer advances for the rest of this run (all pages), so
	// the next tick's `since` re-includes the malformed entry (and, harmlessly,
	// every already-emitted entry after it — idempotent via the description_sha
	// skip). This retries indefinitely without blocking the whole project: this
	// run still emits everything it can parse, just does not mark the failure
	// point as done. Every resume restarts at page 1 (see repoCursorState in
	// operations.go), so the malformed entry's own page is always re-walked —
	// no separate page-freeze is needed here.
	var stuckAt bool

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListProjectMergeRequestsRequest{
			ProjectId:    int64(project.ID),
			UpdatedAfter: updatedAfter,
			Page:         page,
			PerPage:      100,
		})
		if err != nil {
			return time.Time{}, fmt.Errorf("fetch_prs_data for %s: %w", project.PathWithNamespace, err)
		}
		resp, ok := protoResp.(*connectorpb.GitLabListProjectMergeRequestsResponse)
		if !ok {
			return time.Time{}, fmt.Errorf("fetch_prs_data %s: unexpected response type %T", project.PathWithNamespace, protoResp)
		}

		for _, mrRaw := range resp.GetMergeRequests() {
			var mr glPRDescription
			if err := json.Unmarshal(mrRaw, &mr); err != nil {
				logger.WarnWithCtx(ctx, "fetch_prs_data: malformed MR entry; will retry from here next tick",
					zap.String("project", project.PathWithNamespace), zap.Error(err))
				stuckAt = true
				// stuckAt (checked at the latestUpdated advance below) pins the
				// high-water mark at this entry, so a later entry's success in
				// this same run cannot advance the cursor past this failure; see
				// TestFetchPRsDataOp_MalformedEntryRetriedNotSkippedOrAborted.
				continue // pinned by stuckAt, see comment above
			}
			if err := emitPRDescription(ctx, s3Client, sink, opID, orgID, groupName, project, mr); err != nil {
				// Abort the project loop on emit failure so the checkpoint never
				// advances past a page whose descriptions were not all
				// persisted. The description_sha lets a consumer skip the
				// dup on retry.
				return latestUpdated, fmt.Errorf("emit MR %d in %s: %w", mr.IID, project.PathWithNamespace, err)
			}
			if !stuckAt && mr.UpdatedAt.After(latestUpdated) {
				latestUpdated = mr.UpdatedAt
			}
		}

		finished := resp.GetNextPageUrl() == ""
		if onPage != nil {
			if cpErr := onPage(ctx, latestUpdated); cpErr != nil {
				logger.Error(ctx, "fetch_prs_data: per-page checkpoint failed (continuing)",
					zap.String("project", project.PathWithNamespace),
					zap.Int32("page", page),
					zap.Error(cpErr))
			}
		}

		if finished {
			break
		}
		page++
		if err := checkPageCap("fetch_prs_data merge request listing for "+project.PathWithNamespace, page); err != nil {
			return latestUpdated, err
		}
	}

	return latestUpdated, nil
}

func emitPRDescription(
	ctx context.Context,
	s3Client glS3Writer,
	sink sdk.DataSink,
	opID sdk.OperationID,
	orgID, groupName string,
	project glProject,
	mr glPRDescription,
) error {
	repoFullName := project.PathWithNamespace
	externalID := fmt.Sprintf("%s#PR-%d", repoFullName, mr.IID)

	// project.ID is GitLab's stable platform_repo_id — the key a consumer
	// looks up the parent PR row by (platform_repo_id, pr_number).
	platformRepoID := strconv.Itoa(project.ID)

	meta := map[string]string{
		"repo":             repoFullName,
		"group":            groupName,
		"platform_repo_id": platformRepoID,
	}

	body := mr.Description
	if strings.TrimSpace(body) == "" {
		return nil
	}

	raw := []byte(body)
	s3Key := fmt.Sprintf("descriptions/%s/%s/pull_requests/%s/%d", orgID, connectorID, platformRepoID, mr.IID)
	if err := s3Client.Write(ctx, s3Key, raw, ""); err != nil {
		return fmt.Errorf("s3 write %s: %w", s3Key, err)
	}

	digest := sha256.Sum256(raw)
	sha := "sha256:" + hex.EncodeToString(digest[:])

	payload, err := json.Marshal(map[string]any{
		"s3_key":           s3Key,
		"pr_number":        mr.IID,
		"platform_repo_id": platformRepoID,
		"repo_full_name":   repoFullName,
	})
	if err != nil {
		return fmt.Errorf("marshal description payload: %w", err)
	}

	meta["description_sha"] = sha
	if err := emitRecord(ctx, sink, opID, orgID, externalID, payload, meta, time.Time{}); err != nil {
		return fmt.Errorf("emitting MR description record: %w", err)
	}
	return nil
}

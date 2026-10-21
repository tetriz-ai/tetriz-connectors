// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	repopb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchReposOp lists every repository in the configured Bitbucket workspace
// and emits one normalized RepositoryRecord per repo.
//
// Record key = platform_repo_id (Bitbucket UUID, stable across renames).
// Bitbucket's /2.0/repositories/{workspace} endpoint is paginated by
// page number; each page carries a next_page_url when more pages exist.
//
// Bitbucket does not expose star/fork/watcher counts on the list endpoint —
// Stats fields are all zero.
type FetchReposOp struct{}

func (op *FetchReposOp) OperationID() sdk.OperationID { return opFetchRepos }

func (op *FetchReposOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    op.OperationID(),
			OrganizationID: params.Config.OrganizationID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          err,
		}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	page := int32(1)
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()
	count := 0
	var allRepos []bbRepository

	for {
		// Fail before saveDiscoveredRepos: a truncated listing must not mark the
		// unvisited repos deleted.
		if page > maxListPages {
			return nil, errListPageCap("list workspace repos")
		}
		protoResp, err := restGW.Execute(ctx, &connectorpb.BitbucketListReposRequest{
			Workspace: settings.Workspace,
			Page:      page,
			Pagelen:   100,
		})
		if err != nil {
			return nil, fmt.Errorf("list workspace repos page=%d: %w", page, err)
		}

		resp, ok := protoResp.(*connectorpb.BitbucketListReposResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *BitbucketListReposResponse", protoResp)
		}

		for _, repoRaw := range resp.GetRepos() {
			var r bbRepository
			if err := json.Unmarshal(repoRaw, &r); err != nil {
				logger.WarnWithCtx(ctx, "skipping repo: unmarshal failed", zap.Error(err))
				continue
			}
			if r.UUID == "" {
				// Malformed entry — skip.
				continue
			}

			rec := mapBitbucketRepoToRecord(&r, settings)
			payload, mErr := protojson.Marshal(rec)
			if mErr != nil {
				logger.WarnWithCtx(ctx, "skipping repo: marshal RepositoryRecord failed",
					zap.String("platform_repo_id", rec.GetPlatformRepoId()), zap.Error(mErr))
				continue
			}

			if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
				ConnectorID:    string(connectorID),
				OperationID:    string(op.OperationID()),
				OrganizationID: orgID,
				ExternalID:     rec.GetPlatformRepoId(),
				Payload:        payload,
				FetchedAt:      fetchedAt,
				Metadata: map[string]string{
					"full_name": r.FullName,
				},
			}); emitErr != nil {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID,
					OperationID: op.OperationID(),
					Kind:        sdk.ErrKindSinkFailure,
					Cause:       emitErr,
				}
			}
			// Only accumulate after a successful Emit so a marshal/emit failure
			// does not add the repo to the discovered-repos cache.
			allRepos = append(allRepos, r)
			count++
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
	}

	saveDiscoveredRepos(ctx, params, settings, allRepos)
	logger.Info(ctx, "fetch_repos complete",
		zap.String("workspace", settings.Workspace),
		zap.Int("count", count))
	return params.Cursor, nil
}

// mapBitbucketRepoToRecord normalizes Bitbucket-specific repository fields into
// the cross-vendor RepositoryRecord proto. Bitbucket-specific cold fields
// (workspace_uuid, mainbranch name+type, project_key, project_uuid, scm) go
// into vendor_blob_json.
// tracked_branch is resolved via the same priority order used by fetch_commits:
// per-repo override > org-level TrackedBranch > repo default_branch.
func mapBitbucketRepoToRecord(bb *bbRepository, settings BitbucketSettings) *repopb.RepositoryRecord {
	visibility := repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PUBLIC
	if bb.IsPrivate {
		visibility = repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE
	}

	rec := &repopb.RepositoryRecord{
		PlatformRepoId:  bb.UUID,
		Name:            bb.Slug,
		FullName:        bb.FullName,
		Description:     bb.Description,
		Visibility:      visibility,
		PrimaryLanguage: bb.Language,
		SizeKb:          bb.Size / 1024,
		IsFork:          bb.Parent != nil,
		// Bitbucket has no star/fork/watcher counts on the list endpoint.
		Stats: &repopb.Stats{},
	}

	if bb.Workspace != nil {
		rec.OwnerLogin = bb.Workspace.Slug
	}

	if bb.Mainbranch != nil {
		rec.DefaultBranch = bb.Mainbranch.Name
	}

	if bb.Parent != nil {
		rec.ParentPlatformRepoId = bb.Parent.UUID
	}

	// Populate clone URLs from the links block.
	if bb.Links != nil {
		if bb.Links.HTML != nil {
			rec.WebUrl = bb.Links.HTML.Href
		}
		for _, cl := range bb.Links.Clone {
			switch cl.Name {
			case "https":
				rec.HttpCloneUrl = cl.Href
			case "ssh":
				rec.SshUrl = cl.Href
			}
		}
	}

	if !bb.CreatedOn.IsZero() {
		rec.VendorCreatedAt = timestamppb.New(bb.CreatedOn)
	}
	if !bb.UpdatedOn.IsZero() {
		rec.VendorUpdatedAt = timestamppb.New(bb.UpdatedOn)
	}

	rec.VendorBlobJson = buildBitbucketVendorBlob(bb) // raw JSON: cold vendor JSON

	// Resolve tracked_branch with the same priority order as fetch_commits:
	// per-repo override > org-level TrackedBranch > repo DefaultBranch.
	branch := rec.GetDefaultBranch()
	if settings.TrackedBranch != "" {
		branch = settings.TrackedBranch
	}
	if override, ok := settings.RepoBranchOverrides[bb.FullName]; ok && override != "" {
		branch = override
	}
	rec.TrackedBranch = branch

	return rec
}

// buildBitbucketVendorBlob serializes Bitbucket-specific cold fields that the
// typed cross-vendor RepositoryRecord does not capture. These are stored under
// metadata.vendor.bitbucket in a consumer's repository row.
func buildBitbucketVendorBlob(bb *bbRepository) []byte {
	workspaceUUID := ""
	if bb.Workspace != nil {
		workspaceUUID = bb.Workspace.UUID
	}

	mainbranch := map[string]string{}
	if bb.Mainbranch != nil {
		mainbranch["name"] = bb.Mainbranch.Name
		mainbranch["type"] = bb.Mainbranch.Type
	}

	projectKey := ""
	projectUUID := ""
	if bb.Project != nil {
		projectKey = bb.Project.Key
		projectUUID = bb.Project.UUID
	}

	blob := map[string]interface{}{
		"workspace_uuid": workspaceUUID,
		"mainbranch":     mainbranch,
		"project_key":    projectKey,
		"project_uuid":   projectUUID,
		"scm":            bb.SCM,
	}
	b, err := json.Marshal(blob) // raw JSON: cold vendor JSON
	if err != nil {
		// blob holds only primitive types — marshal cannot fail in practice;
		// log if it ever does so a future field addition can't lose vendor data silently.
		logger.ErrorNoCtx("fetch_repos: failed to marshal bitbucket vendor blob; emitting empty blob",
			zap.String("full_name", bb.FullName), zap.Error(err))
		return []byte("{}")
	}
	return b
}

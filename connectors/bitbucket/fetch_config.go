// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// opFetchConfig captures per-repo governance config (default branch + allowed/
// default merge strategies, which live on the branch object).
const opFetchConfig = "fetch_config"

// FetchConfigOp emits one fetch_config record per discovered repo (scope=REPO,
// record key = repo UUID). Bitbucket exposes merge strategies only on the branch
// object, so this resolves the repo's UUID + main branch, then reads that branch.
// Latest-only; per-repo failures are logged and skipped, never fatal.
type FetchConfigOp struct{}

// OperationID returns the fetch_config operation id.
func (op *FetchConfigOp) OperationID() sdk.OperationID { return opFetchConfig }

// bbBranchConfig is the governance subset of a Bitbucket branch object.
type bbBranchConfig struct {
	DefaultMergeStrategy string   `json:"default_merge_strategy"`
	MergeStrategies      []string `json:"merge_strategies"`
}

// Execute fetches per-repo config and emits fetch_config records.
func (op *FetchConfigOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
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
	fetchedAt := time.Now().UTC()
	orgID := params.Config.OrganizationID

	for _, dr := range settings.DiscoveredRepos {
		if !dr.IsActive() {
			continue // skip DELETED repos, matching the other Bitbucket pull ops
		}
		slug := repoSlugFromFullName(dr.FullName)
		if slug == "" {
			continue
		}

		// Resolve the repo UUID (record key) + main branch.
		repoResp, rErr := restGW.Execute(ctx, &connectorpb.BitbucketGetRepoRequest{
			Workspace: settings.Workspace, RepoSlug: slug,
		})
		if rErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: get repo failed; skipping",
				zap.String("full_name", dr.FullName), zap.Error(rErr))
			continue
		}
		repoObj, ok := repoResp.(*connectorpb.BitbucketGetRepoResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *BitbucketGetRepoResponse", repoResp)
		}
		var repo bbRepository
		if err := json.Unmarshal(repoObj.GetRepo(), &repo); err != nil || repo.UUID == "" {
			logger.WarnWithCtx(ctx, "fetch_config: unmarshal repo failed; skipping",
				zap.String("full_name", dr.FullName), zap.Error(err))
			continue
		}

		branch := dr.DefaultBranch
		if repo.Mainbranch != nil && repo.Mainbranch.Name != "" {
			branch = repo.Mainbranch.Name
		}

		repoSettings := map[string]any{"default_branch": branch}
		if branch != "" {
			brResp, bErr := restGW.Execute(ctx, &connectorpb.BitbucketGetBranchRequest{
				Workspace: settings.Workspace, RepoSlug: slug, Branch: branch,
			})
			if bErr != nil {
				logger.WarnWithCtx(ctx, "fetch_config: get branch failed; merge strategies omitted",
					zap.String("full_name", dr.FullName), zap.Error(bErr))
			} else if br, okBr := brResp.(*connectorpb.BitbucketGetBranchResponse); okBr && len(br.GetRawJson()) > 0 {
				var bc bbBranchConfig
				if err := json.Unmarshal(br.GetRawJson(), &bc); err != nil {
					logger.WarnWithCtx(ctx, "fetch_config: unmarshal branch failed; merge strategies omitted",
						zap.String("full_name", dr.FullName), zap.Error(err))
				} else {
					repoSettings["default_merge_strategy"] = bc.DefaultMergeStrategy
					repoSettings["merge_strategies"] = bc.MergeStrategies
				}
			}
		}

		blob := map[string]any{"repo_settings": repoSettings}

		// Branch restrictions (required approvals + count, passing builds,
		// restrict-merges) need repository:admin — attempt-and-omit on 403.
		if rrResp, rrErr := restGW.Execute(ctx, &connectorpb.BitbucketListBranchRestrictionsRequest{
			Workspace: settings.Workspace, RepoSlug: slug, Pagelen: 100,
		}); rrErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: branch-restrictions unavailable; omitting",
				zap.String("full_name", dr.FullName), zap.Error(rrErr))
		} else if rr, okRR := rrResp.(*connectorpb.BitbucketListBranchRestrictionsResponse); okRR && len(rr.GetRestrictions()) > 0 {
			restrictions := make([]json.RawMessage, 0, len(rr.GetRestrictions()))
			for _, r := range rr.GetRestrictions() {
				restrictions = append(restrictions, json.RawMessage(r))
			}
			blob["branch_restrictions"] = restrictions
		}

		payload, mErr := json.Marshal(blob)
		if mErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: marshal blob failed; skipping",
				zap.String("full_name", dr.FullName), zap.Error(mErr))
			continue
		}
		if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    string(op.OperationID()),
			OrganizationID: orgID,
			ExternalID:     repo.UUID,
			Payload:        payload,
			FetchedAt:      fetchedAt,
			Metadata:       map[string]string{"scope": "REPO", "full_name": dr.FullName},
		}); emitErr != nil {
			return nil, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: op.OperationID(),
				Kind: sdk.ErrKindSinkFailure, Cause: emitErr,
			}
		}
	}

	logger.Info(ctx, "fetch_config complete",
		zap.String("workspace", settings.Workspace), zap.Int("repos", len(settings.DiscoveredRepos)))
	return params.Cursor, nil
}

// repoSlugFromFullName extracts the repo slug from a "workspace/slug" full name.
func repoSlugFromFullName(fullName string) string {
	if i := strings.LastIndexByte(fullName, '/'); i >= 0 && i < len(fullName)-1 {
		return fullName[i+1:]
	}
	return fullName
}

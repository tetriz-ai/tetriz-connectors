// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchConfigOp captures GitLab governance config: one ACCOUNT-scope record for
// the group (default_branch_protection_defaults) and one REPO-scope record per
// discovered project (merge_method, squash_option, default_branch), for the
// consumer's per-repo config record. Latest-only — no cursor. A per-entity fetch failure is logged and
// skipped, never fatal.
type FetchConfigOp struct{}

// OperationID returns the fetch_config operation id.
func (op *FetchConfigOp) OperationID() sdk.OperationID { return opFetchConfig }

// glProjectConfig is the governance subset of a GitLab project object (all
// fields are present in the GET /projects/:id response we already fetch).
type glProjectConfig struct {
	DefaultBranch                          string `json:"default_branch"`
	MergeMethod                            string `json:"merge_method"`
	SquashOption                           string `json:"squash_option"`
	OnlyAllowMergeIfPipelineSucceeds       bool   `json:"only_allow_merge_if_pipeline_succeeds"`
	OnlyAllowMergeIfAllDiscussionsResolved bool   `json:"only_allow_merge_if_all_discussions_are_resolved"`
	RemoveSourceBranchAfterMerge           bool   `json:"remove_source_branch_after_merge"`
}

// Execute fetches group + per-project config and emits fetch_config records.
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

	restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	fetchedAt := time.Now().UTC()
	orgID := params.Config.OrganizationID

	emit := func(externalID string, scope string, fullName string, blob map[string]any) error {
		payload, mErr := json.Marshal(blob)
		if mErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: marshal blob failed; skipping",
				zap.String(logger.ExternalID, externalID), zap.String("scope", scope), zap.Error(mErr))
			return nil
		}
		md := map[string]string{"scope": scope}
		if fullName != "" {
			md["full_name"] = fullName
		}
		if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    string(op.OperationID()),
			OrganizationID: orgID,
			ExternalID:     externalID,
			Payload:        payload,
			FetchedAt:      fetchedAt,
			Metadata:       md,
		}); emitErr != nil {
			return &sdk.SyncError{
				ConnectorID: connectorID, OperationID: op.OperationID(),
				Kind: sdk.ErrKindSinkFailure, Cause: emitErr,
			}
		}
		return nil
	}

	// ACCOUNT scope — group settings (cascading defaults). Keyed "group-<id>":
	// group and project ids are separate sequences, so a bare group id can equal
	// a REPO record's project id and overwrite it.
	if settings.GroupID != 0 {
		if resp, gErr := restGW.Execute(ctx, &connectorpb.GitLabGetGroupRequest{GroupId: settings.GroupID}); gErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: get group failed; skipping account scope", zap.Error(gErr))
		} else if g, ok := resp.(*connectorpb.GitLabGetGroupResponse); ok && len(g.GetRawJson()) > 0 {
			if err := emit(fmt.Sprintf("group-%d", settings.GroupID), "ACCOUNT", "", map[string]any{
				"group_settings": json.RawMessage(g.GetRawJson()),
			}); err != nil {
				return nil, fmt.Errorf("emitting account config: %w", err)
			}
		}
	}

	// REPO scope — per discovered project.
	for _, dr := range settings.DiscoveredRepos {
		if dr.ProjectID == 0 {
			continue
		}
		resp, pErr := restGW.Execute(ctx, &connectorpb.GitLabGetProjectRequest{ProjectId: dr.ProjectID})
		if pErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: get project failed; skipping",
				zap.Int64("project_id", dr.ProjectID), zap.Error(pErr))
			continue
		}
		p, ok := resp.(*connectorpb.GitLabGetProjectResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *GitLabGetProjectResponse", resp)
		}
		var gl glProjectConfig
		if err := json.Unmarshal(p.GetRawJson(), &gl); err != nil {
			logger.WarnWithCtx(ctx, "fetch_config: unmarshal project failed; skipping",
				zap.Int64("project_id", dr.ProjectID), zap.Error(err))
			continue
		}
		blob := map[string]any{
			"repo_settings": map[string]any{
				"default_branch":                               gl.DefaultBranch,
				"merge_method":                                 gl.MergeMethod,
				"squash_option":                                gl.SquashOption,
				"only_allow_merge_if_pipeline_succeeds":        gl.OnlyAllowMergeIfPipelineSucceeds,
				"only_allow_merge_if_all_discussions_resolved": gl.OnlyAllowMergeIfAllDiscussionsResolved,
				"remove_source_branch_after_merge":             gl.RemoveSourceBranchAfterMerge,
			},
		}

		// Approval policy (Premium/role-gated): required count, reset-on-push,
		// author/committer approval. Attempt-and-omit on 403/404.
		if aResp, aErr := restGW.Execute(ctx, &connectorpb.GitLabGetProjectApprovalsRequest{ProjectId: dr.ProjectID}); aErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: approvals unavailable; omitting",
				zap.Int64("project_id", dr.ProjectID), zap.Error(aErr))
		} else if a, okA := aResp.(*connectorpb.GitLabGetProjectApprovalsResponse); okA && len(a.GetRawJson()) > 0 {
			blob["approvals"] = json.RawMessage(a.GetRawJson())
		}

		// Per-branch protection (allowed-to-merge/push, code-owner). One page
		// (per_page=100); protected-branch lists are small.
		if pbResp, pbErr := restGW.Execute(ctx, &connectorpb.GitLabListProtectedBranchesRequest{ProjectId: dr.ProjectID, PerPage: 100}); pbErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: protected_branches unavailable; omitting",
				zap.Int64("project_id", dr.ProjectID), zap.Error(pbErr))
		} else if pb, okPB := pbResp.(*connectorpb.GitLabListProtectedBranchesResponse); okPB && len(pb.GetProtectedBranches()) > 0 {
			branches := make([]json.RawMessage, 0, len(pb.GetProtectedBranches()))
			for _, b := range pb.GetProtectedBranches() {
				branches = append(branches, json.RawMessage(b))
			}
			blob["protected_branches"] = branches
		}

		if err := emit(fmt.Sprintf("%d", dr.ProjectID), "REPO", dr.FullName, blob); err != nil {
			return nil, fmt.Errorf("emitting repo config: %w", err)
		}
	}

	logger.Info(ctx, "fetch_config complete",
		zap.Int64("group_id", settings.GroupID), zap.Int("projects", len(settings.DiscoveredRepos)))
	return params.Cursor, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	repopb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchReposOp lists every project in the configured GitLab group and emits
// one normalized RepositoryRecord per project.
//
// Record key = platform_repo_id (stable numeric GitLab project ID, survives renames).
// The operation re-uses the existing fetchAllProjects helper so pagination and
// group-scoping are consistent with other GitLab ops.
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

	projects, err := fetchAllProjects(ctx, params.VendorGateway, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_repos: %w", err)
	}

	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()

	for _, p := range projects {
		rec := mapGitLabProjectToRecord(&p, settings)
		payload, mErr := protojson.Marshal(rec)
		if mErr != nil {
			logger.WarnWithCtx(ctx, "skipping project: marshal RepositoryRecord failed",
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
				"full_name": p.PathWithNamespace,
			},
		}); emitErr != nil {
			return nil, &sdk.SyncError{
				ConnectorID: connectorID,
				OperationID: op.OperationID(),
				Kind:        sdk.ErrKindSinkFailure,
				Cause:       emitErr,
			}
		}
	}

	saveDiscoveredRepos(ctx, params, settings, projects)
	logger.Info(ctx, "fetch_repos complete",
		zap.Int64("group_id", settings.GroupID),
		zap.Int("count", len(projects)))
	return params.Cursor, nil
}

// mapGitLabProjectToRecord normalizes GitLab-specific project fields into the
// cross-vendor RepositoryRecord proto. GitLab-specific cold fields
// (namespace_id, namespace_path, namespace_kind, import_status, empty_repo)
// go into vendor_blob_json.
// tracked_branch is resolved via the same priority order used by fetch_repo_tree:
// per-repo override > org-level TrackedBranch > project default_branch.
func mapGitLabProjectToRecord(gl *glProject, settings GitLabSettings) *repopb.RepositoryRecord {
	rec := &repopb.RepositoryRecord{
		PlatformRepoId: fmt.Sprintf("%d", gl.ID),
		Name:           gl.Path,
		FullName:       gl.PathWithNamespace,
		OwnerLogin:     glNamespacePath(gl),
		Description:    gl.Description,
		DefaultBranch:  gl.DefaultBranch,
		Visibility:     parseRepoVisibility(gl.Visibility),
		Archived:       gl.Archived,
		IsFork:         gl.ForkedFromProject != nil,
		Topics:         gl.Topics,
		WebUrl:         gl.WebURL,
		SshUrl:         gl.SSHURLToRepo,
		HttpCloneUrl:   gl.HTTPURLToRepo,
		HasIssues:      gl.IssuesEnabled,
		HasWiki:        gl.WikiEnabled,
		Stats: &repopb.Stats{
			Stars:      treewalker.SafeInt32(gl.StarCount),
			Forks:      treewalker.SafeInt32(gl.ForksCount),
			OpenIssues: treewalker.SafeInt32(gl.OpenIssuesCount),
			// GitLab has no watchers/subscribers concept — leave at zero.
		},
	}

	if gl.ForkedFromProject != nil {
		rec.ParentPlatformRepoId = fmt.Sprintf("%d", gl.ForkedFromProject.ID)
	}
	if !gl.CreatedAt.IsZero() {
		rec.VendorCreatedAt = timestamppb.New(gl.CreatedAt)
	}
	if gl.LastActivityAt != nil && !gl.LastActivityAt.IsZero() {
		rec.VendorPushedAt = timestamppb.New(*gl.LastActivityAt)
	}

	rec.VendorBlobJson = buildGitLabVendorBlob(gl) // raw JSON: cold vendor JSON

	// Resolve tracked_branch with the same priority order as fetch_repo_tree:
	// per-repo override > org-level TrackedBranch > repo DefaultBranch.
	branch := gl.DefaultBranch
	if settings.TrackedBranch != "" {
		branch = settings.TrackedBranch
	}
	if overrides := settings.RepoBranchOverrides; overrides != nil {
		if b, ok := overrides[gl.PathWithNamespace]; ok && b != "" {
			branch = b
		}
	}
	rec.TrackedBranch = branch

	return rec
}

// parseRepoVisibility maps a vendor-provided visibility string ("public",
// "private", "internal") to the corresponding RepositoryVisibility enum value.
func parseRepoVisibility(s string) repopb.RepositoryVisibility {
	switch strings.ToLower(s) {
	case "public":
		return repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PUBLIC
	case "private":
		return repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE
	case "internal":
		return repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_INTERNAL
	default:
		return repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_UNSPECIFIED
	}
}

// buildGitLabVendorBlob serializes GitLab-specific cold fields that the typed
// cross-vendor RepositoryRecord does not capture. These are stored under
// metadata.vendor.gitlab in a consumer's repository row.
func buildGitLabVendorBlob(gl *glProject) []byte {
	blob := map[string]interface{}{
		"namespace_id":   glNamespaceID(gl),
		"namespace_path": glNamespacePath(gl),
		"namespace_kind": glNamespaceKind(gl),
		"import_status":  gl.ImportStatus,
		"empty_repo":     gl.EmptyRepo,
	}
	b, err := json.Marshal(blob) // raw JSON: cold vendor JSON
	if err != nil {              // pure unexported mapper (no ctx); blob is all primitives so marshal cannot fail; {} is the safe fallback
		// blob holds only primitive types — marshal cannot fail in practice.
		return []byte("{}")
	}
	return b
}

// glNamespacePath returns the namespace path or empty string when nil.
func glNamespacePath(gl *glProject) string {
	if gl.Namespace == nil {
		return ""
	}
	return gl.Namespace.Path
}

// glNamespaceID returns the namespace ID or 0 when nil.
func glNamespaceID(gl *glProject) int {
	if gl.Namespace == nil {
		return 0
	}
	return gl.Namespace.ID
}

// glNamespaceKind returns the namespace kind or empty string when nil.
func glNamespaceKind(gl *glProject) string {
	if gl.Namespace == nil {
		return ""
	}
	return gl.Namespace.Kind
}

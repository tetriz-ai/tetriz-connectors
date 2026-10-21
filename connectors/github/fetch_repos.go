// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	repopb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchReposOp lists every repository the connector can see and emits one
// fetch_repos record per repo. The listing source is chosen on two axes —
// the credential's ability and the tenant's intent (tenant_type) — by
// SelectRepoListing; see that function for the matrix.
//
// Catalog-all: every accessible repo is emitted regardless of the tracked-repo
// cap, which bounds only the downstream tracked set, not the discovery catalog.
//
// Record key = platform_repo_id (stable numeric GitHub repo ID, survives renames).
//
// Archived repositories ARE emitted intentionally: a consumer's repository table has an
// `archived` column that tracks the current state, and analytics queries filter
// via WHERE archived = FALSE. Emitting archived repos here keeps the table in sync
// with reality without requiring a separate cleanup path.
type FetchReposOp struct{}

func (op *FetchReposOp) OperationID() sdk.OperationID { return opFetchRepos }

func (op *FetchReposOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	creds, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    op.OperationID(),
			OrganizationID: params.Config.OrganizationID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          err,
		}
	}

	fetchPage, err := SelectRepoListing(creds, settings, params.TenantType)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    op.OperationID(),
			OrganizationID: params.Config.OrganizationID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          err,
		}
	}

	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
	page := int32(1)
	orgID := params.Config.OrganizationID
	fetchedAt := time.Now().UTC()
	var allRepos []repoInfo

	for {
		reposRaw, hasNextPage, err := fetchPage(ctx, restGW, page)
		if err != nil {
			return nil, fmt.Errorf("list repos page=%d: %w", page, err)
		}

		for _, repoRaw := range reposRaw {
			var gh ghRepository
			if err := json.Unmarshal(repoRaw, &gh); err != nil {
				logger.WarnWithCtx(ctx, "skipping repo: unmarshal failed", zap.Error(err))
				continue
			}
			if gh.ID == 0 {
				// Malformed entry — skip.
				continue
			}
			if !settings.IncludePrivate && gh.Private {
				continue
			}
			if !inOrgScope(creds, settings, params.TenantType, gh.Owner.Login) {
				continue
			}

			// Fetch merge-method toggles (best-effort; failure does not skip the repo).
			if toggles, tErr := fetchMergeMethodToggles(ctx, params.VendorGateway, gh.Owner.Login, gh.Name); tErr != nil {
				// The wrapped error's text is fixed when it is built, so log the
				// SyncError itself with this run's names filled in.
				logged := tErr
				var se *sdk.SyncError
				if errors.As(tErr, &se) {
					se.ConnectorID, se.OperationID, se.OrganizationID = connectorID, opFetchRepos, orgID
					logged = se
				}
				logger.WarnWithCtx(ctx, "fetch merge method toggles failed; proceeding without them",
					zap.String("repo", gh.FullName), zap.Error(logged))
			} else {
				gh.AllowSquashMerge = toggles.SquashMergeAllowed
				gh.AllowMergeCommit = toggles.MergeCommitAllowed
				gh.AllowRebaseMerge = toggles.RebaseMergeAllowed
			}

			rec := mapGitHubRepoToRecord(&gh, settings)
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
					"full_name": gh.FullName,
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
			allRepos = append(allRepos, repoInfo{
				FullName:      gh.FullName,
				DefaultBranch: gh.DefaultBranch,
				DatabaseID:    gh.ID,
			})
		}

		if !hasNextPage {
			break
		}
		page++
	}

	saveDiscoveredRepos(ctx, params, settings, allRepos)
	logger.Info(ctx, "fetch_repos complete",
		zap.String("listing_source", repoListingSource(creds, params.TenantType)),
		zap.String("org", settings.Organization), // empty for a PAT/individual tenant
		zap.Int32("pages", page))
	return params.Cursor, nil
}

// inOrgScope reports whether a listed repo belongs to the tenant. A PAT lists
// GET /user/repos, every repo the token can reach, including other orgs'. An
// ORGANIZATION tenant naming settings.organization catalogs only that org's
// repos (owner compared case-insensitively, as GitHub logins are), matching
// what fetch_prs and the other per-repo operations read. An INDIVIDUAL tenant,
// or a PAT config without an organization, keeps every repo the token sees.
// The App listings are already scoped by their endpoint.
func inOrgScope(creds GitHubCredentials, settings GitHubSettings, tenantType sdk.TenantKind, owner string) bool {
	if !creds.IsPAT() || tenantType != sdk.TenantKindOrganization || settings.Organization == "" {
		return true
	}
	return strings.EqualFold(owner, settings.Organization)
}

// repoListingSource names the endpoint SelectRepoListing chose, for logging.
func repoListingSource(creds GitHubCredentials, tenantType sdk.TenantKind) string {
	if creds.IsPAT() {
		return "user_repos"
	}
	if tenantType == sdk.TenantKindIndividual {
		return "installation_repositories"
	}
	return "org_repos"
}

// GitHub repo listing filter values. "all" includes private repos; the
// /user/repos visibility filter narrows to public-only when private repos are
// excluded.
const (
	repoListingAll       = "all"
	repoVisibilityPublic = "public"
)

// RepoPageFetcher fetches one page of repos from a listing endpoint, returning
// the raw repo JSON objects and whether another page follows.
type RepoPageFetcher func(ctx context.Context, gw sdk.VendorGateway, page int32) (reposRaw [][]byte, hasNextPage bool, err error)

// SelectRepoListing picks the repo-discovery endpoint from the credential's
// ability and the tenant's intent (tenant_type), per the individual-accounts
// Matrix:
//
//	PAT                        → GET /user/repos             (the token's own repos)
//	App + INDIVIDUAL           → GET /installation/repositories (the install's repos)
//	App + ORGANIZATION         → GET /orgs/{org}/repos        (the live org path)
//	App + UNSPECIFIED          → rejected here as a backstop
//
// The orchestrator already fails the run loud on UNSPECIFIED before Execute
// (FetchReposOp is tagged RequiresTenantType); this returns an error for the
// same case so the selection never silently defaults to the org endpoint if
// reached by a caller that bypassed the guard.
func SelectRepoListing(creds GitHubCredentials, settings GitHubSettings, tenantType sdk.TenantKind) (RepoPageFetcher, error) {
	if creds.IsPAT() {
		return func(ctx context.Context, gw sdk.VendorGateway, page int32) ([][]byte, bool, error) {
			visibility := repoListingAll
			if !settings.IncludePrivate {
				visibility = repoVisibilityPublic
			}
			protoResp, err := gw.Execute(ctx, &vgpb.ListUserReposRequest{
				Visibility: visibility,
				Page:       page,
				PerPage:    100,
			})
			if err != nil {
				return nil, false, fmt.Errorf("list user repos: %w", err)
			}
			resp, ok := protoResp.(*vgpb.ListUserReposResponse)
			if !ok {
				return nil, false, fmt.Errorf("unexpected response type %T, expected *ListUserReposResponse", protoResp)
			}
			return resp.GetRepos(), resp.GetHasNextPage(), nil
		}, nil
	}

	switch tenantType {
	case sdk.TenantKindIndividual:
		return func(ctx context.Context, gw sdk.VendorGateway, page int32) ([][]byte, bool, error) {
			protoResp, err := gw.Execute(ctx, &vgpb.ListInstallationReposRequest{
				Page:    page,
				PerPage: 100,
			})
			if err != nil {
				return nil, false, fmt.Errorf("list installation repos: %w", err)
			}
			resp, ok := protoResp.(*vgpb.ListInstallationReposResponse)
			if !ok {
				return nil, false, fmt.Errorf("unexpected response type %T, expected *ListInstallationReposResponse", protoResp)
			}
			return resp.GetRepos(), resp.GetHasNextPage(), nil
		}, nil
	case sdk.TenantKindOrganization:
		return func(ctx context.Context, gw sdk.VendorGateway, page int32) ([][]byte, bool, error) {
			protoResp, err := gw.Execute(ctx, &vgpb.ListOrgReposRequest{
				Organization: settings.Organization,
				Page:         page,
				PerPage:      100,
				Type:         repoListingAll,
			})
			if err != nil {
				return nil, false, fmt.Errorf("list org repos: %w", err)
			}
			resp, ok := protoResp.(*vgpb.ListOrgReposResponse)
			if !ok {
				return nil, false, fmt.Errorf("unexpected response type %T, expected *ListOrgReposResponse", protoResp)
			}
			return resp.GetRepos(), resp.GetHasNextPage(), nil
		}, nil
	default:
		return nil, fmt.Errorf("fetch_repos: App credential requires a resolved tenant_type but got %s", tenantType)
	}
}

// mapGitHubRepoToRecord normalizes vendor-specific fields into the cross-vendor
// RepositoryRecord proto. GitHub-specific cold fields (node_id, homepage,
// allow_forking, web_commit_signoff_required) go into vendor_blob_json.
// tracked_branch is resolved via the same priority order used by fetch_repo_tree:
// per-repo override > org-level TrackedBranch > repo default_branch.
func mapGitHubRepoToRecord(gh *ghRepository, settings GitHubSettings) *repopb.RepositoryRecord {
	rec := &repopb.RepositoryRecord{
		PlatformRepoId:  fmt.Sprintf("%d", gh.ID),
		Name:            gh.Name,
		FullName:        gh.FullName,
		OwnerLogin:      gh.Owner.Login,
		Description:     gh.Description,
		DefaultBranch:   gh.DefaultBranch,
		Visibility:      parseRepoVisibility(gh.Visibility),
		Archived:        gh.Archived,
		Disabled:        gh.Disabled,
		IsFork:          gh.Fork,
		IsTemplate:      gh.IsTemplate,
		PrimaryLanguage: gh.Language,
		SizeKb:          gh.Size,
		Topics:          gh.Topics,
		WebUrl:          gh.HTMLURL,
		SshUrl:          gh.SSHURL,
		HttpCloneUrl:    gh.CloneURL,
		HasIssues:       gh.HasIssues,
		HasWiki:         gh.HasWiki,
		HasPages:        gh.HasPages,
		HasProjects:     gh.HasProjects,
		Stats: &repopb.Stats{
			Stars:        treewalker.SafeInt32(gh.StargazersCount),
			Forks:        treewalker.SafeInt32(gh.ForksCount),
			Watchers:     treewalker.SafeInt32(gh.WatchersCount),
			Subscribers:  treewalker.SafeInt32(gh.SubscribersCount),
			OpenIssues:   treewalker.SafeInt32(gh.OpenIssuesCount),
			NetworkCount: treewalker.SafeInt32(gh.NetworkCount),
		},
	}

	if gh.License != nil {
		rec.LicenseKey = gh.License.Key
	}
	if gh.Parent != nil {
		rec.ParentPlatformRepoId = fmt.Sprintf("%d", gh.Parent.ID)
	}
	if !gh.CreatedAt.IsZero() {
		rec.VendorCreatedAt = timestamppb.New(gh.CreatedAt)
	}
	if !gh.UpdatedAt.IsZero() {
		rec.VendorUpdatedAt = timestamppb.New(gh.UpdatedAt)
	}
	if !gh.PushedAt.IsZero() {
		rec.VendorPushedAt = timestamppb.New(gh.PushedAt)
	}

	rec.VendorBlobJson = buildGitHubVendorBlob(gh) // raw JSON: cold vendor JSON
	rec.TrackedBranch = settings.resolveRepoBranch(gh.FullName, gh.DefaultBranch)
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

// buildGitHubVendorBlob serializes GitHub-specific cold fields that the typed
// cross-vendor RepositoryRecord does not capture. These are stored under
// metadata.vendor.github in a consumer's repository row.
func buildGitHubVendorBlob(gh *ghRepository) []byte {
	blob := map[string]interface{}{
		"node_id":                     gh.NodeID,
		"homepage":                    gh.Homepage,
		"allow_forking":               gh.AllowForking,
		"web_commit_signoff_required": gh.WebCommitSignoffRequired,
		"allow_squash_merge":          gh.AllowSquashMerge,
		"allow_merge_commit":          gh.AllowMergeCommit,
		"allow_rebase_merge":          gh.AllowRebaseMerge,
	}
	b, err := json.Marshal(blob) // raw JSON: cold vendor JSON
	if err != nil {
		// blob holds only primitive types — marshal cannot fail in practice.
		// Fall back to an empty object so the row still upserts.
		logger.Warn("github: marshal vendor blob failed; falling back to empty object",
			zap.Error(err))
		return []byte("{}")
	}
	return b
}

// repoMergeToggles holds the three merge-method enable flags returned by the
// GitHub GraphQL API for a single repository.
type repoMergeToggles struct {
	SquashMergeAllowed bool
	MergeCommitAllowed bool
	RebaseMergeAllowed bool
}

// fetchMergeMethodToggles queries the GitHub GraphQL API for the three
// merge-method permission flags on a single repository. Returns an error when
// the API call or parse fails; callers should treat this as best-effort and
// continue with the repo record rather than aborting the sync run.
func fetchMergeMethodToggles(ctx context.Context, gw sdk.VendorGateway, owner, repo string) (repoMergeToggles, error) {
	query := buildRepoMergeMethodsQuery(owner, repo)
	body, err := doGraphQL(ctx, gw, query, false)
	if err != nil {
		return repoMergeToggles{}, fmt.Errorf("graphql merge methods for %s/%s: %w", owner, repo, err)
	}
	var data ghGraphQLData
	if err := json.Unmarshal(body, &data); err != nil {
		return repoMergeToggles{}, fmt.Errorf("unmarshal merge methods for %s/%s: %w", owner, repo, err)
	}
	return repoMergeToggles{
		SquashMergeAllowed: data.Repository.SquashMergeAllowed,
		MergeCommitAllowed: data.Repository.MergeCommitAllowed,
		RebaseMergeAllowed: data.Repository.RebaseMergeAllowed,
	}, nil
}

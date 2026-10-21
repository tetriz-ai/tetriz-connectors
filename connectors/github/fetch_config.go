// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchConfigOp captures per-repo governance config and emits one fetch_config
// record per repo (scope=REPO, record key = platform repo id). The default
// branch comes from the repo object; required-review counts and allowed merge
// methods come from the branch's effective rulesets (App tokens do not return
// the allow_*_merge fields on the repo object). Repos are taken from the
// configured repository list, or auto-discovered from the org when none is set.
// Latest-only — no cursor.
type FetchConfigOp struct{}

// OperationID returns the fetch_config operation id.
func (op *FetchConfigOp) OperationID() sdk.OperationID { return opFetchConfig }

// configRepo is the subset of the GitHub repository object fetch_config needs.
type configRepo struct {
	id            int64
	fullName      string
	defaultBranch string
}

// Execute fetches governance config for each in-scope repo and emits a
// fetch_config record. A per-repo fetch failure (e.g. a 403/404 under the
// current scopes) is logged and skipped, never fatal — capture records what it
// can see.
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

	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
	fetchedAt := time.Now().UTC()
	orgID := params.Config.OrganizationID

	repos, err := op.resolveRepos(ctx, restGW, settings)
	if err != nil {
		return nil, fmt.Errorf("fetch_config: resolve repos: %w", err)
	}

	for _, rp := range repos {
		owner, name, ok := splitOwnerRepo(rp.fullName)
		if !ok {
			logger.WarnWithCtx(ctx, "fetch_config: skipping malformed repo full_name",
				zap.String("full_name", rp.fullName))
			continue
		}

		repoSettings := map[string]any{"default_branch": rp.defaultBranch}
		blob := map[string]any{"repo_settings": repoSettings}

		// Effective branch rules carry the governance (required reviews,
		// allowed merge methods). Omit on error rather than fail the sync.
		if rp.defaultBranch != "" {
			rulesResp, rErr := restGW.Execute(ctx, &vgpb.GetBranchRulesRequest{
				Owner: owner, Repo: name, Branch: rp.defaultBranch,
			})
			if rErr != nil {
				logger.WarnWithCtx(ctx, "fetch_config: branch rules unavailable; omitting",
					zap.String("full_name", rp.fullName), zap.Error(rErr))
			} else if rules, okResp := rulesResp.(*vgpb.GetBranchRulesResponse); okResp && len(rules.GetRawJson()) > 0 {
				blob["branch_rules"] = json.RawMessage(rules.GetRawJson())
			}
		}

		payload, mErr := json.Marshal(blob)
		if mErr != nil {
			logger.WarnWithCtx(ctx, "fetch_config: marshal blob failed; skipping",
				zap.String("full_name", rp.fullName), zap.Error(mErr))
			continue
		}

		if emitErr := params.Sink.Emit(ctx, &sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    string(op.OperationID()),
			OrganizationID: orgID,
			ExternalID:     fmt.Sprintf("%d", rp.id),
			Payload:        payload,
			FetchedAt:      fetchedAt,
			Metadata: map[string]string{
				"scope":     "REPO",
				"full_name": rp.fullName,
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

	logger.Info(ctx, "fetch_config complete",
		zap.String("org", settings.Organization), zap.Int("repos", len(repos)))
	return params.Cursor, nil
}

// resolveRepos returns the repos to capture: the explicit settings.Repositories
// list (one GET /repos each), or every org repo when the list is empty.
func (op *FetchConfigOp) resolveRepos(ctx context.Context, restGW sdk.VendorGateway, settings GitHubSettings) ([]configRepo, error) {
	if len(settings.Repositories) > 0 {
		out := make([]configRepo, 0, len(settings.Repositories))
		for _, fullName := range settings.Repositories {
			owner, name, ok := splitOwnerRepo(fullName)
			if !ok {
				logger.WarnWithCtx(ctx, "fetch_config: skipping malformed configured repo",
					zap.String("full_name", fullName))
				continue
			}
			protoResp, err := restGW.Execute(ctx, &vgpb.GetRepositoryRequest{Owner: owner, Repo: name})
			if err != nil {
				logger.WarnWithCtx(ctx, "fetch_config: get repo failed; skipping",
					zap.String("full_name", fullName), zap.Error(err))
				continue
			}
			resp, okResp := protoResp.(*vgpb.GetRepositoryResponse)
			if !okResp {
				return nil, fmt.Errorf("unexpected response type %T, expected *GetRepositoryResponse", protoResp)
			}
			var gh ghRepository
			if err := json.Unmarshal(resp.GetRawJson(), &gh); err != nil || gh.ID == 0 {
				logger.WarnWithCtx(ctx, "fetch_config: unmarshal repo failed; skipping",
					zap.String("full_name", fullName), zap.Error(err))
				continue
			}
			out = append(out, configRepo{id: gh.ID, fullName: gh.FullName, defaultBranch: gh.DefaultBranch})
		}
		return out, nil
	}

	// Auto-discover: every org repo (paginated).
	var out []configRepo
	page := int32(1)
	for {
		protoResp, err := restGW.Execute(ctx, &vgpb.ListOrgReposRequest{
			Organization: settings.Organization, Page: page, PerPage: 100, Type: "all",
		})
		if err != nil {
			return nil, fmt.Errorf("list org repos page=%d: %w", page, err)
		}
		resp, okResp := protoResp.(*vgpb.ListOrgReposResponse)
		if !okResp {
			return nil, fmt.Errorf("unexpected response type %T, expected *ListOrgReposResponse", protoResp)
		}
		for _, raw := range resp.GetRepos() {
			var gh ghRepository
			if err := json.Unmarshal(raw, &gh); err != nil {
				logger.WarnWithCtx(ctx, "fetch_config: unmarshal repo failed; skipping", zap.Error(err))
				continue
			}
			if gh.ID == 0 {
				continue
			}
			if !settings.IncludePrivate && gh.Private {
				continue
			}
			out = append(out, configRepo{id: gh.ID, fullName: gh.FullName, defaultBranch: gh.DefaultBranch})
		}
		if !resp.GetHasNextPage() {
			break
		}
		page++
	}
	return out, nil
}

// splitOwnerRepo splits "owner/repo" into its parts.
func splitOwnerRepo(fullName string) (owner, repo string, ok bool) {
	owner, repo, found := strings.Cut(fullName, "/")
	return owner, repo, found && owner != "" && repo != ""
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const opFetchProjects = "fetch_projects"

// FetchProjectsOp fetches the organization's GitHub Projects (ProjectV2)
// metadata. Targets github.com only — same scope assumption as
// fetch_issues; ProjectV2 fields used here (number, title, shortDescription, url, closed)
// are stable on github.com and on GHES v3.13+ if scope ever extends.
//
// Emits one RawRecord per project, with this payload:
//
//	{
//	  "number": <int64>,
//	  "title": <string>,
//	  "shortDescription": <string>,
//	  "url": <string>,
//	  "closed": <bool>,
//	  "createdAt": <RFC3339 string>
//	}
//
// Project items (issues/PRs/drafts linked into a project) are NOT fetched here —
// each issue's projectItems connection is already selected by buildIssuesQuery, so
// the membership graph comes through the fetch_issues path.
type FetchProjectsOp struct {
	paginationCfg *config.GitHubPaginationConfig
}

func (op *FetchProjectsOp) OperationID() sdk.OperationID { return opFetchProjects }

// ghGQLProjectV2Node is the per-project GraphQL response shape. Field names
// match the JSON keys the consumer expects on the record payload —
// any rename here is a wire-contract change.
type ghGQLProjectV2Node struct {
	Number           int64  `json:"number"`
	Title            string `json:"title"`
	ShortDescription string `json:"shortDescription"`
	URL              string `json:"url"`
	Closed           bool   `json:"closed"`
	// CreatedAt is the ProjectV2 creation timestamp (RFC3339). Jira's
	// project search response carries no analog, so this field stays
	// GitHub-only.
	CreatedAt string `json:"createdAt"`
}

// ghProjectsV2Response is the parsed GraphQL response wrapping the projectsV2 connection.
type ghProjectsV2Response struct {
	Organization struct {
		ProjectsV2 struct {
			Nodes    []ghGQLProjectV2Node `json:"nodes"`
			PageInfo ghPageInfo           `json:"pageInfo"`
		} `json:"projectsV2"`
	} `json:"organization"`
}

func (op *FetchProjectsOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	gw := params.VendorGateway
	pageCfg := opPageConfig(op.paginationCfg, opFetchProjects)
	cursor := ""
	count := 0

	for {
		body, err := doGraphQL(ctx, gw, buildProjectsQuery(settings.Organization, cursor, pageCfg.PageSize), false)
		if err != nil {
			return nil, fmt.Errorf("fetch projects for %s: %w", settings.Organization, orgScopeForbidden(err, op.OperationID(), params.Config.OrganizationID, "the organization's projects"))
		}

		var data ghProjectsV2Response
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, fmt.Errorf("parse projects for %s: %w", settings.Organization, err)
		}

		for _, node := range data.Organization.ProjectsV2.Nodes {
			if node.Number == 0 || node.Title == "" {
				// A project with no number or title identifies nothing — skip it
				// rather than emit a record no consumer can use.
				continue
			}
			payload, marshalErr := json.Marshal(node)
			if marshalErr != nil {
				logger.WarnWithCtx(ctx, "marshal project node failed; skipping",
					zap.String(logger.OrgID, params.Config.OrganizationID),
					zap.Int64("project_number", node.Number),
					zap.Error(marshalErr))
				continue
			}

			externalID := fmt.Sprintf("project-%d", node.Number)
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, payload,
				map[string]string{"organization": settings.Organization}, time.Time{}); emitErr != nil { // a project is current state: no DataDate
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
			count++
		}

		if !data.Organization.ProjectsV2.PageInfo.HasNextPage {
			logger.Info(ctx, "fetched projects",
				zap.String("org", settings.Organization),
				zap.Int("count", count),
			)
			return sdk.NewTimestampCursor(time.Now().UTC()), nil
		}
		if cursor, err = nextGraphQLCursor(cursor, data.Organization.ProjectsV2.PageInfo.EndCursor); err != nil {
			return nil, fmt.Errorf("fetch projects for %s: %w", settings.Organization, err)
		}
	}
}

// buildProjectsQuery builds the GraphQL query to fetch one page of org-level
// ProjectV2 metadata. Selection set is the minimal contract documented above.
// pageSize sets the projectsV2 first: argument; comes from
// GitHubPagination.Operations.fetch_projects.PageSize via opPageConfig.
//
// Targets github.com only; on GitHub Enterprise Server projectsV2 requires v3.10+
// (the connection landed earlier than subIssues, which gates fetch_issues at v3.13+).
func buildProjectsQuery(org, cursor string, pageSize int) string {
	afterClause := ""
	if cursor != "" {
		afterClause = fmt.Sprintf(", after: %q", cursor)
	}
	return fmt.Sprintf(`{
  organization(login: %q) {
    projectsV2(first: %d%s) {
      nodes {
        number
        title
        shortDescription
        url
        closed
        createdAt
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}`, org, pageSize, afterClause)
}

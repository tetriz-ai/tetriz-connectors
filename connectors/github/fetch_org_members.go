// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// errSAMLNotConfigured is returned by fetchSAMLIdentities when the GitHub org
// does not have a SAML identity provider configured. Callers use this to fall
// back to the standard org members GraphQL path.
var errSAMLNotConfigured = errors.New("org has no SAML identity provider configured")

// FetchOrgMembersOp fetches GitHub org member identities for identity enrichment.
//
// It emits one RawRecord per member. A consumer reads these records
// and calls ResolveIdentity with hint_source set, linking GitHub logins to persons.
//
// Two modes based on GitHubSettings.IdentityVerification:
//   - "saml": Fetches SAML externalIdentities → maps login→corporate email (high-confidence).
//   - ""    : Fetches org members via GraphQL → login + optional public email (medium-confidence).
//
// platform_user_id is always the numeric GitHub user ID (stable, never recycled).
// The login (mutable handle) is stored in metadata["login"].
type FetchOrgMembersOp struct{}

func (op *FetchOrgMembersOp) OperationID() sdk.OperationID { return "fetch_org_members" }

// OrgMemberRecord is the payload emitted per member.
// Downstream: A consumer resolves identities from this.
type OrgMemberRecord struct {
	// NumericUserID is the stable GitHub user ID (never recycled).
	NumericUserID int64 `json:"numeric_user_id"`
	// Login is the mutable GitHub username.
	Login string `json:"login"`
	// Email is the corporate email from SAML nameId or public GitHub profile.
	Email string `json:"email,omitempty"`
	// Name is the display name.
	Name string `json:"name,omitempty"`
	// AvatarURL is the profile picture URL.
	AvatarURL string `json:"avatar_url,omitempty"`
	// HintSource is "saml_scim" or "org_members_api".
	HintSource string `json:"hint_source"`
	// Teams is the list of GitHub org team slugs this member belongs to.
	Teams []string `json:"teams,omitempty"`
}

func (op *FetchOrgMembersOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	// Always try SAML first to get high-confidence corporate emails.
	// Fall back to standard GraphQL (public emails only) if SAML isn't configured
	// or the API call fails (e.g. missing admin:org scope).
	var members []OrgMemberRecord
	var hintSource string

	members, err = fetchSAMLIdentities(ctx, params.VendorGateway, settings.Organization)
	if err == nil {
		hintSource = HintSourceSAMLSCIM
	} else {
		// SAML not configured or API call failed — fall back to standard org members API.
		if !errors.Is(err, errSAMLNotConfigured) {
			logger.WarnWithCtx(ctx, "SAML identity fetch failed, falling back to standard org members API",
				zap.String("org", settings.Organization),
				zap.Error(err),
			)
		}
		members, err = fetchOrgMembersGraphQL(ctx, params.VendorGateway, settings.Organization)
		if err != nil {
			return nil, fmt.Errorf("fetching org members via graphql: %w", orgScopeForbidden(err, op.OperationID(), params.Config.OrganizationID, "the organization's members"))
		}
		hintSource = HintSourceOrgMembersAPI
	}

	// Enrich members with GitHub team memberships. Best-effort: failures are logged,
	// not propagated, so a missing read:org scope doesn't break identity sync.
	loginToTeams, teamsErr := fetchOrgTeamMemberships(ctx, params.VendorGateway, settings.Organization)
	if teamsErr != nil {
		logger.WarnWithCtx(ctx, "skipping team enrichment due to error", zap.Error(teamsErr))
	}

	for i := range members {
		members[i].HintSource = hintSource
		if loginToTeams != nil {
			members[i].Teams = loginToTeams[members[i].Login]
		}
		payload, marshalErr := json.Marshal(members[i])
		if marshalErr != nil {
			logger.WarnWithCtx(ctx, "failed to marshal member record", zap.String(logger.Login, members[i].Login), zap.Error(marshalErr))
			continue
		}

		externalID := fmt.Sprintf("member-%d", members[i].NumericUserID)
		if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, payload, map[string]string{
			"hint_source": hintSource,
		}, time.Time{}); emitErr != nil {
			return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
		}
	}

	logger.Info(ctx, "fetched org members",
		zap.String("org", settings.Organization),
		zap.String("hint_source", hintSource),
		zap.Int("count", len(members)),
	)

	// No cursor needed — fetch_org_members always does a full refresh.
	return &sdk.Cursor{Data: json.RawMessage(fmt.Sprintf(`{"fetched_at":"%s"}`, time.Now().UTC().Format(time.RFC3339)))}, nil
}

// --- SAML path ---

// ghSAMLResponse is the parsed GraphQL response for the SAML identities query.
type ghSAMLResponse struct {
	Organization struct {
		SAMLIdentityProvider *struct {
			ExternalIdentities struct {
				Edges []struct {
					Node struct {
						GUID         string `json:"guid"`
						SAMLIdentity *struct {
							NameID string `json:"nameId"`
						} `json:"samlIdentity"`
						User *struct {
							Login      string `json:"login"`
							DatabaseID int64  `json:"databaseId"`
							Name       string `json:"name"`
							AvatarURL  string `json:"avatarUrl"`
						} `json:"user"`
					} `json:"node"`
				} `json:"edges"`
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
			} `json:"externalIdentities"`
		} `json:"samlIdentityProvider"`
	} `json:"organization"`
}

// fetchSAMLIdentities uses the GitHub GraphQL SAML externalIdentities API to fetch
// login → corporate email mappings. This is high-confidence identity data.
func fetchSAMLIdentities(ctx context.Context, gw sdk.VendorGateway, org string) ([]OrgMemberRecord, error) {
	var members []OrgMemberRecord
	cursor := ""

	for {
		query := buildSAMLIdentitiesQuery(org, cursor)
		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			return nil, fmt.Errorf("fetch SAML identities for %s: %w", org, err)
		}

		var data ghSAMLResponse
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, fmt.Errorf("parse SAML identities for %s: %w", org, err)
		}

		if data.Organization.SAMLIdentityProvider == nil {
			return nil, errSAMLNotConfigured
		}

		for _, edge := range data.Organization.SAMLIdentityProvider.ExternalIdentities.Edges {
			node := edge.Node
			if node.User == nil {
				continue // pending invitation — no GitHub user yet
			}

			email := ""
			if node.SAMLIdentity != nil {
				email = node.SAMLIdentity.NameID
			}

			members = append(members, OrgMemberRecord{
				NumericUserID: node.User.DatabaseID,
				Login:         node.User.Login,
				Email:         email,
				Name:          node.User.Name,
				AvatarURL:     node.User.AvatarURL,
			})
		}

		pageInfo := data.Organization.SAMLIdentityProvider.ExternalIdentities.PageInfo
		if !pageInfo.HasNextPage {
			break
		}
		if cursor, err = nextGraphQLCursor(cursor, pageInfo.EndCursor); err != nil {
			return nil, fmt.Errorf("fetch SAML identities for %s: %w", org, err)
		}
	}

	return members, nil
}

func buildSAMLIdentitiesQuery(org, cursor string) string {
	afterClause := ""
	if cursor != "" {
		afterClause = fmt.Sprintf(`, after: "%s"`, cursor)
	}
	return fmt.Sprintf(`{
  organization(login: %q) {
    samlIdentityProvider {
      externalIdentities(first: 100%s) {
        edges {
          node {
            guid
            samlIdentity {
              nameId
            }
            user {
              login
              databaseId
              name
              avatarUrl
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}`, org, afterClause)
}

// --- Standard org members path ---

// ghOrgMembersResponse is the parsed GraphQL response for org members.
type ghOrgMembersResponse struct {
	Organization struct {
		MembersWithRole struct {
			Nodes []struct {
				Login      string `json:"login"`
				DatabaseID int64  `json:"databaseId"`
				Email      string `json:"email"`
				Name       string `json:"name"`
				AvatarURL  string `json:"avatarUrl"`
			} `json:"nodes"`
			PageInfo struct {
				HasNextPage bool   `json:"hasNextPage"`
				EndCursor   string `json:"endCursor"`
			} `json:"pageInfo"`
		} `json:"membersWithRole"`
	} `json:"organization"`
}

// fetchOrgMembersGraphQL uses the GitHub GraphQL org members API to fetch login + optional email.
// Email is only present for members who have made it public on their GitHub profile.
func fetchOrgMembersGraphQL(ctx context.Context, gw sdk.VendorGateway, org string) ([]OrgMemberRecord, error) {
	var members []OrgMemberRecord
	cursor := ""

	for {
		query := buildOrgMembersQuery(org, cursor)
		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			return nil, fmt.Errorf("fetch org members for %s: %w", org, err)
		}

		var data ghOrgMembersResponse
		if err := json.Unmarshal(body, &data); err != nil {
			return nil, fmt.Errorf("parse org members for %s: %w", org, err)
		}

		for _, node := range data.Organization.MembersWithRole.Nodes {
			if node.DatabaseID == 0 {
				continue
			}
			members = append(members, OrgMemberRecord{
				NumericUserID: node.DatabaseID,
				Login:         node.Login,
				Email:         node.Email,
				Name:          node.Name,
				AvatarURL:     node.AvatarURL,
			})
		}

		logger.Debug(ctx, "fetched org members page",
			zap.String("org", org),
			zap.Int("count", len(data.Organization.MembersWithRole.Nodes)),
			zap.Bool("has_next_page", data.Organization.MembersWithRole.PageInfo.HasNextPage),
		)

		if !data.Organization.MembersWithRole.PageInfo.HasNextPage {
			break
		}
		if cursor, err = nextGraphQLCursor(cursor, data.Organization.MembersWithRole.PageInfo.EndCursor); err != nil {
			return nil, fmt.Errorf("fetch org members for %s: %w", org, err)
		}
	}

	return members, nil
}

func buildOrgMembersQuery(org, cursor string) string {
	afterClause := ""
	if cursor != "" {
		afterClause = fmt.Sprintf(`, after: "%s"`, cursor)
	}
	return fmt.Sprintf(`{
  organization(login: %q) {
    membersWithRole(first: 100%s) {
      nodes {
        login
        databaseId
        email
        name
        avatarUrl
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}`, org, afterClause)
}

// --- Org teams path ---

// ghOrgTeamsResponse is the parsed GraphQL response for org teams with members.
type ghOrgTeamsResponse struct {
	Organization struct {
		Teams struct {
			Nodes []struct {
				Slug    string `json:"slug"`
				Members struct {
					Nodes []struct {
						Login string `json:"login"`
					} `json:"nodes"`
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"members"`
			} `json:"nodes"`
			PageInfo struct {
				HasNextPage bool   `json:"hasNextPage"`
				EndCursor   string `json:"endCursor"`
			} `json:"pageInfo"`
		} `json:"teams"`
	} `json:"organization"`
}

// ghSingleTeamMembersResponse is the parsed GraphQL response for paginating a single team's members.
type ghSingleTeamMembersResponse struct {
	Organization struct {
		Team *struct {
			Members struct {
				Nodes []struct {
					Login string `json:"login"`
				} `json:"nodes"`
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
			} `json:"members"`
		} `json:"team"`
	} `json:"organization"`
}

// fetchOrgTeamMemberships fetches all GitHub org teams and returns a login→[]teamSlug map.
// It enriches OrgMemberRecord with team membership information.
// Best-effort: returns an empty map (not an error) if the token lacks read:org scope.
//
// For teams with >100 members, it paginates through the full member list using
// a per-team follow-up query. This ensures team counts match what GitHub shows.
func fetchOrgTeamMemberships(ctx context.Context, gw sdk.VendorGateway, org string) (map[string][]string, error) {
	loginToTeams := make(map[string][]string)
	cursor := ""

	for {
		afterClause := ""
		if cursor != "" {
			afterClause = fmt.Sprintf(`, after: "%s"`, cursor)
		}
		query := fmt.Sprintf(`{
  organization(login: %q) {
    teams(first: 100%s) {
      nodes {
        slug
        members(first: 100) {
          nodes { login }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`, org, afterClause)

		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			logger.WarnWithCtx(ctx, "failed to fetch org teams — team memberships will not be included",
				zap.String("org", org),
				zap.Error(err),
			)
			return loginToTeams, nil
		}

		var data ghOrgTeamsResponse
		if err := json.Unmarshal(body, &data); err != nil {
			logger.WarnWithCtx(ctx, "failed to parse org teams response", zap.String("org", org), zap.Error(err))
			return loginToTeams, nil
		}

		for _, team := range data.Organization.Teams.Nodes {
			if team.Slug == "" {
				continue
			}
			for _, member := range team.Members.Nodes {
				if member.Login != "" {
					loginToTeams[member.Login] = append(loginToTeams[member.Login], team.Slug)
				}
			}
			// Paginate remaining members if this team has more than 100.
			if paginateErr := fetchTeamMembers(ctx, gw, org, team.Slug, team.Members.PageInfo.EndCursor, loginToTeams); paginateErr != nil {
				logger.WarnWithCtx(ctx, "failed to paginate team members — count may be incomplete",
					zap.String("team", team.Slug),
					zap.Error(paginateErr),
				)
			}
		}

		if !data.Organization.Teams.PageInfo.HasNextPage {
			break
		}
		if cursor, err = nextGraphQLCursor(cursor, data.Organization.Teams.PageInfo.EndCursor); err != nil {
			logger.WarnWithCtx(ctx, "org teams pagination stopped — team memberships may be incomplete",
				zap.String("org", org), zap.Error(err))
			break
		}
	}

	logger.Info(ctx, "fetched org team memberships",
		zap.String("org", org),
		zap.Int("members_with_teams", len(loginToTeams)),
	)
	return loginToTeams, nil
}

// fetchTeamMembers paginates through a single team's members starting from the given cursor.
// Appends results directly into loginToTeams.
func fetchTeamMembers(ctx context.Context, gw sdk.VendorGateway, org, teamSlug, memberCursor string, loginToTeams map[string][]string) error {
	cursor := memberCursor
	for {
		query := fmt.Sprintf(`{
  organization(login: %q) {
    team(slug: %q) {
      members(first: 100, after: %q) {
        nodes { login }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`, org, teamSlug, cursor)

		body, err := doGraphQL(ctx, gw, query, false)
		if err != nil {
			return fmt.Errorf("fetching members for team %s: %w", teamSlug, err)
		}

		var data ghSingleTeamMembersResponse
		if err := json.Unmarshal(body, &data); err != nil {
			return fmt.Errorf("parsing members for team %s: %w", teamSlug, err)
		}
		if data.Organization.Team == nil {
			return nil
		}

		for _, member := range data.Organization.Team.Members.Nodes {
			if member.Login != "" {
				loginToTeams[member.Login] = append(loginToTeams[member.Login], teamSlug)
			}
		}

		if !data.Organization.Team.Members.PageInfo.HasNextPage {
			break
		}
		if cursor, err = nextGraphQLCursor(cursor, data.Organization.Team.Members.PageInfo.EndCursor); err != nil {
			return fmt.Errorf("fetching members for team %s: %w", teamSlug, err)
		}
	}
	return nil
}

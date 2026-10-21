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

// FetchGroupMembersOp fetches GitLab group member identities for identity enrichment.
//
// It emits one RawRecord per member. A consumer reads these records
// and calls ResolveIdentity with hint_source set, linking GitLab usernames to persons.
//
// Always does a full refresh — no cursor. Uses /groups/{id}/members/all which
// includes both direct and inherited members.
type FetchGroupMembersOp struct{}

func (op *FetchGroupMembersOp) OperationID() sdk.OperationID { return "fetch_org_members" }

// GroupMemberRecord is the payload emitted per member.
// Downstream: A consumer resolves identities from this.
type GroupMemberRecord struct {
	// NumericUserID is the stable GitLab user ID (never recycled).
	NumericUserID int64 `json:"numeric_user_id"`
	// Login is the GitLab username.
	Login string `json:"login"`
	// Email is the corporate or public email from the GitLab profile.
	Email string `json:"email,omitempty"`
	// Name is the display name.
	Name string `json:"name,omitempty"`
	// AvatarURL is the profile picture URL.
	AvatarURL string `json:"avatar_url,omitempty"`
	// HintSource identifies where the identity data came from.
	HintSource string `json:"hint_source"`
	// Groups is the list of GitLab subgroup names the member belongs to.
	Groups []string `json:"groups,omitempty"`
}

// glGroupMember is the minimal struct for JSON unmarshaling of GitLab member API response.
type glGroupMember struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	Name      string `json:"name"`
	AvatarURL string `json:"avatar_url"`
}

func (op *FetchGroupMembersOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	_, settings, err := parseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: params.Config.OrganizationID, Kind: sdk.ErrKindConfigInvalid, Cause: err}
	}

	hintSource := settings.HintSource()
	restGW := params.VendorGateway.ForGroup(constants.GroupGitLabREST)
	page := int32(1)
	count := 0

	// Fetch subgroup memberships for team enrichment. Best-effort: if this fails,
	// members are still emitted without group data.
	// When there are no subgroups, all members are assigned to the root group
	// so the org still appears as a team in the Teams tab.
	userGroups := op.fetchSubgroupMemberships(ctx, restGW, settings.GroupID, settings.GroupName)

	for {
		protoResp, err := restGW.Execute(ctx, &connectorpb.GitLabListGroupMembersRequest{
			GroupId: settings.GroupID,
			Page:    page,
			PerPage: 100,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch group members: %w", err)
		}

		resp, ok := protoResp.(*connectorpb.GitLabListGroupMembersResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T, expected *GitLabListGroupMembersResponse", protoResp)
		}

		for _, memberRaw := range resp.GetMembers() {
			var m glGroupMember
			if err := json.Unmarshal(memberRaw, &m); err != nil {
				logger.WarnWithCtx(ctx, "failed to unmarshal group member", zap.Error(err))
				continue
			}
			if m.ID == 0 {
				continue
			}

			// The group members API doesn't return email for non-admin tokens.
			// Fetch the user profile to get their public/commit email.
			if m.Email == "" {
				email := op.fetchUserEmail(ctx, restGW, m.ID)
				if email != "" {
					m.Email = email
				}
			}

			record := GroupMemberRecord{
				NumericUserID: m.ID,
				Login:         m.Username,
				Email:         m.Email,
				Name:          m.Name,
				AvatarURL:     m.AvatarURL,
				HintSource:    hintSource,
				Groups:        userGroups[m.ID],
			}

			payload, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				logger.WarnWithCtx(ctx, "failed to marshal member record", zap.Int64("member_id", m.ID), zap.Error(marshalErr))
				continue
			}

			externalID := fmt.Sprintf("member-%d", m.ID)
			if emitErr := emitRecord(ctx, params.Sink, op.OperationID(), params.Config.OrganizationID, externalID, payload, map[string]string{
				"hint_source": hintSource,
			}, time.Time{}); emitErr != nil {
				return nil, &sdk.SyncError{ConnectorID: connectorID, OperationID: op.OperationID(), Kind: sdk.ErrKindSinkFailure, Cause: emitErr}
			}
			count++
		}

		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
		if err := checkPageCap("group member listing", page); err != nil {
			return nil, err
		}
	}

	logger.Info(ctx, "fetched group members",
		zap.Int64("group_id", settings.GroupID),
		zap.String("group", settings.GroupName),
		zap.Int("count", count),
	)

	// No cursor — fetch_org_members always does a full refresh.
	return &sdk.Cursor{Data: json.RawMessage(fmt.Sprintf(`{"fetched_at":"%s"}`, time.Now().UTC().Format(time.RFC3339)))}, nil
}

// fetchSubgroupMemberships fetches all subgroups of the root group, then fetches
// direct members of each subgroup. Returns a map of userID → []subgroupName.
// When no subgroups exist, all root-group members are assigned to the root group
// name so the org still appears as a team.
// Best-effort: returns an empty map on any error.
func (op *FetchGroupMembersOp) fetchSubgroupMemberships(ctx context.Context, gw sdk.VendorGateway, rootGroupID int64, rootGroupName string) map[int64][]string {
	result := make(map[int64][]string)

	// Step 1: List all subgroups.
	type subgroup struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	var subgroups []subgroup
	page := int32(1)
	for {
		protoResp, err := gw.Execute(ctx, &connectorpb.GitLabListSubgroupsRequest{
			GroupId: rootGroupID,
			Page:    page,
			PerPage: 100,
		})
		if err != nil {
			logger.WarnWithCtx(ctx, "failed to list subgroups for team enrichment", zap.Error(err))
			return result
		}
		resp, ok := protoResp.(*connectorpb.GitLabListSubgroupsResponse)
		if !ok {
			return result
		}
		for _, raw := range resp.GetSubgroups() {
			var sg subgroup
			if err := json.Unmarshal(raw, &sg); err != nil {
				logger.WarnWithCtx(ctx, "skipping malformed subgroup entry", zap.Error(err))
				continue
			}
			if sg.ID != 0 && sg.Name != "" {
				subgroups = append(subgroups, sg)
			}
		}
		if resp.GetNextPageUrl() == "" {
			break
		}
		page++
		if err := checkPageCap("subgroup listing", page); err != nil {
			logger.WarnWithCtx(ctx, "subgroup listing for team enrichment stopped", zap.Error(err))
			return result
		}
	}

	logger.Info(ctx, "found subgroups for team enrichment", zap.Int("count", len(subgroups)))

	if len(subgroups) == 0 {
		// No subgroups — use the root group as the team. We fetch direct members
		// of the root group so every member is assigned to it, and at least one
		// team appears in the Teams tab.
		logger.Info(ctx, "no subgroups found, using root group as team", zap.String("group_name", rootGroupName))
		page = 1
		for {
			protoResp, err := gw.Execute(ctx, &connectorpb.GitLabListGroupDirectMembersRequest{
				GroupId: rootGroupID,
				Page:    page,
				PerPage: 100,
			})
			if err != nil {
				logger.WarnWithCtx(ctx, "failed to list root group members for team fallback", zap.Error(err))
				return result
			}
			resp, ok := protoResp.(*connectorpb.GitLabListGroupDirectMembersResponse)
			if !ok {
				return result
			}
			for _, raw := range resp.GetMembers() {
				var m struct {
					ID int64 `json:"id"`
				}
				if err := json.Unmarshal(raw, &m); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed root group member entry", zap.Error(err))
					continue
				}
				if m.ID == 0 {
					continue
				}
				result[m.ID] = append(result[m.ID], rootGroupName)
			}
			if resp.GetNextPageUrl() == "" {
				break
			}
			page++
			if err := checkPageCap("root group member listing", page); err != nil {
				logger.WarnWithCtx(ctx, "root group member listing for team fallback stopped", zap.Error(err))
				return result
			}
		}
		return result
	}

	// Step 2: For each subgroup, fetch direct members and map userID → subgroup name.
	for _, sg := range subgroups {
		page = 1
		for {
			protoResp, err := gw.Execute(ctx, &connectorpb.GitLabListGroupDirectMembersRequest{
				GroupId: sg.ID,
				Page:    page,
				PerPage: 100,
			})
			if err != nil {
				logger.WarnWithCtx(ctx, "failed to list subgroup members", zap.String("subgroup", sg.Name), zap.Error(err))
				break
			}
			resp, ok := protoResp.(*connectorpb.GitLabListGroupDirectMembersResponse)
			if !ok {
				break
			}
			for _, raw := range resp.GetMembers() {
				var m struct {
					ID int64 `json:"id"`
				}
				if err := json.Unmarshal(raw, &m); err != nil {
					logger.WarnWithCtx(ctx, "skipping malformed subgroup member entry", zap.String("subgroup", sg.Name), zap.Error(err))
					continue
				}
				if m.ID == 0 {
					continue
				}
				result[m.ID] = append(result[m.ID], sg.Name)
			}
			if resp.GetNextPageUrl() == "" {
				break
			}
			page++
			if err := checkPageCap("subgroup member listing", page); err != nil {
				logger.WarnWithCtx(ctx, "subgroup member listing stopped", zap.String("subgroup", sg.Name), zap.Error(err))
				break
			}
		}
	}

	return result
}

// fetchUserEmail fetches a GitLab user's profile to retrieve their public or
// commit email. The group members API doesn't return email for non-admin OAuth
// tokens, but the /users/{id} endpoint returns public_email. Non-fatal: returns
// empty string on any error.
func (op *FetchGroupMembersOp) fetchUserEmail(ctx context.Context, gw sdk.VendorGateway, userID int64) string {
	protoResp, err := gw.Execute(ctx, &connectorpb.GitLabGetUserRequest{UserId: userID})
	if err != nil {
		logger.Debug(ctx, "failed to fetch user profile for email", zap.Int64(logger.UserID, userID), zap.Error(err))
		return ""
	}
	resp, ok := protoResp.(*connectorpb.GitLabGetUserResponse)
	if !ok {
		return ""
	}
	var profile struct {
		PublicEmail string `json:"public_email"`
		Email       string `json:"email"`
	}
	if err := json.Unmarshal(resp.GetUser(), &profile); err != nil {
		logger.WarnWithCtx(ctx, "failed to unmarshal user profile for email enrichment", zap.Int64(logger.UserID, userID), zap.Error(err))
		return ""
	}
	// Prefer public_email (always visible), fall back to email field.
	if profile.PublicEmail != "" {
		return profile.PublicEmail
	}
	return profile.Email
}

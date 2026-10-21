// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	gitlabAuthorizeURL = "https://gitlab.com/oauth/authorize"
	// gitlabOAuthStateTTL is the TTL for the CSRF state token during the GitLab
	// OAuth flow. Duplicated from oauthStateTTL in bitbucket_setup.go so each
	// connector owns its own constant.
	gitlabOAuthStateTTL = 10 * time.Minute
	groupSelectionTTL   = 5 * time.Minute
)

// connectGitLab generates a CSRF state token, stores it in Redis, and
// returns the GitLab authorize URL.
func (s *Service) connectGitLab(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}

	if s.cfg.GitLabOAuth == nil {
		return nil, status.Error(codes.FailedPrecondition, "gitlab oauth not configured")
	}

	// Generate 32-byte random state token.
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		logger.Error(ctx, "connectGitLab: failed to generate state", zap.Error(err))
		return nil, status.Errorf(codes.Internal, "generate state: %v", err)
	}
	state := hex.EncodeToString(stateBytes)

	// Store state in Redis with TTL.
	stateData, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: orgID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal state data: %v", err)
	}

	if err := s.redis.Set(ctx, glconfig.OAuthStateKey(state), stateData, gitlabOAuthStateTTL).Err(); err != nil {
		logger.Error(ctx, "connectGitLab: failed to store state in Redis",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "store state: %v", err)
	}

	// Build authorize URL. Use url.Values so spaces encode as %20 (not +),
	// which GitLab requires for the scope parameter.
	params := url.Values{}
	params.Set("client_id", s.cfg.GitLabOAuth.ClientID)
	params.Set("redirect_uri", s.cfg.GitLabOAuth.CallbackURL)
	params.Set("response_type", "code")
	params.Set("state", state)
	params.Set("scope", "api read_user read_repository")
	authorizeURL := gitlabAuthorizeURL + "?" + params.Encode()

	logger.Info(ctx, "gitlab setup: authorize URL generated",
		zap.String(logger.OrgID, orgID))

	return &connintv1.ConnectIntegrationResponse{RedirectUrl: authorizeURL}, nil
}

// groupSelectionData is the Redis-stored state for the GitLab group picker flow.
type groupSelectionData struct {
	OrgID        string        `json:"org_id"`
	RefreshToken string        `json:"refresh_token"`
	AccessToken  string        `json:"access_token"` // needed for webhook registration
	Groups       []GitLabGroup `json:"groups"`
	User         *GitLabUser   `json:"user"`
}

// ValidateAndSaveGitLabConnection validates the CSRF state, exchanges
// the authorization code for tokens, and either auto-selects the group
// (single) or returns a group list for the admin to pick from (multiple).
func (s *Service) ValidateAndSaveGitLabConnection(ctx context.Context, req *connintv1.ValidateAndSaveGitLabConnectionRequest) (*connintv1.ValidateAndSaveGitLabConnectionResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	authCode := req.GetAuthCode()
	stateToken := req.GetState()

	if orgID == "" || authCode == "" || stateToken == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id, auth_code, and state are required")
	}

	// 1. Validate CSRF state.
	key := glconfig.OAuthStateKey(stateToken)
	// GETDEL reads and consumes the single-use state atomically, so two
	// concurrent callbacks cannot both pass, and a mismatched org burns it.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if err != nil {
		logger.Error(ctx, "gitlab setup: invalid or expired state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "invalid or expired state")
	}
	var stateData struct {
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		logger.Error(ctx, "gitlab setup: failed to decode stored state data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode state data")
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "gitlab setup: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return nil, status.Error(codes.InvalidArgument, "state mismatch")
	}

	// 2. Exchange auth code for tokens.
	if s.glClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "gitlab oauth client not configured")
	}
	tokenResp, err := s.glClient.ExchangeAuthCode(ctx, authCode)
	if err != nil {
		logger.Error(ctx, "gitlab setup: token exchange failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "token exchange: %v", err)
	}

	// 3. Fetch the authenticated user's identity and groups.
	user, err := s.glClient.FetchCurrentUser(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "gitlab setup: fetch current user failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "fetch current user: %v", err)
	}

	groups, err := s.glClient.FetchGroups(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "gitlab setup: fetch groups failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "fetch groups: %v", err)
	}

	// 4. Group selection.
	if len(groups) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no groups found")
	}

	// Single group: auto-select and save immediately.
	if len(groups) == 1 {
		if err := s.saveGitLabConfig(ctx, orgID, &groups[0], tokenResp.RefreshToken, tokenResp.AccessToken, user); err != nil {
			return nil, fmt.Errorf("saving git lab config: %w", err)
		}
		return &connintv1.ValidateAndSaveGitLabConnectionResponse{}, nil
	}

	// Multiple groups: store tokens + groups in Redis, return list for picker.
	selectionBytes := make([]byte, 16)
	if _, err := rand.Read(selectionBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "generate session key: %v", err)
	}
	selectionKey := hex.EncodeToString(selectionBytes)

	selectionData, err := json.Marshal(groupSelectionData{ //#nosec G117 -- OAuth refresh/access tokens are intentionally serialized to the short-lived selection store to complete the connect flow
		OrgID:        orgID,
		RefreshToken: tokenResp.RefreshToken,
		AccessToken:  tokenResp.AccessToken,
		Groups:       groups,
		User:         user,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal selection data: %v", err)
	}

	// Encrypt before parking in Redis — the blob holds the customer's GitLab
	// OAuth access/refresh tokens, which must not sit as plaintext at rest.
	encryptedData, err := s.encryptSelectionBlob(selectionData)
	if err != nil {
		logger.Error(ctx, "gitlab setup: failed to encrypt group selection data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "encrypt selection data: %v", err)
	}

	if err := s.redis.Set(ctx, glconfig.GroupSelectionKey(selectionKey), encryptedData, groupSelectionTTL).Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "store group selection: %v", err)
	}

	protoGroups := make([]*connintv1.GitLabGroup, len(groups))
	for i, g := range groups {
		protoGroups[i] = &connintv1.GitLabGroup{Id: g.ID, Path: g.Path, FullPath: g.FullPath, Name: g.Name}
	}

	logger.Info(ctx, "gitlab setup: multiple groups, awaiting selection",
		zap.String(logger.OrgID, orgID), zap.Int("count", len(groups)))

	return &connintv1.ValidateAndSaveGitLabConnectionResponse{
		SelectionKey: selectionKey,
		Groups:       protoGroups,
	}, nil
}

// saveGitLabConfig persists a GitLab connector config for the given group.
func (s *Service) saveGitLabConfig(ctx context.Context, orgID string, group *GitLabGroup, refreshToken, accessToken string, user *GitLabUser) error {
	if group == nil {
		return status.Error(codes.Internal, "group is required")
	}
	if user == nil {
		return status.Error(codes.Internal, "authorizing user is required")
	}

	// Generate 32-byte random webhook secret.
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return status.Errorf(codes.Internal, "generate webhook secret: %v", err)
	}
	webhookSecret := hex.EncodeToString(secretBytes)

	// Best-effort: register webhook. Log on failure but continue.
	var hookID int64
	if s.glClient != nil && s.cfg.GitLabOAuth != nil && s.cfg.GitLabOAuth.WebhookURL != "" {
		var hookErr error
		hookID, hookErr = s.glClient.RegisterGroupWebhook(ctx, accessToken, group.ID, s.cfg.GitLabOAuth.WebhookURL, webhookSecret)
		if hookErr != nil {
			logger.Error(ctx, "gitlab setup: webhook registration failed (best-effort)",
				zap.String(logger.OrgID, orgID), zap.Int64("group_id", group.ID), zap.Error(hookErr))
		}
	}

	creds, err := json.Marshal(glconfig.GitLabCredentials{
		OAuthRefreshToken: refreshToken,
		WebhookSecret:     webhookSecret,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	settings, err := json.Marshal(glconfig.GitLabSettings{
		GroupID:                    group.ID,
		GroupPath:                  group.Path,
		GroupFullPath:              group.FullPath,
		GroupName:                  group.Name,
		WebhookHookID:              hookID,
		AuthorizingUserID:          user.ID,
		AuthorizingUserUsername:    user.Username,
		AuthorizingUserDisplayName: user.Name,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	// Guard: only one VCS connector (GitHub, GitLab, Bitbucket) per org.
	if err := s.checkSingleVCSGuard(ctx, orgID, connectorIDGitLab); err != nil {
		return err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDGitLab,
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
		Enabled:        true,
	}
	// A reconnect over a live row keeps every settings key this flow does not
	// produce, and the operation overrides.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, connCfg, mergeProducedSettings(settings)); err != nil {
		logger.Error(ctx, "gitlab setup: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, connectorIDGitLab)

	logger.Info(ctx, "gitlab setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.String("group_path", group.Path))

	// Best-effort: update org logo from group avatar.
	if group.AvatarURL != "" && s.branding != nil {
		if err := s.branding.UpdateOrgLogo(ctx, orgID, group.AvatarURL); err != nil {
			logger.Error(ctx, "gitlab setup: failed to update org logo (best-effort)", zap.String(logger.OrgID, orgID), zap.Error(err))
		}
	}

	return nil
}

// GetGitLabGroups returns the group list for a pending group selection session.
func (s *Service) GetGitLabGroups(ctx context.Context, req *connintv1.GetGitLabGroupsRequest) (*connintv1.GetGitLabGroupsResponse, error) {
	selectionKey := req.GetSelectionKey()
	if selectionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "selection_key is required")
	}

	data, err := s.readGroupSelection(ctx, selectionKey)
	if err != nil {
		return nil, fmt.Errorf("reading group selection: %w", err)
	}

	groups := make([]*connintv1.GitLabGroup, len(data.Groups))
	for i, g := range data.Groups {
		groups[i] = &connintv1.GitLabGroup{Id: g.ID, Path: g.Path, FullPath: g.FullPath, Name: g.Name}
	}

	return &connintv1.GetGitLabGroupsResponse{Groups: groups}, nil
}

// SelectGitLabGroup finalizes a GitLab connection with the admin's chosen group.
func (s *Service) SelectGitLabGroup(ctx context.Context, req *connintv1.SelectGitLabGroupRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	selectionKey := req.GetSelectionKey()
	groupID := req.GetGroupId()

	if orgID == "" || selectionKey == "" || groupID == 0 {
		return nil, status.Error(codes.InvalidArgument, "org_id, selection_key, and group_id are required")
	}

	data, err := s.readGroupSelection(ctx, selectionKey)
	if err != nil {
		return nil, fmt.Errorf("reading group selection: %w", err)
	}

	if data.OrgID != orgID {
		return nil, status.Error(codes.InvalidArgument, "org_id mismatch")
	}

	// Find selected group.
	var selected *GitLabGroup
	for i := range data.Groups {
		if data.Groups[i].ID == groupID {
			selected = &data.Groups[i]
			break
		}
	}
	if selected == nil {
		return nil, status.Errorf(codes.InvalidArgument, "group %d not in session", groupID)
	}

	if err := s.saveGitLabConfig(ctx, orgID, selected, data.RefreshToken, data.AccessToken, data.User); err != nil {
		return nil, fmt.Errorf("saving git lab config: %w", err)
	}

	// Delete Redis key (single-use).
	if err := s.redis.Del(ctx, glconfig.GroupSelectionKey(selectionKey)).Err(); err != nil {
		logger.Error(ctx, "group picker: failed to delete session",
			zap.String("selection_key", selectionKey), zap.Error(err))
	}

	return &emptypb.Empty{}, nil
}

// readGroupSelection reads and unmarshals group selection data from Redis.
func (s *Service) readGroupSelection(ctx context.Context, selectionKey string) (*groupSelectionData, error) {
	stored, err := s.redis.Get(ctx, glconfig.GroupSelectionKey(selectionKey)).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "gitlab group picker: session read failed (expired or invalid)",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.NotFound, "session expired or invalid")
	}

	decrypted, err := s.decryptSelectionBlob([]byte(stored))
	if err != nil {
		logger.Error(ctx, "gitlab group picker: failed to decrypt session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decrypt session data")
	}

	var data groupSelectionData
	if err := json.Unmarshal(decrypted, &data); err != nil {
		logger.Error(ctx, "gitlab group picker: failed to decode session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode session data")
	}
	return &data, nil
}

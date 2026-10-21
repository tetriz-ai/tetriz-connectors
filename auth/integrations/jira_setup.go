// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	intjira "github.com/tetriz-ai/tetriz-connectors/auth/integrations/jira"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// jiraOAuthScopes is the URL-encoded space-separated scope list requested
// during the OAuth authorize redirect. Decoded:
//
//	read:jira-work                  — REST v3 (issues, projects, comments, worklogs)
//	read:jira-user                  — connecting user's display name
//	offline_access                  — refresh tokens
//	read:sprint:jira-software       — Agile sprint sub-resources
//	read:board-scope:jira-software  — Agile board listing (granular)
//	read:project:jira               — required pair with read:board-scope:jira-software;
//	                                  Atlassian validates against granular scopes once any
//	                                  granular scope is present, so classic read:jira-work
//	                                  alone is not sufficient for /rest/agile/1.0/board.
const (
	jiraAuthorizeURL     = "https://auth.atlassian.com/authorize"
	jiraOAuthAudience    = "api.atlassian.com"
	jiraOAuthScopes      = "read%3Ajira-work+read%3Ajira-user+offline_access+read%3Asprint%3Ajira-software+read%3Aboard-scope%3Ajira-software+read%3Aproject%3Ajira"
	jiraOAuthStateTTL    = 10 * time.Minute
	jiraSiteSelectionTTL = 15 * time.Minute
)

// connectJira generates a CSRF state token, stores it in Redis, and
// returns the Atlassian authorize URL for Jira Cloud.
func (s *Service) connectJira(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}

	if s.cfg.JiraOAuth == nil {
		return nil, status.Error(codes.FailedPrecondition, "jira oauth not configured")
	}

	// Generate 32-byte random state token.
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		logger.Error(ctx, "connectJira: failed to generate state", zap.Error(err))
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

	if err := s.redis.Set(ctx, jiraconfig.OAuthStateKey(state), stateData, jiraOAuthStateTTL).Err(); err != nil {
		logger.Error(ctx, "connectJira: failed to store state in Redis",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "store state: %v", err)
	}

	// Build authorize URL with Atlassian-specific parameters.
	authorizeURL := fmt.Sprintf(
		"%s?audience=%s&client_id=%s&scope=%s&redirect_uri=%s&state=%s&response_type=code&prompt=consent",
		jiraAuthorizeURL,
		jiraOAuthAudience,
		s.cfg.JiraOAuth.ClientID,
		jiraOAuthScopes,
		s.cfg.JiraOAuth.CallbackURL,
		state,
	)

	logger.Info(ctx, "jira setup: authorize URL generated",
		zap.String(logger.OrgID, orgID))

	return &connintv1.ConnectIntegrationResponse{RedirectUrl: authorizeURL}, nil
}

// ValidateAndSaveJiraConnection validates the CSRF state, exchanges
// the authorization code for tokens, fetches the user's accessible cloud sites,
// and either auto-persists the config (single site) or returns a selection_key
// for the caller to pick a site via SelectJiraCloudSite.
func (s *Service) ValidateAndSaveJiraConnection(ctx context.Context, req *connintv1.ValidateAndSaveJiraConnectionRequest) (*connintv1.ValidateAndSaveJiraConnectionResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	authCode := req.GetAuthCode()
	stateToken := req.GetState()

	if orgID == "" || authCode == "" || stateToken == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id, auth_code, and state are required")
	}

	// 1. Validate CSRF state.
	key := jiraconfig.OAuthStateKey(stateToken)
	// GETDEL reads and consumes the single-use state atomically, so two
	// concurrent callbacks cannot both pass, and a mismatched org burns it.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if err != nil {
		logger.Error(ctx, "jira setup: invalid or expired state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "invalid or expired state")
	}
	var stateData struct {
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		logger.Error(ctx, "jira setup: failed to decode stored state data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode state data")
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "jira setup: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return nil, status.Error(codes.InvalidArgument, "state mismatch")
	}

	// 2. Exchange auth code for tokens.
	if s.jiraClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "jira oauth client not configured")
	}
	tokenResp, err := s.jiraClient.ExchangeAuthCode(ctx, authCode)
	if err != nil {
		logger.Error(ctx, "jira setup: token exchange failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "token exchange: %v", err)
	}

	// 3. Fetch accessible cloud sites using the new access token.
	sites, err := s.jiraClient.FetchAccessibleResources(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "jira setup: fetch accessible resources failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "fetch accessible resources: %v", err)
	}

	if len(sites) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no Jira cloud sites found")
	}

	// 4. Fetch the connecting user's display name (best-effort — don't fail setup if this errors).
	connectedByName := ""
	if userName, fetchErr := s.jiraClient.FetchCurrentUser(ctx, tokenResp.AccessToken); fetchErr != nil {
		logger.Error(ctx, "jira setup: failed to fetch current user (non-fatal)",
			zap.String(logger.OrgID, orgID), zap.Error(fetchErr))
	} else {
		connectedByName = userName
	}

	// 5. Single site: auto-select and save immediately.
	if len(sites) == 1 {
		if err := s.saveJiraConfig(ctx, orgID, &sites[0], tokenResp.RefreshToken, connectedByName); err != nil {
			return nil, fmt.Errorf("saving jira config: %w", err)
		}
		return &connintv1.ValidateAndSaveJiraConnectionResponse{}, nil
	}

	// 6. Multiple sites: store tokens + sites in Redis, return list for picker.
	selectionBytes := make([]byte, 16)
	if _, err := rand.Read(selectionBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "generate session key: %v", err)
	}
	selectionKey := hex.EncodeToString(selectionBytes)

	selectionData, err := json.Marshal(jiraSiteSelectionData{ //#nosec G117 -- OAuth refresh token is serialized only to be encrypted before parking in the short-lived selection store (see encryptSelectionBlob below); never stored or logged in plaintext
		OrgID:           orgID,
		RefreshToken:    tokenResp.RefreshToken,
		Sites:           sites,
		ConnectedByName: connectedByName,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal selection data: %v", err)
	}

	// Encrypt before parking in Redis — the blob holds the customer's Jira
	// OAuth refresh token, which must not sit as plaintext at rest.
	encryptedData, err := s.encryptSelectionBlob(selectionData)
	if err != nil {
		logger.Error(ctx, "jira setup: failed to encrypt site selection data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "encrypt selection data: %v", err)
	}

	if err := s.redis.Set(ctx, jiraconfig.SiteSelectionKey(selectionKey), encryptedData, jiraSiteSelectionTTL).Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "store site selection: %v", err)
	}

	protoSites := make([]*connintv1.JiraCloudSite, len(sites))
	for i, site := range sites {
		protoSites[i] = &connintv1.JiraCloudSite{Id: site.ID, Name: site.Name, Url: site.URL}
	}

	logger.Info(ctx, "jira setup: multiple sites, awaiting selection",
		zap.String(logger.OrgID, orgID), zap.Int("count", len(sites)))

	return &connintv1.ValidateAndSaveJiraConnectionResponse{
		SelectionKey: selectionKey,
		Sites:        protoSites,
	}, nil
}

// jiraSiteSelectionData is the Redis-stored state for the cloud site picker flow.
type jiraSiteSelectionData struct {
	OrgID           string              `json:"org_id"`
	RefreshToken    string              `json:"refresh_token"`
	Sites           []intjira.CloudSite `json:"sites"`
	ConnectedByName string              `json:"connected_by_name"`
}

// SelectJiraCloudSite finalizes a Jira connection with the admin's chosen cloud site.
func (s *Service) SelectJiraCloudSite(ctx context.Context, req *connintv1.SelectJiraCloudSiteRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	selectionKey := req.GetSelectionKey()
	cloudID := req.GetCloudId()

	if orgID == "" || selectionKey == "" || cloudID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id, selection_key, and cloud_id are required")
	}

	data, err := s.readJiraSiteSelection(ctx, selectionKey)
	if err != nil {
		return nil, fmt.Errorf("reading jira site selection: %w", err)
	}

	if data.OrgID != orgID {
		return nil, status.Error(codes.InvalidArgument, "org_id mismatch")
	}

	// Find the selected site.
	var selected *intjira.CloudSite
	for i := range data.Sites {
		if data.Sites[i].ID == cloudID {
			selected = &data.Sites[i]
			break
		}
	}
	if selected == nil {
		return nil, status.Errorf(codes.InvalidArgument, "cloud site %q not in session", cloudID)
	}

	if err := s.saveJiraConfig(ctx, orgID, selected, data.RefreshToken, data.ConnectedByName); err != nil {
		return nil, fmt.Errorf("saving jira config: %w", err)
	}

	// Delete Redis key (single-use).
	if err := s.redis.Del(ctx, jiraconfig.SiteSelectionKey(selectionKey)).Err(); err != nil {
		logger.Error(ctx, "jira site picker: failed to delete session",
			zap.String("selection_key", selectionKey), zap.Error(err))
	}

	return &emptypb.Empty{}, nil
}

// readJiraSiteSelection reads and unmarshals site selection data from Redis.
func (s *Service) readJiraSiteSelection(ctx context.Context, selectionKey string) (*jiraSiteSelectionData, error) {
	stored, err := s.redis.Get(ctx, jiraconfig.SiteSelectionKey(selectionKey)).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "jira site picker: session read failed (expired or invalid)",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.NotFound, "session expired or invalid")
	}

	decrypted, err := s.decryptSelectionBlob([]byte(stored))
	if err != nil {
		logger.Error(ctx, "jira site picker: failed to decrypt session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decrypt session data")
	}

	var data jiraSiteSelectionData
	if err := json.Unmarshal(decrypted, &data); err != nil {
		logger.Error(ctx, "jira site picker: failed to decode session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode session data")
	}
	return &data, nil
}

// saveJiraConfig persists a Jira connector config for the given cloud site.
func (s *Service) saveJiraConfig(ctx context.Context, orgID string, site *intjira.CloudSite, refreshToken, connectedByName string) error {
	creds, err := json.Marshal(jiraconfig.JiraCredentials{
		OAuthRefreshToken: refreshToken,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	settings, err := json.Marshal(jiraconfig.JiraSettings{
		CloudID:         site.ID,
		CloudName:       site.Name,
		SiteURL:         site.URL,
		ConnectedByName: connectedByName,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDJira,
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
		Enabled:        true,
	}
	// A reconnect over a live row keeps every settings key this flow does not
	// produce, and the operation overrides.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, connCfg, mergeProducedSettings(settings)); err != nil {
		logger.Error(ctx, "jira setup: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, connectorIDJira)

	logger.Info(ctx, "jira setup: connection saved",
		zap.String(logger.OrgID, orgID),
		zap.String("cloud_id", site.ID),
		zap.String("site_name", site.Name))

	return nil
}

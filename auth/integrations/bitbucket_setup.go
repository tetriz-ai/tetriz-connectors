// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	bitbucketAuthorizeURL = "https://bitbucket.org/site/oauth2/authorize"
	oauthStateTTL         = 10 * time.Minute
)

// connectBitbucket generates a CSRF state token, stores it in Redis, and
// returns the Bitbucket authorize URL.
func (s *Service) connectBitbucket(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}

	if s.cfg.BitbucketOAuth == nil {
		return nil, status.Error(codes.FailedPrecondition, "bitbucket oauth not configured")
	}

	// Generate 32-byte random state token.
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		logger.Error(ctx, "connectBitbucket: failed to generate state", zap.Error(err))
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

	if err := s.redis.Set(ctx, bbconfig.OAuthStateKey(state), stateData, oauthStateTTL).Err(); err != nil {
		logger.Error(ctx, "connectBitbucket: failed to store state in Redis",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "store state: %v", err)
	}

	// Build authorize URL. Pin redirect_uri explicitly (RFC 6749 §4.1.1) so the
	// authorization code can only be delivered to our registered callback,
	// rather than relying on Bitbucket's default for the consumer.
	params := url.Values{}
	params.Set("client_id", s.cfg.BitbucketOAuth.ClientID)
	params.Set("redirect_uri", s.cfg.BitbucketOAuth.CallbackURL)
	params.Set("response_type", "code")
	params.Set("state", state)
	authorizeURL := bitbucketAuthorizeURL + "?" + params.Encode()

	logger.Info(ctx, "bitbucket setup: authorize URL generated",
		zap.String(logger.OrgID, orgID))

	return &connintv1.ConnectIntegrationResponse{RedirectUrl: authorizeURL}, nil
}

const workspaceSelectionTTL = 5 * time.Minute

// callbackErrVendorRejected is the key a host's UI maps to its own banner. It
// travels as a FailedPrecondition message because that is the channel the
// callback already forwards verbatim; a new status code would collide with
// Unauthenticated, which this codebase reserves for the caller's own auth.
const callbackErrVendorRejected = "bitbucket_vendor_rejected"

// vendorRefusedCredentials reports whether Bitbucket rejected the token itself.
// Only a refusal is terminal; a timeout, a 5xx or a decode failure is worth
// retrying, and the callback banner keys off that difference — telling an admin
// to stop retrying during a vendor outage is worse than saying nothing.
func vendorRefusedCredentials(err error) bool {
	var apiErr *bitbucketAPIError
	return errors.As(err, &apiErr) && apiErr.Refused()
}

// ValidateAndSaveBitbucketConnection validates the CSRF state, exchanges
// the authorization code for tokens, and either auto-selects the workspace
// (single) or returns a workspace list for the admin to pick from (multiple).
func (s *Service) ValidateAndSaveBitbucketConnection(ctx context.Context, req *connintv1.ValidateAndSaveBitbucketConnectionRequest) (*connintv1.ValidateAndSaveBitbucketConnectionResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	authCode := req.GetAuthCode()
	stateToken := req.GetState()

	if orgID == "" || authCode == "" || stateToken == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id, auth_code, and state are required")
	}

	// 1. Validate CSRF state.
	key := bbconfig.OAuthStateKey(stateToken)
	// GETDEL reads and consumes the single-use state atomically, so two
	// concurrent callbacks cannot both pass, and a mismatched org burns it.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if err != nil {
		logger.Error(ctx, "bitbucket setup: invalid or expired state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.InvalidArgument, "invalid or expired state")
	}
	var stateData struct {
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		logger.Error(ctx, "bitbucket setup: failed to decode stored state data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode state data")
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "bitbucket setup: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return nil, status.Error(codes.InvalidArgument, "state mismatch")
	}

	// 2. Exchange auth code for tokens.
	if s.bbClient == nil {
		return nil, status.Error(codes.FailedPrecondition, "bitbucket oauth client not configured")
	}
	tokenResp, err := s.bbClient.ExchangeAuthCode(ctx, authCode)
	if err != nil {
		logger.Error(ctx, "bitbucket setup: token exchange failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		if vendorRefusedCredentials(err) {
			return nil, status.Error(codes.FailedPrecondition, callbackErrVendorRejected)
		}
		return nil, status.Errorf(codes.Internal, "token exchange: %v", err)
	}

	// 3. Fetch the authenticated user's identity and workspaces.
	//
	// Bitbucket answers a rejected token with a bare 401 that names no cause, so
	// the granted scopes are logged alongside: they are the only thing that
	// separates a consumer missing a scope from a token refused for any other
	// reason, and they are otherwise unrecoverable after the exchange.
	user, err := s.bbClient.FetchCurrentUser(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "bitbucket setup: fetch current user failed",
			zap.String(logger.OrgID, orgID),
			zap.String("granted_scopes", tokenResp.Scopes), zap.Error(err))
		if vendorRefusedCredentials(err) {
			return nil, status.Error(codes.FailedPrecondition, callbackErrVendorRejected)
		}
		return nil, status.Errorf(codes.Internal, "fetch current user: %v", err)
	}

	workspaces, err := s.bbClient.FetchWorkspaces(ctx, tokenResp.AccessToken)
	if err != nil {
		logger.Error(ctx, "bitbucket setup: fetch workspaces failed",
			zap.String(logger.OrgID, orgID),
			zap.String("granted_scopes", tokenResp.Scopes), zap.Error(err))
		if vendorRefusedCredentials(err) {
			return nil, status.Error(codes.FailedPrecondition, callbackErrVendorRejected)
		}
		return nil, status.Errorf(codes.Internal, "fetch workspaces: %v", err)
	}

	// 4. Workspace selection.
	if len(workspaces) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no workspaces found")
	}

	// Single workspace: auto-select and save immediately.
	if len(workspaces) == 1 {
		if err := s.saveBitbucketConfig(ctx, orgID, &workspaces[0], tokenResp.RefreshToken, user); err != nil {
			return nil, fmt.Errorf("saving bitbucket config: %w", err)
		}
		return &connintv1.ValidateAndSaveBitbucketConnectionResponse{}, nil
	}

	// Multiple workspaces: store tokens + workspaces in Redis, return list for picker.
	selectionBytes := make([]byte, 16)
	if _, err := rand.Read(selectionBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "generate session key: %v", err)
	}
	selectionKey := hex.EncodeToString(selectionBytes)

	selectionData, err := json.Marshal(workspaceSelectionData{ //#nosec G117 -- OAuth refresh token is intentionally serialized to the short-lived selection store to complete the connect flow
		OrgID:        orgID,
		RefreshToken: tokenResp.RefreshToken,
		Workspaces:   workspaces,
		User:         user,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal selection data: %v", err)
	}

	// Encrypt before parking in Redis — the blob holds the customer's Bitbucket
	// OAuth refresh token, which must not sit as plaintext at rest.
	encryptedData, err := s.encryptSelectionBlob(selectionData)
	if err != nil {
		logger.Error(ctx, "bitbucket setup: failed to encrypt workspace selection data",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "encrypt selection data: %v", err)
	}

	if err := s.redis.Set(ctx, bbconfig.WorkspaceSelectionKey(selectionKey), encryptedData, workspaceSelectionTTL).Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "store workspace selection: %v", err)
	}

	protoWorkspaces := make([]*connintv1.BitbucketWorkspace, len(workspaces))
	for i, ws := range workspaces {
		protoWorkspaces[i] = &connintv1.BitbucketWorkspace{Id: ws.Slug, Name: ws.Name}
	}

	logger.Info(ctx, "bitbucket setup: multiple workspaces, awaiting selection",
		zap.String(logger.OrgID, orgID), zap.Int("count", len(workspaces)))

	return &connintv1.ValidateAndSaveBitbucketConnectionResponse{
		SelectionKey: selectionKey,
		Workspaces:   protoWorkspaces,
	}, nil
}

// workspaceSelectionData is the Redis-stored state for the workspace picker flow.
type workspaceSelectionData struct {
	OrgID        string               `json:"org_id"`
	RefreshToken string               `json:"refresh_token"`
	Workspaces   []BitbucketWorkspace `json:"workspaces"`
	User         *BitbucketUser       `json:"user"`
}

// saveBitbucketConfig persists a Bitbucket connector config for the given workspace.
func (s *Service) saveBitbucketConfig(ctx context.Context, orgID string, ws *BitbucketWorkspace, refreshToken string, user *BitbucketUser) error {
	if ws == nil {
		return status.Error(codes.Internal, "workspace is required")
	}
	if user == nil {
		return status.Error(codes.Internal, "authorizing user is required")
	}

	creds, err := json.Marshal(bbconfig.BitbucketCredentials{
		OAuthRefreshToken: refreshToken,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	settings, err := json.Marshal(bbconfig.BitbucketSettings{
		Workspace:                  ws.Slug,
		WorkspaceUUID:              ws.UUID,
		WorkspaceName:              ws.Name,
		AuthorizingUserUUID:        user.UUID,
		AuthorizingUserDisplayName: user.DisplayName,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	// Guard: only one VCS connector (GitHub, GitLab, Bitbucket) per org.
	if err := s.checkSingleVCSGuard(ctx, orgID, connectorIDBitbucket); err != nil {
		return err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    connectorIDBitbucket,
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
		Enabled:        true,
	}
	// A reconnect over a live row keeps every settings key this flow does not
	// produce, and the operation overrides.
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, connCfg, mergeProducedSettings(settings)); err != nil {
		logger.Error(ctx, "bitbucket setup: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, connectorIDBitbucket)

	logger.Info(ctx, "bitbucket setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.String("workspace", ws.Slug))

	// Best-effort: update org logo from workspace avatar.
	if ws.AvatarURL != "" && s.branding != nil {
		if err := s.branding.UpdateOrgLogo(ctx, orgID, ws.AvatarURL); err != nil {
			logger.Error(ctx, "bitbucket setup: failed to update org logo (best-effort)", zap.String(logger.OrgID, orgID), zap.Error(err))
		}
	}

	return nil
}

// GetBitbucketWorkspaces returns the workspace list for a pending workspace selection session.
func (s *Service) GetBitbucketWorkspaces(ctx context.Context, req *connintv1.GetBitbucketWorkspacesRequest) (*connintv1.GetBitbucketWorkspacesResponse, error) {
	selectionKey := req.GetSelectionKey()
	if selectionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "selection_key is required")
	}

	data, err := s.readWorkspaceSelection(ctx, selectionKey)
	if err != nil {
		return nil, fmt.Errorf("reading workspace selection: %w", err)
	}

	workspaces := make([]*connintv1.BitbucketWorkspace, len(data.Workspaces))
	for i, ws := range data.Workspaces {
		workspaces[i] = &connintv1.BitbucketWorkspace{Id: ws.Slug, Name: ws.Name}
	}

	return &connintv1.GetBitbucketWorkspacesResponse{Workspaces: workspaces}, nil
}

// SelectBitbucketWorkspace finalizes a Bitbucket connection with the admin's chosen workspace.
func (s *Service) SelectBitbucketWorkspace(ctx context.Context, req *connintv1.SelectBitbucketWorkspaceRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	selectionKey := req.GetSelectionKey()
	slug := req.GetWorkspaceSlug()

	if orgID == "" || selectionKey == "" || slug == "" {
		return nil, status.Error(codes.InvalidArgument, "org_id, selection_key, and workspace_slug are required")
	}

	data, err := s.readWorkspaceSelection(ctx, selectionKey)
	if err != nil {
		return nil, fmt.Errorf("reading workspace selection: %w", err)
	}

	if data.OrgID != orgID {
		return nil, status.Error(codes.InvalidArgument, "org_id mismatch")
	}

	// Find selected workspace.
	var selected *BitbucketWorkspace
	for i := range data.Workspaces {
		if data.Workspaces[i].Slug == slug {
			selected = &data.Workspaces[i]
			break
		}
	}
	if selected == nil {
		return nil, status.Errorf(codes.InvalidArgument, "workspace %q not in session", slug)
	}

	if err := s.saveBitbucketConfig(ctx, orgID, selected, data.RefreshToken, data.User); err != nil {
		return nil, fmt.Errorf("saving bitbucket config: %w", err)
	}

	// Delete Redis key (single-use).
	if err := s.redis.Del(ctx, bbconfig.WorkspaceSelectionKey(selectionKey)).Err(); err != nil {
		logger.Error(ctx, "workspace picker: failed to delete session",
			zap.String("selection_key", selectionKey), zap.Error(err))
	}

	return &emptypb.Empty{}, nil
}

// readWorkspaceSelection reads and unmarshals workspace selection data from Redis.
func (s *Service) readWorkspaceSelection(ctx context.Context, selectionKey string) (*workspaceSelectionData, error) {
	stored, err := s.redis.Get(ctx, bbconfig.WorkspaceSelectionKey(selectionKey)).Result()
	if err != nil {
		logger.WarnWithCtx(ctx, "bitbucket workspace picker: session read failed (expired or invalid)",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.NotFound, "session expired or invalid")
	}

	decrypted, err := s.decryptSelectionBlob([]byte(stored))
	if err != nil {
		logger.Error(ctx, "bitbucket workspace picker: failed to decrypt session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decrypt session data")
	}

	var data workspaceSelectionData
	if err := json.Unmarshal(decrypted, &data); err != nil {
		logger.Error(ctx, "bitbucket workspace picker: failed to decode session data",
			zap.String("selection_key", selectionKey), zap.Error(err))
		return nil, status.Error(codes.Internal, "decode session data")
	}
	return &data, nil
}

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

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	connectordao "github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// tornStateRejectReason is the errdetails.ErrorInfo.Reason the save handlers stamp
// on the FailedPrecondition they return when a pre-persist re-resolve lands on a
// different account than the caller pinned (the torn-state guard). The save step's
// re-resolve ALSO returns FailedPrecondition for unrelated causes — the single-VCS
// guard, the ops-disabled off switch, the missing-client branch — so a bare code
// match cannot tell the torn state apart from those. The caller matches on this exact
// reason and surfaces only it as the "different account/owner" reject; the other
// causes fall through to their correct user message. The literal is a cross-service
// contract: a host that matches on it keeps its own copy, so this string must not
// change without changing the host.
const tornStateRejectReason = "CONNECTOR_TORN_STATE_ACCOUNT_MISMATCH"

// errTornStateAccountMismatch builds the FailedPrecondition the save handlers return
// when the pre-persist re-resolve lands on a different account than the pinned owner.
// It attaches an errdetails.ErrorInfo{Reason: tornStateRejectReason} so the caller can
// match this reject precisely, distinct from other FailedPrecondition causes. The
// status detail survives the gRPC hop (serialized into grpc-status-details-bin); if
// detail attachment ever fails, fall back to the bare status so the save still
// rejects (fail-closed) rather than persisting under a torn state.
func errTornStateAccountMismatch(msg string) error {
	st := status.New(codes.FailedPrecondition, msg)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: tornStateRejectReason})
	if err != nil {
		// Attaching a static ErrorInfo cannot fail in practice; log if it ever does,
		// then fall back to the bare status (still rejects — fail-closed).
		logger.ErrorNoCtx("torn-state reject: errdetails attachment failed, using bare status", zap.Error(err))
		return st.Err() //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return withDetails.Err() //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
}

// githubInstallURL generates a CSRF state token bound to orgID, stores it in
// Redis, and returns the GitHub App install URL with that state appended.
// GitHub echoes the state back to SetupCallbackHandler, which forwards it to
// ValidateInstallation for verification — without this an attacker could trick a
// logged-in admin into binding the attacker's installation to their org.
func (s *Service) githubInstallURL(ctx context.Context, orgID string) (string, error) {
	stateBytes := make([]byte, 32)
	if _, err := rand.Read(stateBytes); err != nil {
		return "", status.Errorf(codes.Internal, "generate state: %v", err)
	}
	state := hex.EncodeToString(stateBytes)

	stateData, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: orgID})
	if err != nil {
		return "", status.Errorf(codes.Internal, "marshal state data: %v", err)
	}
	if err := s.redis.Set(ctx, ghconfig.OAuthStateKey(state), stateData, oauthStateTTL).Err(); err != nil {
		logger.Error(ctx, "githubInstallURL: failed to store state in Redis",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return "", status.Errorf(codes.Internal, "store state: %v", err)
	}

	return fmt.Sprintf("https://github.com/apps/%s/installations/new?state=%s",
		s.cfg.GitHubApp.Slug, url.QueryEscape(state)), nil
}

// validateGitHubInstallState consumes the CSRF state token GitHub echoed back
// and confirms it was issued for orgID. The token is deleted on read regardless
// of outcome to prevent replay. Mirrors the Slack/Bitbucket OAuth state check.
func (s *Service) validateGitHubInstallState(ctx context.Context, orgID, stateToken string) error {
	key := ghconfig.OAuthStateKey(stateToken)
	// GETDEL atomically reads and deletes in one round trip, so two concurrent
	// callbacks racing on the same state token cannot both observe it present —
	// only the winner gets the value, the loser gets redis.Nil. A separate
	// Get-then-Del has a window where both pass.
	storedJSON, err := s.redis.GetDel(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		logger.Error(ctx, "validateGitHubInstallState: invalid or expired state",
			zap.String(logger.OrgID, orgID))
		return status.Error(codes.InvalidArgument, "invalid or expired state token")
	}
	if err != nil {
		logger.Error(ctx, "validateGitHubInstallState: redis error reading state",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Errorf(codes.Internal, "read state: %v", err)
	}

	var stateData struct {
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal([]byte(storedJSON), &stateData); err != nil {
		return status.Errorf(codes.Internal, "parse state data: %v", err)
	}
	if stateData.OrgID != orgID {
		logger.Error(ctx, "validateGitHubInstallState: state org_id mismatch",
			zap.String("expected", stateData.OrgID), zap.String("got", orgID))
		return status.Error(codes.InvalidArgument, "state org_id mismatch")
	}
	return nil
}

// resolveInstallation verifies + consumes the CSRF state, fetches the
// installation from GitHub, runs the single-VCS guard, and resolves tenant_type.
// It performs NO persist — this is the read-only resolve both ValidateInstallation
// and SaveInstallation share (SaveInstallation re-runs it sans state, which was
// already consumed; SaveInstallation independently requires a single-use validation receipt). It returns the installation info plus the resolved
// tenant_type. A consumeState=false call skips the CSRF check (state already
// consumed by the validate step).
func (s *Service) resolveInstallation(ctx context.Context, orgID, installationID, stateToken string, consumeState bool) (*intgithub.InstallationInfo, commonv1.TenantType, error) {
	if installationID == "" {
		logger.Error(ctx, "resolveInstallation: missing installation_id")
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.InvalidArgument, "installation_id is required")
	}
	if !intgithub.ValidInstallationID(installationID) {
		logger.Error(ctx, "resolveInstallation: malformed installation_id", zap.String("installation_id", installationID))
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.InvalidArgument, "installation_id must be a positive decimal number")
	}
	if orgID == "" {
		logger.Error(ctx, "resolveInstallation: missing org_id")
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.InvalidArgument, "org_id is required")
	}

	if consumeState {
		if stateToken == "" {
			logger.Error(ctx, "resolveInstallation: missing state", zap.String(logger.OrgID, orgID))
			return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.InvalidArgument, "state is required")
		}
		// Verify and consume the CSRF state before any side effect, so a forged
		// callback cannot bind an attacker's installation to this org.
		if err := s.validateGitHubInstallState(ctx, orgID, stateToken); err != nil {
			return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
		}
	}

	if s.ghClient == nil {
		logger.Error(ctx, "resolveInstallation: github app client not configured")
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.FailedPrecondition, "github app client not configured")
	}

	info, err := s.ghClient.GetInstallation(ctx, installationID)
	if err != nil {
		if errors.Is(err, intgithub.ErrInstallationNotFound) {
			logger.Error(ctx, "resolveInstallation: installation not found on GitHub",
				zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID))
			return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.NotFound, "installation not found")
		}
		logger.Error(ctx, "resolveInstallation: failed to get installation from GitHub",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID), zap.Error(err))
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Errorf(codes.Internal, "get installation: %v", err)
	}

	// Guard: only one VCS connector (GitHub, GitLab, Bitbucket) per org.
	if err := s.checkSingleVCSGuard(ctx, orgID, connectorIDGitHub); err != nil {
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// Resolve tenant_type the same way listGitHubReposFresh does (the tenant
	// directory). Fail loud on a lookup error rather than
	// defaulting to enabled — a silent enable would ingest before the individual
	// confirm step and bypass the cap.
	orgTenantType, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "resolveInstallation: resolve tenant type failed",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID), zap.Error(err))
		return nil, commonv1.TenantType_TENANT_TYPE_UNSPECIFIED, status.Error(codes.Unavailable, "could not resolve tenant type, please try again")
	}
	return info, commonv1.TenantType(orgTenantType), nil
}

// ValidateInstallation resolves the installing account + tenant kind for a GitHub
// App callback WITHOUT persisting anything (the resolve step of pin-before-save).
// It consumes the single-use CSRF state and returns a five-minute receipt
// required by SaveInstallation. The caller pins the resolved account as the individual tenant's
// owner and only then calls SaveInstallation — so a different-owner install never
// persists a credential. Idempotent only insofar as it has no side effect beyond
// consuming the state; a retry needs a fresh state token (a fresh connect).
func (s *Service) ValidateInstallation(ctx context.Context, req *connintv1.ValidateInstallationRequest) (*connintv1.ValidateInstallationResponse, error) {
	if req.GetCode() == "" || len(req.GetCode()) > 1024 {
		return nil, status.Error(codes.InvalidArgument, "GitHub OAuth code is required")
	}
	if s.redis == nil {
		return nil, status.Error(codes.FailedPrecondition, "GitHub installation validation storage unavailable")
	}

	info, tenantType, err := s.resolveInstallation(ctx, orgid.Canonical(req.GetOrgId()), req.GetInstallationId(), req.GetState(), true)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	if err = s.ghClient.VerifyUserInstallation(ctx, req.GetCode(), req.GetInstallationId(), info); err != nil {
		return nil, status.Error(codes.PermissionDenied, "GitHub user cannot authorize this installation")
	}
	if info.AccountID <= 0 || fmt.Sprintf("%d", info.ID) != req.GetInstallationId() {
		return nil, status.Error(codes.FailedPrecondition, "invalid GitHub installation identity")
	}
	receipt, err := s.issueInstallationValidation(ctx, installationValidation{OrgID: orgid.Canonical(req.GetOrgId()), InstallationID: req.GetInstallationId(), AccountID: info.AccountID})
	if err != nil {
		return nil, status.Errorf(status.Code(err), "%s", status.Convert(err).Message())
	}
	return &connintv1.ValidateInstallationResponse{
		ValidationToken: receipt,
		AccountId:       fmt.Sprintf("%d", info.AccountID),
		AccountLogin:    info.AccountLogin,
		TenantType:      tenantType,
	}, nil
}

// SaveInstallation upserts a connector config for an installation the caller already
// validated (ValidateInstallation) and owner-verified (PinIndividualOwner). The
// CSRF state was consumed by validation. Its single-use receipt is consumed here
// and the installation account is re-fetched and checked before persisting.
// An INDIVIDUAL tenant lands disabled (a later ConfirmConnectorRepos enables it,
// mirroring the PAT path); an organization install lands enabled. The
// upsert is keyed on (org_id, connector_id). A failed save consumes its receipt;
// restart connect for a fresh state, code and validation receipt.
func (s *Service) SaveInstallation(ctx context.Context, req *connintv1.SaveInstallationRequest) (*connintv1.SaveInstallationResponse, error) {
	installationID := req.GetInstallationId()
	orgID := orgid.Canonical(req.GetOrgId())

	proof, err := s.consumeInstallationValidation(ctx, req.GetValidationToken(), orgID, installationID)
	if err != nil {
		return nil, status.Errorf(status.Code(err), "%s", status.Convert(err).Message())
	}
	info, tenantType, err := s.resolveInstallation(ctx, orgID, installationID, "", false)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	if info.AccountID != proof.AccountID || fmt.Sprintf("%d", info.ID) != installationID {
		if tenantType == commonv1.TenantType_TENANT_TYPE_INDIVIDUAL {
			return nil, errTornStateAccountMismatch("installation resolved to a different account than the one connected")
		}
		return nil, status.Errorf(codes.FailedPrecondition, "installation account changed after validation; reconnect")
	}
	// Pre-persist torn-state guard (INDIVIDUAL only): the caller pinned the account
	// ValidateInstallation resolved; this save re-resolved the installation. If the
	// installation's account changed between the two GitHub fetches, reject BEFORE
	// persisting so nothing is saved under the wrong owner (a clean, retryable
	// drop-off). An org has no single owner to guard.
	if tenantType == commonv1.TenantType_TENANT_TYPE_INDIVIDUAL &&
		req.GetExpectedAccountId() != "" && fmt.Sprintf("%d", info.AccountID) != req.GetExpectedAccountId() {
		logger.WarnWithCtx(ctx, "SaveInstallation: re-resolved account differs from pinned account (torn state)",
			zap.String(logger.OrgID, orgID),
			zap.String("installation_id", installationID),
			zap.String("expected_account_id", req.GetExpectedAccountId()),
			zap.String("resolved_account_id", fmt.Sprintf("%d", info.AccountID)))
		return nil, errTornStateAccountMismatch("installation resolved to a different account than the one connected") //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: installationID})
	if err != nil {
		logger.Error(ctx, "SaveInstallation: failed to marshal credentials",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}
	settings, err := json.Marshal(ghconfig.GitHubSettings{Organization: info.AccountLogin, IncludePrivate: true, InstallationID: installationID})
	if err != nil {
		logger.Error(ctx, "SaveInstallation: failed to marshal settings",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal settings: %v", err)
	}

	connCfg := &sdk.ConnectorConfig{
		ConnectorID:    "github",
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
	}
	// Only an explicit ORGANIZATION enables on install. Everything else
	// (individual, or an UNSPECIFIED/unknown tenant_type) takes the
	// preserve-enabled upsert, which never silently enables: a first connect
	// lands disabled until ConfirmConnectorRepos enables it, and a re-install of
	// an already-confirmed connector keeps its live enabled bit. Defaulting the
	// unknown case to disabled, not enabled, holds the cap.
	var saveErr error
	if tenantType == commonv1.TenantType_TENANT_TYPE_ORGANIZATION {
		connCfg.Enabled = true
		// A reinstall over a live row keeps every settings key this flow does not
		// produce, and the operation overrides.
		saveErr = s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, connCfg, mergeProducedSettings(settings))
	} else {
		saveErr = s.connectorDAO.SaveCredentialsPreserveEnabled(ctx, connCfg)
	}
	if isClaimedByAnotherOrganization(saveErr, "uniq_github_installation_per_org") {
		logger.WarnWithCtx(ctx, "SaveInstallation: installation already connected to another organization",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID))
		return nil, status.Error(codes.AlreadyExists, "this GitHub App installation is already connected to another organization")
	}
	if saveErr != nil {
		logger.Error(ctx, "SaveInstallation: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.String("installation_id", installationID), zap.Error(saveErr))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", saveErr)
	}
	s.resetConnectorHealth(ctx, orgID, "github")

	// Log tenant_type, not connCfg.Enabled: on the preserve-enabled path the
	// persisted value lives at the DB layer and is not reflected on the struct.
	logger.Info(ctx, "github setup: installation saved",
		zap.String(logger.OrgID, orgID),
		zap.String("installation_id", installationID),
		zap.String("account", info.AccountLogin),
		zap.String("tenant_type", tenantType.String()))

	// An ORGANIZATION install enables the connector right here; individual and
	// PAT/token connects enable later in ConfirmConnectorRepos, which fires its own
	// kickoff. Seed repo discovery now, so repository records arrive now, not at
	// the next 6-hourly fetch_repos run.
	if tenantType == commonv1.TenantType_TENANT_TYPE_ORGANIZATION {
		s.kickoffInitialRepoSync(ctx, orgID, sdk.ConnectorID(connectorIDGitHub), sdk.OperationID(githubRepoDiscoveryOp), tenantType)
	}

	// Best-effort async: fetch GitHub org avatar and update org logo without blocking the response.
	goroutine.RunWithDefaultTimeout(ctx, func(bgCtx context.Context) {
		if avatarURL, err := s.ghClient.GetOrgAvatarURL(bgCtx, info.AccountLogin); err != nil {
			logger.Error(bgCtx, "github setup: failed to fetch org avatar for org logo (best-effort)",
				zap.String(logger.OrgID, orgID), zap.Error(err))
		} else if avatarURL != "" && s.branding != nil {
			if err := s.branding.UpdateOrgLogo(bgCtx, orgID, avatarURL); err != nil {
				logger.Error(bgCtx, "github setup: failed to update org logo (best-effort)", zap.String(logger.OrgID, orgID), zap.Error(err))
			}
		}
	})

	return &connintv1.SaveInstallationResponse{}, nil
}

// resolveConnectorToken runs the guards (off switch, single-VCS), builds the PAT
// credential blob, and resolves the account it authenticates as via GET /user.
// It performs NO persist — this is the read-only resolve both ValidateConnectorToken
// and SubmitConnectorToken share. It returns the resolved vendor user and the
// marshaled credential blob (reused by the save step). Only GitHub PAT credentials
// are supported; any other connector_id returns Unimplemented.
func (s *Service) resolveConnectorToken(ctx context.Context, orgID, connectorID, token string) (*vgpb.GetAuthenticatedUserResponse, []byte, error) {
	if orgID == "" || connectorID == "" || token == "" {
		logger.Error(ctx, "resolveConnectorToken: missing required fields", zap.String(logger.OrgID, orgID))
		return nil, nil, status.Error(codes.InvalidArgument, "org_id, connector_id and token are required")
	}
	if connectorID != connectorIDGitHub {
		logger.Error(ctx, "resolveConnectorToken: unsupported connector",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID))
		return nil, nil, status.Errorf(codes.Unimplemented, "token connect is not supported for connector %q", connectorID)
	}

	// Mirror the guards the App path and ConnectIntegration enforce, so a direct
	// or stale client cannot validate/persist a PAT credential for an ops-disabled
	// connector or create a second active VCS. A caller's catalog check covers the
	// allow-list, not the ops off switch, so the off-switch check must live here.
	if !s.connectorEnabled(connectorID) {
		logger.WarnWithCtx(ctx, "resolveConnectorToken: connector ops-disabled",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID))
		return nil, nil, status.Errorf(codes.FailedPrecondition, "connector %q is not enabled", connectorID)
	}
	if err := s.checkSingleVCSGuard(ctx, orgID, sdk.ConnectorID(connectorID)); err != nil {
		return nil, nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	if s.gatewayFactory == nil {
		logger.Error(ctx, "resolveConnectorToken: gateway factory not configured")
		return nil, nil, status.Error(codes.FailedPrecondition, "token connect not available")
	}

	creds, err := json.Marshal(ghconfig.GitHubCredentials{PersonalAccessToken: token})
	if err != nil {
		logger.Error(ctx, "resolveConnectorToken: marshal credentials", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, nil, status.Errorf(codes.Internal, "marshal credentials: %v", err)
	}

	gw, err := s.gatewayFactory(ctx, &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connectorID),
		OrganizationID: orgID,
		Credentials:    creds,
	})
	if err != nil {
		logger.Error(ctx, "resolveConnectorToken: build gateway", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, nil, status.Errorf(codes.Internal, "validate token: %v", err)
	}

	protoResp, err := gw.ForGroup(constants.GroupGitHubREST).Execute(ctx, &vgpb.GetAuthenticatedUserRequest{})
	if err != nil {
		// Distinguish a genuinely bad credential (401/403 → the user must fix the
		// token) from a transient vendor condition (429 / 5xx / timeout → retry).
		// Mapping every failure to InvalidArgument would tell a user their valid
		// token is bad during a GitHub rate-limit or outage.
		logger.WarnWithCtx(ctx, "resolveConnectorToken: get authenticated user failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		// Only a classified auth failure (401/403) means the token is bad. Every
		// other case — a transient vendor condition, or a plain unclassified error
		// like a context timeout — must not tell the user their token is invalid.
		var syncErr *sdk.SyncError
		if errors.As(err, &syncErr) && syncErr.Kind == sdk.ErrKindAuth {
			return nil, nil, status.Error(codes.InvalidArgument, "invalid or unauthorized token")
		}
		return nil, nil, status.Error(codes.Unavailable, "could not reach the vendor to validate the token, please try again")
	}
	user, ok := protoResp.(*vgpb.GetAuthenticatedUserResponse)
	if !ok {
		logger.Error(ctx, "resolveConnectorToken: unexpected response type", zap.String(logger.OrgID, orgID))
		return nil, nil, status.Error(codes.Internal, "validate token: unexpected response")
	}
	if user.GetLogin() == "" || user.GetId() == 0 {
		logger.Error(ctx, "resolveConnectorToken: token resolved to an empty account", zap.String(logger.OrgID, orgID))
		return nil, nil, status.Error(codes.InvalidArgument, "token resolved to an empty account")
	}
	return user, creds, nil
}

// guardExpectedAccount is the PAT path's pre-persist torn-state guard. When
// expectedAccountID is set and the tenant is INDIVIDUAL, it rejects (before any
// persist) if the just-resolved account differs from the account the caller pinned.
// An org has no single owner to guard, and an empty expectedAccountID disables
// the check — so a missing tenant_type cannot wrongly block a save. Resolves
// tenant_type via the tenant directory; fails closed on
// a lookup error rather than persisting blind.
func (s *Service) guardExpectedAccount(ctx context.Context, orgID, expectedAccountID, resolvedAccountID string) error {
	if expectedAccountID == "" {
		return nil
	}
	orgTenantType, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "guardExpectedAccount: resolve tenant type failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return status.Error(codes.Unavailable, "could not resolve tenant type, please try again")
	}
	if commonv1.TenantType(orgTenantType) != commonv1.TenantType_TENANT_TYPE_INDIVIDUAL {
		return nil
	}
	if resolvedAccountID != expectedAccountID {
		logger.WarnWithCtx(ctx, "guardExpectedAccount: re-resolved account differs from pinned account (torn state)",
			zap.String(logger.OrgID, orgID),
			zap.String("expected_account_id", expectedAccountID),
			zap.String("resolved_account_id", resolvedAccountID))
		return errTornStateAccountMismatch("credential resolved to a different account than the one connected") //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	return nil
}

// tokenAccount builds the proto ConnectorAccount the connect RPCs return from a
// resolved vendor user.
func tokenAccount(user *vgpb.GetAuthenticatedUserResponse) *connintv1.ConnectorAccount {
	return &connintv1.ConnectorAccount{
		Handle:              user.GetLogin(),
		AccountId:           fmt.Sprintf("%d", user.GetId()),
		CredentialExpiresAt: user.GetCredentialExpiresAt(),
	}
}

// ValidateConnectorToken validates a pasted connector credential against the
// vendor and returns the account it authenticates as WITHOUT persisting anything
// (the resolve step of pin-before-save). The caller pins the resolved account as the
// individual tenant's owner and only then calls SubmitConnectorToken to save — so
// a different-owner token never persists a credential. Safe to retry: it has no
// side effect.
func (s *Service) ValidateConnectorToken(ctx context.Context, req *connintv1.ValidateConnectorTokenRequest) (*connintv1.ValidateConnectorTokenResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	user, _, err := s.resolveConnectorToken(ctx, orgID, req.GetConnectorId(), req.GetToken())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// Resolve tenant_type so the caller can pin the owner only for an INDIVIDUAL
	// tenant and skip the pin for an organization (enable-on-install). The App
	// path resolves it the same way (resolveInstallation, then the tenant directory). Fail
	// loud rather than defaulting: a wrong tenant_type would either skip an
	// individual's owner pin or wrongly reject an org connect.
	orgTenantType, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "ValidateConnectorToken: resolve tenant type failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Error(codes.Unavailable, "could not resolve tenant type, please try again")
	}
	return &connintv1.ValidateConnectorTokenResponse{
		Account:    tokenAccount(user),
		TenantType: commonv1.TenantType(orgTenantType),
	}, nil
}

// SubmitConnectorToken persists a pasted connector credential the caller has already
// validated (ValidateConnectorToken) and owner-verified (PinIndividualOwner). It
// re-validates the token against the vendor (a second GET /user) before saving —
// the cost traded for never persisting a credential before the owner pin succeeds.
// A first connect saves the config disabled (a later ConfirmConnectorRepos enables
// it); a re-submit preserves the existing enabled state via the (org_id,
// connector_id) upsert, so a retry after a transient save failure is idempotent.
func (s *Service) SubmitConnectorToken(ctx context.Context, req *connintv1.SubmitConnectorTokenRequest) (*connintv1.SubmitConnectorTokenResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	user, creds, err := s.resolveConnectorToken(ctx, orgID, connectorID, req.GetToken())
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// Pre-persist torn-state guard (INDIVIDUAL only): the caller pinned the account
	// ValidateConnectorToken resolved; this save independently re-resolved the
	// token. If the token was swapped between the two resolves, the credential
	// about to persist belongs to a different account than the one pinned — reject
	// BEFORE writing so nothing persists (a clean, retryable drop-off). An org has
	// no single owner to guard, and an empty expected_account_id disables the
	// check, so a missing tenant_type cannot wrongly block a save.
	if err := s.guardExpectedAccount(ctx, orgID, req.GetExpectedAccountId(), tokenAccount(user).GetAccountId()); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	// Persist the PAT credential. organization is intentionally empty — a PAT
	// (single-user) connector lists repos via /user/repos, which never reads
	// settings.organization (validateSettings exempts the PAT path).
	settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
	if err != nil {
		logger.Error(ctx, "SubmitConnectorToken: marshal settings", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "marshal settings: %v", err)
	}
	// Preserve the existing enabled state on a re-submit (e.g. PAT rotation after
	// the connector was already confirmed+enabled). SaveCredentialsPreserveEnabled
	// is a single upsert that never writes `enabled` on conflict, so a concurrent
	// ConfirmConnectorRepos that enables the connector between calls cannot be
	// clobbered back to false. A first connect (no existing row) lands disabled
	// until ConfirmConnectorRepos enables it.
	if err := s.connectorDAO.SaveCredentialsPreserveEnabled(ctx, &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connectorID),
		OrganizationID: orgID,
		Credentials:    creds,
		Settings:       settings,
	}); err != nil {
		logger.Error(ctx, "SubmitConnectorToken: save connector config", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, sdk.ConnectorID(connectorID))

	logger.Info(ctx, "SubmitConnectorToken: token credential saved",
		zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID),
		zap.String("account", user.GetLogin()))

	return &connintv1.SubmitConnectorTokenResponse{Account: tokenAccount(user)}, nil
}

// maxConfirmRepos is a generous absolute ceiling on the repo list this handler
// will persist — a backstop against an unbounded list, NOT a plan
// limit (the caller enforces any plan limit before calling).
const maxConfirmRepos = 500

// ConfirmConnectorRepos is the convergence step both connect paths (App install
// and PAT submit) land on: it records the repos the tenant elected to track and
// flips the connector enabled. Only settings.repositories and the enabled bit
// are written; the credential the connect step persisted is left untouched. The
// caller enforces any plan limit before calling, so an over-limit list never
// reaches this persist.
func (s *Service) ConfirmConnectorRepos(ctx context.Context, req *connintv1.ConfirmConnectorReposRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()
	repoFullNames := req.GetRepoFullNames()
	if orgID == "" || connectorID == "" {
		logger.Error(ctx, "ConfirmConnectorRepos: missing required fields", zap.String(logger.OrgID, orgID))
		return nil, status.Error(codes.InvalidArgument, "org_id and connector_id are required")
	}
	if connectorID != connectorIDGitHub {
		logger.Error(ctx, "ConfirmConnectorRepos: unsupported connector",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.Unimplemented, "repo confirmation is not supported for connector %q", connectorID)
	}
	if len(repoFullNames) > maxConfirmRepos {
		logger.Error(ctx, "ConfirmConnectorRepos: repo list exceeds absolute ceiling",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Int("count", len(repoFullNames)))
		return nil, status.Errorf(codes.InvalidArgument, "too many repositories (max %d)", maxConfirmRepos)
	}

	// Write settings.repositories + enabled=true in one locked UPDATE that never
	// rewrites credentials — unlike a Get-then-Save, which would re-stamp the
	// credentials column with Get-time bytes and clobber a concurrent
	// SaveCredentialsPreserveEnabled PAT rotation.
	mutator := func(current json.RawMessage) (json.RawMessage, error) {
		var settings ghconfig.GitHubSettings
		if len(current) > 0 {
			if err := json.Unmarshal(current, &settings); err != nil {
				return nil, fmt.Errorf("decode connector settings: %w", err)
			}
		}
		settings.Repositories = repoFullNames
		out, err := json.Marshal(settings)
		if err != nil {
			return nil, fmt.Errorf("encode connector settings: %w", err)
		}
		return out, nil
	}
	if err := s.connectorDAO.EnableConnectorWithSettings(ctx, orgID, sdk.ConnectorID(connectorID), mutator); err != nil {
		if connectordao.IsNotFound(err) {
			logger.Error(ctx, "ConfirmConnectorRepos: connector config not found",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
			return nil, status.Errorf(codes.FailedPrecondition, "connector %q is not connected", connectorID)
		}
		logger.Error(ctx, "ConfirmConnectorRepos: enable connector with settings",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}

	logger.Info(ctx, "ConfirmConnectorRepos: connector enabled with confirmed repos",
		zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID),
		zap.Int("repo_count", len(repoFullNames)))

	// This is the enable-moment for individual App-installs and every PAT/token
	// connect (SaveInstallation's ORGANIZATION branch is the only other one), so
	// seed repo discovery now rather than waiting for the next 6-hourly fetch_repos
	// tick. tenant_type drives repo-discovery scope and is not on the request, so
	// resolve it best-effort; an unresolved intent skips the kickoff (cron backstop).
	if tenantType := s.resolveTenantTypeBestEffort(ctx, orgID); tenantType != commonv1.TenantType_TENANT_TYPE_UNSPECIFIED {
		s.kickoffInitialRepoSync(ctx, orgID, sdk.ConnectorID(connectorID), sdk.OperationID(githubRepoDiscoveryOp), tenantType)
	}

	return &emptypb.Empty{}, nil
}

// DisableConnectorConfigsForOrg disables every enabled config of an org and
// returns the count. It is idempotent, so a caller can repeat it.
func (s *Service) DisableConnectorConfigsForOrg(ctx context.Context, req *connintv1.DisableConnectorConfigsForOrgRequest) (*connintv1.DisableConnectorConfigsForOrgResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		logger.Error(ctx, "DisableConnectorConfigsForOrg: missing org_id")
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}
	n, err := s.connectorDAO.DisableConfigsForOrg(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "DisableConnectorConfigsForOrg: disable configs failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "disable connector configs: %v", err)
	}
	logger.Info(ctx, "DisableConnectorConfigsForOrg: disabled connector configs",
		zap.String(logger.OrgID, orgID), zap.Int64("disabled_count", n))
	return &connintv1.DisableConnectorConfigsForOrgResponse{DisabledCount: n}, nil
}

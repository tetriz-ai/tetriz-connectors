// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	intjira "github.com/tetriz-ai/tetriz-connectors/auth/integrations/jira"
	intlinear "github.com/tetriz-ai/tetriz-connectors/auth/integrations/linear"
	bbconfig "github.com/tetriz-ai/tetriz-connectors/connectors/bitbucket"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	linearconfig "github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/scopedstore"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/internal/tokencache"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	connectordao "github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Connector ID literals used across the integrations package for catalog
// lookups, OAuth dispatch, and webhook routing. These mirror each connector
// package's unexported sdk.ConnectorID value.
const (
	connectorIDGitHub    = "github"
	connectorIDBitbucket = "bitbucket"
	connectorIDJira      = "jira"
	connectorIDGitLab    = "gitlab"
	connectorIDLinear    = "linear"
)

// githubRepoDiscoveryOp is GitHub's repo-listing operation (opFetchRepos in the
// github connector). It runs on a 6-hourly cron, so a connector enabled between
// ticks emits no repository records until the next tick. Running it once the
// moment the connector is enabled emits them immediately; the cron stays the backstop.
const githubRepoDiscoveryOp = "fetch_repos"

// initialRepoSyncTimeout bounds the on-connect repo-discovery run. It must match
// the scheduled connector.sync task budget, NOT goroutine.DefaultTimeout (10s):
// fetch_repos paginates the repo list and makes a per-repo GraphQL call, so a
// 10s ceiling would deadline-exceed mid-run for any non-trivial org and never
// emit the repository records — the very case this kickoff exists for.
const initialRepoSyncTimeout = 10 * time.Minute

type connectorInfo struct {
	Enabled     bool
	ConnectedBy string
	// PendingRepoSelection is true for a disabled config whose connect flow has
	// not yet elected any repos (GitHub `settings.repositories` empty) — the
	// individual-accounts resume state. It distinguishes "awaiting repo
	// selection" (Finish setup) from a genuine suspension (expired/revoked auth
	// on a config that already had repos), which a bare !Enabled cannot.
	PendingRepoSelection bool
	// HealthState mirrors sdk.ConnectorConfig.HealthState — an enabled
	// connector whose credential was confirmed dead (auth_revoked) still needs
	// reconnect surfaced to the admin, which a bare Enabled cannot express.
	HealthState sdk.HealthState
}

// Service holds integration business logic — GitHub App management,
// Bitbucket OAuth, GitLab OAuth, integration catalog, and connector config lifecycle.
// This is an internal service with no auth awareness; the caller
// handles all authentication and authorization.
type Service struct {
	connintv1.UnimplementedIntegrationServiceServer
	cfg          *connectorconfig.Config
	connectorDAO connectordao.IConnectorConfigDAO
	// tx, when set, groups store writes that must land together.
	tx           connectordao.TxRunner
	syncStateDAO connectordao.ISyncStateDAO
	syncRunDAO   connectordao.ISyncRunDAO
	ghClient     intgithub.GitHubAppClient
	bbClient     BitbucketOAuthClient
	jiraClient   intjira.OAuthClient
	glClient     GitLabOAuthClient
	linearClient intlinear.OAuthClient
	redis        *redis.Client
	// namespace is the runner's TETRIZ_NAMESPACE: the runner caches access
	// tokens under it, so the token this service forgets must carry it too.
	namespace   string
	sink        sdk.DataSink
	taskCleaner *connectorsource.SyncTaskCleaner
	// tenants resolves an org's tenant intent; branding receives the logo a
	// connect flow learns from the vendor. Both may be nil.
	tenants  sdk.TenantDirectory
	branding BrandingSink
	registry *sdk.ConnectorRegistry
	// settingsHook is told when a VCS setting changes that the host's derived
	// data depends on. Nil skips the notification.
	settingsHook SettingsChangeHook
	// enc encrypts OAuth tokens parked in Redis during the group/workspace picker
	// flow. nil = no encryption (a local run without a key); it mirrors the
	// DAO's credential-encryption contract.
	enc cryptutil.Encryptor
	// The connect-time seams for reading a customer's Kiro bucket — see KiroBucketReader.
	kiroRegionOf    KiroRegionResolver
	kiroReaderFor   KiroReaderFactory
	kiroPrincipalOf KiroPrincipalResolver
	// gatewayFactory builds a one-shot VendorGateway from a ConnectorConfig; nil
	// when vendor credentials are not configured (local/dev), which disables the
	// token-connect path.
	gatewayFactory sdk.VendorGatewayFactory
	// operationRunner runs a connector operation directly (satisfied by the
	// orchestrator), which kicks off repo discovery the moment a connector is
	// enabled rather than waiting for the scheduled fetch_repos cron.
	operationRunner connectorsource.OperationRunner
}

// Deps bundles everything Service needs. A host fills it and passes it to
// NewService.
type Deps struct {
	Cfg *connectorconfig.Config
	// Tx runs every store call in a tenant scope: the organization the call
	// names, or the system scope for a list across organizations. A Postgres
	// host passes store.PostgresTxRunner{}, which row-level security requires;
	// the memory and file stores take store.NoopTxRunner{}.
	Tx           connectordao.TxRunner
	ConnectorDAO connectordao.IConnectorConfigDAO
	SyncStateDAO connectordao.ISyncStateDAO
	SyncRunDAO   connectordao.ISyncRunDAO
	GHClient     intgithub.GitHubAppClient
	BBClient     BitbucketOAuthClient
	JiraClient   intjira.OAuthClient
	GLClient     GitLabOAuthClient
	LinearClient intlinear.OAuthClient
	Redis        *redis.Client
	// Namespace is the TETRIZ_NAMESPACE of the runner sharing Redis; empty
	// for an unprefixed runner.
	Namespace    string
	Sink         sdk.DataSink
	TaskCleaner  *connectorsource.SyncTaskCleaner
	Tenants      sdk.TenantDirectory
	Branding     BrandingSink
	Registry     *sdk.ConnectorRegistry
	SettingsHook SettingsChangeHook
	// Enc encrypts OAuth tokens parked in Redis. Nil stores them unencrypted.
	Enc cryptutil.Encryptor
	// GatewayFactory builds a one-shot VendorGateway from a ConnectorConfig; nil
	// when vendor credentials are not configured (local/dev), disabling the
	// token-connect path.
	GatewayFactory sdk.VendorGatewayFactory
	// The three connect-time seams for reading a customer's Kiro bucket. Each is
	// nil by default and falls back to the real object store (see KiroBucketReader).
	KiroRegionOf    KiroRegionResolver
	KiroReaderFor   KiroReaderFactory
	KiroPrincipalOf KiroPrincipalResolver
}

// NewService creates a new integration Service.
func NewService(deps Deps) *Service {
	configs := scopedstore.Configs(deps.Tx, deps.ConnectorDAO)
	states := scopedstore.States(deps.Tx, deps.SyncStateDAO)
	runs := scopedstore.Runs(deps.Tx, deps.SyncRunDAO)
	orch := runner.NewOrchestrator(deps.Registry, configs, states, runs, deps.Sink, deps.GatewayFactory)
	if deps.Tx != nil {
		orch = orch.WithTxRunner(deps.Tx)
	}
	return &Service{
		cfg:             deps.Cfg,
		kiroRegionOf:    deps.KiroRegionOf,
		kiroReaderFor:   deps.KiroReaderFor,
		kiroPrincipalOf: deps.KiroPrincipalOf,
		connectorDAO:    configs,
		tx:              deps.Tx,
		syncStateDAO:    states,
		syncRunDAO:      runs,
		ghClient:        deps.GHClient,
		bbClient:        deps.BBClient,
		jiraClient:      deps.JiraClient,
		glClient:        deps.GLClient,
		linearClient:    deps.LinearClient,
		redis:           deps.Redis,
		namespace:       deps.Namespace,
		sink:            deps.Sink,
		taskCleaner:     deps.TaskCleaner,
		tenants:         deps.Tenants,
		branding:        deps.Branding,
		registry:        deps.Registry,
		settingsHook:    deps.SettingsHook,
		enc:             deps.Enc,
		gatewayFactory:  deps.GatewayFactory,
		// It shares the same DataSink, so an on-connect run emits exactly as a
		// scheduled run does.
		operationRunner: orch,
	}
}

// connectorEnabled reports whether the given connector's feature flag is on.
// Connectors absent from the EnabledConnectors map default to disabled.
func (s *Service) connectorEnabled(connectorID string) bool {
	return s.cfg.EnabledConnectors[connectorID]
}

// kickoffInitialRepoSync runs a connector's repo-discovery op once, in the
// background, right after the connector is enabled on connect. It emits through
// the same DataSink as a scheduled run, so a consumer receives the repository
// records immediately instead of waiting up to 6h for the next scheduled
// fetch_repos tick. Best-effort: a failure is logged, never surfaced
// to the connect response, and the cron remains the backstop. No-op when no gateway
// factory is configured (local/dev), matching the token-connect path.
//
// Runs under initialRepoSyncTimeout (not the 10s goroutine default) so a large
// org's repo listing completes. It is not deduped against a concurrent scheduled
// run of the same op; that is safe because repo discovery is idempotent (repos
// upsert), so an overlap only wastes vendor calls, never corrupts data.
func (s *Service) kickoffInitialRepoSync(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, tenantType commonv1.TenantType) {
	if s.operationRunner == nil || s.gatewayFactory == nil {
		return
	}
	goroutine.Run(ctx, initialRepoSyncTimeout, func(bgCtx context.Context) {
		if err := s.operationRunner.RunOperation(bgCtx, orgID, connectorID, opID, sdk.TenantKind(tenantType), "on_connect"); err != nil {
			logger.Error(bgCtx, "connector setup: initial repo-discovery kickoff failed (best-effort; scheduled sync is the backstop)",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.ConnectorID, string(connectorID)),
				zap.String(logger.OperationID, string(opID)),
				zap.Error(err))
		}
	})
}

// resolveTenantTypeBestEffort resolves the org's tenant_type via management
// settings (the same source resolveInstallation uses), returning
// TENANT_TYPE_UNSPECIFIED and logging on failure. Repo discovery keys its scope
// on tenant intent, so a best-effort caller that cannot resolve it skips the
// kickoff rather than guessing — the scheduled sync (which resolves intent at
// build time) is the backstop.
func (s *Service) resolveTenantTypeBestEffort(ctx context.Context, orgID string) commonv1.TenantType {
	if s.tenants == nil {
		return commonv1.TenantType_TENANT_TYPE_UNSPECIFIED
	}
	orgTenantType, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.WarnWithCtx(ctx, "connector setup: could not resolve tenant_type for repo-discovery kickoff (skipping; scheduled sync is the backstop)",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return commonv1.TenantType_TENANT_TYPE_UNSPECIFIED
	}
	return commonv1.TenantType(orgTenantType)
}

// resetConnectorHealth runs after a fresh, successful credential write. It
// forgets the connector's cached access token, so the next request uses the new
// credentials rather than the replaced account's token, and clears an
// auth_revoked health-state gate (see sdk.HealthState), which otherwise keeps
// the orchestrator skipping this connector's scheduled syncs. Best-effort: a
// failure here is logged, not returned, since the credential write it follows
// already succeeded.
func (s *Service) resetConnectorHealth(ctx context.Context, orgID string, connectorID sdk.ConnectorID) {
	// The cache key and the store both name the organization canonically.
	orgID = orgid.Canonical(orgID)
	if s.redis != nil {
		if err := tokencache.ForgetIn(ctx, s.redis, s.namespace, connectorID, orgID); err != nil {
			logger.Error(ctx, "failed to forget the cached access token after a credential write",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		}
	}
	transitioned, err := s.connectorDAO.ResetHealthState(ctx, orgID, connectorID)
	if err != nil {
		// The counter makes this loss visible in the metrics; the log below is its durable record.
		obs.HealthWriteFailuresTotal.WithLabelValues(tenantlabel.OrgID(orgID), string(connectorID), obs.HealthWriteReset).Inc()
		logger.Error(ctx, "failed to reset connector health state",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		return
	}
	if transitioned {
		obs.HealthStateTransitionsTotal.WithLabelValues(
			tenantlabel.OrgID(orgID), string(connectorID), string(sdk.HealthStateHealthy),
		).Inc()
		logger.Info(ctx, "connector health state transitioned to healthy",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)))
	}
}

// encryptSelectionBlob encrypts a marshaled picker-session blob (which carries
// the customer's OAuth access/refresh tokens) before it is parked in Redis.
// With no encryptor configured (local/dev) the plaintext passes through, matching
// the connector DAO's credential-encryption contract. A failure must abort the
// flow — never store plaintext on the encrypt path.
func (s *Service) encryptSelectionBlob(blob []byte) ([]byte, error) {
	if s.enc == nil {
		return blob, nil
	}
	ciphertext, err := s.enc.Encrypt(blob)
	if err != nil {
		return nil, fmt.Errorf("encrypting selection blob: %w", err)
	}
	return ciphertext, nil
}

// decryptSelectionBlob reverses encryptSelectionBlob. With an encryptor, a
// blob that is not an envelope is refused rather than passed through: the
// encrypt path never parks plaintext, so a plaintext blob in Redis was not
// written by this flow and its tokens are not used.
func (s *Service) decryptSelectionBlob(blob []byte) ([]byte, error) {
	if s.enc == nil {
		return blob, nil
	}
	if !cryptutil.IsEnvelope(blob) {
		return nil, errors.New("decrypting selection blob: the stored session is not encrypted")
	}
	plaintext, err := s.enc.Decrypt(blob)
	if err != nil {
		return nil, fmt.Errorf("decrypting selection blob: %w", err)
	}
	return plaintext, nil
}

// ConnectIntegration handles connector activation. OAuth-based connectors
// (e.g. GitHub) return a redirect URL.
func (s *Service) ConnectIntegration(ctx context.Context, req *connintv1.ConnectIntegrationRequest) (*connintv1.ConnectIntegrationResponse, error) {
	connectorID := req.GetConnectorId()

	// Off-switch guard: reject any connector turned off via EnabledConnectors
	// before starting a connect/OAuth flow. The catalog already renders these as
	// OPS_DISABLED; enforcing it here makes the connect RPC consistent for ALL
	// connectors (not just Slack), so a direct API call or a stale client bundle
	// can't initiate — and corrupt — a connection for a disabled connector.
	if !s.connectorEnabled(connectorID) {
		logger.WarnWithCtx(ctx, "ConnectIntegration: connector not enabled", zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.FailedPrecondition, "connector %q is not enabled", connectorID)
	}

	// Guard: only one VCS connector (GitHub, GitLab, Bitbucket) per org.
	// Check early so the user gets immediate feedback before starting an OAuth flow.
	if s.isVCSConnector(sdk.ConnectorID(connectorID)) {
		if err := s.checkSingleVCSGuard(ctx, orgid.Canonical(req.GetOrgId()), sdk.ConnectorID(connectorID)); err != nil {
			return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
		}
	}

	switch connectorID {
	case connectorIDGitHub:
		orgID := orgid.Canonical(req.GetOrgId())
		if orgID == "" {
			return nil, status.Error(codes.InvalidArgument, "org_id is required")
		}
		url, err := s.githubInstallURL(ctx, orgID)
		if err != nil {
			return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
		}
		return &connintv1.ConnectIntegrationResponse{RedirectUrl: url}, nil

	case connectorIDSlackComms:
		return s.connectSlackComms(ctx, req) //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()

	case connectorIDBitbucket:
		return s.connectBitbucket(ctx, req) //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()

	case connectorIDJira:
		return s.connectJira(ctx, req) //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()

	case connectorIDGitLab:
		return s.connectGitLab(ctx, req) //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()

	case connectorIDLinear:
		return s.connectLinear(ctx, req) //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()

	case "github-issues":
		orgID := orgid.Canonical(req.GetOrgId())
		_, err := s.connectorDAO.Get(ctx, orgID, connectorIDGitHub)
		if err != nil {
			// GitHub VCS not connected — redirect to GitHub App install flow.
			logger.Info(ctx, "ConnectIntegration: github not connected, redirecting to install flow",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			url, urlErr := s.githubInstallURL(ctx, orgID)
			if urlErr != nil {
				return nil, urlErr //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
			}
			return &connintv1.ConnectIntegrationResponse{RedirectUrl: url}, nil
		}
		// GitHub is already connected — just flip the issues flag in its settings.
		if err := s.setGitHubIssuesFlag(ctx, "ConnectIntegration", orgID, true); err != nil {
			logger.Error(ctx, "ConnectIntegration: failed to enable github issues",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return nil, status.Errorf(codes.Internal, "enable github issues: %v", err)
		}
		return &connintv1.ConnectIntegrationResponse{}, nil

	default:
		logger.WarnWithCtx(ctx, "ConnectIntegration: unsupported connector requested",
			zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.InvalidArgument, "unsupported connector: %s", connectorID)
	}
}

// setGitHubIssuesFlag writes only the github_issues_enabled key of the GitHub
// connector's settings, under the row lock PatchSettings holds, so a
// concurrent credential write or another settings key written meanwhile
// survives. Unparseable stored settings start fresh, as before.
func (s *Service) setGitHubIssuesFlag(ctx context.Context, caller, orgID string, enabled bool) error {
	return s.connectorDAO.PatchSettings(ctx, orgID, connectorIDGitHub, func(current json.RawMessage) (json.RawMessage, error) { //nolint:wrapcheck // the caller wraps it
		settingsMap := map[string]interface{}{}
		if len(current) > 0 {
			if err := json.Unmarshal(current, &settingsMap); err != nil || settingsMap == nil {
				logger.WarnWithCtx(ctx, caller+": github settings unparseable, starting fresh",
					zap.String(logger.OrgID, orgID), zap.Error(err))
				settingsMap = map[string]interface{}{}
			}
		}
		settingsMap["github_issues_enabled"] = enabled
		out, err := json.Marshal(settingsMap)
		if err != nil {
			return nil, fmt.Errorf("marshal github settings: %w", err)
		}
		return out, nil
	})
}

// DisconnectIntegration loads the connector config, best-effort revokes the
// GitHub App installation, and soft-deletes the connector config from the DB.
func (s *Service) DisconnectIntegration(ctx context.Context, req *connintv1.DisconnectIntegrationRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	// github-issues is a flag on the GitHub VCS connector, not a standalone record.
	// Disabling it only clears the flag — the GitHub connector itself stays connected.
	if connectorID == "github-issues" {
		_, err := s.connectorDAO.Get(ctx, orgID, connectorIDGitHub)
		if err != nil {
			// GitHub not connected at all — nothing to disable.
			logger.Info(ctx, "DisconnectIntegration: github not connected, nothing to disable for github-issues",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return &emptypb.Empty{}, nil
		}
		if err := s.setGitHubIssuesFlag(ctx, "DisconnectIntegration", orgID, false); err != nil {
			logger.Error(ctx, "DisconnectIntegration: failed to disable github issues",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return nil, status.Errorf(codes.Internal, "disable github issues: %v", err)
		}
		return &emptypb.Empty{}, nil
	}

	cfg, err := s.connectorDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "DisconnectIntegration: connector config not found",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "connector not found: %s", connectorID)
	}

	if sdk.ConnectorID(connectorID) == connectorIDGitHub && s.ghClient != nil && cfg.Credentials != nil {
		var creds ghconfig.GitHubCredentials
		if jsonErr := json.Unmarshal(cfg.Credentials, &creds); jsonErr != nil {
			logger.WarnWithCtx(ctx, "DisconnectIntegration: github credentials unparseable, skipping installation revocation",
				zap.String(logger.OrgID, orgID), zap.Error(jsonErr))
		} else if creds.InstallationID != "" {
			if delErr := s.ghClient.DeleteInstallation(ctx, creds.InstallationID); delErr != nil {
				logger.Error(ctx, "best-effort GitHub installation revocation failed",
					zap.String("installation_id", creds.InstallationID),
					zap.Error(delErr))
			}
		}
	}

	// Jira: best-effort revoke refresh token, clear Redis cache, wipe credentials.
	if sdk.ConnectorID(connectorID) == connectorIDJira {
		s.bestEffortRevokeJiraToken(ctx, orgID, cfg.Credentials)

		// Only the cache clear needs Redis; the revoke and the wipe always run.
		if s.redis != nil {
			if delErr := s.redis.Del(ctx, schedule.Namespaced(s.namespace, jiraconfig.AccessTokenKey(orgID))).Err(); delErr != nil {
				logger.Error(ctx, "DisconnectIntegration: failed to clear jira access token cache",
					zap.String(logger.OrgID, orgID), zap.Error(delErr))
			}
		}

		s.wipeCredentials(ctx, cfg, "DisconnectIntegration: failed to clear jira credentials")
	}

	// Bitbucket: clear cached access token from Redis and wipe credentials
	// before soft-delete. There's no Bitbucket API to revoke OAuth access.
	if sdk.ConnectorID(connectorID) == connectorIDBitbucket {
		// Only the cache clear needs Redis; the revoke and the wipe always run.
		if s.redis != nil {
			if delErr := s.redis.Del(ctx, schedule.Namespaced(s.namespace, bbconfig.AccessTokenKey(orgID))).Err(); delErr != nil {
				logger.Error(ctx, "DisconnectIntegration: failed to clear bitbucket access token cache",
					zap.String(logger.OrgID, orgID), zap.Error(delErr))
			}
		}

		s.wipeCredentials(ctx, cfg, "DisconnectIntegration: failed to clear bitbucket credentials")
	}

	if sdk.ConnectorID(connectorID) == connectorIDGitLab {
		// Load settings to get group_id and hook_id.
		var settings glconfig.GitLabSettings
		var creds glconfig.GitLabCredentials
		settingsErr := json.Unmarshal(cfg.Settings, &settings)
		credsErr := json.Unmarshal(cfg.Credentials, &creds)
		if settingsErr != nil {
			logger.WarnWithCtx(ctx, "gitlab disconnect: settings unparseable, skipping webhook cleanup (best-effort)",
				zap.String(logger.OrgID, orgID), zap.Error(settingsErr))
		}
		if credsErr != nil {
			logger.WarnWithCtx(ctx, "gitlab disconnect: credentials unparseable, skipping webhook/token cleanup (best-effort)",
				zap.String(logger.OrgID, orgID), zap.Error(credsErr))
		}
		if settingsErr == nil && credsErr == nil {
			// Best-effort delete webhook (needs a live access token).
			// We use the refresh token to get one if possible.
			if settings.WebhookHookID != 0 && s.glClient != nil && creds.OAuthRefreshToken != "" {
				if accessToken, tokenErr := s.glClient.ExchangeRefreshToken(ctx, creds.OAuthRefreshToken); tokenErr != nil {
					logger.WarnWithCtx(ctx, "gitlab disconnect: refresh-token exchange failed, skipping webhook delete (best-effort)",
						zap.String(logger.OrgID, orgID), zap.Error(tokenErr))
				} else if delErr := s.glClient.DeleteGroupWebhook(ctx, accessToken, settings.GroupID, settings.WebhookHookID); delErr != nil {
					logger.Error(ctx, "gitlab disconnect: webhook cleanup failed (best-effort)",
						zap.String(logger.OrgID, orgID), zap.Error(delErr))
				}
			}
			// Best-effort revoke refresh token.
			if creds.OAuthRefreshToken != "" && s.glClient != nil {
				if revokeErr := s.glClient.RevokeToken(ctx, creds.OAuthRefreshToken); revokeErr != nil {
					logger.Error(ctx, "gitlab disconnect: token revocation failed (best-effort)",
						zap.String(logger.OrgID, orgID), zap.Error(revokeErr))
				}
			}
		}
		// Clear cached access token.
		// Only the cache clear needs Redis; the revoke and the wipe always run.
		if s.redis != nil {
			if delErr := s.redis.Del(ctx, schedule.Namespaced(s.namespace, glconfig.AccessTokenKey(orgID))).Err(); delErr != nil {
				logger.Error(ctx, "DisconnectIntegration: failed to clear gitlab access token cache",
					zap.String(logger.OrgID, orgID), zap.Error(delErr))
			}
		}
		// Wipe credentials.
		s.wipeCredentials(ctx, cfg, "DisconnectIntegration: failed to clear gitlab credentials")
	}

	// Linear: best-effort revoke token, wipe credentials.
	if sdk.ConnectorID(connectorID) == connectorIDLinear && s.linearClient != nil {
		var creds linearconfig.LinearCredentials
		if credsErr := json.Unmarshal(cfg.Credentials, &creds); credsErr != nil {
			// Can't parse stored creds — skip revoke (best-effort); the wipe below
			// still disables the config. Log so a recurring failure is visible.
			logger.WarnWithCtx(ctx, "linear disconnect: unmarshal credentials failed, skipping token revoke",
				zap.String(logger.OrgID, orgID), zap.Error(credsErr))
		} else {
			if creds.AccessToken != "" {
				if revokeErr := s.linearClient.RevokeToken(ctx, creds.AccessToken); revokeErr != nil {
					logger.Error(ctx, "linear disconnect: token revocation failed (best-effort)",
						zap.String(logger.OrgID, orgID), zap.Error(revokeErr))
				}
			}
			// Auth-code grants also issue a refresh token; revoke it too, else it
			// stays live in Linear after disconnect and could mint new access
			// tokens if recovered from a leak/backup.
			if creds.RefreshToken != "" {
				if revokeErr := s.linearClient.RevokeToken(ctx, creds.RefreshToken); revokeErr != nil {
					logger.Error(ctx, "linear disconnect: refresh-token revocation failed (best-effort)",
						zap.String(logger.OrgID, orgID), zap.Error(revokeErr))
				}
			}
		}
		s.wipeCredentials(ctx, cfg, "linear disconnect: failed to wipe credentials")
		// Purge the cached access token so it does not linger in Redis (up to its
		// TTL) after the org disconnects.
		if s.redis != nil {
			if delErr := s.redis.Del(ctx, schedule.Namespaced(s.namespace, linearconfig.AccessTokenKey(orgID))).Err(); delErr != nil {
				logger.Error(ctx, "linear disconnect: failed to purge cached token (best-effort)",
					zap.String(logger.OrgID, orgID), zap.Error(delErr))
			}
		}
	}

	// Clean up sync state (cursors) and sync runs so a future reconnect starts fresh.
	s.cleanupSyncState(ctx, orgID, sdk.ConnectorID(connectorID))

	if err := s.connectorDAO.Delete(ctx, orgID, sdk.ConnectorID(connectorID)); err != nil {
		logger.Error(ctx, "DisconnectIntegration: failed to delete connector config",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to delete connector: %v", err)
	}
	// Only a real removal reaches here — the github-issues branch returns
	// before the config delete, so it is not counted.
	obs.DisconnectsTotal.WithLabelValues(tenantlabel.OrgID(orgID), connectorID).Inc()
	// Reset only after Delete succeeds: resetting first would leave a row that
	// failed to soft-delete both live AND healthy, resuming scheduled syncs with
	// credentials that (for some vendors) were already wiped earlier in this flow.
	s.resetConnectorHealth(ctx, orgID, sdk.ConnectorID(connectorID))

	return &emptypb.Empty{}, nil
}

// wipeCredentials clears a disconnecting connector's stored credentials before
// the soft delete. It writes only the credentials, and only while the row still
// carries the credentials the disconnect read, so it never rewrites settings,
// enabled or overrides and never revives a row another disconnect removed.
func (s *Service) wipeCredentials(ctx context.Context, cfg *sdk.ConnectorConfig, failMsg string) {
	swapped, err := s.connectorDAO.CompareAndSwapCredentials(ctx, cfg.OrganizationID, cfg.ConnectorID, cfg.CredentialsUpdatedAt, json.RawMessage("{}"))
	if err != nil {
		logger.Error(ctx, failMsg, zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
		return
	}
	if !swapped {
		logger.Info(ctx, "DisconnectIntegration: credentials changed since the read; leaving them to the soft delete",
			zap.String(logger.OrgID, cfg.OrganizationID), zap.String(logger.ConnectorID, string(cfg.ConnectorID)))
	}
}

// cleanupSyncState removes all cursor state, sync run history, and queued asynq
// tasks for a connector. This ensures a reconnect triggers a fresh full sync
// instead of resuming from stale cursors or being blocked by stuck retry tasks.
// Failures are logged but do not block the disconnect — best-effort cleanup.
func (s *Service) cleanupSyncState(ctx context.Context, orgID string, connectorID sdk.ConnectorID) {
	if s.syncStateDAO != nil {
		if err := s.syncStateDAO.DeleteByConnector(ctx, orgID, connectorID); err != nil {
			logger.Error(ctx, "cleanupSyncState: failed to delete cursors",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		}
	}
	if s.syncRunDAO != nil {
		if err := s.syncRunDAO.DeleteRunsByConnector(ctx, orgID, connectorID); err != nil {
			logger.Error(ctx, "cleanupSyncState: failed to delete sync runs",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		}
	}
	if s.taskCleaner != nil {
		s.taskCleaner.DeleteTasksForConnector(ctx, orgID, connectorID)
	}
}

// GetIntegrations returns the full integration catalog with real connector status
// overlaid from the connector_configs table.
func (s *Service) GetIntegrations(ctx context.Context, req *connintv1.GetIntegrationsRequest) (*connintv1.GetIntegrationsResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	configs, err := s.connectorDAO.ListByOrg(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "GetIntegrations: failed to list connectors",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "list connectors: %v", err)
	}

	// PENDING_REPO_SELECTION is an INDIVIDUAL-tenant resume state: only the
	// individual connect flow saves a credential disabled-until-ConfirmConnectorRepos.
	// An ORG's disabled GitHub App config ALSO has no elected repos (orgs leave
	// settings.repositories empty and auto-discover), but it is a genuine suspension
	// (revoked/expired auth) and must route to reconnect — never to "Finish setup",
	// which ConfirmConnectorRepos rejects for an org (no repo picker). So the
	// pending classification is gated on tenant_type=INDIVIDUAL. tenant_type is
	// resolved once via the tenant directory; a lookup
	// failure fails closed to NOT pending, so a transient blip can never route an
	// org to the broken Finish-setup path. Resolved lazily — only when a disabled
	// GitHub config that could be pending actually exists — so the common
	// all-enabled catalog read pays nothing.
	tenantIsIndividual := s.resolveIsIndividualForPending(ctx, orgID, configs)

	connectedMap := make(map[string]connectorInfo, len(configs))
	for _, c := range configs {
		info := connectorInfo{Enabled: c.Enabled, HealthState: c.HealthState}
		info.ConnectedBy = extractConnectedBy(c)
		// A disabled GitHub config with no elected repos is mid-connect (resume at
		// the repo picker), not suspended — but only for an INDIVIDUAL tenant (see
		// the tenantIsIndividual resolution above).
		if tenantIsIndividual && !c.Enabled && c.ConnectorID == connectorIDGitHub && !hasElectedRepos(c) {
			info.PendingRepoSelection = true
		}
		connectedMap[string(c.ConnectorID)] = info
	}

	resp := staticIntegrationsResponse()
	for _, cat := range resp.GetCategories() {
		for _, integration := range cat.GetIntegrations() {
			if info, found := connectedMap[integration.GetId()]; found {
				switch {
				// An enabled connector whose credential was confirmed dead still needs
				// reconnect surfaced — SUSPENDED already renders the "Auth expired" /
				// Reconnect affordance for the !Enabled case, so route this one there too
				// rather than reporting CONNECTED on a connector that cannot sync.
				case info.Enabled && info.HealthState == sdk.HealthStateAuthRevoked:
					integration.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED
				case info.Enabled:
					integration.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED
				case info.PendingRepoSelection:
					integration.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_PENDING_REPO_SELECTION
				default:
					integration.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED
				}
				integration.ConnectedBy = info.ConnectedBy
			}
		}
	}

	s.linkGitHubIssues(ctx, resp, orgID, connectedMap)

	s.applyFeatureFlags(resp)
	return resp, nil
}

// resolveIsIndividualForPending reports whether the tenant is INDIVIDUAL, but
// ONLY does the tenant directory lookup when a config that could be classified
// PENDING_REPO_SELECTION exists (a disabled GitHub config with no elected repos).
// With no such candidate the answer is irrelevant, so it returns false without a
// network call — keeping the common all-enabled catalog read free of an extra RPC.
// A lookup failure returns false (fail closed): the candidate config then falls
// through to SUSPENDED, which is correct for an org and a safe, retryable state
// for an individual — never the org-breaking pending route.
func (s *Service) resolveIsIndividualForPending(ctx context.Context, orgID string, configs []*sdk.ConnectorConfig) bool {
	hasPendingCandidate := false
	for _, c := range configs {
		if !c.Enabled && c.ConnectorID == connectorIDGitHub && !hasElectedRepos(c) {
			hasPendingCandidate = true
			break
		}
	}
	if !hasPendingCandidate {
		return false
	}
	orgTenantType, err := s.tenants.TenantType(ctx, orgID)
	if err != nil {
		logger.WarnWithCtx(ctx, "GetIntegrations: resolve tenant type for pending classification failed; treating as not pending",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return false
	}
	return commonv1.TenantType(orgTenantType) == commonv1.TenantType_TENANT_TYPE_INDIVIDUAL
}

// findIntegration locates an integration by ID across all categories in the response.
func findIntegration(resp *connintv1.GetIntegrationsResponse, id string) *connintv1.Integration {
	for _, cat := range resp.GetCategories() {
		for _, intg := range cat.GetIntegrations() {
			if intg.GetId() == id {
				return intg
			}
		}
	}
	return nil
}

// linkGitHubIssues sets the linked connector and status for the github-issues
// virtual integration, which piggybacks on the github connector's connection.
func (s *Service) linkGitHubIssues(ctx context.Context, resp *connintv1.GetIntegrationsResponse, orgID string, connectedMap map[string]connectorInfo) {
	intg := findIntegration(resp, "github-issues")
	if intg == nil {
		return
	}
	intg.ParentConnectorId = connectorIDGitHub

	githubInfo, ok := connectedMap[connectorIDGitHub]
	if !ok || !githubInfo.Enabled {
		return
	}

	githubCfg, err := s.connectorDAO.Get(ctx, orgID, connectorIDGitHub)
	if err != nil {
		// connectedMap reports github enabled but its config load failed —
		// leave github-issues at its default status.
		logger.WarnWithCtx(ctx, "linkGitHubIssues: failed to load github config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	var settings ghconfig.GitHubSettings
	if err := json.Unmarshal(githubCfg.Settings, &settings); err != nil {
		logger.WarnWithCtx(ctx, "linkGitHubIssues: github settings unparseable, leaving default status",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	if !settings.GitHubIssuesEnabled {
		return
	}
	// github-issues piggybacks on the GitHub connection for data — if GitHub's
	// credential is confirmed dead, github-issues cannot work either, so it
	// must mirror the parent's SUSPENDED status rather than claim CONNECTED.
	if githubInfo.HealthState == sdk.HealthStateAuthRevoked {
		intg.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_SUSPENDED
		return
	}
	intg.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_CONNECTED
}

// applyFeatureFlags overrides catalog statuses for integrations whose
// EnabledConnectors off switch is off, forcing them to OPS_DISABLED.
// OPS_DISABLED is distinct from COMING_SOON: it marks a built connector
// that has been turned off operationally, so a status override downstream
// cannot accidentally resurface it.
func (s *Service) applyFeatureFlags(resp *connintv1.GetIntegrationsResponse) {
	for _, cat := range resp.GetCategories() {
		for _, intg := range cat.GetIntegrations() {
			if intg.GetStatus() == connintv1.IntegrationStatus_INTEGRATION_STATUS_COMING_SOON {
				continue
			}
			if !s.connectorEnabled(intg.GetId()) {
				intg.Status = connintv1.IntegrationStatus_INTEGRATION_STATUS_OPS_DISABLED
				intg.ConnectedBy = ""
			}
		}
	}
}

// extractConnectedBy returns the display name of the user who connected
// the integration. Each connector stores this differently in its settings JSON.
func extractConnectedBy(cfg *sdk.ConnectorConfig) string {
	if cfg.Settings == nil {
		return ""
	}
	switch cfg.ConnectorID {
	case connectorIDBitbucket:
		var s bbconfig.BitbucketSettings
		if err := json.Unmarshal(cfg.Settings, &s); err != nil {
			logger.Warn("extractConnectedBy: bitbucket settings unparseable", zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			return ""
		}
		return s.AuthorizingUserDisplayName
	case connectorIDJira:
		var s jiraconfig.JiraSettings
		if err := json.Unmarshal(cfg.Settings, &s); err != nil {
			logger.Warn("extractConnectedBy: jira settings unparseable", zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			return ""
		}
		return s.ConnectedByName
	case connectorIDGitLab:
		var s glconfig.GitLabSettings
		if err := json.Unmarshal(cfg.Settings, &s); err != nil {
			logger.Warn("extractConnectedBy: gitlab settings unparseable", zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			return ""
		}
		return s.AuthorizingUserDisplayName
	case connectorIDLinear:
		var s linearconfig.LinearSettings
		if err := json.Unmarshal(cfg.Settings, &s); err != nil {
			logger.Warn("extractConnectedBy: linear settings unparseable", zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			return ""
		}
		return s.ConnectedByName
	}
	return ""
}

// hasElectedRepos reports whether a GitHub config has any repositories elected
// in settings.repositories — the set ConfirmConnectorRepos writes when the
// connect flow completes. A disabled config with no elected repos is mid-connect
// (PendingRepoSelection); one with elected repos that is disabled was suspended
// after connecting. Unparseable settings degrade to false (treat as no repos).
func hasElectedRepos(cfg *sdk.ConnectorConfig) bool {
	if cfg == nil || len(cfg.Settings) == 0 {
		return false
	}
	var settings ghconfig.GitHubSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		logger.Warn("hasElectedRepos: github settings unparseable", zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
		return false
	}
	return len(settings.Repositories) > 0
}

// bestEffortRevokeJiraToken revokes the Jira OAuth refresh token. Errors are logged but never returned.
func (s *Service) bestEffortRevokeJiraToken(ctx context.Context, orgID string, rawCreds json.RawMessage) {
	if s.jiraClient == nil || rawCreds == nil {
		return
	}
	var creds jiraconfig.JiraCredentials
	if err := json.Unmarshal(rawCreds, &creds); err != nil {
		logger.Error(ctx, "disconnect: failed to unmarshal jira credentials",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	if creds.OAuthRefreshToken == "" {
		return
	}
	if err := s.jiraClient.RevokeToken(ctx, creds.OAuthRefreshToken); err != nil {
		logger.Error(ctx, "best-effort Jira token revocation failed",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	logger.Info(ctx, "Jira refresh token revoked", zap.String(logger.OrgID, orgID))
}

// GetVCSSettings returns the VCS-specific settings for a connector (tracked_branch).
func (s *Service) GetVCSSettings(ctx context.Context, req *connintv1.GetVCSSettingsRequest) (*connintv1.GetVCSSettingsResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	if !s.isVCSConnector(sdk.ConnectorID(connectorID)) {
		logger.WarnWithCtx(ctx, "not a VCS connector", zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a VCS connector", connectorID)
	}

	cfg, err := s.connectorDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "GetVCSSettings: connector config not found",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "connector not found: %s", connectorID)
	}

	trackedBranch := extractTrackedBranch(cfg.Settings)
	overrides := extractRepoBranchOverrides(cfg.Settings)
	excludePatterns, repoExcludeOverrides := extractFileExcludeSettings(cfg.Settings)
	trackAll, repoAll := extractAllBranchSettings(cfg.Settings)
	return &connintv1.GetVCSSettingsResponse{
		Settings: &connintv1.VCSSettings{
			TrackedBranch:            trackedBranch,
			RepoBranchOverrides:      overrides,
			FileExcludePatterns:      excludePatterns,
			RepoFileExcludeOverrides: repoExcludeOverrides,
			TrackAllBranches:         trackAll,
			RepoAllBranches:          repoAll,
		},
	}, nil
}

// extractFileExcludeSettings reads admin-configurable file-exclude patterns
// from the connector settings JSON. Unset → empty slice / nil map.
func extractFileExcludeSettings(raw json.RawMessage) ([]string, map[string]*connintv1.RepoFileExcludePatterns) {
	var settings struct {
		FileExcludePatterns      []string            `json:"file_exclude_patterns,omitempty"`
		RepoFileExcludeOverrides map[string][]string `json:"repo_file_exclude_overrides,omitempty"`
	}
	if len(raw) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(raw, &settings); err != nil {
		logger.Warn("extractFileExcludeSettings: settings unparseable, treating as no exclude patterns", zap.Error(err))
		return nil, nil
	}
	if len(settings.RepoFileExcludeOverrides) == 0 {
		return settings.FileExcludePatterns, nil
	}
	wrapped := make(map[string]*connintv1.RepoFileExcludePatterns, len(settings.RepoFileExcludeOverrides))
	for repo, patterns := range settings.RepoFileExcludeOverrides {
		wrapped[repo] = &connintv1.RepoFileExcludePatterns{Patterns: patterns}
	}
	return settings.FileExcludePatterns, wrapped
}

// UpdateVCSSettings updates the VCS-specific settings (tracked branch + branch
// scope + file-exclude patterns). A branch-scope change triggers a PR-count
// reprocess (no vendor calls); a file-exclude change triggers a diff-analysis
// reset (orthogonal). Both are non-fatal — the settings save is what must
// succeed.
func (s *Service) UpdateVCSSettings(ctx context.Context, req *connintv1.UpdateVCSSettingsRequest) (*emptypb.Empty, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	if !s.isVCSConnector(sdk.ConnectorID(connectorID)) {
		logger.WarnWithCtx(ctx, "not a VCS connector", zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a VCS connector", connectorID)
	}

	cfg, err := s.connectorDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "UpdateVCSSettings: connector config not found",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "connector not found: %s", connectorID)
	}

	oldTrackedBranch := extractTrackedBranch(cfg.Settings)
	newTrackedBranch := req.GetSettings().GetTrackedBranch()
	oldTrackAll, oldRepoAll := extractAllBranchSettings(cfg.Settings)
	newTrackAll := req.GetSettings().GetTrackAllBranches()
	newRepoAll := req.GetSettings().GetRepoAllBranches()
	oldExcludePatterns, oldRepoExcludes := extractFileExcludeSettings(cfg.Settings)
	newExcludePatterns := req.GetSettings().GetFileExcludePatterns()
	newRepoExcludes := req.GetSettings().GetRepoFileExcludeOverrides()

	newOverrides := req.GetSettings().GetRepoBranchOverrides()

	// The VCS keys this request owns, applied to the stored settings under
	// PatchSettings' row lock: every other key, and the credentials, enabled
	// flag and overrides, are left as they are.
	applyVCSSettings := func(current json.RawMessage) (json.RawMessage, error) {
		var settingsMap map[string]json.RawMessage
		if err := json.Unmarshal(current, &settingsMap); err != nil {
			logger.WarnWithCtx(ctx, "UpdateVCSSettings: existing settings unparseable, starting fresh",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
			settingsMap = make(map[string]json.RawMessage)
		}
		if newTrackedBranch == "" {
			delete(settingsMap, "tracked_branch")
		} else {
			val, err := json.Marshal(newTrackedBranch)
			if err != nil {
				return nil, fmt.Errorf("marshal tracked_branch: %w", err)
			}
			settingsMap["tracked_branch"] = val
		}
		if len(newOverrides) > 0 {
			val, err := json.Marshal(newOverrides)
			if err != nil {
				return nil, fmt.Errorf("marshal repo_branch_overrides: %w", err)
			}
			settingsMap["repo_branch_overrides"] = val
		} else {
			delete(settingsMap, "repo_branch_overrides")
		}
		if newTrackAll {
			settingsMap["track_all_branches"] = json.RawMessage("true")
		} else {
			delete(settingsMap, "track_all_branches")
		}
		if len(newRepoAll) > 0 {
			val, err := json.Marshal(newRepoAll)
			if err != nil {
				return nil, fmt.Errorf("marshal repo_all_branches: %w", err)
			}
			settingsMap["repo_all_branches"] = val
		} else {
			delete(settingsMap, "repo_all_branches")
		}
		if len(newExcludePatterns) > 0 {
			val, err := json.Marshal(newExcludePatterns)
			if err != nil {
				return nil, fmt.Errorf("marshal file_exclude_patterns: %w", err)
			}
			settingsMap["file_exclude_patterns"] = val
		} else {
			delete(settingsMap, "file_exclude_patterns")
		}
		if len(newRepoExcludes) > 0 {
			unwrapped := make(map[string][]string, len(newRepoExcludes))
			for repo, wrapped := range newRepoExcludes {
				unwrapped[repo] = wrapped.GetPatterns()
			}
			val, err := json.Marshal(unwrapped)
			if err != nil {
				return nil, fmt.Errorf("marshal repo_file_exclude_overrides: %w", err)
			}
			settingsMap["repo_file_exclude_overrides"] = val
		} else {
			delete(settingsMap, "repo_file_exclude_overrides")
		}
		updatedSettings, err := json.Marshal(settingsMap)
		if err != nil {
			return nil, fmt.Errorf("marshal settings: %w", err)
		}
		return updatedSettings, nil
	}

	// Validate each override branch exists on GitHub.
	if connectorID == connectorIDGitHub && s.ghClient != nil && len(newOverrides) > 0 {
		var creds ghconfig.GitHubCredentials
		if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
			logger.WarnWithCtx(ctx, "UpdateVCSSettings: github credentials unparseable, skipping branch validation",
				zap.String(logger.OrgID, orgID), zap.Error(err))
		} else {
			for repoFullName, branch := range newOverrides {
				parts := strings.SplitN(repoFullName, "/", 2)
				if len(parts) != 2 {
					logger.Info(ctx, "UpdateVCSSettings: skipping invalid repo name in overrides",
						zap.String("repo", repoFullName))
					continue
				}
				exists, err := s.ghClient.BranchExists(ctx, creds.InstallationID, parts[0], parts[1], branch)
				if err != nil {
					logger.Error(ctx, "UpdateVCSSettings: branch validation failed",
						zap.String("repo", repoFullName), zap.String("branch", branch), zap.Error(err))
					return nil, status.Errorf(codes.Internal, "validate branch %s on %s: %v", branch, repoFullName, err)
				}
				if !exists {
					return nil, status.Errorf(codes.InvalidArgument, "branch %q does not exist on repository %s", branch, repoFullName)
				}
			}
		}
	}

	if err := s.connectorDAO.PatchSettings(ctx, orgID, sdk.ConnectorID(connectorID), applyVCSSettings); err != nil {
		logger.Error(ctx, "UpdateVCSSettings: failed to save connector config",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}

	// On a branch-scope change (tracked-branch retarget OR an all-branches
	// toggle) reprocess PR counts under the new scope — no cursor reset / vendor
	// re-fetch (see ReprocessConnectorPRCounts). Non-fatal: the settings save
	// already succeeded; the scheduler does the actual recompute on its next tick.
	if s.settingsHook != nil &&
		(oldTrackedBranch != newTrackedBranch || oldTrackAll != newTrackAll || !stringSetEqual(oldRepoAll, newRepoAll)) {
		resp, err := s.settingsHook.ReprocessConnectorPRCounts(ctx, &workqueuev1.ReprocessConnectorPRCountsRequest{
			OrganizationId: orgID,
			ConnectorId:    connectorID,
		})
		if err != nil {
			logger.Error(ctx, "UpdateVCSSettings: failed to reprocess PR counts (non-critical)",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		} else {
			logger.Info(ctx, "UpdateVCSSettings: reprocessing PR counts after branch-scope change",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.ConnectorID, connectorID),
				zap.String("old_branch", oldTrackedBranch),
				zap.String("new_branch", newTrackedBranch),
				zap.Bool("old_track_all", oldTrackAll),
				zap.Bool("new_track_all", newTrackAll),
				zap.Int64("rows_reset", resp.GetRowsReset()),
			)
		}
	}

	// Reset diff_analyzed_at when admin file-exclude patterns change so the
	// consumer's diff analysis re-runs with the new globs over already-fetched
	// diffs (no S3 / vendor API churn). Non-fatal: update succeeds even if
	// the host is down.
	if s.settingsHook != nil && excludePatternsChanged(oldExcludePatterns, newExcludePatterns, oldRepoExcludes, newRepoExcludes) {
		resp, err := s.settingsHook.ResetPRDiffAnalysisStatus(ctx, &workqueuev1.ResetPRDiffAnalysisStatusRequest{
			OrganizationId: orgID,
			ConnectorId:    connectorID,
		})
		if err != nil {
			logger.Error(ctx, "UpdateVCSSettings: failed to reset diff analysis (non-critical)",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		} else {
			logger.Info(ctx, "UpdateVCSSettings: reset diff analysis after exclude-pattern change",
				zap.String(logger.OrgID, orgID),
				zap.String(logger.ConnectorID, connectorID),
				zap.Int64("rows_reset", resp.GetRowsReset()),
			)
		}
	}

	return &emptypb.Empty{}, nil
}

// excludePatternsChanged reports whether the admin file-exclude pattern set
// differs between two snapshots. A cheap pre-check that skips the
// ResetPRDiffAnalysisStatus gRPC when nothing relevant changed.
func excludePatternsChanged(oldOrg, newOrg []string, oldRepo, newRepo map[string]*connintv1.RepoFileExcludePatterns) bool {
	if !stringSliceEqual(oldOrg, newOrg) {
		return true
	}
	if len(oldRepo) != len(newRepo) {
		return true
	}
	for repo, val := range newRepo {
		oldVal, ok := oldRepo[repo]
		if !ok {
			return true
		}
		if !stringSliceEqual(oldVal.GetPatterns(), val.GetPatterns()) {
			return true
		}
	}
	return false
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// stringSetEqual reports whether a and b contain the same elements regardless
// of order — for fields whose semantics are a set (e.g. repo_all_branches),
// where a reorder must NOT count as a change. Using ordered stringSliceEqual
// there would trigger a spurious full cursor reset on an innocuous reorder.
func stringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}

// ListVCSRepos returns the list of repos accessible to the VCS connector, enriched
// with any per-repo branch overrides from saved settings.
func (s *Service) ListVCSRepos(ctx context.Context, req *connintv1.ListVCSReposRequest) (*connintv1.ListVCSReposResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	if !s.isVCSConnector(sdk.ConnectorID(connectorID)) {
		logger.WarnWithCtx(ctx, "not a VCS connector", zap.String(logger.ConnectorID, connectorID))
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a VCS connector", connectorID)
	}

	cfg, err := s.connectorDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "ListVCSRepos: connector config not found",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.NotFound, "connector not found: %s", connectorID)
	}

	overrides := extractRepoBranchOverrides(cfg.Settings)
	tracked := extractTrackedRepoSet(cfg.Settings)

	var repos []discoveredRepo
	switch {
	case req.GetFresh() && connectorID == connectorIDGitHub:
		// fresh=true bypasses the cached discovered_repos set (which the ~6h
		// catalog job warms), lists repos live over the credential's correct
		// endpoint, and writes the live set back so the next cached read is
		// current. Used by the connect/edit picker where a repo created seconds
		// ago must be visible immediately.
		liveRepos, err := s.listGitHubReposFresh(ctx, cfg)
		if err != nil {
			return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
		}
		repos = liveRepos
	default:
		// Read discovered repos from settings (populated by fetch_repos/fetch_projects).
		repos = extractDiscoveredRepos(cfg.Settings)

		// Cold-cache fallback: with no cached repos for GitHub, list live over the
		// credential's correct endpoint (PAT → /user/repos, App → installation/org
		// repos) and warm discovered_repos — identical to the fresh=true path. The
		// outer gatewayFactory guard preserves the prior behavior for a connector
		// with no gateway configured: it returns the empty cached set rather than
		// erroring (listGitHubReposFresh also guards a nil factory internally).
		if len(repos) == 0 && connectorID == connectorIDGitHub && s.gatewayFactory != nil {
			liveRepos, err := s.listGitHubReposFresh(ctx, cfg)
			if err != nil {
				return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
			}
			repos = liveRepos
		}
	}

	protoRepos := make([]*connintv1.VCSRepo, 0, len(repos))
	for _, r := range repos {
		protoRepos = append(protoRepos, &connintv1.VCSRepo{
			FullName:       r.FullName,
			DefaultBranch:  r.DefaultBranch,
			OverrideBranch: overrides[r.FullName],
			Private:        r.Private,
			Tracked:        tracked[r.FullName],
		})
	}

	return &connintv1.ListVCSReposResponse{
		Repos:       protoRepos,
		GrantStatus: grantStatusForListing(cfg),
	}, nil
}

// grantStatusForListing classifies how fully the credential backing this listing
// is authorised, so the connect-flow picker can show the awaiting-org-approval
// state (CONNECTOR_GRANT_STATUS_PENDING_APPROVAL) instead of a bare "no repos
// found". It returns GRANTED for a fine-grained PAT and UNSPECIFIED (no approval
// concept → a client treats it as granted) for the App / OAuth credentials.
//
// Limitation — PENDING_APPROVAL is not emitted: GitHub gives the token HOLDER no
// observable signal that a fine-grained PAT is awaiting org approval. The
// unapproved-org case and a genuinely-empty personal account both return an empty
// 200 from GET /user/repos, byte-for-byte indistinguishable, and no header,
// field, or token-holder-readable endpoint separates them (the pending/404 the
// spike saw is only on a DIRECT org-resource read, which the PAT listing does not
// do). So an empty listing must classify as GRANTED — never PENDING — to honour
// the contract that an empty personal account is not mislabelled "pending".
// Detecting PENDING would need the user-declared target org captured at connect
// plus an org-resource probe, and neither exists.
func grantStatusForListing(cfg *sdk.ConnectorConfig) connintv1.ConnectorGrantStatus {
	if cfg.ConnectorID != connectorIDGitHub {
		return connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_UNSPECIFIED
	}
	var creds ghconfig.GitHubCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		// Unparseable credentials: do not assert a grant state we cannot verify.
		logger.ErrorNoCtx("grantStatusForListing: unparseable credentials, defaulting to UNSPECIFIED",
			zap.String(logger.ConnectorID, string(cfg.ConnectorID)), zap.Error(err))
		return connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_UNSPECIFIED
	}
	if creds.IsPAT() {
		return connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_GRANTED
	}
	// App credential: no PAT approval gate.
	return connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_UNSPECIFIED
}

// extractTrackedBranch reads the tracked_branch field from a settings JSON blob.
func extractTrackedBranch(settings json.RawMessage) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(settings, &m); err != nil {
		logger.Warn("extractTrackedBranch: settings unparseable", zap.Error(err))
		return ""
	}
	raw, ok := m["tracked_branch"]
	if !ok {
		return ""
	}
	var val string
	if err := json.Unmarshal(raw, &val); err != nil {
		logger.Warn("extractTrackedBranch: tracked_branch field unparseable", zap.Error(err))
		return ""
	}
	return val
}

// extractAllBranchSettings reads the track_all_branches flag and
// repo_all_branches list from a settings JSON blob.
// Unparseable settings degrade to (false, nil) — the pre-feature default.
func extractAllBranchSettings(settings json.RawMessage) (bool, []string) {
	var s struct {
		TrackAllBranches bool     `json:"track_all_branches"`
		RepoAllBranches  []string `json:"repo_all_branches"`
	}
	if err := json.Unmarshal(settings, &s); err != nil {
		logger.Warn("extractAllBranchSettings: settings unparseable", zap.Error(err))
		return false, nil
	}
	return s.TrackAllBranches, s.RepoAllBranches
}

// discoveredRepo is a generic repo representation read from any VCS connector's settings.
type discoveredRepo struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
	// DatabaseID is GitHub's numeric repo ID (a consumer's repository key); carried
	// into the discovered_repos cache so a later explicit-list sync can key tree
	// records correctly. Same json tag as ghconfig.GitHubDiscoveredRepo.
	DatabaseID int64 `json:"database_id,omitempty"`
}

// extractTrackedRepoSet reads settings.repositories — the explicit set
// ConfirmConnectorRepos persists when the connect/edit flow completes — into a
// membership set keyed by full name, so ListVCSRepos can mark which discovered
// repos are currently synced. Unparseable settings degrade to an empty set
// (every repo reads as untracked), never an error on a read path.
func extractTrackedRepoSet(settings json.RawMessage) map[string]bool {
	var s struct {
		Repositories []string `json:"repositories"`
	}
	if err := json.Unmarshal(settings, &s); err != nil {
		logger.Warn("extractTrackedRepoSet: settings unparseable", zap.Error(err))
		return nil
	}
	if len(s.Repositories) == 0 {
		return nil
	}
	set := make(map[string]bool, len(s.Repositories))
	for _, name := range s.Repositories {
		set[name] = true
	}
	return set
}

// extractDiscoveredRepos reads the discovered_repos array from a settings JSON blob.
func extractDiscoveredRepos(settings json.RawMessage) []discoveredRepo {
	var s struct {
		DiscoveredRepos []discoveredRepo `json:"discovered_repos"`
	}
	if err := json.Unmarshal(settings, &s); err != nil {
		logger.Warn("extractDiscoveredRepos: settings unparseable", zap.Error(err))
		return nil
	}
	return s.DiscoveredRepos
}

// ghFreshRepo is the minimal slice of the GitHub REST repository object the
// fresh picker needs. Mirrors the github package's unexported repo type, which
// lives in another package and cannot be reused here.
type ghFreshRepo struct {
	ID            int64  `json:"id"`
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Private       bool   `json:"private"`
}

// listGitHubReposFresh lists repos live over the endpoint that matches the
// credential's ability and the tenant's intent, then warms the discovered_repos
// cache. The endpoint matrix is the connector's github.SelectRepoListing (the
// single shared source — PAT → GET /user/repos, App+INDIVIDUAL → GET
// /installation/repositories, App+ORGANIZATION → GET /orgs/{org}/repos), so this
// path and FetchReposOp can never drift. The cache write-back stays separate
// because the github saveDiscoveredRepos needs an ExecutionParams.ConfigUpdater
// the handler does not have.
//
// tenant_type is not in the handler context, so it is resolved via
// the tenant directory. A lookup
// failure fails loud rather than defaulting to the org endpoint (which would be
// wrong for a PAT/individual tenant), matching selectRepoListing's UNSPECIFIED
// backstop.
func (s *Service) listGitHubReposFresh(ctx context.Context, cfg *sdk.ConnectorConfig) ([]discoveredRepo, error) {
	if s.gatewayFactory == nil {
		logger.Error(ctx, "ListVCSRepos: gateway factory not configured for fresh listing",
			zap.String(logger.OrgID, cfg.OrganizationID))
		return nil, status.Error(codes.FailedPrecondition, "fresh repo listing not available")
	}

	var creds ghconfig.GitHubCredentials
	if err := json.Unmarshal(cfg.Credentials, &creds); err != nil {
		return nil, status.Errorf(codes.Internal, "parse credentials: %v", err)
	}
	var settings ghconfig.GitHubSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		return nil, status.Errorf(codes.Internal, "parse settings: %v", err)
	}

	// Resolve tenant_type to pick the App endpoint (PAT ignores it). Fail loud
	// on a lookup error so the App path never silently defaults to the org
	// endpoint for an individual tenant.
	var tenantType commonv1.TenantType
	if !creds.IsPAT() {
		orgTenantType, err := s.tenants.TenantType(ctx, cfg.OrganizationID)
		if err != nil {
			logger.Error(ctx, "ListVCSRepos: resolve tenant type failed for fresh listing",
				zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
			return nil, status.Error(codes.Unavailable, "could not resolve tenant type, please try again")
		}
		tenantType = commonv1.TenantType(orgTenantType)
	}

	gw, err := s.gatewayFactory(ctx, cfg)
	if err != nil {
		logger.Error(ctx, "ListVCSRepos: build gateway for fresh listing",
			zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "list repos: %v", err)
	}
	restGW := gw.ForGroup(constants.GroupGitHubREST)

	fetchPage, err := ghconfig.SelectRepoListing(creds, settings, sdk.TenantKind(tenantType))
	if err != nil {
		logger.WarnWithCtx(ctx, "ListVCSRepos: cannot select listing endpoint for fresh listing",
			zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}

	var repos []discoveredRepo
	for page := int32(1); ; page++ {
		reposRaw, hasNextPage, err := fetchPage(ctx, restGW, page)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list repos page=%d: %v", page, err)
		}
		for _, repoRaw := range reposRaw {
			var gh ghFreshRepo
			if err := json.Unmarshal(repoRaw, &gh); err != nil {
				logger.WarnWithCtx(ctx, "ListVCSRepos: skipping unparseable repo node", zap.Error(err))
				continue
			}
			if gh.ID == 0 {
				continue
			}
			if !settings.IncludePrivate && gh.Private {
				continue
			}
			repos = append(repos, discoveredRepo{
				FullName:      gh.FullName,
				DefaultBranch: gh.DefaultBranch,
				Private:       gh.Private,
				DatabaseID:    gh.ID,
			})
		}
		if !hasNextPage {
			break
		}
	}

	s.warmDiscoveredRepos(ctx, cfg, repos)
	return repos, nil
}

// warmDiscoveredRepos writes the fresh live repo set back into
// settings.discovered_repos so a later cached ListVCSRepos read is current AND a
// later explicit-list sync can enrich its tracked repos with the numeric repo ID
// + default branch (fetch_repo_tree keys on the numeric ID). It runs even when an
// explicit settings.repositories list is set: discovered_repos is the metadata
// cache of ALL accessible repos, distinct from repositories (the tracked
// selection), and this only ever touches the cache — never the selection — so it
// cannot overwrite an operator-pinned list. This is what lets "Edit tracked repos"
// add a repo absent from the old cache and still sync its tree correctly: the
// fresh picker listing caches the new repo's id here before ConfirmConnectorRepos
// saves the selection. A write failure is logged, not surfaced (best-effort; the
// live set is already returned to the caller).
//
// It uses PatchSettings (a row-locked, settings-only update of the live
// deleted_at IS NULL row) rather than a Get+Save upsert, for two reasons: a user
// who disconnects GitHub mid-listing must not have the connector resurrected by a
// late warm (an upsert clears deleted_at and rewrites credentials) — a deleted row
// is simply NotFound here and the best-effort warm no-ops; and the merge into the
// current raw settings JSON preserves keys that are not fields on GitHubSettings
// (e.g. the admin file-exclude patterns), which a re-marshal of the narrowed
// struct would silently drop.
func (s *Service) warmDiscoveredRepos(ctx context.Context, cfg *sdk.ConnectorConfig, repos []discoveredRepo) {
	discovered := make([]ghconfig.GitHubDiscoveredRepo, 0, len(repos))
	for _, r := range repos {
		discovered = append(discovered, ghconfig.GitHubDiscoveredRepo{
			FullName:      r.FullName,
			DefaultBranch: r.DefaultBranch,
			Private:       r.Private,
			DatabaseID:    r.DatabaseID,
		})
	}
	discoveredJSON, err := json.Marshal(discovered)
	if err != nil {
		logger.WarnWithCtx(ctx, "ListVCSRepos: marshal discovered_repos for cache warm failed; skipping warm", zap.Error(err))
		return
	}
	if err := s.connectorDAO.PatchSettings(ctx, cfg.OrganizationID, cfg.ConnectorID, func(current json.RawMessage) (json.RawMessage, error) {
		merged := map[string]json.RawMessage{}
		if len(current) > 0 {
			if err := json.Unmarshal(current, &merged); err != nil {
				return nil, fmt.Errorf("unmarshal current settings: %w", err)
			}
		}
		merged["discovered_repos"] = discoveredJSON
		return json.Marshal(merged)
	}); err != nil {
		logger.WarnWithCtx(ctx, "ListVCSRepos: warm discovered_repos cache failed",
			zap.String(logger.OrgID, cfg.OrganizationID), zap.Error(err))
	}
}

// extractRepoBranchOverrides reads the repo_branch_overrides map from a settings JSON blob.
func extractRepoBranchOverrides(settings json.RawMessage) map[string]string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(settings, &m); err != nil {
		logger.Warn("extractRepoBranchOverrides: settings unparseable", zap.Error(err))
		return nil
	}
	raw, ok := m["repo_branch_overrides"]
	if !ok {
		return nil
	}
	var overrides map[string]string
	if err := json.Unmarshal(raw, &overrides); err != nil {
		logger.Warn("extractRepoBranchOverrides: repo_branch_overrides field unparseable", zap.Error(err))
		return nil
	}
	return overrides
}

// mergeProducedSettings returns a settings mutator for
// SaveCredentialsEnableWithSettings: the keys a connect flow produces replace
// the stored ones, and every other stored key is kept. On a first connect
// (no stored settings) the produced settings are stored as they are.
func mergeProducedSettings(produced json.RawMessage) func(current json.RawMessage) (json.RawMessage, error) {
	return func(current json.RawMessage) (json.RawMessage, error) {
		if len(current) == 0 || string(current) == "null" {
			return produced, nil
		}
		merged := map[string]json.RawMessage{}
		if err := json.Unmarshal(current, &merged); err != nil {
			return nil, fmt.Errorf("parse stored settings: %w", err)
		}
		var fresh map[string]json.RawMessage
		if err := json.Unmarshal(produced, &fresh); err != nil {
			return nil, fmt.Errorf("parse produced settings: %w", err)
		}
		for k, v := range fresh {
			merged[k] = v
		}
		out, err := json.Marshal(merged)
		if err != nil {
			return nil, fmt.Errorf("marshal merged settings: %w", err)
		}
		return out, nil
	}
}

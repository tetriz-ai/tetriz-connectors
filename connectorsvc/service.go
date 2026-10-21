// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/scopedstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// configFetchToStatus maps a ConfigDAO.Get error to a gRPC status code,
// distinguishing genuine "not found" from underlying DB / RLS failures.
// Without this, every transient DB error returns NotFound and the caller
// silently degrades (skips the branch filter, drops to no-config behavior)
// instead of surfacing the failure.
func configFetchToStatus(err error, connectorID string) error {
	if store.IsNotFound(err) {
		return status.Errorf(codes.NotFound, "connector config not found: %s", connectorID)
	}
	return status.Errorf(codes.Internal, "fetch connector config %s: %v", connectorID, err)
}

// vcsDiscoveredRepo / vcsConnectorSettings mirror the JSON shape of
// connector_configs.settings for VCS connectors. Shared so the read RPCs
// (GetDiscoveredRepos, GetVCSTrackedBranches) have one authoritative
// declaration of the storage shape.
//
// Status is the per-repo lifecycle marker connectors stamp on each entry
// (see bitbucket's BitbucketRepoStatus). The cross-vendor view stores it
// as a string — the enum's wire form is the proto name (e.g.
// "BITBUCKET_REPO_STATUS_DELETED") via gen_sql's MarshalJSON. Empty /
// missing is treated as active.
type vcsDiscoveredRepo struct {
	FullName string `json:"full_name"`
	Status   string `json:"status,omitempty"`
}

// isActive reports whether a discovered repo should be returned by
// cross-vendor read RPCs. Matches the bitbucket-internal
// BitbucketDiscoveredRepo.IsActive() policy: a "*_DELETED" suffix
// (per the enum-naming convention) means stop including it; empty /
// "*_UNSPECIFIED" / "*_ACTIVE" all mean active.
//
// Using a suffix match keeps this vendor-agnostic — any future
// connector enum that follows the standard PREFIX_STATUS_DELETED naming
// just works without service.go touching its proto.
func (r vcsDiscoveredRepo) isActive() bool {
	return !strings.HasSuffix(r.Status, "_DELETED")
}

type vcsConnectorSettings struct {
	DiscoveredRepos     []vcsDiscoveredRepo `json:"discovered_repos"`
	TrackedBranch       string              `json:"tracked_branch"`
	RepoBranchOverrides map[string]string   `json:"repo_branch_overrides"`
	// TrackAllBranches / RepoAllBranches opt repos into counting every branch.
	// Resolution precedence (most specific wins):
	// repo in RepoAllBranches → all; repo in RepoBranchOverrides → that branch;
	// TrackAllBranches → all; TrackedBranch → that branch; else omitted.
	TrackAllBranches bool     `json:"track_all_branches"`
	RepoAllBranches  []string `json:"repo_all_branches"`
}

// resolveRepoBranchPolicy returns the effective branch policy for one repo:
// (allBranches=true, "") when the repo tracks every branch, (false, branch)
// for a specific branch, or (false, "") when nothing is configured (caller
// omits the repo). The override-precedence order matches the per-connector
// settings resolvers; the terminal fallback differs by design — this read-side
// resolver has no auto-detected default-branch to fall back to (DiscoveredRepos
// carries no default branch here), so an unconfigured repo yields "" and is
// dropped, whereas the fetch-side resolver falls back to the repo's default.
func (s vcsConnectorSettings) resolveRepoBranchPolicy(repoFullName string) (allBranches bool, branch string) {
	for _, r := range s.RepoAllBranches {
		if r == repoFullName {
			return true, ""
		}
	}
	if v, ok := s.RepoBranchOverrides[repoFullName]; ok && v != "" {
		return false, v
	}
	if s.TrackAllBranches {
		return true, ""
	}
	return false, s.TrackedBranch
}

// Service holds the connector infrastructure components initialised at startup.
// It also implements ConnectorServiceServer, the gRPC API other services call.
type Service struct {
	connectorpb.UnimplementedConnectorServiceServer
	ConfigDAO   store.IConnectorConfigDAO
	SyncRunDAO  store.ISyncRunDAO
	ScheduleDAO store.IScheduleDAO
	Registry    *sdk.ConnectorRegistry
	Sink        sdk.DataSink
	// SlackChannels serves ListCommsSlackChannels. When nil, that RPC returns
	// Unimplemented.
	SlackChannels SlackChannelLister
	// ChannelCache caches ListCommsSlackChannels results. When nil, every call
	// reads from Slack.
	ChannelCache *SlackChannelCache
	// SecretAuthorizer decides which callers GetSecret lends a token to. When
	// nil, GetSecret denies every call: the broker lends nothing until the
	// host states who may borrow what.
	SecretAuthorizer SecretAuthorizer
}

// SecretAuthorizer decides whether the caller of GetSecret may borrow the
// token of orgID's grant for connectorID. orgID is always the canonical form
// (a lower-case hyphenated UUID): GetSecret canonicalises the request's
// spelling, and refuses one that is not a UUID, before calling it. It returns
// nil to allow the call; an error, or a panic, denies it with
// PermissionDenied, and the cause is logged, not sent to the caller. ctx is
// the RPC's context, so it carries the caller's gRPC metadata and peer, and
// whatever identity the host's interceptors attached.
type SecretAuthorizer func(ctx context.Context, orgID, connectorID string) error

// AllowConnectors returns a SecretAuthorizer that lends only the named
// connectors' tokens, to any caller that reaches the RPC. It checks what is
// borrowed, not who borrows it, so use it only on a server whose network or
// interceptors already admit trusted callers alone.
func AllowConnectors(connectorIDs ...string) SecretAuthorizer {
	allowed := make(map[string]struct{}, len(connectorIDs))
	for _, id := range connectorIDs {
		allowed[id] = struct{}{}
	}
	return func(_ context.Context, _, connectorID string) error {
		if _, ok := allowed[connectorID]; !ok {
			return fmt.Errorf("connector %q is not on the secret allowlist", connectorID)
		}
		return nil
	}
}

// ServiceOption configures a Service built by NewService.
type ServiceOption func(*Service)

// WithSecretAuthorizer sets the policy GetSecret checks every caller
// against. Without it, GetSecret denies every call. It gates GetSecret only:
// GetConnectionStatus, ListCommsSlackChannels, ReportGrantInvalid and
// PurgeOrgGrants are not checked against it and rely on the host's own
// authentication of their callers, as the README says.
func WithSecretAuthorizer(a SecretAuthorizer) ServiceOption {
	return func(s *Service) { s.SecretAuthorizer = a }
}

// authorizeSecret applies the SecretAuthorizer, denying by default when
// none is set. A denial is audited without the token.
func (s *Service) authorizeSecret(ctx context.Context, orgID, connectorID string) error {
	var err error
	if s.SecretAuthorizer == nil {
		err = errors.New("no SecretAuthorizer is configured, so every GetSecret call is denied")
	} else {
		err = s.callAuthorizer(ctx, orgID, connectorID)
	}
	if err != nil {
		logger.WarnWithCtx(ctx, "connector GetSecret denied",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return status.Error(codes.PermissionDenied, "caller may not borrow this connector's secret")
	}
	return nil
}

// callAuthorizer runs the SecretAuthorizer and turns a panic in it into a
// denial, so a faulty policy denies instead of crashing the server.
func (s *Service) callAuthorizer(ctx context.Context, orgID, connectorID string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("SecretAuthorizer panicked: %v", r)
		}
	}()
	return s.SecretAuthorizer(ctx, orgID, connectorID)
}

// NewService creates a new connector Service. Every store call it makes runs in
// a tenant scope through tx: the organization the call names, or the system
// scope for a list across organizations. A Postgres host passes
// store.PostgresTxRunner{}, which row-level security requires; the memory and
// file stores take store.NoopTxRunner{}.
//
// GetSecret denies every call unless an option such as WithSecretAuthorizer
// sets the policy for who may borrow a token.
func NewService(
	tx store.TxRunner,
	configDAO store.IConnectorConfigDAO,
	syncRunDAO store.ISyncRunDAO,
	scheduleDAO store.IScheduleDAO,
	registry *sdk.ConnectorRegistry,
	sink sdk.DataSink,
	slackChannels SlackChannelLister,
	channelCache *SlackChannelCache,
	opts ...ServiceOption,
) *Service {
	s := &Service{
		ConfigDAO:     scopedstore.Configs(tx, configDAO),
		SyncRunDAO:    scopedstore.Runs(tx, syncRunDAO),
		ScheduleDAO:   scopedstore.Schedules(tx, scheduleDAO),
		Registry:      registry,
		Sink:          sink,
		SlackChannels: slackChannels,
		ChannelCache:  channelCache,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ListEnabledConfigs returns enabled connector configs for a given connector type.
func (s *Service) ListEnabledConfigs(ctx context.Context, req *connectorpb.ListEnabledConfigsRequest) (*connectorpb.ListEnabledConfigsResponse, error) {
	connectorID := req.GetConnectorId()
	if connectorID == "" {
		logger.WarnWithCtx(ctx, "connector_id is required")
		return nil, status.Error(codes.InvalidArgument, "connector_id is required")
	}

	// It returns no credentials, so it reads none: one row that does not
	// decrypt cannot fail it for every organization.
	orgIDs, err := s.ConfigDAO.ListEnabledOrganizations(ctx, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "failed to list connector configs", zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "list connector configs: %v", err)
	}

	var enabled []*connectorpb.EnabledConnectorConfig
	for _, orgID := range orgIDs {
		enabled = append(enabled, &connectorpb.EnabledConnectorConfig{
			OrganizationId: orgID,
			ConnectorId:    connectorID,
		})
	}

	return &connectorpb.ListEnabledConfigsResponse{Configs: enabled}, nil
}

// ListEnabledOrganizations returns org IDs that have the given connector enabled.
func (s *Service) ListEnabledOrganizations(ctx context.Context, req *connectorpb.ListEnabledOrganizationsRequest) (*connectorpb.ListEnabledOrganizationsResponse, error) {
	// It reads no credentials, so one row that does not decrypt cannot fail it.
	orgIDs, err := s.ConfigDAO.ListEnabledOrganizations(ctx, sdk.ConnectorID(req.GetConnectorId()))
	if err != nil {
		logger.Error(ctx, "failed to list enabled connector configs", zap.String(logger.ConnectorID, req.GetConnectorId()), zap.Error(err))
		return nil, fmt.Errorf("list enabled organizations: %w", err)
	}
	if orgIDs == nil {
		orgIDs = []string{}
	}
	return &connectorpb.ListEnabledOrganizationsResponse{OrganizationIds: orgIDs}, nil
}

// GetActiveVCSConnector returns the currently enabled VCS connector for an org.
// Returns an empty connector_id if none is enabled.
func (s *Service) GetActiveVCSConnector(ctx context.Context, req *connectorpb.GetActiveVCSConnectorRequest) (*connectorpb.GetActiveVCSConnectorResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		logger.WarnWithCtx(ctx, "org_id is required")
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}

	configs, err := s.ConfigDAO.ListByOrg(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "failed to list org connectors", zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, fmt.Errorf("list org connectors: %w", err)
	}

	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		c, err := s.Registry.Get(cfg.ConnectorID)
		if err != nil {
			logger.WarnWithCtx(ctx, "skipping enabled connector not found in registry",
				zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(cfg.ConnectorID)), zap.Error(err))
			continue
		}
		if sdk.HasCategory(c, sdk.CategoryVCS) {
			return &connectorpb.GetActiveVCSConnectorResponse{
				ConnectorId: string(cfg.ConnectorID),
			}, nil
		}
	}

	return &connectorpb.GetActiveVCSConnectorResponse{}, nil
}

// GetDiscoveredRepos returns discovered repository names for a VCS connector.
// Reads from the connector config's discovered_repos settings field.
func (s *Service) GetDiscoveredRepos(ctx context.Context, req *connectorpb.GetDiscoveredReposRequest) (*connectorpb.GetDiscoveredReposResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	cfg, err := s.ConfigDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "GetDiscoveredRepos: failed to fetch config",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, configFetchToStatus(err, connectorID)
	}

	// Extract discovered_repos from settings JSON (same shape across all VCS connectors).
	var settings vcsConnectorSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		logger.Error(ctx, "GetDiscoveredRepos: malformed settings JSON",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return &connectorpb.GetDiscoveredReposResponse{}, nil
	}

	names := make([]string, 0, len(settings.DiscoveredRepos))
	for _, repo := range settings.DiscoveredRepos {
		if repo.FullName == "" || !repo.isActive() {
			continue
		}
		names = append(names, repo.FullName)
	}

	return &connectorpb.GetDiscoveredReposResponse{RepoNames: names}, nil
}

// GetVCSTrackedBranches returns the per-repo effective base branch for a VCS
// connector, computed from {discovered_repos, tracked_branch,
// repo_branch_overrides}. Per-repo override wins; tracked_branch is the
// default. Repos with no effective branch (both unset) are omitted so
// consumers can apply a single tuple-match predicate without re-implementing
// the resolution. Consumers resolve the branch the same way.
// resolveEffectiveBranch.
func (s *Service) GetVCSTrackedBranches(ctx context.Context, req *connectorpb.GetVCSTrackedBranchesRequest) (*connectorpb.GetVCSTrackedBranchesResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	connectorID := req.GetConnectorId()

	cfg, err := s.ConfigDAO.Get(ctx, orgID, sdk.ConnectorID(connectorID))
	if err != nil {
		logger.Error(ctx, "GetVCSTrackedBranches: failed to fetch config",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return nil, configFetchToStatus(err, connectorID)
	}

	var settings vcsConnectorSettings
	if err := json.Unmarshal(cfg.Settings, &settings); err != nil {
		logger.Error(ctx, "GetVCSTrackedBranches: malformed settings JSON",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, connectorID), zap.Error(err))
		return &connectorpb.GetVCSTrackedBranchesResponse{}, nil
	}

	policies := make([]*connectorpb.RepoBranchPolicy, 0, len(settings.DiscoveredRepos))
	for _, repo := range settings.DiscoveredRepos {
		if repo.FullName == "" || !repo.isActive() {
			continue
		}
		allBranches, branch := settings.resolveRepoBranchPolicy(repo.FullName)
		if !allBranches && branch == "" {
			continue
		}
		policies = append(policies, &connectorpb.RepoBranchPolicy{
			RepoFullName: repo.FullName,
			BaseBranch:   branch,
			AllBranches:  allBranches,
		})
	}
	return &connectorpb.GetVCSTrackedBranchesResponse{
		RepoBranches: policies,
		// Surface the org-level policy so read consumers can complete the per-repo
		// list for repos absent from discovered_repos.
		// Raw stored values; the consumer decides how to apply them.
		OrgTrackedBranch: settings.TrackedBranch,
		OrgAllBranches:   settings.TrackAllBranches,
	}, nil
}

// GetConnectorSyncStatus returns the org's true per-connector vendor-sync
// freshness. See store.ConnectorSyncStatus for the "synced" definition.
func (s *Service) GetConnectorSyncStatus(ctx context.Context, req *connectorpb.GetConnectorSyncStatusRequest) (*connectorpb.GetConnectorSyncStatusResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if orgID == "" {
		logger.WarnWithCtx(ctx, "org_id is required")
		return nil, status.Error(codes.InvalidArgument, "org_id is required")
	}

	byConnector, err := s.SyncRunDAO.GetConnectorSyncStatusByOrg(ctx, orgID)
	if err != nil {
		logger.Error(ctx, "GetConnectorSyncStatus: failed to fetch sync status",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "get connector sync status: %v", err)
	}

	statuses := make([]*connectorpb.ConnectorSyncStatus, 0, len(byConnector))
	for connectorID, st := range byConnector {
		var lastSuccessfulAt *timestamppb.Timestamp
		if st.LastSuccessfulAt != nil {
			lastSuccessfulAt = timestamppb.New(*st.LastSuccessfulAt)
		}
		statuses = append(statuses, &connectorpb.ConnectorSyncStatus{
			ConnectorId:          string(connectorID),
			LastSuccessfulSyncAt: lastSuccessfulAt,
			SyncState:            connectorSyncStateToProto(st.State),
		})
	}
	// Deterministic order: byConnector is a Go map, and range order is
	// randomized — an unsorted response makes every caller's rendering (and
	// any test asserting on order) flaky across identical calls.
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].GetConnectorId() < statuses[j].GetConnectorId() })

	return &connectorpb.GetConnectorSyncStatusResponse{Statuses: statuses}, nil
}

// connectorSyncStateToProto maps store.ConnectorSyncState to its wire enum. The
// two vocabularies are kept separate on purpose (a service defines its own core entities rather
// than exporting the store's) — this is the one place a new dao state must
// also be added here, or it silently falls through to STILL_SYNCING.
func connectorSyncStateToProto(s store.ConnectorSyncState) connectorpb.ConnectorSyncState {
	switch s {
	case store.ConnectorSyncStateSynced:
		return connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_SYNCED
	case store.ConnectorSyncStateFailed:
		return connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_FAILED
	case store.ConnectorSyncStateStillSyncing:
		return connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_STILL_SYNCING
	default:
		return connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_STILL_SYNCING
	}
}

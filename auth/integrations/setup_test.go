// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// seedGitHubState writes a CSRF state entry to miniredis for the given orgID,
// mirroring what githubInstallURL stores at ConnectIntegration time.
func seedGitHubState(t *testing.T, rc *redis.Client, state, orgID string) {
	t.Helper()
	data, err := json.Marshal(struct {
		OrgID string `json:"org_id"`
	}{OrgID: orgID})
	require.NoError(t, err)
	require.NoError(t, rc.Set(context.Background(), ghconfig.OAuthStateKey(state), data, time.Minute).Err())
}

// hasTornStateReason reports whether err is a FailedPrecondition carrying the
// dedicated torn-state errdetails reason. The caller keys its different_owner/
// different-account mapping on exactly this, so a FailedPrecondition from another
// save-step cause (single-VCS guard, ops off switch) must NOT carry it.
func hasTornStateReason(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == tornStateRejectReason {
			return true
		}
	}
	return false
}

// TestValidateInstallation covers the resolve (no-save) step of the App connect
// flow: CSRF verification + consumption, GetInstallation, the single-VCS guard,
// tenant-type resolution, and the resolved account it surfaces — and asserts that
// NOTHING is persisted (no Save / SaveCredentialsPreserveEnabled). The save path
// is covered by TestSaveInstallation.
func TestValidateInstallation(t *testing.T) {
	const validState = "gh-state-token-abcdef1234"

	tests := []struct {
		name           string
		installationID string
		orgID          string
		state          string
		seedState      string // state token to store in Redis (empty = don't seed)
		seedOrg        string // orgID bound to the seeded state
		ghGetResult    *intgithub.InstallationInfo
		ghGetErr       error
		ghProofErr     error
		tenantType     commonv1.TenantType
		tenantTypeErr  error
		existingVCS    []*sdk.ConnectorConfig
		wantErr        bool
		wantCode       codes.Code
		// wantResolve is whether the resolve work (GetInstallation + guard +
		// tenant-type lookup) is expected to run. A CSRF/arg rejection must NOT.
		wantResolve      bool
		wantAccountID    string
		wantAccountLogin string
	}{
		{
			name:           "ResolveOrg",
			installationID: "12345",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "test-org", AccountID: 67890, AccountType: "Organization",
			},
			tenantType:       commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			wantResolve:      true,
			wantAccountID:    "67890",
			wantAccountLogin: "test-org",
		},
		{
			name:           "ResolveIndividual",
			installationID: "12345",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			tenantType:       commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
			wantResolve:      true,
			wantAccountID:    "583231",
			wantAccountLogin: "octocat",
		},
		{
			// A tenant-type lookup failure must fail loud (Unavailable).
			name:           "TenantTypeLookupFailsLoud",
			installationID: "12345",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        testOrgID,
			ghGetResult:    &intgithub.InstallationInfo{ID: 12345, AccountLogin: "octocat", AccountID: 583231},
			tenantTypeErr:  fmt.Errorf("mgmt down"),
			wantErr:        true,
			wantCode:       codes.Unavailable,
			wantResolve:    true,
		},
		{
			name: "UserCannotAccessInstallation", installationID: "12345", orgID: testOrgID, state: validState, seedState: validState, seedOrg: testOrgID,
			ghGetResult: &intgithub.InstallationInfo{ID: 12345, AccountID: 67890}, tenantType: commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			ghProofErr: intgithub.ErrUserInstallationDenied, wantResolve: true, wantErr: true, wantCode: codes.PermissionDenied,
		},
		{
			name:           "MissingState",
			installationID: "12345",
			orgID:          testOrgID,
			state:          "",
			wantErr:        true,
			wantCode:       codes.InvalidArgument,
			wantResolve:    false,
		},
		{
			// The core CSRF case: a forged/unissued state is rejected and nothing is
			// resolved or persisted.
			name:           "UnknownState",
			installationID: "12345",
			orgID:          testOrgID,
			state:          "attacker-supplied-state",
			wantErr:        true,
			wantCode:       codes.InvalidArgument,
			wantResolve:    false,
		},
		{
			name:           "StateOrgMismatch",
			installationID: "12345",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        "org-other-999",
			wantErr:        true,
			wantCode:       codes.InvalidArgument,
			wantResolve:    false,
		},
		{
			name:           "MissingInstallationID",
			installationID: "",
			orgID:          testOrgID,
			state:          validState,
			wantErr:        true,
			wantCode:       codes.InvalidArgument,
			wantResolve:    false,
		},
		{
			name:           "MissingOrgID",
			installationID: "12345",
			orgID:          "",
			state:          validState,
			wantErr:        true,
			wantCode:       codes.InvalidArgument,
			wantResolve:    false,
		},
		{
			name:           "InstallationNotFound",
			installationID: "99999",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        testOrgID,
			ghGetErr:       intgithub.ErrInstallationNotFound,
			wantErr:        true,
			wantCode:       codes.NotFound,
			wantResolve:    true,
		},
		{
			name:           "GitHubAPIError",
			installationID: "12345",
			orgID:          testOrgID,
			state:          validState,
			seedState:      validState,
			seedOrg:        testOrgID,
			ghGetErr:       fmt.Errorf("network timeout"),
			wantErr:        true,
			wantCode:       codes.Internal,
			wantResolve:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

			if tt.seedState != "" {
				seedGitHubState(t, rc, tt.seedState, tt.seedOrg)
			}

			ghClient := intgithubmocks.NewMockGitHubAppClient(ctrl)
			// dao is wired but the resolve step must NEVER persist — no Save /
			// SaveCredentialsPreserveEnabled expectation is set, so any persist call
			// fails the test (the "nothing saved before pin" invariant).
			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			mgmt := sdkmocks.NewMockTenantDirectory(ctrl)

			if tt.wantResolve {
				ghClient.EXPECT().GetInstallation(gomock.Any(), tt.installationID).
					Return(tt.ghGetResult, tt.ghGetErr)
			}
			if tt.wantResolve && tt.ghGetErr == nil && tt.ghGetResult != nil {
				dao.EXPECT().ListByOrg(gomock.Any(), tt.orgID).Return(tt.existingVCS, nil)
				mgmt.EXPECT().TenantType(gomock.Any(), tt.orgID).
					Return(sdk.TenantKind(tt.tenantType), tt.tenantTypeErr)
			}

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GHClient: ghClient, Redis: rc, Tenants: mgmt, Registry: testRegistry()})

			if tt.wantResolve && tt.ghGetErr == nil && tt.tenantTypeErr == nil {
				ghClient.EXPECT().VerifyUserInstallation(gomock.Any(), "test-code", tt.installationID, tt.ghGetResult).Return(tt.ghProofErr)
			}
			resp, err := svc.ValidateInstallation(context.Background(), &connintv1.ValidateInstallationRequest{
				InstallationId: tt.installationID,
				OrgId:          tt.orgID,
				State:          tt.state,
				Code:           "test-code",
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
			} else {
				require.NoError(t, err)
				assert.Len(t, resp.GetValidationToken(), 64)
				assert.Equal(t, tt.wantAccountID, resp.GetAccountId())
				assert.Equal(t, tt.wantAccountLogin, resp.GetAccountLogin())
				assert.Equal(t, tt.tenantType, resp.GetTenantType())
			}

			// A consumed state token must be deleted (replay protection).
			if tt.seedState != "" && tt.wantResolve {
				assert.False(t, mr.Exists(ghconfig.OAuthStateKey(tt.seedState)),
					"state token should be consumed on read")
			}
		})
	}
}

// TestSaveInstallation covers the save step of the App connect flow. The CSRF
// state was already consumed by ValidateInstallation, so SaveInstallation carries
// no state and re-fetches the installation from GitHub before persisting. An
// ORGANIZATION enables on install (full Save with Enabled=true); everything else
// (individual, or an UNSPECIFIED/unknown type) takes the enabled-preserving
// upsert that never forces enabled on the struct.
func TestSaveInstallation(t *testing.T) {
	tests := []struct {
		name               string
		installationID     string
		orgID              string
		expectedAcctID     string // request.expected_account_id (pre-persist torn-state guard)
		validatedAccountID int64
		ghGetResult        *intgithub.InstallationInfo
		ghGetErr           error
		tenantType         commonv1.TenantType
		tenantTypeErr      error
		existingVCS        []*sdk.ConnectorConfig
		saveErr            error
		wantErr            bool
		wantCode           codes.Code
		// wantNoPersist is true when the handler must reject BEFORE any DAO write
		// (the torn-state guard) — no Save / SaveCredentialsPreserveEnabled is set,
		// so a persist call fails the test.
		wantNoPersist bool
		// wantTornReason asserts the FailedPrecondition carries the dedicated
		// torn-state errdetails reason (true) or must NOT carry it (a non-torn-state
		// FailedPrecondition like the single-VCS guard, false). Only meaningful when
		// wantCode == FailedPrecondition.
		wantTornReason bool
		// wantGuardTrips is true when the single-VCS guard rejects inside
		// resolveInstallation — that returns BEFORE the tenant directory, so the tenant-type
		// lookup must not be expected.
		wantGuardTrips bool
		// wantEnabled is asserted only on the org Save arm (individual goes through
		// the preserve-enabled upsert, which never sets Enabled on the struct).
		wantEnabled bool
	}{
		{
			name:           "OrgEnablesOnInstall",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "test-org", AccountID: 67890, AccountType: "Organization",
			},
			tenantType:  commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			wantEnabled: true,
		},
		{name: "OrganizationAccountChangedAfterValidation", installationID: "12345", orgID: testOrgID, validatedAccountID: 123,
			ghGetResult: &intgithub.InstallationInfo{ID: 12345, AccountID: 67890}, tenantType: commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			wantErr: true, wantCode: codes.FailedPrecondition, wantNoPersist: true,
		},
		{
			// An org install ignores expected_account_id even if it differs — orgs
			// have no single owner to torn-state-guard. Still enables on install.
			name:           "OrgIgnoresExpectedAccountMismatch",
			installationID: "12345",
			orgID:          testOrgID,
			expectedAcctID: "00000",
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "test-org", AccountID: 67890, AccountType: "Organization",
			},
			tenantType:  commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			wantEnabled: true,
		},
		{
			// First connect for an individual: the preserve-enabled upsert lands
			// disabled (a later ConfirmConnectorRepos enables it).
			name:           "IndividualFirstConnectLandsDisabled",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			tenantType: commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
		},
		{
			// Pre-persist torn-state guard: an individual whose re-resolved account
			// differs from the pinned expected_account_id is rejected with
			// FailedPrecondition BEFORE any DAO write — nothing persists.
			name:           "IndividualTornStatePersistsNothing",
			installationID: "12345",
			orgID:          testOrgID,
			expectedAcctID: "111111",
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "intruder", AccountID: 999, AccountType: "User",
			},
			tenantType:     commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
			wantErr:        true,
			wantCode:       codes.FailedPrecondition,
			wantNoPersist:  true,
			wantTornReason: true, // caller maps ONLY this to different_owner
		},
		{
			// A different FailedPrecondition cause in the save window: another VCS is
			// already active, so resolveInstallation's single-VCS guard rejects BEFORE
			// the torn-state check and before any persist. This FailedPrecondition must
			// NOT carry the torn-state reason — the caller must surface it as save_failed,
			// not different_owner.
			name:           "SingleVCSGuardIsNotTornState",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			existingVCS:    []*sdk.ConnectorConfig{{ConnectorID: "gitlab", Enabled: true}},
			wantErr:        true,
			wantCode:       codes.FailedPrecondition,
			wantNoPersist:  true,
			wantTornReason: false, // a non-torn FailedPrecondition: no reason detail
			wantGuardTrips: true,  // single-VCS guard returns before the tenant directory lookup
		},
		{
			// A matching expected_account_id for an individual passes the guard and
			// persists normally.
			name:           "IndividualMatchingExpectedAccountSaves",
			installationID: "12345",
			orgID:          testOrgID,
			expectedAcctID: "583231",
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			tenantType: commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
		},
		{
			// An individual re-connect of an already-enabled
			// connector must NOT reset enabled. The handler routes through
			// SaveCredentialsPreserveEnabled (gomock fails the test if Save is called).
			name:           "IndividualReconnectPreservesEnabled",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			existingVCS: []*sdk.ConnectorConfig{{ConnectorID: "github", Enabled: true}},
			tenantType:  commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
		},
		{
			// An UNSPECIFIED/unknown tenant_type must NOT enable — preserve-enabled
			// upsert (lands disabled), never the full Save.
			name:           "UnspecifiedTenantTypeDefaultsDisabled",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231,
			},
			tenantType: commonv1.TenantType_TENANT_TYPE_UNSPECIFIED,
		},
		{
			name:           "OrgSaveFailsInternal",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult:    &intgithub.InstallationInfo{ID: 12345, AccountLogin: "test-org", AccountID: 1},
			tenantType:     commonv1.TenantType_TENANT_TYPE_ORGANIZATION,
			wantEnabled:    true,
			saveErr:        fmt.Errorf("db connection lost"),
			wantErr:        true,
			wantCode:       codes.Internal,
		},
		{
			name:           "IndividualSaveFailsInternal",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult: &intgithub.InstallationInfo{
				ID: 12345, AccountLogin: "octocat", AccountID: 583231, AccountType: "User",
			},
			tenantType: commonv1.TenantType_TENANT_TYPE_INDIVIDUAL,
			saveErr:    fmt.Errorf("db connection lost"),
			wantErr:    true,
			wantCode:   codes.Internal,
		},
		{
			// The save step re-fetches the installation; an InstallationNotFound on
			// re-fetch surfaces NotFound (still nothing persisted).
			name:           "InstallationNotFoundOnReFetch",
			installationID: "99999",
			orgID:          testOrgID,
			ghGetErr:       intgithub.ErrInstallationNotFound,
			wantErr:        true,
			wantCode:       codes.NotFound,
		},
		{
			// A tenant-type lookup failure during the save step must fail loud
			// (Unavailable) and persist nothing — resolveInstallation makes the same
			// tenant directory call on both steps, so the save step inherits the guard.
			name:           "TenantTypeLookupFailsLoud",
			installationID: "12345",
			orgID:          testOrgID,
			ghGetResult:    &intgithub.InstallationInfo{ID: 12345, AccountLogin: "octocat", AccountID: 583231},
			tenantTypeErr:  fmt.Errorf("mgmt down"),
			wantErr:        true,
			wantCode:       codes.Unavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ghClient := intgithubmocks.NewMockGitHubAppClient(ctrl)
			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			mgmt := sdkmocks.NewMockTenantDirectory(ctrl)

			// SaveInstallation re-fetches the installation (the resolve step's CSRF
			// state was already consumed). No state is supplied or checked here.
			ghClient.EXPECT().GetInstallation(gomock.Any(), tt.installationID).
				Return(tt.ghGetResult, tt.ghGetErr)

			if tt.ghGetErr == nil && tt.ghGetResult != nil {
				dao.EXPECT().ListByOrg(gomock.Any(), tt.orgID).Return(tt.existingVCS, nil)
				// The single-VCS guard rejects inside resolveInstallation, before the
				// tenant-type lookup, so the tenant directory is only reached when it passes.
				if !tt.wantGuardTrips {
					mgmt.EXPECT().TenantType(gomock.Any(), tt.orgID).
						Return(sdk.TenantKind(tt.tenantType), tt.tenantTypeErr)
				}
			}
			// A tenant-type lookup failure or a torn-state reject returns before any
			// persist, so the DAO write + avatar fetch are only reached on a clean
			// lookup that passes the guard.
			if tt.ghGetErr == nil && tt.ghGetResult != nil && tt.tenantTypeErr == nil && !tt.wantNoPersist {
				if tt.tenantType == commonv1.TenantType_TENANT_TYPE_ORGANIZATION {
					dao.EXPECT().SaveCredentialsEnableWithSettings(gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig, _ func(json.RawMessage) (json.RawMessage, error)) error {
							assert.Equal(t, tt.wantEnabled, cfg.Enabled,
								"saved config Enabled must follow tenant_type")
							return tt.saveErr
						})
				} else {
					dao.EXPECT().SaveCredentialsPreserveEnabled(gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, cfg *sdk.ConnectorConfig) error {
							assert.Equal(t, sdk.ConnectorID("github"), cfg.ConnectorID)
							assert.Equal(t, tt.orgID, cfg.OrganizationID)
							assert.False(t, cfg.Enabled,
								"non-org persist must not force Enabled on the struct")
							return tt.saveErr
						})
				}
				ghClient.EXPECT().GetOrgAvatarURL(gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
				if tt.saveErr == nil {
					dao.EXPECT().ResetHealthState(gomock.Any(), tt.orgID, sdk.ConnectorID("github")).Return(true, nil)
				}
			}

			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = rc.Close() })
			accountID := int64(1)
			if tt.ghGetResult != nil {
				accountID = tt.ghGetResult.AccountID
			}
			if tt.validatedAccountID != 0 {
				accountID = tt.validatedAccountID
			}
			receipt := seedInstallationValidation(t, rc, tt.orgID, tt.installationID, accountID)
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, GHClient: ghClient, Redis: rc, Tenants: mgmt, Registry: testRegistry()})

			_, err := svc.SaveInstallation(context.Background(), &connintv1.SaveInstallationRequest{
				InstallationId:    tt.installationID,
				OrgId:             tt.orgID,
				ExpectedAccountId: tt.expectedAcctID,
				ValidationToken:   receipt,
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
				if tt.wantCode == codes.FailedPrecondition {
					assert.Equal(t, tt.wantTornReason, hasTornStateReason(err),
						"only the torn-state reject carries the dedicated reason; other FailedPrecondition causes must not")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestConnectIntegration_GitHubInstall_MintsState guards the state-minting half
// of the GitHub-App install-binding CSRF fix: both entry points that kick off an
// install (the `github` VCS case and the `github-issues` case when GitHub is not
// yet connected) must embed a `state=` token in the redirect URL AND persist it
// in Redis bound to the requesting org — otherwise the callback's
// ValidateInstallation check has nothing to verify against and the CSRF guard is
// silently a no-op. The required path is `github-issues`; `github` is included
// because it mints through the same githubInstallURL helper.
func TestConnectIntegration_GitHubInstall_MintsState(t *testing.T) {
	tests := []struct {
		name        string
		connectorID string
		// setupDAO wires the per-connector DAO expectations that precede minting.
		setupDAO func(dao *daomocks.MockIConnectorConfigDAO)
	}{
		{
			// github-issues with GitHub not connected: Get("github") errors, so the
			// handler falls through to the install redirect. This is the path the
			// existing tests never exercised.
			name:        "GitHubIssues_GitHubNotConnected",
			connectorID: "github-issues",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
					Return(nil, fmt.Errorf("record not found"))
			},
		},
		{
			// github VCS case mints through the same helper; the VCS guard lists the
			// org's connectors first.
			name:        "GitHub",
			connectorID: "github",
			setupDAO: func(dao *daomocks.MockIConnectorConfigDAO) {
				dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			tt.setupDAO(dao)

			// github-issues is absent from the default test config's enabled map;
			// enable it so the off-switch guard doesn't reject before minting.
			cfg := testConfig()
			cfg.EnabledConnectors["github-issues"] = true

			svc := NewService(Deps{Cfg: cfg, ConnectorDAO: dao, Redis: rc, Registry: testRegistry()})

			resp, err := svc.ConnectIntegration(context.Background(), &connintv1.ConnectIntegrationRequest{
				ConnectorId: tt.connectorID,
				OrgId:       testOrgID,
			})
			require.NoError(t, err)

			// The redirect URL must carry a state token.
			redirectURL := resp.GetRedirectUrl()
			parsed, parseErr := url.Parse(redirectURL)
			require.NoError(t, parseErr)
			state := parsed.Query().Get("state")
			require.NotEmpty(t, state, "redirect URL must include a state query param: %s", redirectURL)

			// That token must exist in Redis under the key the callback reads, bound
			// to the requesting org — the binding the CSRF check verifies.
			stored, getErr := rc.Get(context.Background(), ghconfig.OAuthStateKey(state)).Result()
			require.NoError(t, getErr, "minted state must be stored at oauth_state:github:<token>")

			var stateData struct {
				OrgID string `json:"org_id"`
			}
			require.NoError(t, json.Unmarshal([]byte(stored), &stateData))
			assert.Equal(t, testOrgID, stateData.OrgID, "stored state must bind the request's org")
		})
	}
}

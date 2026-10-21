// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	connectormocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()
	os.Exit(testguard.Run(m))
}

// TestSlackChannelRefreshLockTTLExceedsBackgroundFetchBudget locks in the
// invariant that the singleflight lock TTL must outlast the longest
// background refresh it guards. TryLockRefresh's release() is an
// unconditional Del (not ownership-checked, see SlackChannelCache), so a TTL
// that could expire before the holder finishes lets a second refresh acquire
// the lock and then lose it to the first refresh's stale release — two
// background refreshes running concurrently instead of one.
func TestSlackChannelRefreshLockTTLExceedsBackgroundFetchBudget(t *testing.T) {
	assert.Greater(t, slackChannelRefreshLockTTL, slackChannelBackgroundFetchBudget)
}

func TestService_GetConnectorSyncStatus(t *testing.T) {
	synced := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		orgID      string
		setupDAO   func(*connectormocks.MockISyncRunDAO)
		wantCode   codes.Code
		wantStatus map[string]*connectorpb.ConnectorSyncStatus
	}{
		{
			name:     "empty org_id returns invalid argument",
			orgID:    "",
			wantCode: codes.InvalidArgument,
		},
		{
			name:  "DAO error returns internal",
			orgID: "org-1",
			setupDAO: func(m *connectormocks.MockISyncRunDAO) {
				m.EXPECT().GetConnectorSyncStatusByOrg(gomock.Any(), "org-1").Return(nil, fmt.Errorf("db down"))
			},
			wantCode: codes.Internal,
		},
		{
			name:  "translates each store.ConnectorSyncState to its wire enum, carrying the timestamp independently of state",
			orgID: "org-1",
			setupDAO: func(m *connectormocks.MockISyncRunDAO) {
				m.EXPECT().GetConnectorSyncStatusByOrg(gomock.Any(), "org-1").Return(map[sdk.ConnectorID]store.ConnectorSyncStatus{
					"github": {LastSuccessfulAt: &synced, State: store.ConnectorSyncStateSynced},
					"gitlab": {LastSuccessfulAt: nil, State: store.ConnectorSyncStateStillSyncing},
					// A FAILED connector can still carry a last-known-good timestamp
					// from before the current failure — the two fields are independent.
					"bitbucket": {LastSuccessfulAt: &synced, State: store.ConnectorSyncStateFailed},
				}, nil)
			},
			wantStatus: map[string]*connectorpb.ConnectorSyncStatus{
				"github":    {ConnectorId: "github", LastSuccessfulSyncAt: timestamppb.New(synced), SyncState: connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_SYNCED},
				"gitlab":    {ConnectorId: "gitlab", LastSuccessfulSyncAt: nil, SyncState: connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_STILL_SYNCING},
				"bitbucket": {ConnectorId: "bitbucket", LastSuccessfulSyncAt: timestamppb.New(synced), SyncState: connectorpb.ConnectorSyncState_CONNECTOR_SYNC_STATE_FAILED},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDAO := connectormocks.NewMockISyncRunDAO(ctrl)
			svc := &Service{SyncRunDAO: mockDAO}

			if tt.setupDAO != nil {
				tt.setupDAO(mockDAO)
			}

			resp, err := svc.GetConnectorSyncStatus(context.Background(), &connectorpb.GetConnectorSyncStatusRequest{OrgId: tt.orgID})

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)
			got := make(map[string]*connectorpb.ConnectorSyncStatus, len(resp.GetStatuses()))
			for _, st := range resp.GetStatuses() {
				got[st.GetConnectorId()] = st
			}
			require.Len(t, got, len(tt.wantStatus))
			for connectorID, want := range tt.wantStatus {
				gotSt, ok := got[connectorID]
				require.True(t, ok, "missing status for connector %s", connectorID)
				assert.Equal(t, want.GetSyncState(), gotSt.GetSyncState())
				if want.GetLastSuccessfulSyncAt() == nil {
					assert.Nil(t, gotSt.GetLastSuccessfulSyncAt())
				} else {
					require.NotNil(t, gotSt.GetLastSuccessfulSyncAt())
					assert.True(t, want.GetLastSuccessfulSyncAt().AsTime().Equal(gotSt.GetLastSuccessfulSyncAt().AsTime()))
				}
			}
		})
	}
}

// TestService_GetConnectorSyncStatus_DeterministicOrder pins that the
// response is sorted by connector_id — the DAO returns a Go map, whose
// range order is randomized, so an unsorted response would flake any
// consumer or test asserting on order across identical calls.
func TestService_GetConnectorSyncStatus_DeterministicOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDAO := connectormocks.NewMockISyncRunDAO(ctrl)
	svc := &Service{SyncRunDAO: mockDAO}

	mockDAO.EXPECT().GetConnectorSyncStatusByOrg(gomock.Any(), "org-1").Return(map[sdk.ConnectorID]store.ConnectorSyncStatus{
		"slack":  {},
		"github": {},
		"jira":   {},
	}, nil)

	resp, err := svc.GetConnectorSyncStatus(context.Background(), &connectorpb.GetConnectorSyncStatusRequest{OrgId: "org-1"})
	require.NoError(t, err)

	var gotIDs []string
	for _, st := range resp.GetStatuses() {
		gotIDs = append(gotIDs, st.GetConnectorId())
	}
	assert.Equal(t, []string{"github", "jira", "slack"}, gotIDs)
}

func TestListEnabledConfigs(t *testing.T) {
	tests := []struct {
		name        string
		connectorID string
		setupDAO    func(*connectormocks.MockIConnectorConfigDAO)
		wantCode    codes.Code
		wantOrgIDs  []string
	}{
		{
			name:        "lists the enabled organizations",
			connectorID: "github",
			setupDAO: func(m *connectormocks.MockIConnectorConfigDAO) {
				m.EXPECT().ListEnabledOrganizations(gomock.Any(), sdk.ConnectorID("github")).Return([]string{"org-1", "org-3"}, nil)
			},
			wantOrgIDs: []string{"org-1", "org-3"},
		},
		{
			name:        "empty result",
			connectorID: "slack",
			setupDAO: func(m *connectormocks.MockIConnectorConfigDAO) {
				m.EXPECT().ListEnabledOrganizations(gomock.Any(), sdk.ConnectorID("slack")).Return(nil, nil)
			},
			wantOrgIDs: []string{},
		},
		{
			name:        "empty connector ID returns invalid argument",
			connectorID: "",
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "DAO error returns internal",
			connectorID: "github",
			setupDAO: func(m *connectormocks.MockIConnectorConfigDAO) {
				m.EXPECT().ListEnabledOrganizations(gomock.Any(), sdk.ConnectorID("github")).Return(nil, fmt.Errorf("db connection failed"))
			},
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
			svc := &Service{ConfigDAO: mockDAO}

			if tt.setupDAO != nil {
				tt.setupDAO(mockDAO)
			}

			resp, err := svc.ListEnabledConfigs(context.Background(), &connectorpb.ListEnabledConfigsRequest{
				ConnectorId: tt.connectorID,
			})

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)

			gotOrgIDs := make([]string, len(resp.Configs))
			for i, c := range resp.Configs {
				gotOrgIDs[i] = c.OrganizationId
			}
			assert.Equal(t, tt.wantOrgIDs, gotOrgIDs)
		})
	}
}

// TestGetVCSTrackedBranches locks in the resolution semantics — per-repo
// override wins over tracked_branch default; non-overridden repos take the
// default; repos with no effective branch are dropped. The handler owns
// the resolution so consumers can apply a single tuple-match predicate
// without re-implementing resolveEffectiveBranch.
func TestGetVCSTrackedBranches(t *testing.T) {
	type pair struct {
		repo, branch string
		all          bool
	}
	tests := []struct {
		name        string
		settingsRaw string // JSON for ConnectorConfig.Settings
		daoErr      error  // when set, ConfigDAO.Get returns this error
		wantCode    codes.Code
		wantPairs   []pair
	}{
		{
			name: "tracked default applied to all discovered repos when no overrides",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"tracked_branch": "main"
			}`,
			wantPairs: []pair{
				{repo: "acme/api", branch: "main"},
				{repo: "acme/web", branch: "main"},
			},
		},
		{
			name: "per-repo override wins over tracked default",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"tracked_branch": "main",
				"repo_branch_overrides": {"acme/api": "develop"}
			}`,
			wantPairs: []pair{
				{repo: "acme/api", branch: "develop"},
				{repo: "acme/web", branch: "main"},
			},
		},
		{
			name: "no tracked default + override drops non-overridden repos",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"repo_branch_overrides": {"acme/api": "develop"}
			}`,
			wantPairs: []pair{
				{repo: "acme/api", branch: "develop"},
			},
		},
		{
			name: "no tracked default and no overrides returns empty",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}]
			}`,
			wantPairs: nil,
		},
		{
			name: "empty override value falls through to tracked default",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"tracked_branch": "main",
				"repo_branch_overrides": {"acme/api": ""}
			}`,
			wantPairs: []pair{
				{repo: "acme/api", branch: "main"},
				{repo: "acme/web", branch: "main"},
			},
		},
		{
			name: "empty discovered_repos returns empty even when tracked is set",
			settingsRaw: `{
				"discovered_repos": [],
				"tracked_branch": "main"
			}`,
			wantPairs: nil,
		},
		{
			// DELETED entries are skipped so analytics RPCs that consume
			// GetVCSTrackedBranches don't keep querying stopped-syncing
			// repos. Empty / *_ACTIVE / *_UNSPECIFIED remain in scope.
			name: "DELETED status skipped; ACTIVE + no status included",
			settingsRaw: `{
				"discovered_repos": [
					{"full_name": "acme/active", "status": "BITBUCKET_REPO_STATUS_ACTIVE"},
					{"full_name": "acme/gone", "status": "BITBUCKET_REPO_STATUS_DELETED"},
					{"full_name": "acme/nostatus"}
				],
				"tracked_branch": "main"
			}`,
			wantPairs: []pair{
				{repo: "acme/active", branch: "main"},
				{repo: "acme/nostatus", branch: "main"},
			},
		},
		{
			name: "malformed settings JSON returns empty without error",
			// Handler treats unmarshal failure as "no policy" rather than
			// failing the whole analytics request — defense in depth.
			settingsRaw: `{not json`,
			wantPairs:   nil,
		},
		{
			name: "track_all_branches applies all-branches policy to every discovered repo",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"track_all_branches": true
			}`,
			wantPairs: []pair{
				{repo: "acme/api", all: true},
				{repo: "acme/web", all: true},
			},
		},
		{
			name: "repo_all_branches wins over a per-repo branch override and org tracked default",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"tracked_branch": "main",
				"repo_branch_overrides": {"acme/api": "develop"},
				"repo_all_branches": ["acme/api"]
			}`,
			wantPairs: []pair{
				{repo: "acme/api", all: true},
				{repo: "acme/web", branch: "main"},
			},
		},
		{
			name: "per-repo branch override wins over org track_all_branches",
			settingsRaw: `{
				"discovered_repos": [{"full_name": "acme/web"}, {"full_name": "acme/api"}],
				"track_all_branches": true,
				"repo_branch_overrides": {"acme/api": "develop"}
			}`,
			wantPairs: []pair{
				{repo: "acme/api", branch: "develop"},
				{repo: "acme/web", all: true},
			},
		},
		{
			name:     "DAO returns gorm.ErrRecordNotFound → NotFound (config genuinely missing)",
			daoErr:   gorm.ErrRecordNotFound,
			wantCode: codes.NotFound,
		},
		{
			name:     "DAO returns generic error → Internal (transient DB / RLS failure, not silent NotFound)",
			daoErr:   fmt.Errorf("connection refused"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			mockDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
			svc := &Service{ConfigDAO: mockDAO}

			if tt.daoErr != nil {
				mockDAO.EXPECT().Get(gomock.Any(), "org-1", sdk.ConnectorID("github")).
					Return(nil, tt.daoErr)
			} else {
				mockDAO.EXPECT().Get(gomock.Any(), "org-1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{
						ConnectorID:    "github",
						OrganizationID: "org-1",
						Settings:       json.RawMessage(tt.settingsRaw),
					}, nil)
			}

			resp, err := svc.GetVCSTrackedBranches(context.Background(), &connectorpb.GetVCSTrackedBranchesRequest{
				OrgId:       "org-1",
				ConnectorId: "github",
			})

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)

			gotPairs := make([]pair, 0, len(resp.GetRepoBranches()))
			for _, p := range resp.GetRepoBranches() {
				gotPairs = append(gotPairs, pair{repo: p.GetRepoFullName(), branch: p.GetBaseBranch(), all: p.GetAllBranches()})
			}
			// repo_branches is a set, not ordered — ElementsMatch ignores order
			// and treats nil and empty as equivalent.
			assert.ElementsMatch(t, tt.wantPairs, gotPairs)
		})
	}
}

// TestGetDiscoveredRepos locks in the cross-vendor read filter: any entry
// whose Status ends in "_DELETED" is excluded (matches the bitbucket
// IsActive() policy applied internally in fetchAllRepos). Empty /
// "*_ACTIVE" / "*_UNSPECIFIED" all stay in scope.
func TestGetDiscoveredRepos(t *testing.T) {
	tests := []struct {
		name        string
		settingsRaw string
		daoErr      error
		wantCode    codes.Code
		wantNames   []string
	}{
		{
			name: "all active repos returned",
			settingsRaw: `{
				"discovered_repos": [
					{"full_name": "acme/web", "status": "BITBUCKET_REPO_STATUS_ACTIVE"},
					{"full_name": "acme/api", "status": "BITBUCKET_REPO_STATUS_ACTIVE"}
				]
			}`,
			wantNames: []string{"acme/api", "acme/web"},
		},
		{
			name: "DELETED skipped; ACTIVE + no status included",
			settingsRaw: `{
				"discovered_repos": [
					{"full_name": "acme/active", "status": "BITBUCKET_REPO_STATUS_ACTIVE"},
					{"full_name": "acme/gone", "status": "BITBUCKET_REPO_STATUS_DELETED"},
					{"full_name": "acme/nostatus"}
				]
			}`,
			wantNames: []string{"acme/active", "acme/nostatus"},
		},
		{
			name: "UNSPECIFIED treated as active (the value of an entry with no status)",
			settingsRaw: `{
				"discovered_repos": [
					{"full_name": "acme/web", "status": "BITBUCKET_REPO_STATUS_UNSPECIFIED"}
				]
			}`,
			wantNames: []string{"acme/web"},
		},
		{
			name: "empty full_name skipped even when active",
			settingsRaw: `{
				"discovered_repos": [
					{"full_name": "", "status": "BITBUCKET_REPO_STATUS_ACTIVE"},
					{"full_name": "acme/web", "status": "BITBUCKET_REPO_STATUS_ACTIVE"}
				]
			}`,
			wantNames: []string{"acme/web"},
		},
		{
			name:        "empty discovered_repos returns empty",
			settingsRaw: `{"discovered_repos": []}`,
			wantNames:   []string{},
		},
		{
			// Handler treats unmarshal failure as "no policy" rather than
			// failing the whole analytics request — defense in depth.
			name:        "malformed settings JSON returns empty without error",
			settingsRaw: `{not json`,
			wantNames:   []string{},
		},
		{
			name:     "DAO returns gorm.ErrRecordNotFound → NotFound",
			daoErr:   gorm.ErrRecordNotFound,
			wantCode: codes.NotFound,
		},
		{
			name:     "DAO returns generic error → Internal",
			daoErr:   fmt.Errorf("connection refused"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			mockDAO := connectormocks.NewMockIConnectorConfigDAO(ctrl)
			svc := &Service{ConfigDAO: mockDAO}

			if tt.daoErr != nil {
				mockDAO.EXPECT().Get(gomock.Any(), "org-1", sdk.ConnectorID("bitbucket")).
					Return(nil, tt.daoErr)
			} else {
				mockDAO.EXPECT().Get(gomock.Any(), "org-1", sdk.ConnectorID("bitbucket")).
					Return(&sdk.ConnectorConfig{
						ConnectorID:    "bitbucket",
						OrganizationID: "org-1",
						Settings:       json.RawMessage(tt.settingsRaw),
					}, nil)
			}

			resp, err := svc.GetDiscoveredRepos(context.Background(), &connectorpb.GetDiscoveredReposRequest{
				OrgId:       "org-1",
				ConnectorId: "bitbucket",
			})

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantNames, resp.GetRepoNames())
		})
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	intmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/hooks"

	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestUpdateVCSSettings_BranchScopeChange proves a branch-scope change triggers
// a PR-count reprocess and NEVER resets sync cursors (no re-fetch, zero vendor
// calls); a non-branch change does neither. The cursor-reset path
// (DeleteByConnector) is asserted absent on every branch transition — that is
// the "no re-fetch" guarantee.
func TestUpdateVCSSettings_BranchScopeChange(t *testing.T) {
	t.Parallel()
	const connectorID = connectorIDGitHub

	tests := []struct {
		name          string
		oldSettings   string
		reqSettings   *connintv1.VCSSettings
		wantReprocess bool
		wantDiffReset bool  // ResetPRDiffAnalysisStatus — the orthogonal exclude-pattern path
		reprocessErr  error // the settings hook returns this; settings save must still succeed
	}{
		{
			name:          "tracked-branch retarget triggers reprocess",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "develop"},
			wantReprocess: true,
		},
		{
			name:          "track-all-branches toggle ON triggers reprocess",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "main", TrackAllBranches: true},
			wantReprocess: true,
		},
		{
			name:          "track-all-branches toggle OFF (narrowing) triggers reprocess",
			oldSettings:   `{"tracked_branch":"main","track_all_branches":true}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "main"},
			wantReprocess: true,
		},
		{
			name:          "per-repo all-branches set change triggers reprocess",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "main", RepoAllBranches: []string{"org/repo-a"}},
			wantReprocess: true,
		},
		{
			name:          "no branch change does NOT trigger reprocess",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "main"},
			wantReprocess: false,
		},
		{
			name:          "only file-exclude change resets diff analysis, NOT PR counts",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "main", FileExcludePatterns: []string{"*.lock"}},
			wantReprocess: false,
			wantDiffReset: true,
		},
		{
			name:          "reprocess RPC error is non-fatal; settings still save",
			oldSettings:   `{"tracked_branch":"main"}`,
			reqSettings:   &connintv1.VCSSettings{TrackedBranch: "develop"},
			wantReprocess: true,
			reprocessErr:  errors.New("host unavailable"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			connDAO := daomocks.NewMockIConnectorConfigDAO(ctrl)
			syncDAO := daomocks.NewMockISyncStateDAO(ctrl)
			hook := intmocks.NewMockSettingsChangeHook(ctrl)

			connDAO.EXPECT().
				Get(gomock.Any(), testOrgID, sdk.ConnectorID(connectorID)).
				Return(&sdk.ConnectorConfig{
					ConnectorID:    sdk.ConnectorID(connectorID),
					OrganizationID: testOrgID,
					Settings:       []byte(tt.oldSettings),
				}, nil)
			connDAO.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID(connectorID), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, m func(json.RawMessage) (json.RawMessage, error)) error {
					_, err := m([]byte(tt.oldSettings))
					return err
				})

			// The "no re-fetch" guarantee: cursors are never reset on a settings
			// change. If DeleteByConnector is ever called, gomock fails the test.
			syncDAO.EXPECT().
				DeleteByConnector(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(0)

			if tt.wantReprocess {
				hook.EXPECT().
					ReprocessConnectorPRCounts(gomock.Any(), &workqueuev1.ReprocessConnectorPRCountsRequest{
						OrganizationId: testOrgID,
						ConnectorId:    connectorID,
					}).
					Return(&workqueuev1.ReprocessConnectorPRCountsResponse{RowsReset: 7}, tt.reprocessErr).
					Times(1)
			} else {
				hook.EXPECT().ReprocessConnectorPRCounts(gomock.Any(), gomock.Any()).Times(0)
			}

			// ResetPRDiffAnalysisStatus is orthogonal: it fires on a file-exclude
			// change, never on a branch change. Asserting it absent on branch-only
			// changes guards against re-coupling the two paths.
			if tt.wantDiffReset {
				hook.EXPECT().
					ResetPRDiffAnalysisStatus(gomock.Any(), gomock.Any()).
					Return(&workqueuev1.ResetPRDiffAnalysisStatusResponse{}, nil).
					Times(1)
			} else {
				hook.EXPECT().ResetPRDiffAnalysisStatus(gomock.Any(), gomock.Any()).Times(0)
			}

			// All vendor clients are nil: the reprocess path makes no vendor call,
			// so the test structurally cannot re-fetch.
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: connDAO, SyncStateDAO: syncDAO, Registry: testRegistry(), SettingsHook: hook})

			_, err := svc.UpdateVCSSettings(context.Background(), &connintv1.UpdateVCSSettingsRequest{
				OrgId:       testOrgID,
				ConnectorId: connectorID,
				Settings:    tt.reqSettings,
			})
			require.NoError(t, err, "settings save must succeed even when the reprocess RPC errors")
		})
	}
}

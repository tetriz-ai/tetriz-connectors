// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"sync"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// fakeOperationRunner records the RunOperation invocation and signals done once,
// so a test can wait on the backgrounded kickoff goroutine deterministically.
type fakeOperationRunner struct {
	mu      sync.Mutex
	called  bool
	orgID   string
	connID  sdk.ConnectorID
	opID    sdk.OperationID
	trigger string
	done    chan struct{}
}

func (f *fakeOperationRunner) RunOperation(_ context.Context, orgID string, connID sdk.ConnectorID, opID sdk.OperationID, _ sdk.TenantKind, trigger string) error {
	f.mu.Lock()
	f.called, f.orgID, f.connID, f.opID, f.trigger = true, orgID, connID, opID, trigger
	f.mu.Unlock()
	close(f.done)
	return nil
}

func nopGatewayFactory(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
	return nil, nil
}

// TestKickoffInitialRepoSync guards the on-connect repo-discovery seed: with a
// runner and a gateway factory it runs the repo-discovery op tagged "on_connect";
// with no gateway factory (local/dev) or no runner it is a silent no-op — a
// connect must never fail on the best-effort kickoff.
func TestKickoffInitialRepoSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		withRunner  bool
		withGateway bool
		wantCalled  bool
	}{
		{name: "runner and gateway set — seeds repo discovery on connect", withRunner: true, withGateway: true, wantCalled: true},
		{name: "no gateway factory (local/dev) — no-op", withRunner: true, withGateway: false, wantCalled: false},
		{name: "no runner — no-op, no panic", withRunner: false, withGateway: true, wantCalled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeOperationRunner{done: make(chan struct{})}
			s := &Service{}
			if tt.withRunner {
				s.operationRunner = runner
			}
			if tt.withGateway {
				s.gatewayFactory = nopGatewayFactory
			}

			s.kickoffInitialRepoSync(context.Background(), "org-1",
				sdk.ConnectorID(connectorIDGitHub),
				sdk.OperationID(githubRepoDiscoveryOp),
				commonv1.TenantType_TENANT_TYPE_ORGANIZATION)

			if tt.wantCalled {
				select {
				case <-runner.done:
				case <-time.After(2 * time.Second):
					t.Fatal("RunOperation was not invoked within the timeout")
				}
				runner.mu.Lock()
				defer runner.mu.Unlock()
				assert.True(t, runner.called)
				assert.Equal(t, "org-1", runner.orgID)
				assert.Equal(t, sdk.ConnectorID(connectorIDGitHub), runner.connID)
				assert.Equal(t, sdk.OperationID(githubRepoDiscoveryOp), runner.opID)
				assert.Equal(t, "on_connect", runner.trigger)
				return
			}

			select {
			case <-runner.done:
				t.Fatal("RunOperation should not have been invoked")
			case <-time.After(200 * time.Millisecond):
			}
			runner.mu.Lock()
			defer runner.mu.Unlock()
			assert.False(t, runner.called)
		})
	}
}

// TestConfirmConnectorReposKicksOffRepoDiscovery guards the call-site wiring: a
// successful ConfirmConnectorRepos (the enable-moment for individual and PAT
// connects) resolves tenant_type and fires the repo-discovery kickoff. This is
// the path SaveInstallation's ORGANIZATION branch does NOT cover.
func TestConfirmConnectorReposKicksOffRepoDiscovery(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().EnableConnectorWithSettings(gomock.Any(), "org-1", sdk.ConnectorID(connectorIDGitHub), gomock.Any()).Return(nil)

	mgmt := sdkmocks.NewMockTenantDirectory(ctrl)
	mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
		Return(sdk.TenantKind(commonv1.TenantType_TENANT_TYPE_ORGANIZATION), nil)

	runner := &fakeOperationRunner{done: make(chan struct{})}
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry(), Tenants: mgmt})
	// White-box: swap in a fake runner and a non-nil gateway factory so the
	// best-effort kickoff actually fires (NewService builds a real orchestrator).
	svc.operationRunner = runner
	svc.gatewayFactory = nopGatewayFactory

	_, err := svc.ConfirmConnectorRepos(context.Background(), &connintv1.ConfirmConnectorReposRequest{
		OrgId:         "org-1",
		ConnectorId:   connectorIDGitHub,
		RepoFullNames: []string{"acme/repo"},
	})
	require.NoError(t, err)

	select {
	case <-runner.done:
	case <-time.After(2 * time.Second):
		t.Fatal("ConfirmConnectorRepos did not kick off repo discovery")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	assert.Equal(t, "org-1", runner.orgID)
	assert.Equal(t, sdk.ConnectorID(connectorIDGitHub), runner.connID)
	assert.Equal(t, sdk.OperationID(githubRepoDiscoveryOp), runner.opID)
	assert.Equal(t, "on_connect", runner.trigger)
}

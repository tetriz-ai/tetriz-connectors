// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	intgithub "github.com/tetriz-ai/tetriz-connectors/auth/integrations/github"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	intgithubmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/auth/integrations/github"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSaveInstallationRequiresValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	gh := intgithubmocks.NewMockGitHubAppClient(ctrl)
	svc := NewService(Deps{Cfg: testConfig(), GHClient: gh, Registry: testRegistry()})
	_, err := svc.SaveInstallation(context.Background(), &connintv1.SaveInstallationRequest{OrgId: testOrgID, InstallationId: "777"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func seedInstallationValidation(t *testing.T, rc *redis.Client, org, id string, account int64) string {
	t.Helper()
	token := strings.Repeat("a", 64)
	value, err := json.Marshal(installationValidation{OrgID: org, InstallationID: id, AccountID: account})
	require.NoError(t, err)
	require.NoError(t, rc.Set(context.Background(), installationValidationPrefix+token, value, time.Minute).Err())
	return token
}
func TestInstallationValidationRejectsWrongBindingAndReplay(t *testing.T) {
	for _, tt := range []struct {
		name, org, id string
		expire        bool
	}{
		{name: "wrong org", org: "other", id: "777"}, {name: "wrong installation", org: testOrgID, id: "778"}, {name: "expired", org: testOrgID, id: "777", expire: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = rc.Close() })
			token := seedInstallationValidation(t, rc, testOrgID, "777", 123)
			if tt.expire {
				mr.FastForward(2 * time.Minute)
			}
			svc := NewService(Deps{Cfg: testConfig(), Redis: rc, Registry: testRegistry()})
			_, err := svc.SaveInstallation(context.Background(), &connintv1.SaveInstallationRequest{OrgId: tt.org, InstallationId: tt.id, ValidationToken: token})
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = svc.consumeInstallationValidation(context.Background(), token, testOrgID, "777")
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}
func TestInstallationValidationIsSingleUse(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	svc := NewService(Deps{Cfg: testConfig(), Redis: rc, Registry: testRegistry()})
	token, err := svc.issueInstallationValidation(context.Background(), installationValidation{OrgID: testOrgID, InstallationID: "777", AccountID: 123})
	require.NoError(t, err)
	_, err = svc.consumeInstallationValidation(context.Background(), token, testOrgID, "777")
	require.NoError(t, err)
	_, err = svc.consumeInstallationValidation(context.Background(), token, testOrgID, "777")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestInstallationValidationConcurrentConsumption(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	svc := NewService(Deps{Cfg: testConfig(), Redis: rc, Registry: testRegistry()})
	token := seedInstallationValidation(t, rc, testOrgID, "777", 123)
	var wg sync.WaitGroup
	var successes atomic.Int32
	start := make(chan struct{})
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.consumeInstallationValidation(context.Background(), token, testOrgID, "777")
			if err == nil {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int32(1), successes.Load())
}

func TestValidatedIndividualAccountChangeKeepsTornStateReason(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	gh := intgithubmocks.NewMockGitHubAppClient(ctrl)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	tenants := sdkmocks.NewMockTenantDirectory(ctrl)
	original := &intgithub.InstallationInfo{ID: 777, AccountID: 123, AccountLogin: "owner", AccountType: "User"}
	changed := &intgithub.InstallationInfo{ID: 777, AccountID: 456, AccountLogin: "other", AccountType: "User"}
	gomock.InOrder(gh.EXPECT().GetInstallation(gomock.Any(), "777").Return(original, nil), gh.EXPECT().GetInstallation(gomock.Any(), "777").Return(changed, nil))
	gh.EXPECT().VerifyUserInstallation(gomock.Any(), "callback-code", "777", original).Return(nil)
	dao.EXPECT().ListByOrg(gomock.Any(), testOrgID).Return(nil, nil).Times(2)
	tenants.EXPECT().TenantType(gomock.Any(), testOrgID).Return(sdk.TenantKindIndividual, nil).Times(2)
	svc := NewService(Deps{Cfg: testConfig(), Redis: rc, GHClient: gh, ConnectorDAO: dao, Tenants: tenants, Registry: testRegistry()})
	seedGitHubState(t, rc, "state", testOrgID)
	resolved, err := svc.ValidateInstallation(ctx, &connintv1.ValidateInstallationRequest{OrgId: testOrgID, InstallationId: "777", State: "state", Code: "callback-code"})
	require.NoError(t, err)
	_, err = svc.SaveInstallation(ctx, &connintv1.SaveInstallationRequest{OrgId: testOrgID, InstallationId: "777", ValidationToken: resolved.GetValidationToken(), ExpectedAccountId: resolved.GetAccountId()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.True(t, hasTornStateReason(err))
}

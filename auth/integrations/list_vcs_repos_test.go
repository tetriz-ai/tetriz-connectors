// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	commonv1 "github.com/tetriz-ai/tetriz-connectors/gen/common/v1"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ghRepoJSON builds the minimal GitHub REST repo object the fresh listing parses.
func ghRepoJSON(t *testing.T, id int64, fullName, defaultBranch string, private bool) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"id":             id,
		"full_name":      fullName,
		"default_branch": defaultBranch,
		"private":        private,
	})
	require.NoError(t, err)
	return b
}

// newReposDelegate wires a Service with a connector DAO, a gateway factory that
// returns the supplied VendorGateway, and a management client — the three deps
// the fresh ListVCSRepos path exercises.
func newReposDelegate(t *testing.T, gw sdk.VendorGateway) (*Service, *daomocks.MockIConnectorConfigDAO, *fwmocks.MockTenantDirectory) {
	t.Helper()
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	mgmt := fwmocks.NewMockTenantDirectory(ctrl)
	factory := sdk.VendorGatewayFactory(func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return gw, nil
	})
	svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Tenants: mgmt, Registry: testRegistry(), GatewayFactory: factory})
	return svc, dao, mgmt
}

func TestListVCSRepos_Fresh(t *testing.T) {
	t.Parallel()

	t.Run("fresh=true individual lists installation repos live and warms the cache", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// App + INDIVIDUAL → GET /installation/repositories.
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.ListInstallationReposRequest{})).
			Return(proto.Message(&vgpb.ListInstallationReposResponse{
				Repos: [][]byte{
					ghRepoJSON(t, 1, "octo/alpha", "main", false),
					ghRepoJSON(t, 2, "octo/beta", "trunk", true),
				},
				HasNextPage: false,
			}), nil)

		svc, dao, mgmt := newReposDelegate(t, gw)
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: "67890"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{
				ConnectorID:    "github",
				OrganizationID: testOrgID,
				Credentials:    creds,
				Settings:       settings,
			}, nil)
		mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
			Return(sdk.TenantKind(commonv1.TenantType_TENANT_TYPE_INDIVIDUAL), nil)

		// The live set is written back into discovered_repos via PatchSettings (a
		// settings-only patch of the live row). The mutator must merge into the
		// current raw settings, preserving keys that are not GitHubSettings fields
		// (e.g. the admin file_exclude_patterns), and set the live discovered_repos.
		dao.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID("github"), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, mut func(json.RawMessage) (json.RawMessage, error)) error {
				current := json.RawMessage(`{"include_private":true,"file_exclude_patterns":["*.md"]}`)
				out, err := mut(current)
				require.NoError(t, err)
				var merged map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(out, &merged))
				assert.JSONEq(t, `["*.md"]`, string(merged["file_exclude_patterns"]), "admin settings key must survive the warm")
				var discovered []ghconfig.GitHubDiscoveredRepo
				require.NoError(t, json.Unmarshal(merged["discovered_repos"], &discovered))
				require.Len(t, discovered, 2)
				assert.Equal(t, "octo/alpha", discovered[0].FullName)
				assert.Equal(t, "octo/beta", discovered[1].FullName)
				assert.True(t, discovered[1].Private)
				return nil
			})

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 2)
		assert.Equal(t, "octo/alpha", resp.GetRepos()[0].GetFullName())
		assert.Equal(t, "octo/beta", resp.GetRepos()[1].GetFullName())
		// An App (installation) credential has no PAT approval gate → UNSPECIFIED
		// (a client treats it as granted).
		assert.Equal(t, connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_UNSPECIFIED, resp.GetGrantStatus())
	})

	t.Run("fresh=true warms discovered_repos even with an explicit tracked list (edit-repos path)", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// The picker lists all accessible repos fresh, including octo/gamma which is
		// NOT in the current tracked set — the "Edit tracked repos" case adding a repo.
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.ListUserReposRequest{})).
			Return(proto.Message(&vgpb.ListUserReposResponse{
				Repos: [][]byte{
					ghRepoJSON(t, 11, "me/alpha", "main", true),
					ghRepoJSON(t, 22, "me/gamma", "trunk", true),
				},
				HasNextPage: false,
			}), nil)

		svc, dao, _ := newReposDelegate(t, gw)
		// An already-connected individual tracks only me/alpha (explicit list); the
		// cache is warmed anyway, so a freshly-added repo is cached.
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true, Repositories: []string{"me/alpha"}})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{PersonalAccessToken: "ghp_xxx"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		// The warm MUST run despite the explicit list, caching both repos' numeric IDs
		// + default branches so a later edit-save sync can key fetch_repo_tree records.
		dao.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID("github"), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, mut func(json.RawMessage) (json.RawMessage, error)) error {
				out, err := mut(json.RawMessage(`{"include_private":true,"repositories":["me/alpha"]}`))
				require.NoError(t, err)
				var merged map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(out, &merged))
				// The tracked selection is untouched; only the metadata cache is warmed.
				assert.JSONEq(t, `["me/alpha"]`, string(merged["repositories"]), "warm must not touch the tracked selection")
				var discovered []ghconfig.GitHubDiscoveredRepo
				require.NoError(t, json.Unmarshal(merged["discovered_repos"], &discovered))
				require.Len(t, discovered, 2)
				byName := map[string]ghconfig.GitHubDiscoveredRepo{}
				for _, d := range discovered {
					byName[d.FullName] = d
				}
				assert.Equal(t, int64(22), byName["me/gamma"].DatabaseID, "newly-available repo's numeric ID must be cached for tree keying")
				assert.Equal(t, "trunk", byName["me/gamma"].DefaultBranch)
				return nil
			})

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 2)
	})

	t.Run("fresh=true PAT lists user repos (no tenant_type lookup)", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// PAT → GET /user/repos, regardless of tenant_type.
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.ListUserReposRequest{})).
			Return(proto.Message(&vgpb.ListUserReposResponse{
				Repos:       [][]byte{ghRepoJSON(t, 7, "me/solo", "main", false)},
				HasNextPage: false,
			}), nil)

		svc, dao, mgmt := newReposDelegate(t, gw)
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{PersonalAccessToken: "ghp_xxx"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		// A PAT ignores tenant_type, so the tenant directory must NOT be called — gomock
		// fails the test on an unexpected call.
		_ = mgmt
		dao.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID("github"), gomock.Any()).Return(nil)

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 1)
		assert.Equal(t, "me/solo", resp.GetRepos()[0].GetFullName())
		// A fine-grained PAT listing classifies GRANTED (an empty personal account
		// would too — pending is not inferrable from /user/repos alone; see
		// grantStatusForListing).
		assert.Equal(t, connintv1.ConnectorGrantStatus_CONNECTOR_GRANT_STATUS_GRANTED, resp.GetGrantStatus())
	})

	t.Run("fresh=false returns the cached set without any live call or warm", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		// No gateway interaction is expected on the cached path — an unexpected
		// Execute/ForGroup fails the test.
		gw := fwmocks.NewMockVendorGateway(ctrl)

		svc, dao, _ := newReposDelegate(t, gw)
		settings, err := json.Marshal(ghconfig.GitHubSettings{
			IncludePrivate: true,
			DiscoveredRepos: []ghconfig.GitHubDiscoveredRepo{
				{FullName: "octo/cached", DefaultBranch: "main"},
			},
		})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: "67890"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		// dao.PatchSettings must NOT be called on the cached path (gomock enforces).

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: false,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 1)
		assert.Equal(t, "octo/cached", resp.GetRepos()[0].GetFullName())
	})

	t.Run("marks tracked from settings.repositories so the edit picker pre-checks the synced set", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		// Cached path — no gateway interaction expected.
		gw := fwmocks.NewMockVendorGateway(ctrl)

		svc, dao, _ := newReposDelegate(t, gw)
		// Three discovered repos; only alpha + gamma are in the tracked set.
		settings, err := json.Marshal(ghconfig.GitHubSettings{
			IncludePrivate: true,
			Repositories:   []string{"octo/alpha", "octo/gamma"},
			DiscoveredRepos: []ghconfig.GitHubDiscoveredRepo{
				{FullName: "octo/alpha", DefaultBranch: "main"},
				{FullName: "octo/beta", DefaultBranch: "main"},
				{FullName: "octo/gamma", DefaultBranch: "main"},
			},
		})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: "67890"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: false,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 3)
		tracked := map[string]bool{}
		for _, r := range resp.GetRepos() {
			tracked[r.GetFullName()] = r.GetTracked()
		}
		assert.True(t, tracked["octo/alpha"], "alpha is in settings.repositories → tracked")
		assert.False(t, tracked["octo/beta"], "beta is discovered but not tracked")
		assert.True(t, tracked["octo/gamma"], "gamma is in settings.repositories → tracked")
	})

	t.Run("fresh=false cold cache (PAT) lists user repos live and warms — no installation_id error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		// A PAT connector with an empty discovered_repos cache must fall back to the
		// credential-correct GET /user/repos endpoint — not the App installation
		// listing, which would error with "parse installation_id: invalid syntax".
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.ListUserReposRequest{})).
			Return(proto.Message(&vgpb.ListUserReposResponse{
				Repos:       [][]byte{ghRepoJSON(t, 9, "me/cold", "main", false)},
				HasNextPage: false,
			}), nil)

		svc, dao, _ := newReposDelegate(t, gw)
		// Empty discovered_repos → cold cache. PAT creds, so no tenant_type lookup.
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{PersonalAccessToken: "ghp_xxx"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		// The cold-cache fallback warms discovered_repos, identical to fresh=true.
		dao.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID("github"), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, mut func(json.RawMessage) (json.RawMessage, error)) error {
				out, err := mut(json.RawMessage(`{"include_private":true}`))
				require.NoError(t, err)
				var merged map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(out, &merged))
				var discovered []ghconfig.GitHubDiscoveredRepo
				require.NoError(t, json.Unmarshal(merged["discovered_repos"], &discovered))
				require.Len(t, discovered, 1)
				assert.Equal(t, "me/cold", discovered[0].FullName)
				return nil
			})

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: false,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 1)
		assert.Equal(t, "me/cold", resp.GetRepos()[0].GetFullName())
	})

	t.Run("fresh=true fails loud when tenant_type lookup fails (no silent org fallback)", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		// The gateway must not be built or called once the tenant_type lookup fails.
		gw := fwmocks.NewMockVendorGateway(ctrl)

		svc, dao, mgmt := newReposDelegate(t, gw)
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: "67890"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
			Return(sdk.TenantKindUnspecified, assert.AnError)

		_, err = svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: true,
		})
		require.Error(t, err)
	})

	t.Run("fresh=true warm is best-effort: a disconnected (deleted) row never resurrects the config and the listing still succeeds", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw)
		gw.EXPECT().Execute(gomock.Any(), gomock.AssignableToTypeOf(&vgpb.ListInstallationReposRequest{})).
			Return(proto.Message(&vgpb.ListInstallationReposResponse{
				Repos:       [][]byte{ghRepoJSON(t, 1, "octo/alpha", "main", false)},
				HasNextPage: false,
			}), nil)

		svc, dao, mgmt := newReposDelegate(t, gw)
		settings, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true})
		require.NoError(t, err)
		creds, err := json.Marshal(ghconfig.GitHubCredentials{InstallationID: "67890"})
		require.NoError(t, err)
		dao.EXPECT().Get(gomock.Any(), testOrgID, sdk.ConnectorID("github")).
			Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: testOrgID, Credentials: creds, Settings: settings}, nil)
		mgmt.EXPECT().TenantType(gomock.Any(), gomock.Any()).
			Return(sdk.TenantKind(commonv1.TenantType_TENANT_TYPE_INDIVIDUAL), nil)
		// The user disconnected mid-listing, so the row is soft-deleted. PatchSettings
		// targets only the live (deleted_at IS NULL) row and returns an error; the warm
		// is best-effort and swallows it. No dao.Save is expected — a resurrecting
		// upsert would fail the test (gomock rejects the unexpected call).
		dao.EXPECT().PatchSettings(gomock.Any(), testOrgID, sdk.ConnectorID("github"), gomock.Any()).
			Return(assert.AnError)

		resp, err := svc.ListVCSRepos(context.Background(), &connintv1.ListVCSReposRequest{
			OrgId: testOrgID, ConnectorId: "github", Fresh: true,
		})
		require.NoError(t, err)
		require.Len(t, resp.GetRepos(), 1)
		assert.Equal(t, "octo/alpha", resp.GetRepos()[0].GetFullName())
	})
}

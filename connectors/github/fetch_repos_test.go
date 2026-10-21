// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	repopb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestSelectRepoListing covers the (credential ability × tenant intent) endpoint
// matrix: PAT→/user/repos, App+INDIVIDUAL→/installation/repositories,
// App+ORGANIZATION→/orgs/{org}/repos, App+UNSPECIFIED→error backstop.
func TestSelectRepoListing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		creds      GitHubCredentials
		settings   GitHubSettings
		tenantType sdk.TenantKind
		wantReq    proto.Message // the request type the fetcher must dispatch
		wantErr    bool
	}{
		{
			name:       "PAT lists the token's own repos via /user/repos",
			creds:      GitHubCredentials{PersonalAccessToken: "github_pat_abc"},
			settings:   GitHubSettings{IncludePrivate: true},
			tenantType: sdk.TenantKindIndividual,
			wantReq:    &vgpb.ListUserReposRequest{},
		},
		{
			name:       "PAT branch is ability-first — wins even for an org intent",
			creds:      GitHubCredentials{PersonalAccessToken: "github_pat_abc"},
			tenantType: sdk.TenantKindOrganization,
			wantReq:    &vgpb.ListUserReposRequest{},
		},
		{
			name:       "App + INDIVIDUAL lists the installation's repos",
			creds:      GitHubCredentials{InstallationID: "111"},
			tenantType: sdk.TenantKindIndividual,
			wantReq:    &vgpb.ListInstallationReposRequest{},
		},
		{
			name:       "App + ORGANIZATION keeps the live /orgs path",
			creds:      GitHubCredentials{InstallationID: "111"},
			settings:   GitHubSettings{Organization: "acme"},
			tenantType: sdk.TenantKindOrganization,
			wantReq:    &vgpb.ListOrgReposRequest{},
		},
		{
			name:       "App + UNSPECIFIED is rejected (never silently org-routed)",
			creds:      GitHubCredentials{InstallationID: "111"},
			tenantType: sdk.TenantKindUnspecified,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fetchPage, err := SelectRepoListing(tt.creds, tt.settings, tt.tenantType)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, fetchPage)
				return
			}
			require.NoError(t, err)

			var gotReq proto.Message
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					gotReq = req
					switch req.(type) {
					case *vgpb.ListUserReposRequest:
						return &vgpb.ListUserReposResponse{HasNextPage: false}, nil
					case *vgpb.ListInstallationReposRequest:
						return &vgpb.ListInstallationReposResponse{HasNextPage: false}, nil
					case *vgpb.ListOrgReposRequest:
						return &vgpb.ListOrgReposResponse{HasNextPage: false}, nil
					default:
						return nil, assert.AnError
					}
				},
			}
			_, _, err = fetchPage(context.Background(), gw, 1)
			require.NoError(t, err)
			assert.IsType(t, tt.wantReq, gotReq)
		})
	}
}

// TestSelectRepoListing_PATVisibility verifies the PAT listing maps
// IncludePrivate to the /user/repos visibility filter.
func TestSelectRepoListing_PATVisibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		includePrivate bool
		wantVisibility string
	}{
		{name: "include private → visibility=all", includePrivate: true, wantVisibility: "all"},
		{name: "exclude private → visibility=public", includePrivate: false, wantVisibility: "public"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fetchPage, err := SelectRepoListing(
				GitHubCredentials{PersonalAccessToken: "github_pat_abc"},
				GitHubSettings{IncludePrivate: tt.includePrivate},
				sdk.TenantKindIndividual,
			)
			require.NoError(t, err)

			var got string
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					got = req.(*vgpb.ListUserReposRequest).GetVisibility()
					return &vgpb.ListUserReposResponse{HasNextPage: false}, nil
				},
			}
			_, _, err = fetchPage(context.Background(), gw, 1)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVisibility, got)
		})
	}
}

// TestMapGitHubRepoToRecord validates field normalization in mapGitHubRepoToRecord.
func TestMapGitHubRepoToRecord(t *testing.T) {
	tests := []struct {
		name       string
		input      *ghRepository
		wantID     string
		wantName   string
		wantFull   string
		wantBranch string
		wantStars  int32
		wantForks  int32
	}{
		{
			name: "basic repo fields are mapped correctly",
			input: &ghRepository{
				ID:            12345,
				Name:          "widgets",
				FullName:      "acme/widgets",
				DefaultBranch: "main",
				Archived:      false,
				Owner: struct {
					Login string `json:"login"`
				}{Login: "acme"},
				StargazersCount: 42,
				ForksCount:      7,
			},
			wantID:     "12345",
			wantName:   "widgets",
			wantFull:   "acme/widgets",
			wantBranch: "main",
			wantStars:  42,
			wantForks:  7,
		},
		{
			name: "fork repo carries parent_platform_repo_id",
			input: &ghRepository{
				ID:       99,
				Name:     "forked-repo",
				FullName: "user/forked-repo",
				Fork:     true,
				Parent: &struct {
					ID int64 `json:"id"`
				}{ID: 55},
				Owner: struct {
					Login string `json:"login"`
				}{Login: "user"},
			},
			wantID:   "99",
			wantName: "forked-repo",
			wantFull: "user/forked-repo",
		},
		{
			name: "license key is extracted from nested license object",
			input: &ghRepository{
				ID:       77,
				Name:     "licensed",
				FullName: "org/licensed",
				Owner: struct {
					Login string `json:"login"`
				}{Login: "org"},
				License: &struct {
					Key string `json:"key"`
				}{Key: "mit"},
			},
			wantID:   "77",
			wantName: "licensed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := mapGitHubRepoToRecord(tt.input, GitHubSettings{})
			require.NotNil(t, rec)

			if tt.wantID != "" {
				assert.Equal(t, tt.wantID, rec.PlatformRepoId)
			}
			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, rec.Name)
			}
			if tt.wantFull != "" {
				assert.Equal(t, tt.wantFull, rec.FullName)
			}
			if tt.wantBranch != "" {
				assert.Equal(t, tt.wantBranch, rec.DefaultBranch)
			}
			if tt.wantStars != 0 || tt.wantForks != 0 {
				require.NotNil(t, rec.Stats)
				assert.Equal(t, tt.wantStars, rec.Stats.Stars)
				assert.Equal(t, tt.wantForks, rec.Stats.Forks)
			}

			// fork-specific assertions
			if tt.input.Parent != nil {
				assert.Equal(t, "55", rec.ParentPlatformRepoId)
			}
			if tt.input.License != nil {
				assert.Equal(t, "mit", rec.LicenseKey)
			}

			// vendor blob must always be non-empty JSON
			assert.NotEmpty(t, rec.VendorBlobJson)
			var blob map[string]interface{}
			require.NoError(t, json.Unmarshal(rec.VendorBlobJson, &blob))
		})
	}
}

// TestFetchReposOp_PaginationLoop verifies that FetchReposOp paginates until
// HasNextPage is false, emitting one record per repo.
func TestFetchReposOp_PaginationLoop(t *testing.T) {
	page := 0

	// Build two pages: page 1 has one repo with HasNextPage=true, page 2 has one repo with HasNextPage=false.
	repoPage1, _ := json.Marshal([]ghRepository{{
		ID:            1,
		Name:          "repo-a",
		FullName:      "org/repo-a",
		DefaultBranch: "main",
		Owner: struct {
			Login string `json:"login"`
		}{Login: "org"},
	}})
	repoPage2, _ := json.Marshal([]ghRepository{{
		ID:            2,
		Name:          "repo-b",
		FullName:      "org/repo-b",
		DefaultBranch: "main",
		Owner: struct {
			Login string `json:"login"`
		}{Login: "org"},
	}})

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			page++
			switch page {
			case 1:
				// Unmarshal the page of repos and return them as raw bytes.
				var repos []ghRepository
				_ = json.Unmarshal(repoPage1, &repos)
				raw := make([][]byte, len(repos))
				for i, r := range repos {
					raw[i], _ = json.Marshal(r)
				}
				return &vgpb.ListOrgReposResponse{Repos: raw, HasNextPage: true}, nil
			default:
				var repos []ghRepository
				_ = json.Unmarshal(repoPage2, &repos)
				raw := make([][]byte, len(repos))
				for i, r := range repos {
					raw[i], _ = json.Marshal(r)
				}
				return &vgpb.ListOrgReposResponse{Repos: raw, HasNextPage: false}, nil
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	// Org App credential + ORGANIZATION intent → the /orgs listing path.
	params.TenantType = sdk.TenantKindOrganization
	// Override settings to use auto-discovery mode (no explicit repos list).
	params.Config.Settings = mustJSON(GitHubSettings{
		Organization:   "org",
		IncludePrivate: true,
	})

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 2, "expected one record per repo across two pages")

	assert.Equal(t, string(opFetchRepos), records[0].OperationID)
	assert.Equal(t, "1", records[0].ExternalID)
	assert.Equal(t, "2", records[1].ExternalID)
}

// TestFetchReposOp_SkipsPrivateWhenNotIncluded verifies that private repos are
// filtered when IncludePrivate is false.
func TestFetchReposOp_SkipsPrivateWhenNotIncluded(t *testing.T) {
	repos := []ghRepository{
		{
			ID: 1, Name: "public", FullName: "org/public", Private: false,
			Owner: struct {
				Login string `json:"login"`
			}{Login: "org"},
		},
		{
			ID: 2, Name: "private", FullName: "org/private", Private: true,
			Owner: struct {
				Login string `json:"login"`
			}{Login: "org"},
		},
	}

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			raw := make([][]byte, len(repos))
			for i, r := range repos {
				raw[i], _ = json.Marshal(r)
			}
			return &vgpb.ListOrgReposResponse{Repos: raw, HasNextPage: false}, nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.TenantType = sdk.TenantKindOrganization
	params.Config.Settings = mustJSON(GitHubSettings{
		Organization:   "org",
		IncludePrivate: false, // private repos must be excluded
	})

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "only the public repo should be emitted")
	assert.Equal(t, "1", records[0].ExternalID)
}

// TestMapGitHubRepoToRecord_Timestamps verifies that vendor timestamps are
// correctly converted from Go time values to proto Timestamps.
func TestMapGitHubRepoToRecord_Timestamps(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	gh := &ghRepository{
		ID:        555,
		Name:      "ts-test",
		FullName:  "org/ts-test",
		CreatedAt: now.Add(-24 * time.Hour),
		UpdatedAt: now,
		PushedAt:  now.Add(-1 * time.Hour),
		Owner: struct {
			Login string `json:"login"`
		}{Login: "org"},
	}
	rec := mapGitHubRepoToRecord(gh, GitHubSettings{})
	require.NotNil(t, rec.VendorCreatedAt)
	require.NotNil(t, rec.VendorUpdatedAt)
	require.NotNil(t, rec.VendorPushedAt)
	assert.Equal(t, now.Unix(), rec.VendorUpdatedAt.AsTime().Unix())
}

// TestMapGitHubRepoToRecord_TrackedBranch validates the single-branch wrapper
// (resolveRepoBranch): per-repo override > org-level TrackedBranch >
// default_branch. The full all-branches precedence is in TestResolveRepoBranchPolicy.
func TestMapGitHubRepoToRecord_TrackedBranch(t *testing.T) {
	tests := []struct {
		name          string
		repoFullName  string
		defaultBranch string
		settings      GitHubSettings
		wantBranch    string
	}{
		{
			name:          "no overrides — empty tracked_branch (a consumer uses default_branch)",
			repoFullName:  "org/repo",
			defaultBranch: "main",
			settings:      GitHubSettings{},
			wantBranch:    "main",
		},
		{
			name:          "org-level tracked_branch overrides default_branch",
			repoFullName:  "org/repo",
			defaultBranch: "master",
			settings:      GitHubSettings{TrackedBranch: "develop"},
			wantBranch:    "develop",
		},
		{
			name:          "per-repo override wins over org-level tracked_branch",
			repoFullName:  "org/repo",
			defaultBranch: "master",
			settings: GitHubSettings{
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"org/repo": "release"},
			},
			wantBranch: "release",
		},
		{
			name:          "per-repo override for different repo does not apply",
			repoFullName:  "org/other-repo",
			defaultBranch: "master",
			settings: GitHubSettings{
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"org/repo": "release"},
			},
			wantBranch: "develop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gh := &ghRepository{
				ID:            1,
				Name:          "repo",
				FullName:      tt.repoFullName,
				DefaultBranch: tt.defaultBranch,
				Owner: struct {
					Login string `json:"login"`
				}{Login: "org"},
			}
			rec := mapGitHubRepoToRecord(gh, tt.settings)
			assert.Equal(t, tt.wantBranch, rec.TrackedBranch)
		})
	}
}

// TestResolveRepoBranchPolicy locks the full precedence:
// RepoAllBranches > RepoBranchOverrides > TrackAllBranches >
// TrackedBranch > auto-detected default.
func TestResolveRepoBranchPolicy(t *testing.T) {
	const repo, def = "org/repo", "main"
	tests := []struct {
		name     string
		settings GitHubSettings
		wantAll  bool
		wantBr   string
	}{
		{
			name:     "nothing configured → auto-detected default",
			settings: GitHubSettings{},
			wantBr:   def,
		},
		{
			name:     "org tracked_branch → that branch",
			settings: GitHubSettings{TrackedBranch: "develop"},
			wantBr:   "develop",
		},
		{
			name:     "org track_all_branches → all",
			settings: GitHubSettings{TrackAllBranches: true},
			wantAll:  true,
		},
		{
			name:     "per-repo override wins over org track_all_branches",
			settings: GitHubSettings{TrackAllBranches: true, RepoBranchOverrides: map[string]string{repo: "release"}},
			wantBr:   "release",
		},
		{
			name:     "repo_all_branches wins over per-repo override and org tracked_branch",
			settings: GitHubSettings{TrackedBranch: "develop", RepoBranchOverrides: map[string]string{repo: "release"}, RepoAllBranches: []string{repo}},
			wantAll:  true,
		},
		{
			name:     "repo_all_branches for a different repo does not apply",
			settings: GitHubSettings{TrackedBranch: "develop", RepoAllBranches: []string{"org/other"}},
			wantBr:   "develop",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.settings.resolveRepoBranchPolicy(repo, def)
			assert.Equal(t, tt.wantAll, got.AllBranches)
			assert.Equal(t, tt.wantBr, got.Branch)
		})
	}
}

// TestFetchRepos_CapturesMergeMethodToggles verifies that the per-repo GraphQL
// merge-method query populates allow_squash_merge / allow_merge_commit /
// allow_rebase_merge in the emitted RepositoryRecord vendor blob.
func TestFetchRepos_CapturesMergeMethodToggles(t *testing.T) {
	repoJSON, _ := json.Marshal(ghRepository{
		ID:       1,
		Name:     "widgets",
		FullName: "acme/widgets",
		Owner: struct {
			Login string `json:"login"`
		}{Login: "acme"},
	})

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			switch req.(type) {
			case *vgpb.ListOrgReposRequest:
				return &vgpb.ListOrgReposResponse{Repos: [][]byte{repoJSON}, HasNextPage: false}, nil
			case *vgpb.GitHubGraphQLRequest:
				// Merge-methods GraphQL response.
				raw, _ := json.Marshal(ghGraphQLData{
					Repository: ghGQLRepository{
						SquashMergeAllowed: true,
						MergeCommitAllowed: false,
						RebaseMergeAllowed: true,
					},
				})
				return &vgpb.GitHubGraphQLResponse{Data: raw}, nil
			default:
				return nil, nil
			}
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)
	params.TenantType = sdk.TenantKindOrganization
	params.Config.Settings = mustJSON(GitHubSettings{
		Organization:   "acme",
		IncludePrivate: true,
	})

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "one repo record expected")

	var rec repopb.RepositoryRecord
	require.NoError(t, protojson.Unmarshal(records[0].Payload, &rec))
	var blob map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.VendorBlobJson, &blob))
	assert.Equal(t, true, blob["allow_squash_merge"], "allow_squash_merge must be true")
	assert.Equal(t, false, blob["allow_merge_commit"], "allow_merge_commit must be false")
	assert.Equal(t, true, blob["allow_rebase_merge"], "allow_rebase_merge must be true")
}

// A PAT lists /user/repos, which spans every org the token can reach. An
// ORGANIZATION tenant naming settings.organization catalogs only that org's
// repos (owner matched case-insensitively); an INDIVIDUAL tenant keeps them
// all.
func TestFetchReposOp_PATOrganizationTenantKeepsOnlyItsOrg(t *testing.T) {
	repo := func(id int64, owner, name string) ghRepository {
		r := ghRepository{ID: id, Name: name, FullName: owner + "/" + name}
		r.Owner.Login = owner
		return r
	}
	repos := []ghRepository{repo(1, "Acme", "api"), repo(2, "other-org", "web"), repo(3, "alice", "dotfiles")}
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			if _, ok := req.(*vgpb.ListUserReposRequest); !ok {
				return nil, fmt.Errorf("unexpected request %T", req)
			}
			raw := make([][]byte, len(repos))
			for i, r := range repos {
				raw[i], _ = json.Marshal(r)
			}
			return &vgpb.ListUserReposResponse{Repos: raw}, nil
		},
	}
	for name, tc := range map[string]struct {
		tenant sdk.TenantKind
		org    string
		want   []string
	}{
		"organization tenant":                 {sdk.TenantKindOrganization, "acme", []string{"1"}},
		"organization tenant, no org setting": {sdk.TenantKindOrganization, "", []string{"1", "2", "3"}},
		"individual tenant":                   {sdk.TenantKindIndividual, "acme", []string{"1", "2", "3"}},
	} {
		t.Run(name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			params := testParams(sink, gw, nil)
			params.TenantType = tc.tenant
			params.Config.Credentials = mustJSON(GitHubCredentials{PersonalAccessToken: "github_pat_abc"})
			params.Config.Settings = mustJSON(GitHubSettings{Organization: tc.org, IncludePrivate: true})

			_, err := (&FetchReposOp{}).Execute(context.Background(), params)
			require.NoError(t, err)
			var got []string
			for _, r := range sink.Records() {
				got = append(got, r.ExternalID)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	repopb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// --- mapGitLabProjectToRecord tests ---

func TestMapGitLabProjectToRecord_BasicFields(t *testing.T) {
	createdAt := time.Date(2024, 3, 10, 12, 0, 0, 0, time.UTC)
	lastActivity := time.Date(2024, 4, 1, 8, 0, 0, 0, time.UTC)

	gl := &glProject{
		ID:                42,
		Path:              "my-repo",
		PathWithNamespace: "my-group/my-repo",
		DefaultBranch:     "main",
		Visibility:        "private",
		Description:       "A test repo",
		Archived:          false,
		CreatedAt:         createdAt,
		LastActivityAt:    &lastActivity,
		StarCount:         10,
		ForksCount:        3,
		OpenIssuesCount:   5,
		IssuesEnabled:     true,
		WikiEnabled:       false,
		WebURL:            "https://gitlab.example.com/my-group/my-repo",
		SSHURLToRepo:      "git@gitlab.example.com:my-group/my-repo.git",
		HTTPURLToRepo:     "https://gitlab.example.com/my-group/my-repo.git",
		Topics:            []string{"go", "backend"},
		Namespace: &struct {
			ID   int    `json:"id"`
			Path string `json:"path"`
			Kind string `json:"kind"`
		}{ID: 7, Path: "my-group", Kind: "group"},
	}

	rec := mapGitLabProjectToRecord(gl, GitLabSettings{})

	assert.Equal(t, "42", rec.PlatformRepoId)
	assert.Equal(t, "my-repo", rec.Name)
	assert.Equal(t, "my-group/my-repo", rec.FullName)
	assert.Equal(t, "my-group", rec.OwnerLogin)
	assert.Equal(t, "A test repo", rec.Description)
	assert.Equal(t, "main", rec.DefaultBranch)
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE, rec.Visibility)
	assert.False(t, rec.Archived)
	assert.False(t, rec.IsFork)
	assert.Empty(t, rec.ParentPlatformRepoId)
	assert.Equal(t, []string{"go", "backend"}, rec.Topics)
	assert.Equal(t, "https://gitlab.example.com/my-group/my-repo", rec.WebUrl)
	assert.Equal(t, "git@gitlab.example.com:my-group/my-repo.git", rec.SshUrl)
	assert.Equal(t, "https://gitlab.example.com/my-group/my-repo.git", rec.HttpCloneUrl)
	assert.True(t, rec.HasIssues)
	assert.False(t, rec.HasWiki)
	require.NotNil(t, rec.Stats)
	assert.Equal(t, int32(10), rec.Stats.Stars)
	assert.Equal(t, int32(3), rec.Stats.Forks)
	assert.Equal(t, int32(5), rec.Stats.OpenIssues)
	assert.Equal(t, int32(0), rec.Stats.Watchers)    // GitLab has no watchers
	assert.Equal(t, int32(0), rec.Stats.Subscribers) // GitLab has no subscribers
	require.NotNil(t, rec.VendorCreatedAt)
	assert.Equal(t, createdAt.Unix(), rec.VendorCreatedAt.Seconds)
	require.NotNil(t, rec.VendorPushedAt)
	assert.Equal(t, lastActivity.Unix(), rec.VendorPushedAt.Seconds)
	assert.Nil(t, rec.VendorUpdatedAt)
}

func TestMapGitLabProjectToRecord_Fork(t *testing.T) {
	gl := &glProject{
		ID:                99,
		Path:              "forked-repo",
		PathWithNamespace: "my-group/forked-repo",
		DefaultBranch:     "main",
		Visibility:        "public",
		ForkedFromProject: &struct {
			ID int `json:"id"`
		}{ID: 50},
	}

	rec := mapGitLabProjectToRecord(gl, GitLabSettings{})

	assert.True(t, rec.IsFork)
	assert.Equal(t, "50", rec.ParentPlatformRepoId)
}

func TestMapGitLabProjectToRecord_NilNamespace(t *testing.T) {
	gl := &glProject{
		ID:                10,
		Path:              "orphan-repo",
		PathWithNamespace: "orphan-repo",
		DefaultBranch:     "main",
		Visibility:        "public",
		Namespace:         nil,
	}

	rec := mapGitLabProjectToRecord(gl, GitLabSettings{})

	assert.Empty(t, rec.OwnerLogin)
	assert.NotNil(t, rec.VendorBlobJson)

	var blob map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.VendorBlobJson, &blob))
	assert.Equal(t, float64(0), blob["namespace_id"])
	assert.Empty(t, blob["namespace_path"])
	assert.Empty(t, blob["namespace_kind"])
}

func TestMapGitLabProjectToRecord_NilLastActivity(t *testing.T) {
	gl := &glProject{
		ID:                5,
		Path:              "new-repo",
		PathWithNamespace: "group/new-repo",
		DefaultBranch:     "main",
		Visibility:        "private",
		LastActivityAt:    nil,
	}

	rec := mapGitLabProjectToRecord(gl, GitLabSettings{})

	assert.Nil(t, rec.VendorPushedAt)
}

func TestBuildGitLabVendorBlob_Fields(t *testing.T) {
	gl := &glProject{
		ID:           1,
		ImportStatus: "finished",
		EmptyRepo:    false,
		Namespace: &struct {
			ID   int    `json:"id"`
			Path string `json:"path"`
			Kind string `json:"kind"`
		}{ID: 7, Path: "my-group", Kind: "group"},
	}

	blob := buildGitLabVendorBlob(gl)
	require.NotNil(t, blob)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(blob, &m))
	assert.Equal(t, float64(7), m["namespace_id"])
	assert.Equal(t, "my-group", m["namespace_path"])
	assert.Equal(t, "group", m["namespace_kind"])
	assert.Equal(t, "finished", m["import_status"])
	assert.Equal(t, false, m["empty_repo"])
}

// --- FetchReposOp integration-style tests ---

func TestFetchReposOp_OperationID(t *testing.T) {
	op := &FetchReposOp{}
	assert.Equal(t, sdk.OperationID("fetch_repos"), op.OperationID())
}

func TestFetchReposOp_SinglePage(t *testing.T) {
	createdAt := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			listReq, ok := req.(*connectorpb.GitLabListProjectsRequest)
			require.True(t, ok, "expected GitLabListProjectsRequest, got %T", req)
			assert.Equal(t, int64(42), listReq.GetGroupId())

			projects := []glProject{
				{
					ID:                101,
					Path:              "repo-a",
					PathWithNamespace: "mygroup/repo-a",
					DefaultBranch:     "main",
					Visibility:        "private",
					CreatedAt:         createdAt,
					StarCount:         5,
					ForksCount:        1,
					Namespace: &struct {
						ID   int    `json:"id"`
						Path string `json:"path"`
						Kind string `json:"kind"`
					}{ID: 7, Path: "mygroup", Kind: "group"},
				},
				{
					ID:                102,
					Path:              "repo-b",
					PathWithNamespace: "mygroup/repo-b",
					DefaultBranch:     "develop",
					Visibility:        "public",
					CreatedAt:         createdAt,
				},
			}
			raw := make([][]byte, len(projects))
			for i, p := range projects {
				raw[i], _ = json.Marshal(p)
			}
			return &connectorpb.GitLabListProjectsResponse{
				Projects: raw,
				// NextPageUrl empty → single page
			}, nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchReposOp{}
	cursor, err := op.Execute(context.Background(), params)
	require.NoError(t, err)
	assert.Equal(t, params.Cursor, cursor, "cursor unchanged for fetch_repos")

	records := sink.Records()
	require.Len(t, records, 2)

	// Validate first record.
	r0 := records[0]
	assert.Equal(t, "gitlab", r0.ConnectorID)
	assert.Equal(t, "fetch_repos", r0.OperationID)
	assert.Equal(t, "org-test", r0.OrganizationID)
	assert.Equal(t, "101", r0.ExternalID)
	assert.Equal(t, "mygroup/repo-a", r0.Metadata["full_name"])

	var rec0 repopb.RepositoryRecord
	require.NoError(t, protojson.Unmarshal(r0.Payload, &rec0))
	assert.Equal(t, "101", rec0.PlatformRepoId)
	assert.Equal(t, "repo-a", rec0.Name)
	assert.Equal(t, "mygroup/repo-a", rec0.FullName)
	assert.Equal(t, "mygroup", rec0.OwnerLogin)
	assert.Equal(t, "main", rec0.DefaultBranch)
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE, rec0.Visibility)
	assert.Equal(t, int32(5), rec0.Stats.Stars)
	assert.Equal(t, int32(1), rec0.Stats.Forks)

	// Validate second record.
	r1 := records[1]
	assert.Equal(t, "102", r1.ExternalID)
}

func TestFetchReposOp_Pagination(t *testing.T) {
	page1 := []glProject{{ID: 1, Path: "p1", PathWithNamespace: "g/p1", DefaultBranch: "main", Visibility: "private"}}
	page2 := []glProject{{ID: 2, Path: "p2", PathWithNamespace: "g/p2", DefaultBranch: "main", Visibility: "public"}}

	callCount := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			callCount++
			listReq := req.(*connectorpb.GitLabListProjectsRequest)

			var projects []glProject
			var nextPageURL string
			switch listReq.GetPage() {
			case 1:
				projects = page1
				nextPageURL = "https://gitlab.example.com/api/v4/groups/42/projects?page=2"
			default:
				projects = page2
			}

			raw := make([][]byte, len(projects))
			for i, p := range projects {
				raw[i], _ = json.Marshal(p)
			}
			return &connectorpb.GitLabListProjectsResponse{
				Projects:    raw,
				NextPageUrl: nextPageURL,
			}, nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	assert.Equal(t, 2, callCount, "should have made 2 gateway calls (one per page)")
	records := sink.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "1", records[0].ExternalID)
	assert.Equal(t, "2", records[1].ExternalID)
}

func TestFetchReposOp_InvalidConfig(t *testing.T) {
	gw := &mockVendorGateway{}
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "gitlab",
			OrganizationID: "org-test",
			Credentials:    json.RawMessage(`{invalid}`),
			Settings:       json.RawMessage(`{}`),
			Enabled:        true,
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.Error(t, err)

	var syncErr *sdk.SyncError
	require.ErrorAs(t, err, &syncErr)
	assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
}

// TestMapGitLabProjectToRecord_TrackedBranch validates branch resolution priority:
// per-repo override > org-level TrackedBranch > repo DefaultBranch.
func TestMapGitLabProjectToRecord_TrackedBranch(t *testing.T) {
	tests := []struct {
		name        string
		projectPath string
		settings    GitLabSettings
		wantBranch  string
	}{
		{
			name:        "no overrides — falls back to project default_branch",
			projectPath: "group/repo",
			settings:    GitLabSettings{GroupID: 1},
			wantBranch:  "main",
		},
		{
			name:        "org-level tracked_branch is carried through",
			projectPath: "group/repo",
			settings:    GitLabSettings{GroupID: 1, TrackedBranch: "develop"},
			wantBranch:  "develop",
		},
		{
			name:        "per-repo override wins over org-level tracked_branch",
			projectPath: "group/repo",
			settings: GitLabSettings{
				GroupID:             1,
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"group/repo": "release"},
			},
			wantBranch: "release",
		},
		{
			name:        "per-repo override for different project does not apply",
			projectPath: "group/other-repo",
			settings: GitLabSettings{
				GroupID:             1,
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"group/repo": "release"},
			},
			wantBranch: "develop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gl := &glProject{
				ID:                1,
				Path:              "repo",
				PathWithNamespace: tt.projectPath,
				DefaultBranch:     "main",
				Visibility:        "private",
			}
			rec := mapGitLabProjectToRecord(gl, tt.settings)
			assert.Equal(t, tt.wantBranch, rec.TrackedBranch)
		})
	}
}

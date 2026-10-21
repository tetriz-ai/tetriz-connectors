// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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

// --- mapBitbucketRepoToRecord unit tests ---

func TestMapBitbucketRepoToRecord_BasicFields(t *testing.T) {
	createdAt := time.Date(2024, 3, 10, 12, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 4, 1, 8, 0, 0, 0, time.UTC)

	bb := &bbRepository{
		UUID:        "{abc-123-uuid}",
		Slug:        "widgets",
		FullName:    "myworkspace/widgets",
		Description: "Core services",
		IsPrivate:   true,
		Language:    "Go",
		Size:        2048, // bytes
		SCM:         "git",
		CreatedOn:   createdAt,
		UpdatedOn:   updatedAt,
		Mainbranch:  &bbBranchRef{Name: "main", Type: "branch"},
		Workspace:   &bbWorkspace{UUID: "{ws-uuid}", Slug: "myworkspace"},
		Project:     &bbProject{Key: "CORE", UUID: "{proj-uuid}"},
		Links: &bbRepoLinks{
			HTML:  &bbLink{Href: "https://bitbucket.org/myworkspace/widgets"},
			Clone: []bbClone{{Name: "https", Href: "https://myworkspace@bitbucket.org/myworkspace/widgets.git"}, {Name: "ssh", Href: "git@bitbucket.org:myworkspace/widgets.git"}},
		},
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})

	assert.Equal(t, "{abc-123-uuid}", rec.PlatformRepoId)
	assert.Equal(t, "widgets", rec.Name)
	assert.Equal(t, "myworkspace/widgets", rec.FullName)
	assert.Equal(t, "myworkspace", rec.OwnerLogin)
	assert.Equal(t, "Core services", rec.Description)
	assert.Equal(t, "main", rec.DefaultBranch)
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE, rec.Visibility)
	assert.Equal(t, "Go", rec.PrimaryLanguage)
	assert.Equal(t, int64(2), rec.SizeKb) // 2048 / 1024
	assert.False(t, rec.IsFork)
	assert.Empty(t, rec.ParentPlatformRepoId)
	assert.Equal(t, "https://bitbucket.org/myworkspace/widgets", rec.WebUrl)
	assert.Equal(t, "https://myworkspace@bitbucket.org/myworkspace/widgets.git", rec.HttpCloneUrl)
	assert.Equal(t, "git@bitbucket.org:myworkspace/widgets.git", rec.SshUrl)
	require.NotNil(t, rec.VendorCreatedAt)
	assert.Equal(t, createdAt.Unix(), rec.VendorCreatedAt.Seconds)
	require.NotNil(t, rec.VendorUpdatedAt)
	assert.Equal(t, updatedAt.Unix(), rec.VendorUpdatedAt.Seconds)
	assert.Nil(t, rec.VendorPushedAt)
	// Bitbucket has no star/fork/watcher counts on list endpoint.
	require.NotNil(t, rec.Stats)
	assert.Equal(t, int32(0), rec.Stats.Stars)
	assert.Equal(t, int32(0), rec.Stats.Forks)
	assert.Equal(t, int32(0), rec.Stats.Watchers)
}

func TestMapBitbucketRepoToRecord_PublicVisibility(t *testing.T) {
	bb := &bbRepository{
		UUID:      "{public-uuid}",
		Slug:      "public-repo",
		FullName:  "ws/public-repo",
		IsPrivate: false,
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PUBLIC, rec.Visibility)
}

func TestMapBitbucketRepoToRecord_ForkCarriesParentID(t *testing.T) {
	bb := &bbRepository{
		UUID:     "{fork-uuid}",
		Slug:     "forked-repo",
		FullName: "ws/forked-repo",
		Parent:   &bbRepository{UUID: "{parent-uuid}"},
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})
	assert.True(t, rec.IsFork)
	assert.Equal(t, "{parent-uuid}", rec.ParentPlatformRepoId)
}

func TestMapBitbucketRepoToRecord_NilWorkspace(t *testing.T) {
	bb := &bbRepository{
		UUID:      "{no-ws-uuid}",
		Slug:      "orphan-repo",
		FullName:  "orphan-repo",
		Workspace: nil,
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})
	assert.Empty(t, rec.OwnerLogin)
}

func TestMapBitbucketRepoToRecord_NilMainbranch(t *testing.T) {
	bb := &bbRepository{
		UUID:       "{no-branch-uuid}",
		Slug:       "repo",
		FullName:   "ws/repo",
		Mainbranch: nil,
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})
	assert.Empty(t, rec.DefaultBranch)
}

func TestMapBitbucketRepoToRecord_ZeroTimestamps(t *testing.T) {
	bb := &bbRepository{
		UUID:     "{ts-uuid}",
		Slug:     "repo",
		FullName: "ws/repo",
		// CreatedOn and UpdatedOn both zero
	}

	rec := mapBitbucketRepoToRecord(bb, BitbucketSettings{})
	assert.Nil(t, rec.VendorCreatedAt)
	assert.Nil(t, rec.VendorUpdatedAt)
}

// --- buildBitbucketVendorBlob unit tests ---

func TestBuildBitbucketVendorBlob_Fields(t *testing.T) {
	bb := &bbRepository{
		UUID:       "{repo-uuid}",
		SCM:        "git",
		Mainbranch: &bbBranchRef{Name: "main", Type: "branch"},
		Workspace:  &bbWorkspace{UUID: "{ws-uuid}", Slug: "ws"},
		Project:    &bbProject{Key: "PROJ", UUID: "{proj-uuid}"},
	}

	blob := buildBitbucketVendorBlob(bb)
	require.NotNil(t, blob)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(blob, &m))
	assert.Equal(t, "{ws-uuid}", m["workspace_uuid"])
	assert.Equal(t, "git", m["scm"])
	assert.Equal(t, "PROJ", m["project_key"])
	assert.Equal(t, "{proj-uuid}", m["project_uuid"])

	mb, ok := m["mainbranch"].(map[string]interface{})
	require.True(t, ok, "mainbranch should be an object")
	assert.Equal(t, "main", mb["name"])
	assert.Equal(t, "branch", mb["type"])
}

func TestBuildBitbucketVendorBlob_NilFields(t *testing.T) {
	bb := &bbRepository{
		UUID:       "{bare-uuid}",
		SCM:        "git",
		Mainbranch: nil,
		Workspace:  nil,
		Project:    nil,
	}

	blob := buildBitbucketVendorBlob(bb)
	require.NotNil(t, blob)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(blob, &m))
	assert.Empty(t, m["workspace_uuid"])
	assert.Empty(t, m["project_key"])
	assert.Empty(t, m["project_uuid"])
}

// --- FetchReposOp integration-style tests ---

func TestFetchReposOp_OperationID(t *testing.T) {
	op := &FetchReposOp{}
	assert.Equal(t, sdk.OperationID("fetch_repos"), op.OperationID())
}

func TestFetchReposOp_SinglePage(t *testing.T) {
	repos := []bbRepository{
		{
			UUID:       "{uuid-1}",
			Slug:       "repo-a",
			FullName:   "myworkspace/repo-a",
			IsPrivate:  true,
			Workspace:  &bbWorkspace{UUID: "{ws-uuid}", Slug: "myworkspace"},
			Mainbranch: &bbBranchRef{Name: "main", Type: "branch"},
		},
		{
			UUID:      "{uuid-2}",
			Slug:      "repo-b",
			FullName:  "myworkspace/repo-b",
			IsPrivate: false,
			Workspace: &bbWorkspace{UUID: "{ws-uuid}", Slug: "myworkspace"},
		},
	}

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			listReq, ok := req.(*connectorpb.BitbucketListReposRequest)
			require.True(t, ok, "expected BitbucketListReposRequest, got %T", req)
			assert.Equal(t, "myworkspace", listReq.GetWorkspace())

			return bbReposResponse(repos, false), nil
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

	r0 := records[0]
	assert.Equal(t, "bitbucket", r0.ConnectorID)
	assert.Equal(t, "fetch_repos", r0.OperationID)
	assert.Equal(t, "org-test", r0.OrganizationID)
	assert.Equal(t, "{uuid-1}", r0.ExternalID)
	assert.Equal(t, "myworkspace/repo-a", r0.Metadata["full_name"])

	// Verify payload is a valid proto-encoded RepositoryRecord.
	var rec0 repopb.RepositoryRecord
	require.NoError(t, protojson.Unmarshal(r0.Payload, &rec0))
	assert.Equal(t, "{uuid-1}", rec0.PlatformRepoId)
	assert.Equal(t, "repo-a", rec0.Name)
	assert.Equal(t, "myworkspace/repo-a", rec0.FullName)
	assert.Equal(t, "myworkspace", rec0.OwnerLogin)
	assert.Equal(t, "main", rec0.DefaultBranch)
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PRIVATE, rec0.Visibility)

	r1 := records[1]
	assert.Equal(t, "{uuid-2}", r1.ExternalID)
	var rec1 repopb.RepositoryRecord
	require.NoError(t, protojson.Unmarshal(r1.Payload, &rec1))
	assert.Equal(t, repopb.RepositoryVisibility_REPOSITORY_VISIBILITY_PUBLIC, rec1.Visibility)
}

func TestFetchReposOp_Pagination(t *testing.T) {
	page1 := []bbRepository{{UUID: "{uuid-p1}", Slug: "p1", FullName: "ws/p1"}}
	page2 := []bbRepository{{UUID: "{uuid-p2}", Slug: "p2", FullName: "ws/p2"}}

	callCount := 0
	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			callCount++
			listReq := req.(*connectorpb.BitbucketListReposRequest)

			switch listReq.GetPage() {
			case 1:
				return bbReposResponse(page1, true), nil
			default:
				return bbReposResponse(page2, false), nil
			}
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
	assert.Equal(t, "{uuid-p1}", records[0].ExternalID)
	assert.Equal(t, "{uuid-p2}", records[1].ExternalID)
}

func TestFetchReposOp_SkipsMalformedRepos(t *testing.T) {
	// A repo with an empty UUID is skipped.
	repos := []bbRepository{
		{UUID: "", Slug: "bad-repo", FullName: "ws/bad-repo"},
		{UUID: "{valid-uuid}", Slug: "good-repo", FullName: "ws/good-repo"},
	}

	gw := &mockVendorGateway{
		executeFunc: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return bbReposResponse(repos, false), nil
		},
	}

	sink := testkit.NewChannelSink()
	params := testParams(sink, gw, nil)

	op := &FetchReposOp{}
	_, err := op.Execute(context.Background(), params)
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 1, "malformed repo (empty UUID) must be skipped")
	assert.Equal(t, "{valid-uuid}", records[0].ExternalID)
}

func TestFetchReposOp_InvalidConfig(t *testing.T) {
	gw := &mockVendorGateway{}
	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    "bitbucket",
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

// TestMapBitbucketRepoToRecord_TrackedBranch validates branch resolution priority:
// per-repo override > org-level TrackedBranch > repo DefaultBranch.
func TestMapBitbucketRepoToRecord_TrackedBranch(t *testing.T) {
	tests := []struct {
		name       string
		fullName   string
		settings   BitbucketSettings
		wantBranch string
	}{
		{
			name:       "no overrides — falls back to repo default_branch (empty when mainbranch unset)",
			fullName:   "ws/repo",
			settings:   BitbucketSettings{Workspace: "ws"},
			wantBranch: "",
		},
		{
			name:       "org-level tracked_branch is carried through",
			fullName:   "ws/repo",
			settings:   BitbucketSettings{Workspace: "ws", TrackedBranch: "develop"},
			wantBranch: "develop",
		},
		{
			name:     "per-repo override wins over org-level tracked_branch",
			fullName: "ws/repo",
			settings: BitbucketSettings{
				Workspace:           "ws",
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"ws/repo": "release"},
			},
			wantBranch: "release",
		},
		{
			name:     "per-repo override for different repo does not apply",
			fullName: "ws/other-repo",
			settings: BitbucketSettings{
				Workspace:           "ws",
				TrackedBranch:       "develop",
				RepoBranchOverrides: map[string]string{"ws/repo": "release"},
			},
			wantBranch: "develop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := &bbRepository{
				UUID:     "{test-uuid}",
				Slug:     "repo",
				FullName: tt.fullName,
			}
			rec := mapBitbucketRepoToRecord(bb, tt.settings)
			assert.Equal(t, tt.wantBranch, rec.TrackedBranch)
		})
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vcs

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
)

func TestResolveTrackedRefs(t *testing.T) {
	prev := resolveTrackedRefsTimeout
	resolveTrackedRefsTimeout = 50 * time.Millisecond
	t.Cleanup(func() { resolveTrackedRefsTimeout = prev })
	ctx := context.Background()

	tests := []struct {
		name      string
		setup     func(m *sdkmocks.MockTrackedRefsResolver)
		nilClient bool
		wantOK    bool
		wantRepos int
	}{
		{
			name: "success returns the resolution",
			setup: func(m *sdkmocks.MockTrackedRefsResolver) {
				m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).Return(&workqueuev1.ResolveTrackedRefsResponse{
					Repos: []*workqueuev1.RepoTrackedRefs{{PlatformRepoId: "101", Refs: []string{"main"}}},
				}, nil)
			},
			wantOK:    true,
			wantRepos: 1,
		},
		{
			name: "RPC error degrades — nil response, ok=false, no panic",
			setup: func(m *sdkmocks.MockTrackedRefsResolver) {
				m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).Return(nil, errors.New("host unavailable"))
			},
			wantOK: false,
		},
		{
			name: "context timeout degrades the same as an error",
			setup: func(m *sdkmocks.MockTrackedRefsResolver) {
				m.EXPECT().ResolveTrackedRefs(gomock.Any(), gomock.Any()).DoAndReturn(
					func(rctx context.Context, _ *workqueuev1.ResolveTrackedRefsRequest) (*workqueuev1.ResolveTrackedRefsResponse, error) {
						// The helper installs a deadline; a hung server surfaces as ctx error.
						<-rctx.Done()
						return nil, rctx.Err()
					})
			},
			wantOK: false,
		},
		{
			name:      "nil client degrades without dialing",
			nilClient: true,
			wantOK:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var client sdk.TrackedRefsResolver
			if !tt.nilClient {
				m := sdkmocks.NewMockTrackedRefsResolver(gomock.NewController(t))
				if tt.setup != nil {
					tt.setup(m)
				}
				client = m
			}
			resp, ok := ResolveTrackedRefs(ctx, client, "org-1", "github")
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Len(t, resp.GetRepos(), tt.wantRepos)
			} else {
				assert.Nil(t, resp)
			}
		})
	}
}

// TestJoinCommitSHAs pins the width the list is rendered at. Twelve is not cosmetic: it
// is the canonical width every commit is stored and compared at, so a wider render would
// carry precision no comparison uses at three times the metadata cost, and a raw render
// would not match the rows it is meant to link.
func TestJoinCommitSHAs(t *testing.T) {
	assert.Empty(t, JoinCommitSHAs(nil))
	assert.Equal(t, "9f8e7d6c5b4a",
		JoinCommitSHAs([]string{"9F8E7D6C5B4A3F2E1D0C9B8A7F6E5D4C3B2A1F0E"}),
		"a full forty-character hash renders lowercased at the canonical width")
	assert.Equal(t, "aaaaaaaaaaaa,bbbbbbbbbbbb",
		JoinCommitSHAs([]string{"aaaaaaaaaaaa1111", " bbbbbbbbbbbb2222 ", "aaaaaaaaaaaa9999", ""}),
		"two hashes sharing a canonical prefix are one entry, and blanks are dropped")
	assert.Equal(t, "abc1234", JoinCommitSHAs([]string{"abc1234"}),
		"a shorter hash is passed through, never padded — a provider that abbreviates below "+
			"the canonical width must fail a comparison loudly rather than match the wrong commit")
}

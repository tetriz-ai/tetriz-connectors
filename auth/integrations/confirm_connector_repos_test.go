// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	ghconfig "github.com/tetriz-ai/tetriz-connectors/connectors/github"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestConfirmConnectorRepos(t *testing.T) {
	t.Parallel()

	const orgID = testOrgID

	tests := []struct {
		name        string
		orgID       string
		connectorID string
		repos       []string
		// enableErr, when non-nil, is what EnableConnectorWithSettings returns
		// (after the mutator runs, so a not-found / generic error path is exercised).
		enableErr error
		wantErr   bool
		wantCode  codes.Code
		// wantEnable is whether EnableConnectorWithSettings must be called.
		wantEnable bool
	}{
		{
			name:        "happy path enables connector and writes repos, preserving other settings",
			orgID:       orgID,
			connectorID: "github",
			repos:       []string{"acme/a", "acme/b"},
			wantEnable:  true,
		},
		{
			name:        "missing connector config fails precondition without enable",
			orgID:       orgID,
			connectorID: "github",
			repos:       []string{"acme/a"},
			enableErr:   gorm.ErrRecordNotFound,
			wantEnable:  true,
			wantErr:     true,
			wantCode:    codes.FailedPrecondition,
		},
		{
			name:        "DAO error surfaces Internal",
			orgID:       orgID,
			connectorID: "github",
			repos:       []string{"acme/a"},
			enableErr:   errors.New("db down"),
			wantEnable:  true,
			wantErr:     true,
			wantCode:    codes.Internal,
		},
		{
			name:        "unsupported connector is rejected before any DAO call",
			orgID:       orgID,
			connectorID: "gitlab",
			repos:       []string{"acme/a"},
			wantErr:     true,
			wantCode:    codes.Unimplemented,
		},
		{
			name:        "missing org_id is rejected",
			orgID:       "",
			connectorID: "github",
			repos:       []string{"acme/a"},
			wantErr:     true,
			wantCode:    codes.InvalidArgument,
		},
		{
			name:        "repo list over the absolute ceiling is rejected before any DAO call",
			orgID:       orgID,
			connectorID: "github",
			repos:       makeRepos(maxConfirmRepos + 1),
			wantErr:     true,
			wantCode:    codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)

			if tt.wantEnable {
				dao.EXPECT().EnableConnectorWithSettings(gomock.Any(), tt.orgID, sdk.ConnectorID("github"), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _ sdk.ConnectorID, mutator func(json.RawMessage) (json.RawMessage, error)) error {
						// Seed live settings with a non-default field so the test pins
						// that the mutator preserves existing settings while setting repos.
						seed, err := json.Marshal(ghconfig.GitHubSettings{IncludePrivate: true, Organization: "acme"})
						require.NoError(t, err)
						out, err := mutator(seed)
						require.NoError(t, err)
						var got ghconfig.GitHubSettings
						require.NoError(t, json.Unmarshal(out, &got))
						assert.Equal(t, tt.repos, got.Repositories, "mutator must set the confirmed repos")
						assert.True(t, got.IncludePrivate, "mutator must preserve IncludePrivate")
						assert.Equal(t, "acme", got.Organization, "mutator must preserve existing settings fields")
						return tt.enableErr
					})
			}

			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry()})

			_, err := svc.ConfirmConnectorRepos(context.Background(), &connintv1.ConfirmConnectorReposRequest{
				OrgId:         tt.orgID,
				ConnectorId:   tt.connectorID,
				RepoFullNames: tt.repos,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)
		})
	}
}

// makeRepos builds n distinct repo full-names for ceiling-boundary cases.
func makeRepos(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "acme/repo-" + itoa(i)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

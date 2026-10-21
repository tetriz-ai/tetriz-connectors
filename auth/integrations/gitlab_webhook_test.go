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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func gitlabConfig(org, secret string) *sdk.ConnectorConfig {
	creds, _ := json.Marshal(map[string]string{"webhook_secret": secret})
	return &sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: org,
		Credentials:    creds,
	}
}

// HandleGitLabWebhook runs under TENANT_SCOPE_SYSTEM: GitLab posts org-less
// deliveries to one shared endpoint, so the handler scans every org's config to
// match the X-Gitlab-Token before it knows the org. These cases lock in that the
// cross-tenant scan authenticates correctly and resolves the right org — the
// behavior the SYSTEM scope exists to enable, and that an empty/forged token
// never matches.
func TestHandleGitLabWebhook(t *testing.T) {
	const secret = "gl-secret-xyz"
	tests := []struct {
		name     string
		token    string
		configs  []*sdk.ConnectorConfig
		listErr  error
		wantErr  bool
		wantCode codes.Code
	}{
		{
			name:     "missing token rejected before any config scan",
			token:    "",
			wantErr:  true,
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "no config matches the token",
			token:    "does-not-match",
			configs:  []*sdk.ConnectorConfig{gitlabConfig("org-1", secret)},
			wantErr:  true,
			wantCode: codes.PermissionDenied,
		},
		{
			name:  "unparseable credentials are skipped, matching config still wins",
			token: secret,
			configs: []*sdk.ConnectorConfig{
				{ConnectorID: "gitlab", OrganizationID: "org-bad", Credentials: []byte("{not json")},
				gitlabConfig("org-2", secret),
			},
			wantErr: false,
		},
		{
			name:  "token matches the owning org among several configs",
			token: secret,
			configs: []*sdk.ConnectorConfig{
				gitlabConfig("org-other", "different-secret"),
				gitlabConfig("org-owner", secret),
			},
			wantErr: false,
		},
		{
			name:     "config list failure surfaces Internal",
			token:    "any",
			listErr:  assert.AnError,
			wantErr:  true,
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			// An empty token returns PermissionDenied before the scan, so the DAO
			// must not be touched — no expectation set asserts that early exit.
			if tt.token != "" {
				dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("gitlab")).
					Return(tt.configs, tt.listErr)
			}

			svc := NewService(Deps{Cfg: webhookConfig(), ConnectorDAO: dao, Sink: testkit.NewChannelSink(), Registry: testRegistry()})

			_, err := svc.HandleGitLabWebhook(context.Background(), &connintv1.HandleGitLabWebhookRequest{
				TokenHeader: tt.token,
				EventType:   "Pipeline Hook", // unrecognized event → acknowledged and dropped, no emit machinery needed
				Body:        []byte("{}"),
			})

			if tt.wantErr {
				require.Error(t, err)
				st, _ := status.FromError(err)
				assert.Equal(t, tt.wantCode, st.Code())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

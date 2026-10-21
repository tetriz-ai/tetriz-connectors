// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const testWebhookSecret = "test-webhook-secret"

func signPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func webhookConfig() *connectorconfig.Config {
	return &connectorconfig.Config{
		GitHubAppWebhookSecret: testWebhookSecret,
	}
}

func configWithInstallation(installationID string, enabled bool) *sdk.ConnectorConfig {
	creds, _ := json.Marshal(map[string]string{"installation_id": installationID})
	return &sdk.ConnectorConfig{
		ConnectorID:    "github",
		OrganizationID: "org-789",
		Credentials:    creds,
		Enabled:        enabled,
	}
}

func installationPayload(action string, installationID int64) []byte {
	return []byte(fmt.Sprintf(`{"action":"%s","installation":{"id":%d,"account":{"login":"test-org"}}}`, action, installationID))
}

// ---------------------------------------------------------------------------
// Signature verification tests (via HandleWebhook RPC)
// ---------------------------------------------------------------------------

func TestHandleWebhook_SignatureVerification(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		signature string
		wantErr   bool
		wantCode  codes.Code
	}{
		{
			name:      "ValidSignature",
			payload:   []byte(`{"action":"created"}`),
			signature: signPayload(testWebhookSecret, []byte(`{"action":"created"}`)),
			wantErr:   false,
		},
		{
			name:      "InvalidSignature",
			payload:   []byte(`{"action":"created"}`),
			signature: signPayload("wrong-secret", []byte(`{"action":"created"}`)),
			wantErr:   true,
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "MissingSignature",
			payload:   []byte(`{}`),
			signature: "",
			wantErr:   true,
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "MalformedSignature",
			payload:   []byte(`{}`),
			signature: "abc123",
			wantErr:   true,
			wantCode:  codes.PermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			svc := NewService(Deps{Cfg: webhookConfig(), ConnectorDAO: daomocks.NewMockIConnectorConfigDAO(ctrl), Registry: testRegistry()})

			_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{
				Body: tt.payload, SignatureHeader: tt.signature, EventType: "ping",
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

// ---------------------------------------------------------------------------
// Installation event tests (via HandleWebhook RPC)
// ---------------------------------------------------------------------------

func TestHandleWebhook_InstallationEvents(t *testing.T) {
	tests := []struct {
		name            string
		action          string
		installationID  int64
		existingConfigs []*sdk.ConnectorConfig
		wantEnabled     *bool // nil if no Save expected
		wantCode        codes.Code
	}{
		{
			name:            "InstallationDeleted",
			action:          "deleted",
			installationID:  12345,
			existingConfigs: []*sdk.ConnectorConfig{configWithInstallation("12345", true)},
			wantEnabled:     boolPtr(false),
		},
		{
			name:            "InstallationSuspend",
			action:          "suspend",
			installationID:  12345,
			existingConfigs: []*sdk.ConnectorConfig{configWithInstallation("12345", true)},
			wantEnabled:     boolPtr(false),
		},
		{
			name:            "InstallationUnsuspend",
			action:          "unsuspend",
			installationID:  12345,
			existingConfigs: []*sdk.ConnectorConfig{configWithInstallation("12345", false)},
			wantEnabled:     boolPtr(true),
		},
		{
			name:            "NoMatchingConfig",
			action:          "deleted",
			installationID:  12345,
			existingConfigs: []*sdk.ConnectorConfig{configWithInstallation("99999", true)},
			wantEnabled:     nil, // Save should NOT be called
			wantCode:        codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("github")).
				Return(tt.existingConfigs, nil)

			if tt.wantEnabled != nil {
				wantEnabled := *tt.wantEnabled
				dao.EXPECT().SetEnabled(gomock.Any(), gomock.Any(), sdk.ConnectorID("github"), wantEnabled).Return(nil)
			}
			// When wantEnabled is nil, no write expectation — gomock will fail if one is made.

			svc := NewService(Deps{Cfg: webhookConfig(), ConnectorDAO: dao, Registry: testRegistry(), Tenants: allOrganizations{}})
			payload := installationPayload(tt.action, tt.installationID)
			sig := signPayload(testWebhookSecret, payload)

			_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{
				Body: payload, SignatureHeader: sig, EventType: "installation",
			})

			assert.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

func boolPtr(v bool) *bool { return &v }

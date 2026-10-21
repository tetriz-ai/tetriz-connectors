// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// An installation "deleted" event whose store read or disable write fails,
// or an "unsuspend" whose re-enable write fails, is not acknowledged: the
// delivery shows as failed and can be redelivered.
func TestWebhookDisableFailureIsNotAcknowledged(t *testing.T) {
	for _, tc := range []string{"list fails", "save fails", "unsuspend fails"} {
		t.Run(tc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
			boom := errors.New("db down")
			if tc == "list fails" {
				dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("github")).Return(nil, boom)
			} else {
				dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{{
					OrganizationID: "00000000-0000-4000-8000-0000000e0b01", ConnectorID: "github", Enabled: tc != "unsuspend fails",
					Credentials: json.RawMessage(`{"installation_id":"4242"}`)}}, nil)
				dao.EXPECT().SetEnabled(gomock.Any(), gomock.Any(), sdk.ConnectorID("github"), tc == "unsuspend fails").Return(boom)
			}
			action := "deleted"
			if tc == "unsuspend fails" {
				action = "unsuspend"
			}
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: dao, Registry: testRegistry(), Tenants: allOrganizations{}})
			body := []byte(`{"action":"` + action + `","installation":{"id":4242,"account":{"login":"acme"}}}`)
			m := hmac.New(sha256.New, []byte("test-secret"))
			m.Write(body)
			_, err := svc.HandleWebhook(context.Background(), &connintv1.HandleWebhookRequest{
				Body: body, SignatureHeader: "sha256=" + hex.EncodeToString(m.Sum(nil)), EventType: "installation"})
			assert.Error(t, err, "a disable that was not written must not be acknowledged")
			assert.Equal(t, codes.Internal, status.Code(err))
		})
	}
}

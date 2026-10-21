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

	glconfig "github.com/tetriz-ai/tetriz-connectors/connectors/gitlab"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	daomocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// A disabled GitLab connector (suspended, paused, awaiting confirmation) must
// not publish webhook records: its scheduled syncs are off, and a webhook that
// still matches its secret would ingest data for it anyway.
func TestEdge_GitLabWebhookForADisabledConnectorPublishesNothing(t *testing.T) {
	const secret = "gl-secret"
	creds, err := json.Marshal(glconfig.GitLabCredentials{OAuthRefreshToken: "rt", WebhookSecret: secret})
	require.NoError(t, err)
	settings, err := json.Marshal(glconfig.GitLabSettings{GroupID: 42, GroupName: "g"})
	require.NoError(t, err)
	cfg := &sdk.ConnectorConfig{ConnectorID: "gitlab", OrganizationID: "org-disabled", Credentials: creds, Settings: settings, Enabled: false}

	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("gitlab")).Return([]*sdk.ConnectorConfig{cfg}, nil)
	sink := testkit.NewChannelSink()
	svc := NewService(Deps{Cfg: webhookConfig(), ConnectorDAO: dao, Sink: sink, Registry: testRegistry()})

	body := []byte(`{"object_kind":"merge_request","object_attributes":{"iid":7,"project_id":1,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-03-09T10:00:00Z"}}`)
	_, _ = svc.HandleGitLabWebhook(context.Background(), &connintv1.HandleGitLabWebhookRequest{
		TokenHeader: secret, EventType: "Merge Request Hook", EventUuid: "u-1", Body: body,
	})
	assert.Empty(t, sink.Records(), "a disabled connector's webhook was published")
}

// Two organizations whose stored secrets differ only in a suffix: a token
// that is a prefix of one secret matches neither.
func TestEdge_GitLabWebhookPrefixTokenMatchesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	dao := daomocks.NewMockIConnectorConfigDAO(ctrl)
	dao.EXPECT().ListByConnector(gomock.Any(), sdk.ConnectorID("gitlab")).
		Return([]*sdk.ConnectorConfig{gitlabConfig("org-a", "secret-aaaa"), gitlabConfig("org-b", "secret-bbbb")}, nil)
	svc := NewService(Deps{Cfg: webhookConfig(), ConnectorDAO: dao, Sink: testkit.NewChannelSink(), Registry: testRegistry()})
	_, err := svc.HandleGitLabWebhook(context.Background(), &connintv1.HandleGitLabWebhookRequest{TokenHeader: "secret-", EventType: "Pipeline Hook", Body: []byte("{}")})
	assert.Error(t, err)
}

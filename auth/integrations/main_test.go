// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

const (
	testOrgID = "org-test-123"
	testSlug  = "acme-app"
)

func testConfig() *connectorconfig.Config {
	return &connectorconfig.Config{
		EnabledConnectors: map[string]bool{
			"github":    true,
			"gitlab":    true,
			"bitbucket": true,
			"jira":      true,
			"linear":    true,
		},
		GitHubApp: &connectorconfig.GitHubAppStaticConfig{
			AppID:    123456,
			Slug:     testSlug,
			ClientID: "Iv1.0000000000000000",
		},
		GitHubAppWebhookSecret: "test-secret",
	}
}

func testConfigWithDisabledConnector(connectorID string) *connectorconfig.Config {
	cfg := testConfig()
	cfg.EnabledConnectors[connectorID] = false
	return cfg
}

// testEncryptor returns a real AES-256-GCM encryptor for tests that assert the
// OAuth-token picker blobs are encrypted at rest in Redis. A fixed 32-byte key
// keeps the test deterministic.
func testEncryptor(t *testing.T) cryptutil.Encryptor {
	t.Helper()
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	enc, err := cryptutil.NewAESGCMEncryptor(key)
	require.NoError(t, err)
	return enc
}

// testRegistry returns a ConnectorRegistry with stub connectors for testing.
// Each stub returns the correct Category so VCS guard logic works.
func testRegistry() *sdk.ConnectorRegistry {
	reg := sdk.NewConnectorRegistry()
	for _, s := range []struct {
		id       sdk.ConnectorID
		category sdk.ConnectorCategory
	}{
		{"github", sdk.CategoryVCS},
		{"gitlab", sdk.CategoryVCS},
		{"bitbucket", sdk.CategoryVCS},
		{"jira", sdk.CategoryIssueTracking},
		{"linear", sdk.CategoryIssueTracking},
	} {
		reg.Register(&stubTestConnector{id: s.id, category: s.category})
	}
	return reg
}

// stubTestConnector is a minimal Connector for registry-based tests.
type stubTestConnector struct {
	id       sdk.ConnectorID
	category sdk.ConnectorCategory
}

func (s *stubTestConnector) ID() sdk.ConnectorID { return s.id }
func (s *stubTestConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{s.category}
}
func (s *stubTestConnector) DisplayName() string                   { return string(s.id) }
func (s *stubTestConnector) Operations() []sdk.OperationDescriptor { return nil }
func (s *stubTestConnector) ValidateConfig(_ context.Context, _ *sdk.ConnectorConfig) error {
	return nil
}

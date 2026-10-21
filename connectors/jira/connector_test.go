// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestJiraConnector_ID(t *testing.T) {
	c := NewConnector(nil)
	assert.Equal(t, sdk.ConnectorID("jira"), c.ID())
}

func TestJiraConnector_DisplayName(t *testing.T) {
	c := NewConnector(nil)
	assert.Equal(t, "Jira", c.DisplayName())
}

func TestJiraConnector_Operations(t *testing.T) {
	c := NewConnector(nil)
	ops := c.Operations()

	require.Len(t, ops, 6)

	expected := []struct {
		id            sdk.OperationID
		ingestionType sdk.IngestionType
		// hasSchedule is false for ops that are registered but not scheduled
		// (DefaultSchedule == ""), such as fetch_comments.
		hasSchedule bool
	}{
		{"fetch_projects", sdk.IngestionPull, true},
		{"fetch_issues", sdk.IngestionPull, true},
		{"fetch_issues_data", sdk.IngestionPull, true},
		{"fetch_comments", sdk.IngestionPull, false},
		{"fetch_worklogs", sdk.IngestionPull, true},
		{"fetch_sprints", sdk.IngestionPull, true},
	}

	for i, exp := range expected {
		assert.Equal(t, exp.id, ops[i].ID, "operation %d ID", i)
		assert.Equal(t, exp.ingestionType, ops[i].IngestionType, "operation %d ingestion type", i)
		if exp.hasSchedule {
			assert.NotEmpty(t, ops[i].DefaultSchedule, "operation %d should have schedule", i)
		} else {
			assert.Empty(t, ops[i].DefaultSchedule, "operation %d should be dormant (empty schedule)", i)
		}
		assert.NotNil(t, ops[i].Factory, "operation %d factory", i)
	}
}

func TestJiraConnector_OperationFactories(t *testing.T) {
	c := NewConnector(nil)
	ops := c.Operations()

	for _, desc := range ops {
		op := desc.Factory()
		assert.Equal(t, desc.ID, op.OperationID(), "factory for %s should return operation with matching ID", desc.ID)
	}
}

func TestJiraConnector_Registration(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil)

	registry.Register(c)

	got, err := registry.Get(c.ID())
	require.NoError(t, err)
	assert.Equal(t, c.ID(), got.ID())
}

func TestJiraConnector_Registration_DuplicatePanics(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil)
	registry.Register(c)

	assert.Panics(t, func() {
		registry.Register(NewConnector(nil))
	})
}

func TestJiraConnector_ValidateConfig_Valid(t *testing.T) {
	c := NewConnector(nil)
	err := c.ValidateConfig(context.Background(), validConfig())
	assert.NoError(t, err)
}

func TestJiraConnector_ValidateConfig_MissingRefreshToken(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Credentials = mustJSON(JiraCredentials{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OAuth refresh token")
}

func TestJiraConnector_ValidateConfig_MissingCloudID(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Settings = mustJSON(JiraSettings{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloud_id")
}

func TestJiraConnector_ValidateConfig_InvalidCredentialsJSON(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Credentials = json.RawMessage(`{invalid}`)

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse credentials")
}

// --- Test helpers ---

func validConfig() *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "jira",
		OrganizationID: "org-test",
		Credentials: mustJSON(JiraCredentials{
			OAuthRefreshToken: "test-refresh-token",
		}),
		Settings: mustJSON(JiraSettings{
			CloudID:   "abc123-def456",
			CloudName: "My Jira Site",
			SiteURL:   "https://myorg.atlassian.net",
		}),
		Enabled: true,
	}
}

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

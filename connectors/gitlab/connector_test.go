// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/gatewaytest"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestGitLabConnector_ID(t *testing.T) {
	c := NewConnector()
	assert.Equal(t, sdk.ConnectorID("gitlab"), c.ID())
}

func TestGitLabConnector_DisplayName(t *testing.T) {
	c := NewConnector()
	assert.Equal(t, "GitLab", c.DisplayName())
}

func TestGitLabConnector_Operations(t *testing.T) {
	c := NewConnector()
	ops := c.Operations()

	require.Len(t, ops, 17)

	expected := []struct {
		id                sdk.OperationID
		ingestionType     sdk.IngestionType
		hasSchedule       bool
		schedule          string
		requiresMultiUser bool
	}{
		{"fetch_projects", sdk.IngestionPull, true, "0 */6 * * *", false},
		{"fetch_prs", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"fetch_commits", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"fetch_issues", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"fetch_reviews", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"fetch_pr_comments", sdk.IngestionPull, true, "*/15 * * * *", false},
		{"webhook_events", sdk.IngestionWebhook, false, "", false},
		// Group members is a whole-group multi-user roll-up: skipped for INDIVIDUAL tenants.
		{"fetch_org_members", sdk.IngestionPull, true, "0 */6 * * *", true},
		{"fetch_mr_diffs", sdk.IngestionPull, true, "*/15 * * * *", false},
		{"drain_defect_lineage", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"drain_defect_scoring", sdk.IngestionPull, true, "*/30 * * * *", false},
		{"fetch_prs_data", sdk.IngestionPull, true, "*/15 * * * *", false},
		{"fetch_repos", sdk.IngestionPull, true, "0 */6 * * *", false},
		{"fetch_repo_tree", sdk.IngestionPull, true, "0 */6 * * *", false},
		{"fetch_branches", sdk.IngestionPull, true, "0 */6 * * *", false},
		{"fetch_file_content", sdk.IngestionPull, true, "0 3 * * *", false},
		{"fetch_config", sdk.IngestionPull, false, "", false},
	}

	for i, exp := range expected {
		assert.Equal(t, exp.id, ops[i].ID, "operation %d ID", i)
		assert.Equal(t, exp.ingestionType, ops[i].IngestionType, "operation %d ingestion type", i)
		if exp.hasSchedule {
			assert.Equal(t, exp.schedule, ops[i].DefaultSchedule, "operation %d schedule", i)
		} else {
			assert.Empty(t, ops[i].DefaultSchedule, "operation %d should not have schedule", i)
		}
		assert.Equal(t, exp.requiresMultiUser, ops[i].RequiresMultiUser, "operation %d requiresMultiUser", i)
		assert.NotNil(t, ops[i].Factory, "operation %d factory", i)
	}
}

func TestGitLabConnector_OperationFactories(t *testing.T) {
	c := NewConnector()
	ops := c.Operations()

	for _, desc := range ops {
		op := desc.Factory()
		assert.Equal(t, desc.ID, op.OperationID(), "factory for %s should return operation with matching ID", desc.ID)
	}
}

func TestGitLabConnector_ValidateConfig_Valid(t *testing.T) {
	c := NewConnector()
	cfg := validConfig()

	err := c.ValidateConfig(context.Background(), cfg)
	assert.NoError(t, err)
}

func TestGitLabConnector_ValidateConfig_MissingRefreshToken(t *testing.T) {
	c := NewConnector()
	cfg := validConfig()
	cfg.Credentials = mustJSON(GitLabCredentials{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "oauth_refresh_token")
}

func TestGitLabConnector_ValidateConfig_ZeroGroupID(t *testing.T) {
	c := NewConnector()
	cfg := validConfig()
	cfg.Settings = mustJSON(GitLabSettings{GroupID: 0})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group_id")
}

func TestGitLabConnector_ValidateConfig_InvalidCredentialsJSON(t *testing.T) {
	c := NewConnector()
	cfg := validConfig()
	cfg.Credentials = json.RawMessage(`{invalid}`)

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse credentials")
}

func TestGitLabConnector_ValidateConfig_InvalidSettingsJSON(t *testing.T) {
	c := NewConnector()
	cfg := validConfig()
	cfg.Settings = json.RawMessage(`{invalid}`)

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse settings")
}

// --- Test helpers ---

func validConfig() *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "gitlab",
		OrganizationID: "org-123",
		Credentials: mustJSON(GitLabCredentials{
			OAuthRefreshToken: "test-refresh-token",
		}),
		Settings: mustJSON(GitLabSettings{
			GroupID:   42,
			GroupPath: "mygroup",
			GroupName: "My Group",
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
	// Every test sees the same gateway config: the vendor defaults plus one
	// simulator-routed org, so the simulator-routing tests never skip.
	gatewaytest.Install()
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/tetriz-ai/tetriz-connectors/internal/gateway/gatewaytest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestGitHubConnector_ID(t *testing.T) {
	c := NewConnector(nil, 0)
	assert.Equal(t, sdk.ConnectorID("github"), c.ID())
}

func TestGitHubConnector_DisplayName(t *testing.T) {
	c := NewConnector(nil, 0)
	assert.Equal(t, "GitHub", c.DisplayName())
}

func TestGitHubConnector_Operations(t *testing.T) {
	c := NewConnector(nil, 0)
	ops := c.Operations()

	require.Len(t, ops, 19)

	expected := []struct {
		id                sdk.OperationID
		ingestionType     sdk.IngestionType
		hasSchedule       bool
		requiresMultiUser bool
	}{
		{"fetch_prs", sdk.IngestionPull, true, false},
		{"fetch_commits", sdk.IngestionPull, true, false},
		{"fetch_issues", sdk.IngestionPull, true, false},
		{"fetch_reviews", sdk.IngestionPull, true, false},
		{"fetch_pr_comments", sdk.IngestionPull, true, false},
		{"fetch_copilot_usage", sdk.IngestionPull, true, true},
		{"webhook_events", sdk.IngestionWebhook, false, false},
		{"fetch_org_members", sdk.IngestionPull, true, true},
		{"fetch_pr_diffs", sdk.IngestionPull, true, false},
		{"drain_defect_lineage", sdk.IngestionPull, true, false},
		{"drain_defect_scoring", sdk.IngestionPull, true, false},
		{"fetch_prs_data", sdk.IngestionPull, true, false},
		{"fetch_issues_data", sdk.IngestionPull, true, false},
		{"fetch_projects", sdk.IngestionPull, false, true},
		{"fetch_repos", sdk.IngestionPull, true, false},
		{"fetch_repo_tree", sdk.IngestionPull, true, false},
		{"fetch_branches", sdk.IngestionPull, true, false},
		{"fetch_file_content", sdk.IngestionPull, true, false},
		{"fetch_config", sdk.IngestionPull, false, false},
	}

	for i, exp := range expected {
		assert.Equal(t, exp.id, ops[i].ID, "operation %d ID", i)
		assert.Equal(t, exp.ingestionType, ops[i].IngestionType, "operation %d ingestion type", i)
		if exp.hasSchedule {
			assert.NotEmpty(t, ops[i].DefaultSchedule, "operation %d should have schedule", i)
		} else {
			assert.Empty(t, ops[i].DefaultSchedule, "operation %d should not have schedule", i)
		}
		assert.Equal(t, exp.requiresMultiUser, ops[i].RequiresMultiUser, "operation %d requiresMultiUser", i)
		assert.NotNil(t, ops[i].Factory, "operation %d factory", i)
	}
}

func TestGitHubConnector_OperationFactories(t *testing.T) {
	c := NewConnector(nil, 0)
	ops := c.Operations()

	// Verify each factory produces an operation with the correct ID.
	for _, desc := range ops {
		op := desc.Factory()
		assert.Equal(t, desc.ID, op.OperationID(), "factory for %s should return operation with matching ID", desc.ID)
	}
}

func TestGitHubConnector_Registration(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil, 0)

	// Should not panic.
	registry.Register(c)

	got, err := registry.Get(c.ID())
	require.NoError(t, err)
	assert.Equal(t, c.ID(), got.ID())
}

func TestGitHubConnector_Registration_DuplicatePanics(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil, 0)
	registry.Register(c)

	assert.Panics(t, func() {
		registry.Register(NewConnector(nil, 0))
	})
}

func TestGitHubConnector_ValidateConfig_Valid(t *testing.T) {
	c := NewConnector(nil, 0)
	cfg := validConfig()

	err := c.ValidateConfig(context.Background(), cfg)
	assert.NoError(t, err)
}

func TestGitHubConnector_ValidateConfig_MissingCredentials(t *testing.T) {
	c := NewConnector(nil, 0)
	cfg := validConfig()
	cfg.Credentials = mustJSON(GitHubCredentials{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "installation_id")
}

func TestGitHubConnector_ValidateConfig_MissingOrganization(t *testing.T) {
	c := NewConnector(nil, 0)
	cfg := validConfig()
	cfg.Settings = mustJSON(GitHubSettings{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "organization is required")
}

func TestGitHubConnector_ValidateConfig_InvalidJSON(t *testing.T) {
	c := NewConnector(nil, 0)
	cfg := validConfig()
	cfg.Credentials = json.RawMessage(`{invalid}`)

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse credentials")
}

// TestGitHubConnector_ValidateConfig_PATWithoutOrganization pins that a PAT
// (single-user) connector validates with no organization set — the org field is
// required only for an App credential; rejecting every empty org would block
// the whole individual flow at connect time.
func TestGitHubConnector_ValidateConfig_PATWithoutOrganization(t *testing.T) {
	t.Parallel()
	c := NewConnector(nil, 0)
	cfg := &sdk.ConnectorConfig{
		ConnectorID:    "github",
		OrganizationID: "tenant-123",
		Credentials:    mustJSON(GitHubCredentials{PersonalAccessToken: "github_pat_abc"}),
		Settings:       mustJSON(GitHubSettings{}), // no organization
		Enabled:        true,
	}
	assert.NoError(t, c.ValidateConfig(context.Background(), cfg))
}

// TestValidateCredentials_XOR locks the exactly-one-ability rule: a credential
// must set installation_id (App) xor personal_access_token (PAT).
func TestValidateCredentials_XOR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		creds     GitHubCredentials
		wantErr   bool
		errSubstr string
	}{
		{
			name:  "App only is valid",
			creds: GitHubCredentials{InstallationID: "12345678"},
		},
		{
			name:  "PAT only is valid",
			creds: GitHubCredentials{PersonalAccessToken: "github_pat_abc"},
		},
		{
			name:      "neither set is rejected",
			creds:     GitHubCredentials{},
			wantErr:   true,
			errSubstr: "installation_id",
		},
		{
			name:      "both set is rejected",
			creds:     GitHubCredentials{InstallationID: "12345678", PersonalAccessToken: "github_pat_abc"},
			wantErr:   true,
			errSubstr: "not both",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateCredentials(tt.creds)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// TestRequiresTenantType pins exactly which descriptors are tagged so the
// orchestrator's fail-loud intent guard applies: fetch_repos (listing source
// branches on intent) and fetch_prs (the individual variant drops the org-scoped
// team reviewer). Every other op stays repo-scoped and untagged.
func TestRequiresTenantType(t *testing.T) {
	t.Parallel()
	c := NewConnector(nil, 0)
	for _, op := range c.Operations() {
		want := op.ID == opFetchRepos || op.ID == opFetchPRs
		assert.Equal(t, want, op.RequiresTenantType, "op %s RequiresTenantType", op.ID)
	}
}

// --- Test helpers ---

func validConfig() *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "github",
		OrganizationID: "org-123",
		Credentials: mustJSON(GitHubCredentials{
			InstallationID: "12345678",
		}),
		Settings: mustJSON(GitHubSettings{
			Organization: "acme",
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
	// Every test sees the same gateway config: the vendor defaults plus one
	// simulator-routed org, so the routing decisions are deterministic.
	gatewaytest.Install()
	os.Exit(testguard.Run(m))
}

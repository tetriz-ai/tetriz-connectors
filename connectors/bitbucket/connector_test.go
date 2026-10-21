// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

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

func TestBitbucketConnector_ID(t *testing.T) {
	c := NewConnector(nil)
	assert.Equal(t, sdk.ConnectorID("bitbucket"), c.ID())
}

func TestBitbucketConnector_DisplayName(t *testing.T) {
	c := NewConnector(nil)
	assert.Equal(t, "Bitbucket", c.DisplayName())
}

func TestBitbucketConnector_Operations(t *testing.T) {
	c := NewConnector(nil)
	ops := c.Operations()

	require.Len(t, ops, 15)

	expected := []struct {
		id                sdk.OperationID
		ingestionType     sdk.IngestionType
		hasSchedule       bool
		requiresMultiUser bool
	}{
		{"fetch_repos", sdk.IngestionPull, true, false},
		{"fetch_prs", sdk.IngestionPull, true, false},
		{"fetch_commits", sdk.IngestionPull, true, false},
		{"fetch_reviews", sdk.IngestionPull, true, false},
		{"fetch_pr_comments", sdk.IngestionPull, true, false},
		// Workspace members is a whole-workspace multi-user roll-up: skipped for INDIVIDUAL tenants.
		{"fetch_workspace_members", sdk.IngestionPull, true, true},
		{"webhook_events", sdk.IngestionWebhook, false, false},
		{"fetch_pr_diffs", sdk.IngestionPull, true, false},
		{"fetch_prs_data", sdk.IngestionPull, true, false},
		{"fetch_repo_tree", sdk.IngestionPull, true, false},
		{"fetch_branches", sdk.IngestionPull, true, false},
		{"fetch_file_content", sdk.IngestionPull, true, false},
		{"fetch_config", sdk.IngestionPull, false, false},
		{"drain_defect_lineage", sdk.IngestionPull, true, false},
		{"drain_defect_scoring", sdk.IngestionPull, true, false},
	}

	for _, desc := range ops {
		assert.NotEqual(t, sdk.OperationID("fetch_issues"), desc.ID,
			"fetch_issues must not be registered (Bitbucket issue tracker disabled — issues live in Jira)")
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

func TestBitbucketConnector_OperationFactories(t *testing.T) {
	c := NewConnector(nil)
	ops := c.Operations()

	for _, desc := range ops {
		op := desc.Factory()
		assert.Equal(t, desc.ID, op.OperationID(), "factory for %s should return operation with matching ID", desc.ID)
	}
}

func TestBitbucketConnector_Registration(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil)

	registry.Register(c)

	got, err := registry.Get(c.ID())
	require.NoError(t, err)
	assert.Equal(t, c.ID(), got.ID())
}

func TestBitbucketConnector_Registration_DuplicatePanics(t *testing.T) {
	registry := sdk.NewConnectorRegistry()
	c := NewConnector(nil)
	registry.Register(c)

	assert.Panics(t, func() {
		registry.Register(NewConnector(nil))
	})
}

func TestBitbucketConnector_ValidateConfig_Valid(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()

	err := c.ValidateConfig(context.Background(), cfg)
	assert.NoError(t, err)
}

func TestBitbucketConnector_ValidateConfig_MissingCredentials(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Credentials = mustJSON(BitbucketCredentials{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials must include")
}

func TestBitbucketConnector_ValidateConfig_MissingWorkspace(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Settings = mustJSON(BitbucketSettings{})

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workspace is required")
}

func TestBitbucketConnector_ValidateConfig_InvalidJSON(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Credentials = json.RawMessage(`{invalid}`)

	err := c.ValidateConfig(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse credentials")
}

func TestBitbucketConnector_ValidateConfig_OAuthAuth(t *testing.T) {
	c := NewConnector(nil)
	cfg := validConfig()
	cfg.Credentials = mustJSON(BitbucketCredentials{
		OAuthAccessToken: "test-oauth-token",
	})

	err := c.ValidateConfig(context.Background(), cfg)
	assert.NoError(t, err)
}

// The runner authenticates Bitbucket through OAuth only: credentials that
// carry an app password (or its username) are refused before any sync, with
// a message that says so, and credentials with no OAuth token are refused too.
func TestBitbucketConnector_ValidateConfig_RefusesAppPasswords(t *testing.T) {
	c := NewConnector(nil)
	for name, tc := range map[string]struct {
		creds string
		want  string
	}{
		"app password":                {`{"username":"testuser","app_password":"test-app-password"}`, "bitbucket: app passwords are not supported; the runner authenticates Bitbucket through OAuth (the connect flow in auth/integrations writes the tokens)"},
		"app password beside a token": {`{"oauth_access_token":"t","app_password":"p"}`, "bitbucket: app passwords are not supported; the runner authenticates Bitbucket through OAuth (the connect flow in auth/integrations writes the tokens)"},
		"no token":                    {`{"webhook_secret":"s"}`, "bitbucket: credentials must include an OAuth token"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Credentials = json.RawMessage(tc.creds)
			err := c.ValidateConfig(context.Background(), cfg)
			require.Error(t, err)
			assert.Equal(t, tc.want, err.Error())
		})
	}
	for _, creds := range []string{`{"oauth_access_token":"t"}`, `{"oauth_refresh_token":"r"}`} {
		cfg := validConfig()
		cfg.Credentials = json.RawMessage(creds)
		assert.NoError(t, c.ValidateConfig(context.Background(), cfg), creds)
	}
}

// --- Test helpers ---

func validConfig() *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		ConnectorID:    "bitbucket",
		OrganizationID: "org-123",
		Credentials: mustJSON(BitbucketCredentials{
			OAuthAccessToken: "test-oauth-token",
		}),
		Settings: mustJSON(BitbucketSettings{
			Workspace: "myworkspace",
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

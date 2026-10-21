// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// DisableIfCredentialsUnchanged and SetEnabled, as the table owner under
// forced row-level security: each writes only enabled, the first only while
// the credentials fence holds, and a missing row is reported.
func TestEnabledOnlyWritesAsTableOwner(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("enabled-only writes as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	in := func(org string, fn func(context.Context) error) error {
		return store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, fn)
	}
	seed := func(org string) *sdk.ConnectorConfig {
		require.NoError(t, in(org, func(s context.Context) error {
			if err := repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "slack_comms", Enabled: true,
				Credentials: json.RawMessage(`{"bot_token":"xoxb-1"}`), Settings: json.RawMessage(`{"team_name":"Acme"}`),
				OperationOverrides: map[sdk.OperationID]json.RawMessage{"send": json.RawMessage(`{"schedule":"0 * * * *"}`)}}); err != nil {
				return err
			}
			_, err := repo.ResetHealthState(s, org, "slack_comms")
			return err
		}))
		return getEnabledOnly(t, in, repo, org)
	}
	unchanged := func(t *testing.T, got *sdk.ConnectorConfig, token string) {
		t.Helper()
		assert.JSONEq(t, `{"bot_token":"`+token+`"}`, string(got.Credentials))
		assert.JSONEq(t, `{"team_name":"Acme"}`, string(got.Settings))
		assert.Contains(t, got.OperationOverrides, sdk.OperationID("send"))
	}

	t.Run("disable, fence held", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000f0e01"
		before := seed(org)
		var disabled bool
		require.NoError(t, in(org, func(s context.Context) (err error) {
			disabled, err = repo.DisableIfCredentialsUnchanged(s, org, "slack_comms", before.CredentialsUpdatedAt)
			return err
		}))
		assert.True(t, disabled)
		got := getEnabledOnly(t, in, repo, org)
		assert.False(t, got.Enabled)
		unchanged(t, got, "xoxb-1")
		assert.True(t, got.CredentialsUpdatedAt.Equal(before.CredentialsUpdatedAt), "the fence is not advanced")
	})

	t.Run("disable, fence lost to a reconnect", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000f0e02"
		before := seed(org)
		require.NoError(t, in(org, func(s context.Context) error {
			if err := repo.SaveCredentialsEnablePreservingSettings(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "slack_comms",
				Enabled: true, Credentials: json.RawMessage(`{"bot_token":"xoxb-2"}`), Settings: json.RawMessage(`{}`)}); err != nil {
				return err
			}
			_, err := repo.ResetHealthState(s, org, "slack_comms")
			return err
		}))
		var disabled bool
		require.NoError(t, in(org, func(s context.Context) (err error) {
			disabled, err = repo.DisableIfCredentialsUnchanged(s, org, "slack_comms", before.CredentialsUpdatedAt)
			return err
		}))
		assert.False(t, disabled, "a reconnect since the read wins")
		got := getEnabledOnly(t, in, repo, org)
		assert.True(t, got.Enabled)
		unchanged(t, got, "xoxb-2")
	})

	t.Run("disable, missing row", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000f0e03"
		var disabled bool
		require.NoError(t, in(org, func(s context.Context) (err error) {
			disabled, err = repo.DisableIfCredentialsUnchanged(s, org, "slack_comms", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			return err
		}))
		assert.False(t, disabled)
	})

	t.Run("set enabled writes only enabled", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000f0e04"
		before := seed(org)
		require.NoError(t, in(org, func(s context.Context) error { return repo.SetEnabled(s, org, "slack_comms", false) }))
		got := getEnabledOnly(t, in, repo, org)
		assert.False(t, got.Enabled)
		unchanged(t, got, "xoxb-1")
		assert.True(t, got.CredentialsUpdatedAt.Equal(before.CredentialsUpdatedAt))
		require.NoError(t, in(org, func(s context.Context) error { return repo.SetEnabled(s, org, "slack_comms", true) }))
		assert.True(t, getEnabledOnly(t, in, repo, org).Enabled)
	})

	t.Run("set enabled, missing row", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000000f0e05"
		err := in(org, func(s context.Context) error { return repo.SetEnabled(s, org, "slack_comms", true) })
		assert.True(t, errors.Is(err, gorm.ErrRecordNotFound), "got %v", err)
	})
}

func getEnabledOnly(t *testing.T, in func(string, func(context.Context) error) error, repo *store.ConnectorConfigRepo, org string) *sdk.ConnectorConfig {
	t.Helper()
	var out *sdk.ConnectorConfig
	require.NoError(t, in(org, func(s context.Context) (err error) {
		out, err = repo.Get(s, org, "slack_comms")
		return err
	}))
	return out
}

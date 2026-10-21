// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// `config put` without --credentials keeps the stored credentials and health
// state, and a first put without it is a usage error.
func TestConfigPutKeepsStoredCredentials(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("config put", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	const org = "00000000-0000-4000-8000-0000000c0f01"

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"acme"}`)
	assert.Equal(t, 2, code, "a first put needs --credentials")
	assert.Contains(t, errOut, "--credentials is required")

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github",
		"--settings", `{"organization":"acme"}`, "--credentials", `{"personal_access_token":"kept"}`)
	require.Equal(t, 0, code, errOut)

	stored := func() *sdk.ConnectorConfig {
		cfg, err := app.FromEnv()
		require.NoError(t, err)
		a, err := app.New(context.Background(), cfg)
		require.NoError(t, err)
		defer a.Close() //nolint:errcheck // test
		var got *sdk.ConnectorConfig
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(context.Background(), org, store.ScopeEnforced, func(ctx context.Context) error {
			var err error
			got, err = a.Configs.Get(ctx, org, "github")
			if err != nil {
				return err
			}
			if got.HealthState == sdk.HealthStateHealthy {
				_, err = a.Configs.MarkAuthRevoked(ctx, org, "github", got.CredentialsUpdatedAt)
			}
			return err
		}))
		return got
	}
	first := stored() // also pauses the connector

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github", "--settings", `{"organization":"renamed"}`)
	require.Equal(t, 0, code, errOut)
	got := stored()
	assert.JSONEq(t, `{"personal_access_token":"kept"}`, string(got.Credentials), "the credentials survive a settings-only put")
	assert.JSONEq(t, `{"organization":"renamed"}`, string(got.Settings))
	assert.Equal(t, sdk.HealthStateAuthRevoked, got.HealthState, "a settings-only put keeps the health state")
	assert.True(t, got.CredentialsUpdatedAt.Equal(first.CredentialsUpdatedAt))
}

// `config put` without --settings keeps the stored settings, whether it
// carries credentials or only toggles the connector.
func TestConfigPutKeepsStoredSettings(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("config put", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	const org = "00000000-0000-4000-8000-0000000c0f02"
	settings := func() (string, bool) {
		cfg, err := app.FromEnv()
		require.NoError(t, err)
		a, err := app.New(context.Background(), cfg)
		require.NoError(t, err)
		defer a.Close() //nolint:errcheck // test
		var got *sdk.ConnectorConfig
		require.NoError(t, store.PostgresTxRunner{}.RunInScope(context.Background(), org, store.ScopeEnforced, func(ctx context.Context) error {
			var err error
			got, err = a.Configs.Get(ctx, org, "github")
			return err
		}))
		return string(got.Settings), got.Enabled
	}

	code, _, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "github",
		"--settings", `{"organization":"acme"}`, "--credentials", `{"personal_access_token":"a"}`)
	require.Equal(t, 0, code, errOut)

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github", "--disabled")
	require.Equal(t, 0, code, errOut)
	s, enabled := settings()
	assert.JSONEq(t, `{"organization":"acme"}`, s, "a toggle keeps the settings")
	assert.False(t, enabled)

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github", "--credentials", `{"personal_access_token":"b"}`)
	require.Equal(t, 0, code, errOut)
	s, enabled = settings()
	assert.JSONEq(t, `{"organization":"acme"}`, s, "a credential write keeps the settings")
	assert.True(t, enabled)
}

// --settings and --credentials must be JSON objects.
func TestConfigPutRequiresJSONObjects(t *testing.T) {
	for _, args := range [][]string{
		{"--settings", `[]`, "--credentials", `{}`},
		{"--settings", `{}`, "--credentials", `null`},
		{"--settings", `"x"`, "--credentials", `{}`},
	} {
		code, _, errOut := runCLI(t, append([]string{"config", "put", "--org", "00000000-0000-4000-8000-0000000c0f03", "--connector", "github"}, args...)...)
		assert.Equal(t, 2, code, "%v", args)
		assert.Contains(t, errOut, "must be a JSON object", "%v", args)
	}
}

// migrate and cursor show read no credentials, so they run without
// TETRIZ_CREDENTIALS_KEY; a command that writes credentials still needs it.
func TestCommandsThatReadNoCredentialsNeedNoKey(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("migrate without a key", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "")
	const org = "00000000-0000-4000-8000-0000000c0f04"

	code, out, errOut := runCLI(t, "migrate")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "applied")

	code, out, errOut = runCLI(t, "cursor", "show", "--org", org, "--connector", "github", "--op", "fetch_prs")
	require.Equal(t, 0, code, errOut)
	assert.Equal(t, "no cursor\n", out)

	code, _, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "github", "--credentials", `{"personal_access_token":"x"}`)
	assert.NotEqual(t, 0, code)
	assert.Contains(t, errOut, "TETRIZ_CREDENTIALS_KEY")
}

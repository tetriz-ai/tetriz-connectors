// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
)

const edgeOrg = "00000000-0000-4000-8000-0000000ed9e1"

// edgeNoStore points the runner at a database and Redis nothing listens on
// and leaves it without a credentials key, so a case that got past flag
// validation would fail differently.
func edgeNoStore(t *testing.T) {
	t.Helper()
	t.Setenv("TETRIZ_POSTGRES_DSN", "host=127.0.0.1 port=1 user=nobody dbname=none connect_timeout=1")
	t.Setenv("TETRIZ_REDIS_ADDR", "127.0.0.1:1")
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "")
}

// --settings and --credentials that are not JSON objects are usage errors
// (exit 2, message on stderr, nothing on stdout), caught before the
// environment or the store is read.
func TestEdge_ConfigPutRejectsNonObjectJSON(t *testing.T) {
	base := []string{"config", "put", "--org", edgeOrg, "--connector", "github"}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"credentials array", []string{"--credentials", "[]"}, "--credentials must be a JSON object"},
		{"credentials null", []string{"--credentials", "null"}, "--credentials must be a JSON object"},
		{"credentials string", []string{"--credentials", `"tok"`}, "--credentials must be a JSON object"},
		{"credentials truncated", []string{"--credentials", `{"personal_access_token":`}, "--credentials must be a JSON object"},
		{"credentials trailing data", []string{"--credentials", `{} {}`}, "--credentials must be a JSON object"},
		{"settings explicitly empty", []string{"--settings", "", "--credentials", "{}"}, "--settings must be a JSON object"},
		{"settings null", []string{"--settings", "null", "--credentials", "{}"}, "--settings must be a JSON object"},
		{"settings number", []string{"--settings", "1"}, "--settings must be a JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeNoStore(t)
			code, out, errOut := runCLI(t, append(append([]string{}, base...), tc.args...)...)
			assert.Equal(t, 2, code)
			assert.Empty(t, out)
			assert.Contains(t, errOut, tc.want)
		})
	}
}

// Flag-level mistakes are usage errors before anything is opened.
func TestEdge_FlagMistakesExit2(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"sync", "--orgg", edgeOrg, "--connector", "github", "--op", "fetch_prs"}, "flag provided but not defined: -orgg"},
		{"tenant is case sensitive", []string{"sync", "--org", edgeOrg, "--connector", "github", "--op", "fetch_prs", "--tenant", "Individual"}, `--tenant must be organization or individual, not "Individual"`},
		{"empty org", []string{"config", "put", "--org", "", "--connector", "github", "--credentials", "{}"}, "usage: tetriz-connectors config put"},
		{"org with spaces", []string{"sync", "--org", " " + edgeOrg, "--connector", "github", "--op", "fetch_prs"}, "--org must be a UUID"},
		{"config put positional", []string{"config", "put", "--org", edgeOrg, "--connector", "github", "--credentials", "{}", "extra"}, `unexpected argument "extra"`},
		{"op of another connector", []string{"sync", "--org", edgeOrg, "--connector", "jira", "--op", "fetch_prs"}, `connector jira has no operation "fetch_prs"`},
		{"connector id is case sensitive", []string{"cursor", "show", "--org", edgeOrg, "--connector", "GitHub", "--op", "fetch_prs"}, `unknown connector "GitHub"`},
		{"bool flag given a value", []string{"config", "put", "--org", edgeOrg, "--connector", "github", "--disabled=maybe"}, "invalid boolean value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edgeNoStore(t)
			code, out, errOut := runCLI(t, tc.args...)
			assert.Equal(t, 2, code)
			assert.Empty(t, out)
			assert.Contains(t, errOut, tc.want)
		})
	}
}

// Valid flags with an unusable credentials key are a runtime error: exit 1,
// "error:" on stderr, nothing on stdout, and no run.
func TestEdge_BadCredentialsKeyExits1(t *testing.T) {
	for _, tc := range []struct{ name, key, want string }{
		{"missing key", "", "TETRIZ_CREDENTIALS_KEY is empty"},
		{"short key", "AAAA", "decodes to 3 bytes, want 32"},
	} {
		for _, args := range [][]string{
			{"sync", "--org", edgeOrg, "--connector", "github", "--op", "fetch_repos"},
			{"config", "put", "--org", edgeOrg, "--connector", "github", "--credentials", "{}"},
			{"serve"},
		} {
			t.Run(tc.name+"/"+strings.Join(args[:1], " "), func(t *testing.T) {
				edgeNoStore(t)
				t.Setenv("TETRIZ_CREDENTIALS_KEY", tc.key)
				code, out, errOut := runCLI(t, args...)
				assert.Equal(t, 1, code)
				assert.Empty(t, out)
				assert.True(t, strings.HasPrefix(errOut, "error: "), errOut)
				assert.Contains(t, errOut, tc.want)
			})
		}
	}
}

// The commands that read no credentials (cursor show, migrate) run with a
// key that would stop serve, sync and config put.
func TestEdge_CredentialFreeCommandsIgnoreABadKey(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("credential-free commands", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "not-a-key")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "")

	code, out, errOut := runCLI(t, "migrate")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "applied ")

	code, out, errOut = runCLI(t, "cursor", "show", "--org", edgeOrg, "--connector", "github", "--op", "fetch_prs")
	require.Equal(t, 0, code, errOut)
	assert.Equal(t, "no cursor\n", out)

	code, out, errOut = runCLI(t, "config", "put", "--org", edgeOrg, "--connector", "github", "--credentials", "{}")
	assert.Equal(t, 1, code)
	assert.Empty(t, out)
	assert.Contains(t, errOut, "TETRIZ_CREDENTIALS_KEY is not base64")
}

// A sync of an organization with no stored config is a failure the caller
// sees (exit 1 and an error), not a "skipped" success that reads as a paused
// connector.
func TestEdge_SyncOfAnUnknownOrganizationFails(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("sync unknown org", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")

	code, out, errOut := runCLI(t, "sync", "--org", edgeOrg, "--connector", "github", "--op", "fetch_repos")
	assert.NotEqual(t, 0, code, "stdout=%q stderr=%q", out, errOut)
	assert.NotContains(t, out, `"status": "skipped"`)
	assert.NotEmpty(t, errOut)
}

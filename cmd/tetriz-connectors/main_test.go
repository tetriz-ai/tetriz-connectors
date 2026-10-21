// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

func runCLI(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errb bytes.Buffer
	code = run(context.Background(), args, &out, &errb)
	return code, out.String(), errb.String()
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(p, []byte(got), 0o644))
	}
	want, err := os.ReadFile(p)
	require.NoError(t, err)
	assert.Equal(t, string(want), got)
}

// help, -h and --help print the usage to stdout and exit 0. No command is a
// usage error: the usage goes to stderr, stdout stays empty, and it exits 2.
func TestHelp(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		code, out, errOut := runCLI(t, arg)
		assert.Equal(t, 0, code, arg)
		assert.Empty(t, errOut, arg)
		golden(t, "help.golden", out)
	}

	code, out, errOut := runCLI(t)
	assert.Equal(t, 2, code, "no command is a usage error")
	assert.Empty(t, out, "a usage error writes nothing to stdout")
	golden(t, "help.golden", errOut)
}

func TestConnectorsListsEveryOperation(t *testing.T) {
	code, out, _ := runCLI(t, "connectors")
	assert.Equal(t, 0, code)
	golden(t, "connectors.golden", out)
}

func TestVersion(t *testing.T) {
	code, out, _ := runCLI(t, "version")
	assert.Equal(t, 0, code)
	assert.NotEmpty(t, strings.TrimSpace(out))
}

func TestUsageErrorsExit2(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"bogus"}, `unknown command "bogus"`},
		{[]string{"config"}, "usage: tetriz-connectors config put"},
		{[]string{"cursor"}, "usage: tetriz-connectors cursor show"},
		{[]string{"sync"}, "usage: tetriz-connectors sync --org ID"},
		{[]string{"sync", "--org", "00000000-0000-4000-8000-000000000001", "--connector", "github", "--op", "fetch_prs", "--tenant", "team"}, "--tenant must be organization or individual"},
		{[]string{"simulator"}, "TETRIZ_SIMULATOR_DSN is required"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			t.Setenv("TETRIZ_SIMULATOR_DSN", "")
			code, _, errOut := runCLI(t, tc.args...)
			assert.Equal(t, 2, code)
			assert.Contains(t, errOut, tc.want)
		})
	}
}

// Every subcommand prints its description and flags for -h and --help and
// exits 0 before doing anything.
func TestSubcommandHelp(t *testing.T) {
	for _, tc := range []struct {
		golden string
		args   []string
	}{
		{"help-serve.golden", []string{"serve", "--help"}},
		{"help-sync.golden", []string{"sync", "--help"}},
		{"help-config-put.golden", []string{"config", "put", "--help"}},
		{"help-cursor-show.golden", []string{"cursor", "show", "--help"}},
		{"help-migrate.golden", []string{"migrate", "--help"}},
		{"help-simulator.golden", []string{"simulator", "--help"}},
		{"help-connectors.golden", []string{"connectors", "--help"}},
		{"help-schema.golden", []string{"schema", "--help"}},
		{"help-version.golden", []string{"version", "-h"}},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			code, out, errOut := runCLI(t, tc.args...)
			assert.Equal(t, 0, code)
			assert.Empty(t, errOut)
			golden(t, tc.golden, out)
		})
	}
}

// `migrate --help` prints help and touches no database, even with a
// configuration that would make the migration itself fail.
func TestMigrateHelpRunsNothing(t *testing.T) {
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "")
	t.Setenv("TETRIZ_POSTGRES_DSN", "host=127.0.0.1 port=1 user=nobody dbname=none connect_timeout=1")
	code, out, errOut := runCLI(t, "migrate", "--help")
	assert.Equal(t, 0, code)
	assert.NotContains(t, out, "applied")
	assert.Empty(t, errOut)
}

// A positional argument a command does not take is a usage error.
func TestExtraArgumentsAreUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"version", "extra"},
		{"connectors", "extra"},
		{"migrate", "extra"},
		{"serve", "extra"},
		{"sync", "--org", "o", "--connector", "c", "--op", "x", "extra"},
		{"cursor", "show", "--org", "o", "--connector", "c", "--op", "x", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, _, errOut := runCLI(t, args...)
			assert.Equal(t, 2, code)
			assert.Contains(t, errOut, `unexpected argument "extra"`)
		})
	}
}

// An id that cannot name anything is a usage error, caught before the store
// is opened: a typo does not read as "never ran".
func TestInvalidIDsExit2(t *testing.T) {
	const org = "00000000-0000-4000-8000-000000000001"
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"cursor-show-unknown-connector", []string{"cursor", "show", "--org", org, "--connector", "nosuch", "--op", "fetch_prs"}},
		{"cursor-show-unknown-op", []string{"cursor", "show", "--org", org, "--connector", "github", "--op", "nosuch"}},
		{"cursor-show-org-not-uuid", []string{"cursor", "show", "--org", "ORG-1", "--connector", "github", "--op", "fetch_prs"}},
		{"sync-org-not-uuid", []string{"sync", "--org", "not-a-uuid", "--connector", "github", "--op", "fetch_prs"}},
		{"config-put-unknown-connector", []string{"config", "put", "--org", org, "--connector", "nosuch", "--credentials", "{}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCLI(t, tc.args...)
			assert.Equal(t, 2, code)
			assert.Empty(t, out)
			golden(t, "invalid-id-"+tc.name+".golden", errOut)
		})
	}
}

// `config --help` and `cursor --help` print their subcommand's help.
func TestGroupHelpShowsTheSubcommand(t *testing.T) {
	for _, tc := range []struct{ group, golden string }{{"config", "help-config-put.golden"}, {"cursor", "help-cursor-show.golden"}} {
		code, out, _ := runCLI(t, tc.group, "--help")
		assert.Equal(t, 0, code)
		golden(t, tc.golden, out)
	}
}

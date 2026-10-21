// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A `config put` without --disabled enables the connector, including one an
// operator or a webhook disabled.
func TestLimitation_ConfigPutWithoutDisabledEnables(t *testing.T) {
	syncEnv(t)
	const org = "00000000-0000-4000-8000-0000000e0e0c"
	code, out, errOut := runCLI(t, "config", "put", "--org", org, "--connector", "bitbucket", "--settings", `{"workspace":"acme"}`, "--credentials", `{}`, "--disabled")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "enabled=false")
	code, out, errOut = runCLI(t, "config", "put", "--org", org, "--connector", "bitbucket", "--settings", `{"workspace":"acme-2"}`)
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "enabled=true", "pinned: a settings put re-enables the disabled connector")
}

// The simulator's GitHub commit history honours `since`, so a second
// fetch_commits run reads only the commits at its cursor boundary (GitHub's
// since is inclusive), not the whole history again.
func TestSimulator_SecondCommitRunReadsOnlyTheBoundary(t *testing.T) {
	c := newSimCLI(t)
	c.put("github", `{"organization":"demo-gh-startup"}`)
	code, out := c.sync("github", "fetch_repos", "repos.jsonl")
	require.Equal(t, 0, code, out)
	for _, run := range []string{"1", "2"} {
		code, out := c.sync("github", "fetch_commits", "commits."+run+".jsonl")
		require.Equal(t, 0, code, out)
	}
	first, second := c.ids("commits.1.jsonl"), c.ids("commits.2.jsonl")
	require.NotEmpty(t, first)
	assert.Less(t, len(second), len(first), "the second run does not read the whole history again")
	assert.Subset(t, first, second, "what it reads again is the boundary the first run ended on")
}

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
)

// `config put` and `cursor show` accept any spelling of a UUID and store the
// canonical lower-case id, which is what the store scopes to under forced
// row-level security.
func TestConfigPutCanonicalizesTheOrg(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("org spelling", err)
		t.Skip("no test database")
	}
	_, dsn, cleanup, err := testdb.SetupAsOwner(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	t.Setenv("TETRIZ_POSTGRES_DSN", dsn)
	t.Setenv("TETRIZ_REDIS_ADDR", miniredis.RunT(t).Addr())
	t.Setenv("TETRIZ_CREDENTIALS_KEY", "")
	t.Setenv("TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS", "1")
	const canonical = "aaaaaaaa-0000-4000-8000-0000000000f1"
	for _, spelling := range []string{"AAAAAAAA-0000-4000-8000-0000000000F1", "urn:uuid:" + canonical, "{" + canonical + "}"} {
		t.Run(spelling, func(t *testing.T) {
			code, out, errOut := runCLI(t, "config", "put", "--org", spelling, "--connector", "github",
				"--settings", `{"organization":"acme"}`, "--credentials", `{"personal_access_token":"x"}`)
			require.Equal(t, 0, code, errOut)
			assert.Equal(t, "stored github config for "+canonical+" (enabled=true)\n", out)
			code, out, errOut = runCLI(t, "cursor", "show", "--org", spelling, "--connector", "github", "--op", "fetch_prs")
			require.Equal(t, 0, code, errOut)
			assert.Equal(t, "no cursor\n", out)
		})
	}
}

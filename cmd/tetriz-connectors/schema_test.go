// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

// `schema` with no arguments lists every connector and operation that has a
// record schema, grouped by connector.
func TestSchemaListsEverySchema(t *testing.T) {
	code, out, errOut := runCLI(t, "schema")
	assert.Equal(t, 0, code, errOut)
	golden(t, "schema.golden", out)
	refs := schemas.List()
	require.NotEmpty(t, refs)
	for _, r := range refs {
		assert.Contains(t, out, "\n  "+r.Operation+"\n", "%s is listed", r)
	}
}

// `schema <connector> <operation>` prints that operation's JSON Schema,
// byte for byte.
func TestSchemaPrintsOneSchema(t *testing.T) {
	code, out, errOut := runCLI(t, "schema", "github", "fetch_prs")
	require.Equal(t, 0, code, errOut)
	want, ok := schemas.Schema("github", "fetch_prs")
	require.True(t, ok)
	assert.Equal(t, strings.TrimRight(string(want), "\n")+"\n", out)
	assert.Contains(t, out, `"$schema"`)
}

// An unknown pair, or one argument or three, is a usage error.
func TestSchemaUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"schema", "github", "no_such_op"}, "no record schema for github no_such_op"},
		{[]string{"schema", "nosuch", "fetch_prs"}, "no record schema for nosuch fetch_prs"},
		{[]string{"schema", "github"}, "usage: tetriz-connectors schema [<connector> <operation>]"},
		{[]string{"schema", "a", "b", "c"}, "usage: tetriz-connectors schema [<connector> <operation>]"},
		{[]string{"schema", "--bogus"}, "flag provided but not defined"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			code, out, errOut := runCLI(t, tc.args...)
			assert.Equal(t, 2, code)
			assert.Empty(t, out)
			assert.Contains(t, errOut, tc.want)
		})
	}
}

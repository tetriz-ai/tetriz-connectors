// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package simroute

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURLKeepsOnlyThePath(t *testing.T) {
	for base, want := range map[string]string{
		"https://api.github.com":              "http://sim:8080/github",
		"https://api.github.com/graphql":      "http://sim:8080/github/graphql",
		"https://ghe.example.com/api/v3":      "http://sim:8080/github/api/v3",
		"https://ghe.example.com/api/v3/a%2F": "http://sim:8080/github/api/v3/a%2F",
	} {
		got, err := URL("http://sim:8080", "/github", base)
		require.NoError(t, err)
		assert.Equal(t, want, got, base)
	}
	_, err := URL("http://sim:8080", "/github", "http://[::1")
	assert.Error(t, err)
}

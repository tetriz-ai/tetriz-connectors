// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Empty GitHub credentials name what is missing, not an App misconfiguration.
func TestGitHubCredentialsWithNeitherTokenNorInstallation(t *testing.T) {
	_, _, err := gatewayDeps{}.githubDoer(json.RawMessage(`{}`))
	require.EqualError(t, err, "github credentials give neither a personal_access_token nor an installation_id")
}

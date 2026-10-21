// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/connectors/github"
)

// The runner's GitHub connector stops early to checkpoint before the task
// deadline, and fetches issues 50 per page, rather than running with the
// connector's zero-value tuning.
func TestRegistryTunesGitHub(t *testing.T) {
	c, err := NewRegistry(nil).Get("github")
	require.NoError(t, err)
	gh, ok := c.(*github.GitHubConnector)
	require.True(t, ok)
	assert.Equal(t, 30*time.Second, gh.CheckpointReserve())
	issues := gh.PaginationConfig().Operations["fetch_issues"]
	require.NotNil(t, issues)
	assert.Equal(t, 50, issues.PageSize)
}

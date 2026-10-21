// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const defaultGitHubRESTBase = "https://api.github.com"

// githubRESTBase returns the REST API base URL for the given org ID.
// For orgs listed in SimulatorOrgIDs it returns the simulator's GitHub base
// (SimulatorBaseURL + "/github") so that operations which construct URLs directly
// (fetch_repo_tree, fetch_pr_diffs) route to the simulator instead of the real
// GitHub API. SimulatorBaseURL is the simulator service root, shared across vendors,
// so the "/github" prefix is required here just as the gateway constructor appends it.
// Falls back to the real GitHub base on config load error.
func githubRESTBase(orgID string) string {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		logger.ErrorNoCtx("simulator: githubRESTBase: config load failed; falling back to real GitHub base", zap.Error(err))
		return defaultGitHubRESTBase
	}
	if !cfg.IsSimulatorOrg(orgID) {
		return defaultGitHubRESTBase
	}
	return cfg.SimulatorBaseURL + "/github"
}

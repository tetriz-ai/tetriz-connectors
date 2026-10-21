// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// defaultGitLabRESTBase is the real GitLab.com REST base, including the
// "/api/v4" version segment that every direct-URL operation appends its path to.
const defaultGitLabRESTBase = "https://gitlab.com/api/v4"

// gitlabRESTBase returns the REST base URL (with the "/api/v4" version segment)
// for orgID. For orgs in SimulatorOrgIDs it returns the simulator's GitLab
// base (SimulatorBaseURL + "/gitlab/api/v4"); otherwise the real base. The
// operations that build URLs directly rather than through a gateway group —
// fetch_mr_diffs, fetch_branches, fetch_repo_tree, fetch_file_content — resolve
// their base here so simulator orgs reach the simulator instead of gitlab.com.
// Falls back to the real base on config load error.
func gitlabRESTBase(orgID string) string {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		logger.ErrorNoCtx("simulator: gitlabRESTBase: config load failed; falling back to real GitLab base", zap.Error(err))
		return defaultGitLabRESTBase
	}
	if !cfg.IsSimulatorOrg(orgID) {
		return defaultGitLabRESTBase
	}
	return cfg.SimulatorBaseURL + "/gitlab/api/v4"
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"net/url"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/gateway"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// defaultBitbucketRESTBase is the real Bitbucket Cloud REST base, including the
// "/2.0" version segment that every direct-URL operation appends its path to.
const defaultBitbucketRESTBase = "https://api.bitbucket.org/2.0"

// defaultBitbucketRESTHost is the host of defaultBitbucketRESTBase — the host a
// paginated `next` URL must match for a real org (the SSRF guard).
const defaultBitbucketRESTHost = "api.bitbucket.org"

// bitbucketRESTBase returns the REST base URL (with the "/2.0" version segment)
// for orgID. For orgs in SimulatorOrgIDs it returns the simulator's Bitbucket
// base (SimulatorBaseURL + "/bitbucket/2.0"); otherwise the real base. The three
// operations that build URLs directly rather than through a gateway group —
// fetch_pr_diffs, fetch_branches, fetch_repo_tree — resolve their base here so
// simulator orgs reach the simulator instead of api.bitbucket.org. Falls back to
// the real base on config load error.
func bitbucketRESTBase(orgID string) string {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		logger.ErrorNoCtx("simulator: bitbucketRESTBase: config load failed; falling back to real Bitbucket base", zap.Error(err))
		return defaultBitbucketRESTBase
	}
	if !cfg.IsSimulatorOrg(orgID) {
		return defaultBitbucketRESTBase
	}
	return cfg.SimulatorBaseURL + "/bitbucket/2.0"
}

// bitbucketRESTHost returns the host of bitbucketRESTBase(orgID). A paginated
// `next` URL is validated against this host before being followed: for a real
// org it is api.bitbucket.org, for a simulator org it is the simulator's host,
// so a multi-page simulator response is followed rather than rejected. Falls
// back to the real host on config load / parse error.
func bitbucketRESTHost(orgID string) string {
	parsed, err := url.Parse(bitbucketRESTBase(orgID))
	if err != nil {
		logger.ErrorNoCtx("simulator: bitbucketRESTHost: parse base failed; falling back to real Bitbucket host", zap.Error(err))
		return defaultBitbucketRESTHost
	}
	return parsed.Host
}

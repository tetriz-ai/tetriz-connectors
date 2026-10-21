// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// JSON field names repeated across response builders in this package — kept as
// constants so payload shape typos surface at compile time.
const (
	fieldNodes      = "nodes"
	fieldPageInfo   = "pageInfo"
	fieldLogin      = "login"
	fieldTotalCount = "totalCount"
	fieldSHA        = "sha"
	fieldType       = "type"
)

// Service registers all simulator HTTP routes for GitHub vendor traffic.
type Service struct {
	dao    dao.IDAO
	vendor string
}

// New returns a Service backed by the given DAO.
func New(d dao.IDAO) *Service {
	return &Service{dao: d, vendor: "github"}
}

// RegisterRoutes mounts all simulator GitHub routes under the given router.
// All paths are prefixed with /github to match the gateway BaseURL.
func (s *Service) RegisterRoutes(r *mux.Router) {
	sub := r.PathPrefix("/github").Subrouter()

	// GraphQL endpoint
	sub.HandleFunc("/graphql", s.handleGraphQL).Methods(http.MethodPost)

	// REST endpoints — REST API routes
	sub.HandleFunc("/orgs/{org}/repos", s.handleRepos).Methods(http.MethodGet)
	sub.HandleFunc("/user", s.handleUser).Methods(http.MethodGet)
	sub.HandleFunc("/user/repos", s.handleUserRepos).Methods(http.MethodGet)
	sub.HandleFunc("/orgs/{org}/copilot/metrics", s.handleCopilotMetrics).Methods(http.MethodGet)
	sub.HandleFunc("/orgs/{org}/copilot/usage/user", s.handleCopilotUserUsage).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/pulls/{number}/reviews", s.handleReviews).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/pulls/{number}/comments", s.handlePRComments).Methods(http.MethodGet)

	// PR diffs and the repo tree.
	sub.HandleFunc("/repos/{owner}/{repo}/pulls/{number}/files", s.handlePRFiles).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/pulls/{number}", s.handlePRDiff).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/branches", s.handleBranches).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/branches/{branch}", s.handleBranchSHA).Methods(http.MethodGet)
	sub.HandleFunc("/repos/{owner}/{repo}/git/trees/{sha}", s.handleGitTree).Methods(http.MethodGet)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// store is the Bitbucket simulator's data-access dependency, declared at the
// consumer so it lists exactly the methods these handlers use. *dao.SimDAO
// satisfies it (as it does github's dao.IDAO); handler tests inject a fake.
type store interface {
	ListPageWindow(ctx context.Context, vendor, org, entityType, repo string, limit, offset int) ([]dao.Row, error)
	GetByPRID(ctx context.Context, vendor, org, repo string, id int) (*dao.Row, error)
	GetByCommitHash(ctx context.Context, vendor, org, repo, hash string) (*dao.Row, error)
}

// Service registers all simulator HTTP routes for Bitbucket vendor traffic.
type Service struct {
	dao    store
	vendor string
}

// New returns a Service backed by the given store.
func New(d store) *Service {
	return &Service{dao: d, vendor: "bitbucket"}
}

// RegisterRoutes mounts the Bitbucket simulator routes under /bitbucket, matching
// the gateway BaseURL. The /2.0 and /1.0 segments mirror the real API's
// versioned paths so the connector's URL construction (including its /2.0→/1.0
// group-URL derivation) works unchanged. The surface covers repo discovery and
// single-repo resolution, PR enumeration and its per-PR enrichments (diffstat,
// commits, activity, comments), repo-level commits and per-commit diffstat, and
// workspace members plus the always-empty v1.0 groups.
func (s *Service) RegisterRoutes(r *mux.Router) {
	sub := r.PathPrefix("/bitbucket").Subrouter()

	sub.HandleFunc("/2.0/repositories/{workspace}", s.handleRepos).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}", s.handleSingleRepo).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests", s.handlePRList).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests/{id}/diffstat", s.handlePRDiffstat).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests/{id}/commits", s.handlePRCommits).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests/{id}/activity", s.handlePRActivity).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests/{id}/comments", s.handlePRComments).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/commits", s.handleCommits).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/diffstat/{sha}", s.handleCommitDiffstat).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/workspaces/{workspace}/members", s.handleMembers).Methods(http.MethodGet)
	sub.HandleFunc("/1.0/groups/{workspace}/", s.handleGroups).Methods(http.MethodGet)

	// The direct-URL operations (fetch_pr_diffs, fetch_branches, fetch_repo_tree)
	// build their own request URLs rather than going through a VendorGateway group,
	// so they reach the simulator by resolving their base from the connector's
	// configured host. The refs/branches list and src/ walk paginate via the
	// envelope's `next`, which the connector follows under a host guard pinned to
	// the simulator's host.
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/pullrequests/{id}/diff", s.handlePRDiff).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/refs/branches", s.handleBranches).Methods(http.MethodGet)
	// {name:.+}: the router matches the decoded path, so a "release%2F1.0" branch
	// arrives as "release/1.0" and must be allowed to span a slash.
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/refs/branches/{name:.+}", s.handleBranchSHA).Methods(http.MethodGet)
	sub.HandleFunc("/2.0/repositories/{workspace}/{repo}/src/{commit}/{path:.*}", s.handleSrc).Methods(http.MethodGet)
}

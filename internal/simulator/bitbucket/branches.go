// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"

	"github.com/gorilla/mux"
)

// keyHash is the Bitbucket payload key for a commit SHA, shared by the branch and
// src responses (the connector reads target.hash / commit.hash).
const keyHash = "hash"

// syntheticBranches is the deterministic branch set every simulator repo exposes.
// "main" is first so it matches the repos' default branch (repo.Mainbranch), which
// the connector flags as is_default; "develop" gives a second branch so the paginated
// listing has more than one page to follow at a small pagelen.
var syntheticBranches = []string{"main", "develop"}

// handleBranches serves GET /2.0/repositories/{workspace}/{repo}/refs/branches.
// It returns the synthetic branch set in the paginated envelope. The connector
// follows the envelope's `next` URL verbatim under a host guard pinned to the
// configured REST host (the simulator's, for a simulator org), so `next` must be
// emitted on this request's own host — writeEnvelope/nextPageURL rebuild from r.URL,
// which is the simulator's, satisfying that guard.
func (s *Service) handleBranches(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo := vars["workspace"], vars["repo"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty
		emptyListEnvelope(w)
		return
	}

	values := make([]json.RawMessage, 0, len(syntheticBranches))
	for _, name := range syntheticBranches {
		values = append(values, branchObject(workspace, repo, name))
	}

	page, pagelen := parsePagination(r)
	writeEnvelope(w, r, windowSlice(values, page, pagelen), page, pagelen)
}

// handleBranchSHA serves GET /2.0/repositories/{workspace}/{repo}/refs/branches/{name}.
// It returns the branch's head object; the connector reads target.hash as the HEAD
// commit SHA to seed the repo-tree walk. The SHA is deterministic per
// (workspace, repo, branch) so it is stable across restarts and agrees with the
// listing route's hash for the same branch.
func (s *Service) handleBranchSHA(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace, repo, name := vars["workspace"], vars["repo"], vars["name"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	// A sim-empty match falls through here: the single-branch route's success
	// shape is the same whether or not the org matched the sim-empty rule.
	writeJSON(w, map[string]any{
		"name":   name,
		"target": map[string]any{keyHash: deriveBranchHash(workspace, repo, name)},
	})
}

// branchObject builds the slim branch payload the connector unmarshals:
// {"name", "target":{"hash"}}.
func branchObject(workspace, repo, name string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck,gosec // marshaling a fixed map shape never fails
		"name":   name,
		"target": map[string]any{keyHash: deriveBranchHash(workspace, repo, name)},
	})
	return b
}

// deriveBranchHash returns a deterministic 40-hex commit SHA for
// (workspace, repo, branch) via FNV-1a, so branch SHAs are stable without a DB hit
// and consistent between the listing and single-branch routes.
func deriveBranchHash(workspace, repo, branch string) string {
	h := fnv.New64a()
	h.Write([]byte(workspace)) //nolint:errcheck,gosec // fnv Write never errors
	h.Write([]byte{0})         //nolint:errcheck,gosec // fnv Write never errors
	h.Write([]byte(repo))      //nolint:errcheck,gosec // fnv Write never errors
	h.Write([]byte{0})         //nolint:errcheck,gosec // fnv Write never errors
	h.Write([]byte(branch))    //nolint:errcheck,gosec // fnv Write never errors
	return fmt.Sprintf("%040x", h.Sum64())
}

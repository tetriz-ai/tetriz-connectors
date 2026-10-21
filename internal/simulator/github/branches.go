// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const (
	defaultBranchesPerPage = 30
	maxBranchesPerPage     = 100
)

// branchEntry is one element of GitHub's GET /repos/{owner}/{repo}/branches.
type branchEntry struct {
	Name   string `json:"name"`
	Commit struct {
		SHA string `json:"sha"`
		URL string `json:"url"`
	} `json:"commit"`
	Protected bool `json:"protected"`
}

// handleBranches serves GET /repos/{owner}/{repo}/branches. A repository's
// branches are its default branch, which is protected, and the head branch of
// each open pull request, which GitHub keeps until the pull request closes.
// The listing is sorted by name and paged with per_page and page and GitHub's
// Link header. An unknown repository is a 404, as on GitHub.
func (s *Service) handleBranches(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org, repo := vars["owner"], vars["repo"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	branches, found, err := s.repoBranches(r.Context(), org, repo)
	if err != nil {
		logger.Error(r.Context(), "simulator: handleBranches", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "Not Found")
		return
	}

	perPage := min(intParam(r.URL.Query().Get("per_page"), defaultBranchesPerPage), maxBranchesPerPage)
	page := intParam(r.URL.Query().Get("page"), 1)
	last := max(1, (len(branches)+perPage-1)/perPage)
	start := (page - 1) * perPage
	out := []branchEntry{}
	if start < len(branches) {
		out = branches[start:min(start+perPage, len(branches))]
	}
	if link := linkHeader(r, page, last); link != "" {
		w.Header().Set("Link", link)
	}
	writeJSON(w, out)
}

// repoBranches returns the branches of org/repo, sorted by name, and whether
// the repository exists.
func (s *Service) repoBranches(ctx context.Context, org, repo string) ([]branchEntry, bool, error) {
	repos, err := s.dao.ListAll(ctx, s.vendor, org, "repo")
	if err != nil {
		return nil, false, fmt.Errorf("listing repos: %w", err)
	}
	defaultBranch, found := "", false
	for _, row := range repos {
		var p struct {
			Name          string `json:"name"`
			DefaultBranch string `json:"default_branch"`
		}
		if json.Unmarshal(row.Payload, &p) == nil && p.Name == repo {
			defaultBranch, found = p.DefaultBranch, true
			break
		}
	}
	if !found {
		return nil, false, nil
	}
	if defaultBranch == "" {
		defaultBranch = "main"
	}

	prs, err := s.dao.ListPaged(ctx, s.vendor, org, "pr", repo, nil, 0)
	if err != nil {
		return nil, false, fmt.Errorf("listing pull requests: %w", err)
	}
	heads := map[string]string{}
	defaultSHA := deriveBranchSHA(org, repo)
	for i, row := range prs {
		var p struct {
			State       string `json:"state"`
			HeadRefName string `json:"headRefName"`
		}
		if json.Unmarshal(row.Payload, &p) != nil {
			continue
		}
		if i == 0 {
			// The same head SHA handleBranchSHA serves for the default branch.
			if sha := extractFirstCommitSHA(row.Payload); sha != "" {
				defaultSHA = sha
			}
		}
		if strings.EqualFold(p.State, "OPEN") && p.HeadRefName != "" && p.HeadRefName != defaultBranch {
			heads[p.HeadRefName] = branchHeadSHA(org, repo, p.HeadRefName)
		}
	}

	out := []branchEntry{newBranchEntry(org, repo, defaultBranch, defaultSHA, true)}
	for name, sha := range heads {
		out = append(out, newBranchEntry(org, repo, name, sha, false))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, true, nil
}

func newBranchEntry(org, repo, name, sha string, protected bool) branchEntry {
	var b branchEntry
	b.Name, b.Protected = name, protected
	b.Commit.SHA = sha
	b.Commit.URL = fmt.Sprintf("https://api.github.com/repos/%s/%s/commits/%s", org, repo, sha)
	return b
}

// branchHeadSHA derives a stable head SHA for a feature branch.
func branchHeadSHA(org, repo, branch string) string {
	h := fnv.New64a()
	for _, part := range []string{org, repo, branch} {
		hashWrite(h, []byte(part))
		hashWrite(h, []byte{0})
	}
	return fmt.Sprintf("%040x", h.Sum64())
}

// handleMergeMethods answers the repository merge-method query
// (squashMergeAllowed, mergeCommitAllowed, rebaseMergeAllowed) that
// fetch_repos sends per repository. Squash and merge commits are allowed on
// every simulated repository; rebase merging is allowed on a stable half of
// them, so both values reach the records. An unknown repository answers as
// GitHub does: a null repository and a NOT_FOUND error.
func (s *Service) handleMergeMethods(ctx context.Context, w http.ResponseWriter, org, repo string) {
	repoID, err := s.repoDatabaseID(ctx, org, repo)
	if err != nil {
		logger.Error(ctx, "simulator: handleMergeMethods", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	var resp map[string]any
	if repoID == 0 {
		resp = map[string]any{
			"data": map[string]any{"repository": nil},
			"errors": []map[string]any{{
				fieldType: "NOT_FOUND",
				"path":    []string{"repository"},
				"message": fmt.Sprintf("Could not resolve to a Repository with the name '%s/%s'.", org, repo),
			}},
		}
	} else {
		resp = map[string]any{"data": map[string]any{"repository": map[string]any{
			"squashMergeAllowed": true,
			"mergeCommitAllowed": true,
			"rebaseMergeAllowed": rebaseAllowed(org, repo),
		}}}
	}
	writeJSON(w, resp)
}

func rebaseAllowed(org, repo string) bool {
	h := fnv.New32a()
	hashWrite(h, []byte(org+"/"+repo))
	return h.Sum32()%2 == 0
}

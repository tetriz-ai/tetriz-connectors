// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleBranchSHA serves GET /repos/{owner}/{repo}/branches/{branch}.
// It returns a deterministic commit SHA derived from the first PR row in the
// simulator DB, which the connector then uses to fetch the repo tree.
func (s *Service) handleBranchSHA(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]
	repo := vars["repo"]
	branch := vars["branch"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}

	sha := deriveBranchSHA(org, repo)

	if !matched {
		// Try to derive SHA from the first commit in the first PR row so that
		// the branch SHA is consistent with any commit data already fetched.
		rows, err := s.dao.ListPaged(r.Context(), s.vendor, org, "pr", repo, nil, 1)
		if err != nil {
			logger.Error(r.Context(), "simulator: handleBranchSHA", zap.Error(err))
			// Fall through to deterministic SHA on error.
		} else if len(rows) > 0 {
			if extracted := extractFirstCommitSHA(rows[0].Payload); extracted != "" {
				sha = extracted
			}
		}
	}

	writeJSON(w, map[string]any{
		"name":   branch,
		"commit": map[string]any{fieldSHA: sha},
	})
}

// handleGitTree serves GET /repos/{owner}/{repo}/git/trees/{sha}.
// It returns a minimal synthetic tree so fetch_repo_tree can record a non-empty
// tree without requiring real tree data in sim_records.
func (s *Service) handleGitTree(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]
	repo := vars["repo"]
	sha := vars[fieldSHA]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	_ = matched

	tree := buildSyntheticTree(org, repo, sha)
	writeJSON(w, map[string]any{
		fieldSHA:    sha,
		"truncated": false,
		"tree":      tree,
	})
}

// hashWrite writes to h, ignoring the error return — hash.Hash.Write never
// fails (see the hash.Hash doc comment).
func hashWrite(h hash.Hash, b []byte) {
	h.Write(b) //nolint:errcheck,gosec // hash.Hash.Write never returns an error
}

// deriveBranchSHA returns a deterministic 40-hex SHA for (org, repo) using an
// FNV-1a hash, so the branch lookup is stable across restarts without a DB hit.
func deriveBranchSHA(org, repo string) string {
	h := fnv.New64a()
	hashWrite(h, []byte(org))
	hashWrite(h, []byte{0})
	hashWrite(h, []byte(repo))
	return fmt.Sprintf("%040x", h.Sum64())
}

// extractFirstCommitSHA returns the oid of the first commit node in a PR
// payload, or empty string if absent.
func extractFirstCommitSHA(payload []byte) string {
	var p struct {
		Commits struct {
			Nodes []struct {
				OID string `json:"oid"`
			} `json:"nodes"`
		} `json:"commits"`
	}
	if err := json.Unmarshal(payload, &p); err != nil { // absent/malformed commit data is a graceful no-op, see doc comment above
		return ""
	}
	if len(p.Commits.Nodes) > 0 {
		return p.Commits.Nodes[0].OID
	}
	return ""
}

// buildSyntheticTree returns a minimal set of tree entries for demo purposes.
// Entries are deterministic for the given (org, repo, sha) triple.
func buildSyntheticTree(org, repo, treeSHA string) []map[string]any {
	files := []struct {
		path string
		size int64
	}{
		{"README.md", 512},
		{"main.go", 2048},
		{"go.mod", 256},
		{"go.sum", 4096},
		{"Makefile", 1024},
	}

	entries := make([]map[string]any, 0, len(files))
	for i, f := range files {
		h := fnv.New64a()
		hashWrite(h, []byte(org))
		hashWrite(h, []byte{0})
		hashWrite(h, []byte(repo))
		hashWrite(h, []byte{0})
		hashWrite(h, []byte(treeSHA))
		hashWrite(h, []byte{0})
		hashWrite(h, []byte(fmt.Sprint(i)))
		blobSHA := fmt.Sprintf("%040x", h.Sum64())

		entries = append(entries, map[string]any{
			"path":    f.path,
			"mode":    "100644",
			fieldType: "blob",
			fieldSHA:  blobSHA,
			"size":    f.size,
		})
	}
	return entries
}

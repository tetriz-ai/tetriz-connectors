// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// srcDirEntry describes one synthetic src/ listing entry before it is rendered to
// the connector's {type,path,size,commit:{hash}} shape.
type srcDirEntry struct {
	path  string
	isDir bool
	size  int64
}

// syntheticSrc maps a directory path (repo-root-relative, no trailing slash) to its
// listing. The tree is deliberately shallow and terminating: the root holds one
// subdirectory ("src") plus files, and "src" holds only files — so the connector's
// recursive BFS walk bottoms out. Any unknown path returns an empty listing.
func syntheticSrc(dir string) []srcDirEntry {
	switch dir {
	case "":
		return []srcDirEntry{
			{path: "src", isDir: true},
			{path: "README.md", size: 512},
			{path: "go.mod", size: 256},
		}
	case "src":
		return []srcDirEntry{
			{path: "src/main.go", size: 2048},
			{path: "src/service.go", size: 4096},
		}
	default:
		return nil
	}
}

// handleSrc serves GET /2.0/repositories/{workspace}/{repo}/src/{commit}/{path:.*}.
// It returns a paginated directory listing of a small synthetic tree keyed by path.
// The connector walks it breadth-first, following the envelope's `next` (rebuilt on
// this request's host, so the simulator-host guard accepts it) and recursing into
// commit_directory entries; the tree terminates because no directory nests another.
func (s *Service) handleSrc(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace := vars["workspace"]
	commit := vars["commit"]
	dir := strings.Trim(vars["path"], "/") // "" at the repo root

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(workspace)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched { // sim-empty
		emptyListEnvelope(w)
		return
	}

	entries := syntheticSrc(dir)
	values := make([]json.RawMessage, 0, len(entries))
	for _, e := range entries {
		values = append(values, srcObject(e, commit))
	}

	page, pagelen := parsePagination(r)
	writeEnvelope(w, r, windowSlice(values, page, pagelen), page, pagelen)
}

// srcObject renders a src entry to the Bitbucket shape the connector unmarshals:
// {"type":"commit_file"|"commit_directory","path","size","commit":{"hash"}}.
func srcObject(e srcDirEntry, commit string) json.RawMessage {
	obj := map[string]any{
		"path":   e.path,
		"commit": map[string]any{keyHash: commit},
	}
	if e.isDir {
		obj["type"] = "commit_directory"
	} else {
		obj["type"] = "commit_file"
		obj["size"] = e.size
	}
	b, _ := json.Marshal(obj) //nolint:errcheck,gosec // marshaling a fixed map shape never fails
	return b
}

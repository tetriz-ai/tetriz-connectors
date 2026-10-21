// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// cannedTree is the small terminating listing every simulated project exposes.
// The connector's tree walk bottoms out because `src` is a tree with no nested
// fetch required for the canned shape (recursive listing is this same page).
var cannedTreeEntries = []struct {
	name string
	typ  string
	path string
	mode string
}{
	{name: "README.md", typ: "blob", path: "README.md", mode: "100644"},
	{name: "main.go", typ: "blob", path: "main.go", mode: "100644"},
	{name: "src", typ: "tree", path: "src", mode: "040000"},
}

// handleRepoTree serves GET /projects/{id}/repository/tree. The body is a
// canned GitLab tree listing {id,name,type,path,mode} — seed does not store
// per-repo trees, and the connector only needs a non-empty terminating walk.
func (s *Service) handleRepoTree(w http.ResponseWriter, r *http.Request) {
	_, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	values := make([]json.RawMessage, 0, len(cannedTreeEntries))
	for _, e := range cannedTreeEntries {
		values = append(values, treeEntryJSON(projectID, e.name, e.typ, e.path, e.mode))
	}
	page, perPage := parsePagination(r)
	writeList(w, windowSlice(values, page, perPage), page, perPage)
}

// handleFileRaw serves GET /projects/{id}/repository/files/{path}/raw.
// An empty path is 404: GitLab requires a file path, and the connector treats
// 404 as errFileContentNotAvailable. Body is text/plain.
func (s *Service) handleFileRaw(w http.ResponseWriter, r *http.Request) {
	_, _, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	rawPath := mux.Vars(r)["path"]
	decoded, err := url.PathUnescape(rawPath)
	if err != nil {
		logger.Debug(r.Context(), "simulator: gitlab invalid file path", zap.Error(err))
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}
	rawPath = decoded
	filePath := strings.Trim(rawPath, "/")
	if filePath == "" {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return
	}

	body := fileRawBody(filePath)
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body)) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

func fileRawBody(path string) string {
	if strings.HasSuffix(strings.ToLower(path), "readme.md") {
		return "# README\n"
	}
	return "package main\n"
}

func treeEntryJSON(projectID int64, name, typ, path, mode string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck,gosec // marshaling a fixed map shape never fails
		"id":   fmt.Sprintf("%040x", uint64(projectID)+uint64(len(path))), //nolint:gosec // synthetic catalog id
		"name": name,
		"type": typ,
		"path": path,
		"mode": mode,
	})
	return b
}

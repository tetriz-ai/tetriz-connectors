// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

const vendorGitLab = "gitlab"

// store is the GitLab simulator's data-access dependency, declared at the
// consumer so it lists exactly the methods these handlers use. *dao.SimDAO
// satisfies it; handler tests inject a fake.
type store interface {
	ListPageWindow(ctx context.Context, vendor, org, entityType, repo string, limit, offset int) ([]dao.Row, error)
	ListPageWindowByProjectID(ctx context.Context, vendor, org, entityType string, projectID int64, limit, offset int) ([]dao.Row, error)
	GetByProjectIDAndIID(ctx context.Context, vendor, org string, projectID int64, iid int) (*dao.Row, error)
}

// Service registers all simulator HTTP routes for GitLab vendor traffic.
type Service struct {
	dao    store
	vendor string
}

// New returns a Service backed by the given store.
func New(d store) *Service {
	return &Service{dao: d, vendor: vendorGitLab}
}

// RegisterRoutes mounts the GitLab simulator routes under /gitlab, matching
// the gateway BaseURL. The /api/v4 segment mirrors the real API so the
// connector's URL construction works unchanged. Group-scoped lists use catalog
// org name after the integer→name map; project-scoped routes reverse-map
// project_id = group_id*100 + repo_index before the same lookup.
func (s *Service) RegisterRoutes(r *mux.Router) {
	sub := r.PathPrefix("/gitlab").Subrouter()
	api := sub.PathPrefix("/api/v4").Subrouter()

	api.HandleFunc("/groups/{id}/projects", s.handleGroupProjects).Methods(http.MethodGet)
	api.HandleFunc("/groups/{id}/issues", s.handleGroupIssues).Methods(http.MethodGet)
	api.HandleFunc("/groups/{id}/members/all", s.handleGroupMembers).Methods(http.MethodGet)

	api.HandleFunc("/projects/{id}/merge_requests/{iid}/commits", s.handleMRCommits).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/merge_requests/{iid}/notes", s.handleMRNotes).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/merge_requests/{iid}/approvals", s.handleMRApprovals).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/merge_requests/{iid}/diffs", s.handleMRDiffs).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/merge_requests", s.handleMRList).Methods(http.MethodGet)

	api.HandleFunc("/projects/{id}/repository/commits/{sha}", s.handleCommitDetail).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/repository/commits", s.handleRepoCommits).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/repository/branches/{name:.+}", s.handleBranch).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/repository/branches", s.handleBranches).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/repository/tree", s.handleRepoTree).Methods(http.MethodGet)
	api.HandleFunc("/projects/{id}/repository/files/{path:.*}/raw", s.handleFileRaw).Methods(http.MethodGet)

	api.HandleFunc("/projects/{id}", s.handleSingleProject).Methods(http.MethodGet)
}

// orgFromGroupPath maps the path group id to a catalog org name and applies
// error rules. ok is false when the handler must stop (unknown id or error rule).
func (s *Service) orgFromGroupPath(w http.ResponseWriter, r *http.Request) (org string, emptyMatch, ok bool) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		logger.Debug(r.Context(), "simulator: gitlab invalid group id", zap.Error(err))
		writeError(w, http.StatusNotFound, "404 Not Found")
		return "", false, false
	}
	org, found := orgNameForGroup(id)
	if !found {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return "", false, false
	}
	emptyMatch, stop := applyRules(w, org)
	if stop {
		return org, false, false
	}
	return org, emptyMatch, true
}

// orgFromProjectPath reverse-maps the path project id to a catalog org name
// (group_id = project_id/100) and applies error rules. A zero repo_index is
// not a valid encoding and 404s the same as an unknown group.
func (s *Service) orgFromProjectPath(w http.ResponseWriter, r *http.Request) (org string, projectID int64, emptyMatch, ok bool) {
	projectID, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		logger.Debug(r.Context(), "simulator: gitlab invalid project id", zap.Error(err))
		writeError(w, http.StatusNotFound, "404 Not Found")
		return "", 0, false, false
	}
	org, found := orgNameForProject(projectID)
	if !found {
		writeError(w, http.StatusNotFound, "404 Not Found")
		return "", 0, false, false
	}
	emptyMatch, stop := applyRules(w, org)
	if stop {
		return org, projectID, false, false
	}
	return org, projectID, emptyMatch, true
}

// findRepo returns the repo row whose payload id equals projectID, or nil if
// absent. Repo payloads key the project as `id`, not `project_id`, so
// ListPageWindowByProjectID cannot see them.
func (s *Service) findRepo(ctx context.Context, org string, projectID int64) (*dao.Row, error) {
	rows, err := s.dao.ListPageWindow(ctx, s.vendor, org, "repo", "", maxPerPage, 0)
	if err != nil {
		return nil, fmt.Errorf("list gitlab repos for %s: %w", org, err)
	}
	for i := range rows {
		if payloadID(rows[i].Payload) == projectID {
			return &rows[i], nil
		}
	}
	return nil, nil //nolint:nilnil // optional-lookup contract: no row, no error
}

func payloadID(payload []byte) int64 {
	var p struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(payload, &p) //nolint:errcheck // missing id → 0, treated as no match
	return p.ID
}

func writeDBError(ctx context.Context, w http.ResponseWriter, label string, err error) {
	logger.Error(ctx, "simulator: gitlab "+label, zap.Error(err))
	writeError(w, http.StatusInternalServerError, "Something went wrong")
}

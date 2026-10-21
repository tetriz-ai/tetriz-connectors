// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// JSON field names shared across the response envelopes (search/project/board/
// sprint/worklog), kept as constants so a typo can't desync a handler from the
// connector's unmarshal.
const (
	fieldValues     = "values"
	fieldTotal      = "total"
	fieldIsLast     = "isLast"
	fieldMaxResults = "maxResults"
	fieldStartAt    = "startAt"
)

// store is the Jira simulator's data-access dependency, declared at the consumer so
// it lists exactly the methods these handlers use. *dao.SimDAO satisfies it (as it
// does github's dao.IDAO and bitbucket's store); handler tests inject a fake.
type store interface {
	ListIssuesAscending(ctx context.Context, vendor, org, project string, since, afterCursor *time.Time, afterID string, limit int) ([]dao.IssueRow, error)
	ListAll(ctx context.Context, vendor, org, entityType string) ([]dao.Row, error)
	GetByIssueKey(ctx context.Context, vendor, org, key string) (*dao.Row, error)
}

// Service registers all simulator HTTP routes for Jira vendor traffic.
type Service struct {
	dao    store
	vendor string
}

// New returns a Service backed by the given store.
func New(d store) *Service {
	return &Service{dao: d, vendor: "jira"}
}

// RegisterRoutes mounts the Jira simulator routes under /jira, matching the
// gateway BaseURL rewrite. The /ex/jira/{cloudId}/rest/... segments mirror
// the real Atlassian API path so the connector's URL construction works unchanged;
// {cloudId} is the demo org name (the demo config sets cloud_id to the catalog org
// name), used directly as the org_name for rules and DB queries. The setup-time
// accessible-resources call is stubbed so a manual probe doesn't 404; it carries no
// per-org data and so sits outside the /ex/jira/{cloudId} tree.
func (s *Service) RegisterRoutes(r *mux.Router) {
	sub := r.PathPrefix("/jira").Subrouter()

	site := sub.PathPrefix("/ex/jira/{cloudId}").Subrouter()
	site.Use(s.requireKnownSite)
	site.HandleFunc("/rest/api/3/search/jql", s.handleSearch).Methods(http.MethodPost)
	site.HandleFunc("/rest/api/3/project/search", s.handleProjectSearch).Methods(http.MethodGet)
	site.HandleFunc("/rest/api/3/issue/{issueIdOrKey}/worklog", s.handleWorklog).Methods(http.MethodGet)
	site.HandleFunc("/rest/api/3/issue/{issueIdOrKey}/comment", s.handleComments).Methods(http.MethodGet)
	site.HandleFunc("/rest/agile/1.0/board", s.handleBoardList).Methods(http.MethodGet)
	site.HandleFunc("/rest/agile/1.0/board/{boardId}/sprint", s.handleBoardSprints).Methods(http.MethodGet)
	sub.HandleFunc("/oauth/token/accessible-resources", s.handleAccessibleResources).Methods(http.MethodGet)
}

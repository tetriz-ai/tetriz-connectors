// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleRepos serves GET /orgs/{org}/repos
func (s *Service) handleRepos(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["org"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	rows, err := s.dao.ListAll(r.Context(), s.vendor, org, "repo")
	if err != nil {
		logger.Error(r.Context(), "simulator: handleRepos", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if len(rows) == 0 {
		unknown, err := s.orgUnknown(r.Context(), org)
		if err != nil {
			logger.Error(r.Context(), "simulator: handleRepos org lookup", zap.Error(err))
			writeError(w, http.StatusInternalServerError, "db error")
			return
		}
		if unknown {
			// GitHub answers an organization that does not exist with 404.
			writeError(w, http.StatusNotFound, "Not Found")
			return
		}
	}

	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	writeJSON(w, nodes)
}

// handleReviews serves GET /repos/{owner}/{repo}/pulls/{number}/reviews
func (s *Service) handleReviews(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]
	repo := vars["repo"]
	number, _ := strconv.Atoi(vars["number"]) //nolint:errcheck // malformed number falls through to a not-found lookup below

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	row, err := s.dao.GetByPRNumber(r.Context(), s.vendor, org, repo, number)
	if err != nil {
		logger.Error(r.Context(), "simulator: handleReviews", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeJSON(w, []any{})
		return
	}

	var prPayload struct {
		Reviews struct {
			Nodes []json.RawMessage `json:"nodes"`
		} `json:"reviews"`
	}
	if err := json.Unmarshal(row.Payload, &prPayload); err != nil {
		logger.WarnWithCtx(r.Context(), "simulator: handleReviews: malformed PR payload", zap.Error(err))
		writeJSON(w, []any{})
		return
	}

	// Convert GraphQL review nodes to REST snake_case shape.
	type gqlReview struct {
		DatabaseID  int    `json:"databaseId"`
		State       string `json:"state"`
		SubmittedAt string `json:"submittedAt"`
		Author      struct {
			Login      string `json:"login"`
			DatabaseID int    `json:"databaseId"`
		} `json:"author"`
	}

	reviews := make([]map[string]any, 0, len(prPayload.Reviews.Nodes))
	for _, node := range prPayload.Reviews.Nodes {
		var gql gqlReview
		if err := json.Unmarshal(node, &gql); err != nil {
			logger.WarnWithCtx(r.Context(), "simulator: handleReviews: malformed review node", zap.Error(err))
			continue
		}
		reviews = append(reviews, map[string]any{
			"id":           gql.DatabaseID,
			"state":        gql.State,
			"submitted_at": gql.SubmittedAt,
			"user": map[string]any{
				fieldLogin: gql.Author.Login,
				"id":       gql.Author.DatabaseID,
				fieldType:  "User",
			},
		})
	}
	writeJSON(w, reviews)
}

// handlePRComments serves GET /repos/{owner}/{repo}/pulls/{number}/comments
func (s *Service) handlePRComments(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["owner"]
	repo := vars["repo"]
	number, _ := strconv.Atoi(vars["number"]) //nolint:errcheck // malformed number falls through to a not-found lookup below

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	row, err := s.dao.GetByPRNumber(r.Context(), s.vendor, org, repo, number)
	if err != nil {
		logger.Error(r.Context(), "simulator: handlePRComments", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if row == nil {
		writeJSON(w, []any{})
		return
	}

	var prPayload struct {
		Comments struct {
			Nodes []json.RawMessage `json:"nodes"`
		} `json:"comments"`
	}
	if err := json.Unmarshal(row.Payload, &prPayload); err != nil {
		logger.WarnWithCtx(r.Context(), "simulator: handlePRComments: malformed PR payload", zap.Error(err))
		writeJSON(w, []any{})
		return
	}

	type gqlComment struct {
		DatabaseID int    `json:"databaseId"`
		Body       string `json:"body"`
		CreatedAt  string `json:"createdAt"`
		Author     struct {
			Login      string `json:"login"`
			DatabaseID int    `json:"databaseId"`
		} `json:"author"`
	}

	comments := make([]map[string]any, 0, len(prPayload.Comments.Nodes))
	for _, node := range prPayload.Comments.Nodes {
		var gql gqlComment
		if err := json.Unmarshal(node, &gql); err != nil {
			logger.WarnWithCtx(r.Context(), "simulator: handlePRComments: malformed comment node", zap.Error(err))
			continue
		}
		comments = append(comments, map[string]any{
			"id":         gql.DatabaseID,
			"body":       gql.Body,
			"created_at": gql.CreatedAt,
			"user": map[string]any{
				fieldLogin: gql.Author.Login,
				"id":       gql.Author.DatabaseID,
				fieldType:  "User",
			},
		})
	}
	writeJSON(w, comments)
}

// handleCopilotMetrics serves GET /orgs/{org}/copilot/metrics
func (s *Service) handleCopilotMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["org"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	rows, err := s.dao.ListAll(r.Context(), s.vendor, org, "copilot_metrics")
	if err != nil {
		logger.Error(r.Context(), "simulator: handleCopilotMetrics", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	writeJSON(w, nodes)
}

// handleCopilotUserUsage serves GET /orgs/{org}/copilot/usage/user
func (s *Service) handleCopilotUserUsage(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	org := vars["org"]

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}
	if matched {
		writeJSON(w, []any{})
		return
	}

	rows, err := s.dao.ListAll(r.Context(), s.vendor, org, "copilot_user")
	if err != nil {
		logger.Error(r.Context(), "simulator: handleCopilotUserUsage", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	writeJSON(w, nodes)
}

// orgChecker is the store method orgUnknown uses. *dao.SimDAO implements it;
// a store without it (a handler test's fake) treats every organization as known.
type orgChecker interface {
	OrgExists(ctx context.Context, vendor, org string) (bool, error)
}

// orgUnknown reports whether the store holds no record of org.
func (s *Service) orgUnknown(ctx context.Context, org string) (bool, error) {
	c, ok := s.dao.(orgChecker)
	if !ok {
		return false, nil
	}
	exists, err := c.OrgExists(ctx, s.vendor, org)
	return !exists, err
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(map[string]string{"message": msg}) //nolint:errcheck // literal map of JSON-safe types, cannot fail
	_, _ = w.Write(b)                                       //nolint:gosec // best-effort write; client disconnects aren't actionable
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
}

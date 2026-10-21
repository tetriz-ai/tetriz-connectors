// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const (
	defaultCommentMaxResults = 50
	jiraTimeLayout           = "2006-01-02T15:04:05.000-0700"
)

// handleComments serves GET /issue/{issueIdOrKey}/comment for fetch_comments.
// The seed stores no comments, so they are derived from the issue row: zero to
// two per issue, a stable choice by key, written by the issue's reporter and
// assignee in turn an hour and a half apart from the issue's creation. The
// list is ordered by creation and windowed by startAt and maxResults. An
// unknown issue is a 404, as on Jira.
func (s *Service) handleComments(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, commentEnvelope(nil, 0, 0, defaultCommentMaxResults))
		return
	}

	key := mux.Vars(r)["issueIdOrKey"]
	startAt := parseNonNegQuery(r, fieldStartAt, 0)
	maxResults := parseNonNegQuery(r, fieldMaxResults, defaultCommentMaxResults)

	row, err := s.dao.GetByIssueKey(r.Context(), s.vendor, cloudID, key)
	if err != nil {
		logger.Error(r.Context(), "jira simulator: comment lookup failed",
			zap.String("cloudId", cloudID), zap.String("issue", key), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "comment lookup failed")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "Issue does not exist or you do not have permission to see it.")
		return
	}

	all := issueComments(row.Payload)
	writeJSON(w, commentEnvelope(window(all, startAt, maxResults), len(all), startAt, maxResults))
}

// issueComments derives an issue's comments from its payload.
func issueComments(payload []byte) []map[string]any {
	var p struct {
		Key    string `json:"key"`
		Fields struct {
			Created  string          `json:"created"`
			Reporter json.RawMessage `json:"reporter"`
			Assignee json.RawMessage `json:"assignee"`
		} `json:"fields"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Key == "" {
		return nil
	}
	created, err := time.Parse(jiraTimeLayout, p.Fields.Created)
	if err != nil {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(p.Key)) //nolint:errcheck // hash.Hash.Write never returns an error
	sum := h.Sum64()
	n := int(sum % 3)
	authors := []json.RawMessage{p.Fields.Reporter, p.Fields.Assignee}
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		at := created.Add(time.Duration(i+1) * 90 * time.Minute).Format(jiraTimeLayout)
		author := authors[i%2]
		if len(author) == 0 || string(author) == "null" {
			author = authors[(i+1)%2]
		}
		id := strconv.FormatUint((sum%1_000_000_000_000)*10+uint64(i), 10)
		text := fmt.Sprintf("Comment %d on %s", i+1, p.Key)
		out = append(out, map[string]any{
			"id":           id,
			"author":       author,
			"updateAuthor": author,
			"body": map[string]any{
				"type": "doc", "version": 1,
				"content": []any{map[string]any{
					"type":    "paragraph",
					"content": []any{map[string]any{"type": "text", "text": text}},
				}},
			},
			"created":   at,
			"updated":   at,
			"jsdPublic": true,
		})
	}
	return out
}

// commentEnvelope builds Jira's startAt-paginated comment list envelope.
func commentEnvelope(comments []map[string]any, total, startAt, maxResults int) map[string]any {
	if comments == nil {
		comments = []map[string]any{}
	}
	return map[string]any{
		"comments":      comments,
		fieldTotal:      total,
		fieldStartAt:    startAt,
		fieldMaxResults: maxResults,
	}
}

// siteChecker is the store method requireKnownSite uses. *dao.SimDAO
// implements it; a store without it (a handler test's fake) treats every
// site as known.
type siteChecker interface {
	OrgExists(ctx context.Context, vendor, org string) (bool, error)
}

// requireKnownSite answers a request for a cloud id the store holds nothing
// for with 404, as Atlassian does for a site the token cannot reach, instead
// of an empty site. A cloud id that matches a fault rule passes through, so
// the rule's handler answers it.
func (s *Service) requireKnownSite(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudID := mux.Vars(r)["cloudId"]
		c, ok := s.dao.(siteChecker)
		if _, _, _, _, matched := checkRule(cloudID); matched || !ok {
			next.ServeHTTP(w, r)
			return
		}
		exists, err := c.OrgExists(r.Context(), s.vendor, cloudID)
		if err != nil {
			logger.Error(r.Context(), "jira simulator: site lookup failed", zap.String("cloudId", cloudID), zap.Error(err))
			writeError(w, http.StatusInternalServerError, "site lookup failed")
			return
		}
		if !exists {
			writeError(w, http.StatusNotFound, "Site not found")
			return
		}
		next.ServeHTTP(w, r)
	})
}

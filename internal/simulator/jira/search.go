// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jira

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

const (
	// jiraSearchTimeLayout is the "updated >=" layout the connector's JQL uses
	// (connectors/jira builds `updated >= "2006-01-02 15:04"`).
	jiraSearchTimeLayout = "2006-01-02 15:04"

	// defaultSearchMaxResults is the page size used when the request omits one.
	defaultSearchMaxResults = 100
)

// The JQL the connector sends is `project = "KEY" AND updated >= "since" ORDER BY
// updated ASC`. The simulator extracts only the project key and the `updated >=`
// bound by regex; everything else (ordering, extra predicates) is accepted and
// ignored. The project key is accepted quoted or bare: the connector quotes it so a
// key that is a reserved JQL word ("DO", "AND") cannot trip Atlassian's parser, and
// matching both shapes keeps this handler decoupled from that formatting choice.
var (
	reJQLProject = regexp.MustCompile(`(?i)project\s*=\s*"?([A-Za-z0-9_]+)"?`)
	reJQLUpdated = regexp.MustCompile(`(?i)updated\s*>=\s*"([^"]+)"`)
)

// pageToken is the opaque JQL-search cursor: base64 of the last returned issue's
// (cursor_val, id) keyset. The connector treats it as opaque and passes it back
// verbatim; the simulator resumes ascending from it.
type pageToken struct {
	CursorVal time.Time `json:"cv"`
	ID        string    `json:"id"`
}

func encodeToken(cv time.Time, id string) string {
	b, _ := json.Marshal(pageToken{CursorVal: cv, ID: id}) //nolint:errcheck // literal struct of JSON-safe types
	return base64.StdEncoding.EncodeToString(b)
}

// decodeToken returns the keyset carried by a nextPageToken. An empty token yields
// (nil, nil) — "start from the first page", not an error. A malformed token returns
// a wrapped error the caller logs before falling back to the first page; the
// connector re-syncs from its stored timestamp cursor if a token ever goes stale,
// so a bad token is never fatal.
func decodeToken(s string) (*pageToken, error) {
	if s == "" {
		return nil, nil //nolint:nilnil // empty token = "first page", the caller's nil-tok branch; not an error
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode nextPageToken base64: %w", err)
	}
	var t pageToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("decode nextPageToken json: %w", err)
	}
	return &t, nil
}

// parseJQL extracts the project key after "project =" and the "updated >=" lower
// bound. A JQL with no project clause yields an empty project (all-projects); a
// missing or unparseable timestamp yields a nil since (no lower bound). Neither is
// an error — the simulator is permissive, returning the stored issues filtered by
// whatever it could parse. A malformed timestamp is logged (the real connector
// always emits a well-formed one, so this signals a client contract drift).
func parseJQL(ctx context.Context, jql string) (project string, since *time.Time) {
	if m := reJQLProject.FindStringSubmatch(jql); len(m) > 1 {
		// Jira matches JQL project keys case-insensitively, and stored keys are
		// upper case, so `project = "plat"` must find PLAT.
		project = strings.ToUpper(m[1])
	}
	if m := reJQLUpdated.FindStringSubmatch(jql); len(m) > 1 {
		t, err := time.Parse(jiraSearchTimeLayout, m[1])
		if err != nil {
			logger.Debug(ctx, "jira simulator: unparseable JQL `updated >=` timestamp, ignoring lower bound",
				zap.String("raw", m[1]), zap.Error(err))
		} else {
			since = &t
		}
	}
	return project, since
}

// handleSearch serves POST /search/jql for fetch_issues / fetch_issues_data. It
// paginates ascending (oldest-first) by an opaque nextPageToken keyset, matching
// the connector's `ORDER BY updated ASC` + high-water-mark cursor.
func (s *Service) handleSearch(w http.ResponseWriter, r *http.Request) {
	cloudID := mux.Vars(r)["cloudId"]
	if status, body, hdr, val, matched := checkRule(cloudID); matched {
		if status > 0 {
			writeRule(w, status, body, hdr, val)
			return
		}
		writeJSON(w, map[string]any{"issues": []json.RawMessage{}, fieldMaxResults: defaultSearchMaxResults})
		return
	}

	var reqBody struct {
		JQL           string `json:"jql"`
		MaxResults    int    `json:"maxResults"`
		NextPageToken string `json:"nextPageToken"`
	}
	// A malformed or empty body degrades to an all-projects, unbounded, first-page
	// query rather than erroring — the simulator is permissive by design.
	_ = json.NewDecoder(r.Body).Decode(&reqBody) //nolint:errcheck // permissive: bad body → default query

	maxResults := reqBody.MaxResults
	if maxResults <= 0 {
		maxResults = defaultSearchMaxResults
	}
	project, since := parseJQL(r.Context(), reqBody.JQL)

	var afterCursor *time.Time
	var afterID string
	tok, err := decodeToken(reqBody.NextPageToken)
	if err != nil {
		// A malformed token is unreachable via the real connector (it echoes a
		// simulator-minted token verbatim); fall back to the first page.
		logger.WarnWithCtx(r.Context(), "jira simulator: ignoring malformed nextPageToken, restarting from first page", zap.Error(err))
	}
	if tok != nil {
		afterCursor = &tok.CursorVal
		afterID = tok.ID
	}

	rows, err := s.dao.ListIssuesAscending(r.Context(), s.vendor, cloudID, project, since, afterCursor, afterID, maxResults)
	if err != nil {
		logger.Error(r.Context(), "jira simulator: issue search failed", zap.String("cloudId", cloudID), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "search failed")
		return
	}

	issues := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		issues = append(issues, json.RawMessage(row.Payload))
	}
	resp := map[string]any{"issues": issues, fieldMaxResults: maxResults}
	// A full page implies more rows may follow, so hand back a keyset token for the
	// next ascending window. A short page is the last page: omit the token, which is
	// how the connector detects completion (empty nextPageToken stops its loop).
	if len(rows) == maxResults {
		last := rows[len(rows)-1]
		resp["nextPageToken"] = encodeToken(last.CursorVal, last.ID)
	}
	writeJSON(w, resp)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

var (
	reOwner    = regexp.MustCompile(`owner:\s*"([^"]+)"`)
	reRepoName = regexp.MustCompile(`name:\s*"([^"]+)"`)
	reCursor   = regexp.MustCompile(`after:\s*"([^"]+)"`)
	rePageSize = regexp.MustCompile(`first:\s*(\d+)`)
	reSince    = regexp.MustCompile(`since:\s*"([^"]+)"`)
)

// historyCursorPrefix marks a commit-history endCursor. History pages by
// position in the filtered listing, not by time, so its cursor is an offset.
const historyCursorPrefix = "history:"

func encodeHistoryCursor(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(historyCursorPrefix + strconv.Itoa(offset)))
}

// historyOffset returns the offset a commit-history query's after: cursor
// names, or 0 for the first page.
func historyOffset(body string) int {
	m := reCursor.FindStringSubmatch(body)
	if len(m) < 2 {
		return 0
	}
	b, err := base64.StdEncoding.DecodeString(m[1])
	if err != nil || !strings.HasPrefix(string(b), historyCursorPrefix) {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimPrefix(string(b), historyCursorPrefix))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// extractSince returns the history query's since: argument, or nil.
func extractSince(body string) *time.Time {
	m := reSince.FindStringSubmatch(body)
	if len(m) < 2 {
		return nil
	}
	t, err := time.Parse(time.RFC3339, m[1])
	if err != nil {
		return nil
	}
	return &t
}

func encodeCursor(t time.Time) string {
	return base64.StdEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano)))
}

func decodeCursor(s string) (time.Time, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("base64 decode cursor: %w", err)
	}
	t, err := time.Parse(time.RFC3339Nano, string(b))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cursor timestamp: %w", err)
	}
	return t, nil
}

func extractOwner(body string) string {
	if m := reOwner.FindStringSubmatch(body); len(m) > 1 {
		return m[1]
	}
	// org-level queries use organization(login: "org")
	re := regexp.MustCompile(`login:\s*"([^"]+)"`)
	if m := re.FindStringSubmatch(body); len(m) > 1 {
		return m[1]
	}
	return ""
}

func extractRepoName(body string) string {
	if m := reRepoName.FindStringSubmatch(body); len(m) > 1 {
		return m[1]
	}
	return ""
}

func extractCursor(body string) *time.Time {
	m := reCursor.FindStringSubmatch(body)
	if len(m) < 2 {
		return nil
	}
	if b, err := base64.StdEncoding.DecodeString(m[1]); err == nil && strings.HasPrefix(string(b), historyCursorPrefix) {
		return nil // a commit-history cursor; handleCommits reads it
	}
	t, err := decodeCursor(m[1])
	if err != nil {
		logger.ErrorNoCtx("simulator: decode cursor failed; treating as no cursor", zap.Error(err))
		return nil
	}
	return &t
}

func extractPageSize(body string) int {
	if m := rePageSize.FindStringSubmatch(body); len(m) > 1 {
		n, _ := strconv.Atoi(m[1]) //nolint:errcheck // malformed page size falls through to the default below
		if n > 0 {
			return n
		}
	}
	return 100
}

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// handleGraphQL dispatches a GraphQL POST request to the appropriate handler.
func (s *Service) handleGraphQL(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		logger.WarnWithCtx(r.Context(), "simulator: handleGraphQL: failed to read body", zap.Error(err))
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	body := string(bodyBytes)

	// Extract the actual GraphQL query string from the JSON body {"query":"..."}.
	// A body that isn't {"query":...}-wrapped is used as-is (raw query text), so a
	// failed unmarshal here is a deliberate fallback, not an error condition.
	var reqBody struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(bodyBytes, &reqBody); err == nil && reqBody.Query != "" { // deliberate fallback to raw body on non-JSON input, see comment above
		body = reqBody.Query
	}

	orgName := extractOwner(body)
	if orgName == "" {
		writeError(w, http.StatusBadRequest, "could not extract org from query")
		return
	}

	statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(orgName)
	if matched && statusCode > 0 {
		writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
		return
	}

	repo := extractRepoName(body)
	before := extractCursor(body)
	pageSize := extractPageSize(body)
	ctx := r.Context()

	// Dispatch by keyword presence — order matters (most specific first).
	switch {
	case strings.Contains(body, "squashMergeAllowed"):
		s.handleMergeMethods(ctx, w, orgName, repo)
	case strings.Contains(body, "membersWithRole") || strings.Contains(body, "samlIdentityProvider"):
		s.handleMembers(ctx, w, orgName, matched)
	case strings.Contains(body, "projectsV2"):
		s.handleProjects(ctx, w, orgName, matched)
	case strings.Contains(body, "defaultBranchRef") || strings.Contains(body, "history"):
		s.handleCommits(ctx, w, orgName, repo, pageSize, matched, body)
	case strings.Contains(body, "subIssues"):
		s.handleIssues(ctx, w, orgName, repo, before, pageSize, matched)
	case strings.Contains(body, "issues"):
		s.handleIssuesData(ctx, w, orgName, repo, before, pageSize, matched)
	case strings.Contains(body, "additions"):
		s.handlePRs(ctx, w, orgName, repo, before, pageSize, matched)
	case strings.Contains(body, "body"):
		s.handlePRsData(ctx, w, orgName, repo, before, pageSize, matched)
	case strings.Contains(body, "pullRequests"):
		s.handlePRList(ctx, w, orgName, repo, before, pageSize, matched)
	default:
		writeError(w, http.StatusBadRequest, "unrecognised GraphQL query")
	}
}

func (s *Service) handlePRs(ctx context.Context, w http.ResponseWriter, orgName, repo string, before *time.Time, pageSize int, emptyRequested bool) {
	repoID, err := s.repoDatabaseID(ctx, orgName, repo)
	if err != nil {
		logger.Error(ctx, "simulator: handlePRs: repo databaseId lookup", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if emptyRequested {
		writeGraphQLRepoResponse(w, repoID, "pullRequests", json.RawMessage(`{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}`))
		return
	}
	rows, err := s.dao.ListPaged(ctx, s.vendor, orgName, "pr", repo, before, pageSize+1)
	if err != nil {
		logger.Error(ctx, "simulator: handlePRs", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	writeGraphQLRepoResponse(w, repoID, "pullRequests", buildPRConnection(rows, pageSize))
}

func (s *Service) handlePRsData(ctx context.Context, w http.ResponseWriter, orgName, repo string, before *time.Time, pageSize int, emptyRequested bool) {
	// Same payload shape as handlePRs — the connector selects different fields
	// but the simulator returns the full payload; unused fields are ignored.
	s.handlePRs(ctx, w, orgName, repo, before, pageSize, emptyRequested)
}

func (s *Service) handlePRList(ctx context.Context, w http.ResponseWriter, orgName, repo string, before *time.Time, pageSize int, emptyRequested bool) {
	// PR list for reviews/comments phase — connector only needs number + updatedAt.
	// Returning full payload is fine; connector ignores extra fields.
	s.handlePRs(ctx, w, orgName, repo, before, pageSize, emptyRequested)
}

// handleCommits serves both commit-history query shapes the connector sends:
// the plain default-branch query (`defaultBranchRef`, no tracked-branch
// override configured) and the branch-pinned query (`ref(qualifiedName: ...)`) — resolveCommitBranch always resolves to the repo's default
// branch, so every simulator org exercises the `ref` shape. GitHub's response
// uses whichever field name matches the query; returning the wrong one leaves
// the connector's ref lookup nil, which it treats as a clean end-of-history
// page rather than an error.
//
// The history holds every commit of the repository's pull requests, newest
// committedDate first, filtered by the query's since: (inclusive, as on
// GitHub) and paged by first: and after:.
func (s *Service) handleCommits(ctx context.Context, w http.ResponseWriter, orgName, repo string, pageSize int, emptyRequested bool, body string) {
	// "qualifiedName" (the ref-lookup's own GraphQL argument name) is used
	// instead of the tighter "ref(qualifiedName" substring so this still
	// matches if the connector's query template ever gets reformatted with
	// different spacing around "ref(" — a query shape neither branch
	// recognizes must be logged, not silently misrouted, since a wrong field
	// name here would make a nil ref/CustomRef lookup read
	// as a clean end-of-history page on the connector side, not an error).
	field := "defaultBranchRef"
	switch {
	case strings.Contains(body, "qualifiedName"):
		field = "ref"
	case strings.Contains(body, "defaultBranchRef"):
		field = "defaultBranchRef"
	default:
		logger.WarnWithCtx(ctx, "simulator: handleCommits: query body matched neither defaultBranchRef nor ref(qualifiedName) shape; defaulting to defaultBranchRef",
			zap.String("org", orgName))
	}
	repoID, err := s.repoDatabaseID(ctx, orgName, repo)
	if err != nil {
		logger.Error(ctx, "simulator: handleCommits: repo databaseId lookup", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if emptyRequested {
		writeGraphQLRepoResponse(w, repoID, field, json.RawMessage(`{"target":{"history":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`))
		return
	}
	rows, err := s.dao.ListPaged(ctx, s.vendor, orgName, "pr", repo, nil, 0)
	if err != nil {
		logger.Error(ctx, "simulator: handleCommits", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	allCommits := historyCommits(ctx, rows, extractSince(body))
	offset := historyOffset(body)
	if offset > len(allCommits) {
		offset = len(allCommits)
	}
	allCommits = allCommits[offset:]
	hasNext := len(allCommits) > pageSize
	if hasNext {
		allCommits = allCommits[:pageSize]
	}
	endCursor := ""
	if hasNext {
		endCursor = encodeHistoryCursor(offset + pageSize)
	}

	conn, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		fieldNodes:    allCommits,
		fieldPageInfo: pageInfo{HasNextPage: hasNext, EndCursor: endCursor},
	})
	inner, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		"target": map[string]any{
			"history": json.RawMessage(conn),
		},
	})
	writeGraphQLRepoResponse(w, repoID, field, inner)
}

// historyCommits returns the distinct commits of the pull request rows whose
// committedDate is at or after since (all of them when since is nil), newest
// first, ties by oid.
func historyCommits(ctx context.Context, rows []dao.Row, since *time.Time) []json.RawMessage {
	type node struct {
		OID           string `json:"oid"`
		CommittedDate string `json:"committedDate"`
	}
	type entry struct {
		raw json.RawMessage
		at  time.Time
		oid string
	}
	var pr struct {
		Commits struct {
			Nodes []json.RawMessage `json:"nodes"`
		} `json:"commits"`
	}
	seen := map[string]bool{}
	var entries []entry
	for _, row := range rows {
		pr.Commits.Nodes = nil
		if err := json.Unmarshal(row.Payload, &pr); err != nil {
			logger.WarnWithCtx(ctx, "simulator: handleCommits: skipping malformed PR payload", zap.Error(err))
			continue
		}
		for _, raw := range pr.Commits.Nodes {
			var n node
			if json.Unmarshal(raw, &n) != nil || n.OID == "" || seen[n.OID] {
				continue
			}
			at, err := time.Parse(time.RFC3339, n.CommittedDate)
			if err != nil {
				continue
			}
			if since != nil && at.Before(*since) {
				continue
			}
			seen[n.OID] = true
			entries = append(entries, entry{raw: raw, at: at, oid: n.OID})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].at.Equal(entries[j].at) {
			return entries[i].at.After(entries[j].at)
		}
		return entries[i].oid < entries[j].oid
	})
	out := make([]json.RawMessage, len(entries))
	for i, e := range entries {
		out[i] = e.raw
	}
	return out
}

func (s *Service) handleIssues(ctx context.Context, w http.ResponseWriter, orgName, repo string, before *time.Time, pageSize int, emptyRequested bool) {
	repoID, err := s.repoDatabaseID(ctx, orgName, repo)
	if err != nil {
		logger.Error(ctx, "simulator: handleIssues: repo databaseId lookup", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}
	if emptyRequested {
		writeGraphQLRepoResponse(w, repoID, "issues", json.RawMessage(`{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}`))
		return
	}
	rows, err := s.dao.ListPaged(ctx, s.vendor, orgName, "pr", repo, before, 0)
	if err != nil {
		logger.Error(ctx, "simulator: handleIssues", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	nodes := extractClosingIssues(rows, orgName, repo)
	pi := pageInfo{HasNextPage: false, EndCursor: ""}
	conn, _ := json.Marshal(map[string]any{fieldNodes: nodes, fieldPageInfo: pi}) //nolint:errcheck // literal map of JSON-safe types, cannot fail
	writeGraphQLRepoResponse(w, repoID, "issues", conn)
}

func (s *Service) handleIssuesData(ctx context.Context, w http.ResponseWriter, orgName, repo string, before *time.Time, pageSize int, emptyRequested bool) {
	s.handleIssues(ctx, w, orgName, repo, before, pageSize, emptyRequested)
}

func (s *Service) handleMembers(ctx context.Context, w http.ResponseWriter, orgName string, emptyRequested bool) {
	if emptyRequested {
		writeGraphQLOrgResponse(w, "membersWithRole", json.RawMessage(`{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}`))
		return
	}
	rows, err := s.dao.ListAll(ctx, s.vendor, orgName, "member")
	if err != nil {
		logger.Error(ctx, "simulator: handleMembers", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	conn, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		fieldNodes:    nodes,
		fieldPageInfo: pageInfo{HasNextPage: false, EndCursor: ""},
	})
	writeGraphQLOrgResponse(w, "membersWithRole", conn)
}

func (s *Service) handleProjects(ctx context.Context, w http.ResponseWriter, orgName string, emptyRequested bool) {
	if emptyRequested {
		writeGraphQLOrgResponse(w, "projectsV2", json.RawMessage(`{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}`))
		return
	}
	rows, err := s.dao.ListAll(ctx, s.vendor, orgName, "project")
	if err != nil {
		logger.Error(ctx, "simulator: handleProjects", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	conn, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		fieldNodes:    nodes,
		fieldPageInfo: pageInfo{HasNextPage: false, EndCursor: ""},
	})
	writeGraphQLOrgResponse(w, "projectsV2", conn)
}

// buildPRConnection constructs the pullRequests connection from DAO rows.
func buildPRConnection(rows []dao.Row, pageSize int) json.RawMessage {
	hasNext := len(rows) > pageSize
	if hasNext {
		rows = rows[:pageSize]
	}
	nodes := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, json.RawMessage(row.Payload))
	}
	endCursor := ""
	if hasNext && len(rows) > 0 {
		endCursor = encodeCursor(rows[len(rows)-1].CursorVal)
	}
	conn, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		fieldNodes:    nodes,
		fieldPageInfo: pageInfo{HasNextPage: hasNext, EndCursor: endCursor},
	})
	return conn
}

// extractClosingIssues pulls closingIssuesReferences.nodes from each PR payload
// and converts them to issue-shaped nodes the connector can parse.
func extractClosingIssues(rows []dao.Row, orgName, repo string) []json.RawMessage {
	type closingRef struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
		State  string `json:"state"`
	}
	type prPayload struct {
		UpdatedAt string `json:"updatedAt"`
		Author    struct {
			Login      string `json:"login"`
			DatabaseID int    `json:"databaseId"`
		} `json:"author"`
		ClosingIssuesReferences struct {
			Nodes []closingRef `json:"nodes"`
		} `json:"closingIssuesReferences"`
	}

	nameWithOwner := orgName + "/" + repo
	var nodes []json.RawMessage
	seen := map[int]bool{}
	for _, row := range rows {
		var pr prPayload
		if err := json.Unmarshal(row.Payload, &pr); err != nil {
			logger.ErrorNoCtx("simulator: extractClosingIssues: skipping malformed PR payload", zap.Error(err))
			continue
		}
		// ParseIssue (the consumer) hard-fails on an empty author.login;
		// fall back to a placeholder so a PR row with no author (e.g. hand-inserted
		// sim_records) can't reintroduce that failure.
		authorLogin := pr.Author.Login
		if authorLogin == "" {
			authorLogin = "ghost"
		}
		for _, ci := range pr.ClosingIssuesReferences.Nodes {
			if seen[ci.Number] {
				continue
			}
			seen[ci.Number] = true
			node, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
				"number":    ci.Number,
				"title":     ci.Title,
				"state":     ci.State,
				"updatedAt": pr.UpdatedAt,
				"createdAt": pr.UpdatedAt,
				"body":      "",
				"url":       fmt.Sprintf("https://github.com/%s/issues/%d", nameWithOwner, ci.Number),
				// Reuse the linking PR's author as the issue's author — no
				// independent issue-author identity is seeded.
				"author":                         map[string]any{fieldLogin: authorLogin, "databaseId": pr.Author.DatabaseID},
				"repository":                     map[string]any{"nameWithOwner": nameWithOwner},
				"subIssues":                      map[string]any{fieldTotalCount: 0},
				"closedByPullRequestsReferences": map[string]any{fieldNodes: []any{}},
				"comments":                       map[string]any{fieldTotalCount: 0, fieldNodes: []any{}, fieldPageInfo: map[string]any{"hasNextPage": false, "endCursor": ""}},
				"assignees":                      map[string]any{fieldNodes: []any{}},
				"labels":                         map[string]any{fieldNodes: []any{}},
				"projectItems":                   map[string]any{fieldNodes: []any{}},
				"participants":                   map[string]any{fieldTotalCount: 0},
			})
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// writeGraphQLRepoResponse wraps a connection JSON under data.repository.<field>.
// databaseID must be the repo's real databaseId, resolved via repoDatabaseID —
// see that helper for why a constant here breaks the PR→repo FK.
func writeGraphQLRepoResponse(w http.ResponseWriter, databaseID int64, field string, conn json.RawMessage) {
	resp, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		"data": map[string]any{
			"repository": map[string]any{
				"databaseId": databaseID,
				field:        json.RawMessage(conn),
			},
		},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

// repoDatabaseID returns the databaseId the simulator assigned to (org, repo)
// in its seeded `repo` entity — the same value handleRepos serves and that the
// connector stores as a consumer's repository id. GraphQL repo-scoped
// responses must echo this: the connector derives a PR's / commit's
// platform_repo_id from repository.databaseId, so a constant would make every
// PR's platform_repo_id collide and never match its repository row,
// leaving a consumer's pull-request repository link NULL. Returns (0, nil) — connector then
// omits platform_repo_id — only when no seeded repo matches by name; a DAO
// error is returned so the caller can fail loudly (a 500) rather than serve a
// success carrying a link-dropping 0.
func (s *Service) repoDatabaseID(ctx context.Context, org, repo string) (int64, error) {
	rows, err := s.dao.ListAll(ctx, s.vendor, org, "repo")
	if err != nil {
		return 0, fmt.Errorf("listing repo entities: %w", err)
	}
	for _, row := range rows {
		var p struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(row.Payload, &p); err != nil {
			logger.WarnWithCtx(ctx, "simulator: repoDatabaseID: skipping malformed repo payload",
				zap.String("org", org), zap.Error(err))
			continue
		}
		if p.Name == repo {
			return p.ID, nil
		}
	}
	logger.WarnWithCtx(ctx, "simulator: repoDatabaseID: no repo entity matched; returning 0 (connector will omit platform_repo_id)",
		zap.String("org", org), zap.String("repo", repo))
	return 0, nil
}

// writeGraphQLOrgResponse wraps a connection JSON under data.organization.<field>.
func writeGraphQLOrgResponse(w http.ResponseWriter, field string, conn json.RawMessage) {
	resp, _ := json.Marshal(map[string]any{ //nolint:errcheck // literal map of JSON-safe types, cannot fail
		"data": map[string]any{
			"organization": map[string]any{
				field: json.RawMessage(conn),
			},
		},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

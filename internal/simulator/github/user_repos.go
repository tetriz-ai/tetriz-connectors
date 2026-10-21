// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// OrgHeader names the simulated organizations a GET /user/repos request
// lists, comma-separated. Without it the request lists every simulated
// organization: the simulated token owner is a member of all of them.
const OrgHeader = "X-Simulator-Organization"

// SimulatedUserLogin is the login GET /user answers with.
const SimulatedUserLogin = "simulator-user"

// orgLister lists the vendor's organizations. *dao.SimDAO implements it.
type orgLister interface {
	ListOrgs(ctx context.Context, vendor string) ([]string, error)
}

// handleUser serves GET /user: the simulated token owner.
func (s *Service) handleUser(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		fieldLogin: SimulatedUserLogin,
		"id":       1,
		"node_id":  "U_simulator",
		fieldType:  "User",
		"name":     "Simulated user",
		"html_url": "https://github.com/" + SimulatedUserLogin,
	})
}

// handleUserRepos serves GET /user/repos, the listing a personal access token
// reads. The request names no organization, so the simulator lists the
// organizations in OrgHeader, or else every simulated GitHub organization,
// as repositories the token owner reaches by organization membership.
//
// It honours GitHub's visibility (all, public, private), affiliation
// (owner, collaborator, organization_member) and type (all, owner, public,
// private, member) filters, answers 422 for type combined with visibility or
// affiliation as GitHub does, sorts by full_name, and pages with per_page
// (default 30, at most 100) and page, with a Link header.
func (s *Service) handleUserRepos(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	typ, visibility, affiliation := q.Get("type"), q.Get("visibility"), q.Get("affiliation")
	if typ != "" && (visibility != "" || affiliation != "") {
		writeError(w, http.StatusUnprocessableEntity, "If you specify visibility or affiliation, you cannot specify type.")
		return
	}
	if visibility == "" {
		visibility = "all"
	}
	if visibility != "all" && visibility != "public" && visibility != "private" {
		writeError(w, http.StatusUnprocessableEntity, "Validation Failed")
		return
	}
	member := true // the token owner reaches every listed repository as an organization member
	switch typ {
	case "", "all", "member":
	case "owner":
		member = false
	case "public", "private":
		visibility = typ
	default:
		writeError(w, http.StatusUnprocessableEntity, "Validation Failed")
		return
	}
	if affiliation != "" && !containsCSV(affiliation, "organization_member") {
		member = false
	}

	orgs, ok := s.userRepoOrgs(w, r)
	if !ok {
		return
	}
	var repos []json.RawMessage
	if member {
		for _, org := range orgs {
			statusCode, ruleBody, extraHeader, extraValue, matched := checkRule(org)
			if matched && statusCode > 0 {
				writeRule(w, statusCode, ruleBody, extraHeader, extraValue)
				return
			}
			if matched {
				continue
			}
			rows, err := s.dao.ListAll(r.Context(), s.vendor, org, "repo")
			if err != nil {
				logger.Error(r.Context(), "simulator: handleUserRepos", zap.Error(err))
				writeError(w, http.StatusInternalServerError, "db error")
				return
			}
			for _, row := range rows {
				repo, keep := userRepo(row.Payload, org, visibility)
				if keep {
					repos = append(repos, repo)
				}
			}
		}
	}
	sort.SliceStable(repos, func(i, j int) bool { return fullName(repos[i]) < fullName(repos[j]) })

	perPage := intParam(q.Get("per_page"), 30)
	if perPage > 100 {
		perPage = 100
	}
	page := intParam(q.Get("page"), 1)
	last := (len(repos) + perPage - 1) / perPage
	if last < 1 {
		last = 1
	}
	start := (page - 1) * perPage
	out := []json.RawMessage{}
	if start < len(repos) {
		out = repos[start:min(start+perPage, len(repos))]
	}
	if link := linkHeader(r, page, last); link != "" {
		w.Header().Set("Link", link)
	}
	writeJSON(w, out)
}

// userRepoOrgs returns the organizations a /user/repos request lists.
func (s *Service) userRepoOrgs(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	if h := r.Header.Get(OrgHeader); h != "" {
		var orgs []string
		for _, o := range strings.Split(h, ",") {
			if o = strings.TrimSpace(o); o != "" {
				orgs = append(orgs, o)
			}
		}
		return orgs, true
	}
	lister, ok := s.dao.(orgLister)
	if !ok {
		writeError(w, http.StatusInternalServerError, "simulator store cannot list organizations")
		return nil, false
	}
	orgs, err := lister.ListOrgs(r.Context(), s.vendor)
	if err != nil {
		logger.Error(r.Context(), "simulator: handleUserRepos list orgs", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "db error")
		return nil, false
	}
	return orgs, true
}

// userRepo fills in the fields /user/repos carries that a seeded repository
// may lack (owner, visibility) and reports whether it passes visibility.
func userRepo(payload []byte, org, visibility string) (json.RawMessage, bool) {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return payload, visibility == "all"
	}
	private, _ := m["private"].(bool)
	vis, _ := m["visibility"].(string)
	if vis == "" {
		vis = "public"
		if private {
			vis = "private"
		}
		m["visibility"] = vis
	}
	if _, ok := m["owner"]; !ok {
		m["owner"] = map[string]any{fieldLogin: org, fieldType: "Organization"}
	}
	if visibility == "public" && vis != "public" || visibility == "private" && vis == "public" {
		return nil, false
	}
	b, err := json.Marshal(m)
	if err != nil {
		return payload, true
	}
	return b, true
}

func fullName(repo json.RawMessage) string {
	var v struct {
		FullName string `json:"full_name"`
	}
	_ = json.Unmarshal(repo, &v) //nolint:errcheck // an undecodable repository sorts first
	return strings.ToLower(v.FullName)
}

func containsCSV(list, want string) bool {
	for _, v := range strings.Split(list, ",") {
		if strings.TrimSpace(v) == want {
			return true
		}
	}
	return false
}

// intParam parses a positive integer query value, falling back to def as
// GitHub does for a missing or invalid one.
func intParam(v string, def int) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	return n
}

// linkHeader builds GitHub's pagination Link header for page of last.
func linkHeader(r *http.Request, page, last int) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	at := func(p int, rel string) string {
		q := r.URL.Query()
		q.Set("page", strconv.Itoa(p))
		u := url.URL{Scheme: scheme, Host: r.Host, Path: r.URL.Path, RawQuery: q.Encode()}
		return fmt.Sprintf("<%s>; rel=%q", u.String(), rel)
	}
	var links []string
	if page > 1 {
		links = append(links, at(page-1, "prev"))
	}
	if page < last {
		links = append(links, at(page+1, "next"), at(last, "last"))
	}
	if page > 1 {
		links = append(links, at(1, "first"))
	}
	return strings.Join(links, ", ")
}

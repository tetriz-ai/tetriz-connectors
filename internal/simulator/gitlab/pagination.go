// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

const (
	defaultPage    = 1
	defaultPerPage = 20
	// maxPerPage caps a request-supplied per_page, mirroring GitLab's own 100-item
	// ceiling. Besides matching the real API, it keeps the page-window arithmetic
	// (page-1)*per_page bounded so a crafted huge per_page cannot overflow into a
	// negative slice bound.
	maxPerPage = 100
	// maxPage caps a request-supplied page for the same reason: without it a crafted
	// page wraps (page-1)*per_page to a negative offset, which Postgres rejects. A
	// page past it is answered as the empty page past the end of any listing.
	maxPage = 1_000_000
)

// parsePagination reads GitLab's page/per_page query params, defaulting to
// page=1, per_page=20 and clamping page to maxPage and per_page to maxPerPage. Non-positive or
// unparseable values fall back to the defaults.
func parsePagination(r *http.Request) (page, perPage int) {
	page, perPage = defaultPage, defaultPerPage
	// Atoi returns 0 on an absent/garbage param, so the >0 guard keeps the default;
	// the parse error carries no more information than that and is intentionally dropped.
	if v, _ := strconv.Atoi(r.URL.Query().Get("page")); v > 0 { //nolint:errcheck // absent/garbage → 0 → default kept
		page = min(v, maxPage)
	}
	if v, _ := strconv.Atoi(r.URL.Query().Get("per_page")); v > 0 { //nolint:errcheck // absent/garbage → 0 → default kept
		perPage = min(v, maxPerPage)
	}
	return page, perPage
}

// writeList emits a bare JSON array (GitLab lists are not a Bitbucket envelope)
// and sets X-Next-Page to the next page number IFF this page was full. The GitLab
// gateway reconstructs next_page_url from that header; omitting it stops paging.
func writeList(w http.ResponseWriter, values []json.RawMessage, page, perPage int) {
	if len(values) == perPage {
		w.Header().Set("X-Next-Page", strconv.Itoa(page+1))
	}
	writeJSON(w, values)
}

// emptyList writes the sim-empty body for a paginated list route: a bare []
// with no X-Next-Page.
func emptyList(w http.ResponseWriter) {
	writeJSON(w, []json.RawMessage{})
}

// windowSlice returns the [(page-1)*perPage, +perPage) window of values, or an
// empty slice past the end. Used by per-MR handlers to paginate an array
// extracted from a single stored payload (list routes are windowed in SQL).
func windowSlice(values []json.RawMessage, page, perPage int) []json.RawMessage {
	start := (page - 1) * perPage
	if start < 0 || start >= len(values) {
		return []json.RawMessage{}
	}
	// perPage is clamped to maxPerPage upstream, so end = start+perPage stays in
	// range and cannot overflow past a start already bounded to [0,len).
	end := min(start+perPage, len(values))
	return values[start:end]
}

// payloads projects DAO rows to their raw JSON payloads for listing.
func payloads(rows []dao.Row) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		out = append(out, json.RawMessage(row.Payload))
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck,gosec // best-effort write; client disconnects aren't actionable
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(errorBody(msg)) //nolint:gosec // best-effort write; client disconnects aren't actionable
}

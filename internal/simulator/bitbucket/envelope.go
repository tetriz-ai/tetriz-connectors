// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

const (
	defaultPage    = 1
	defaultPagelen = 50
	// maxPagelen caps a request-supplied pagelen, mirroring Bitbucket's own 100-item
	// ceiling. Besides matching the real API, it keeps the page-window arithmetic
	// (page-1)*pagelen bounded so a crafted huge pagelen cannot overflow into a
	// negative slice bound.
	maxPagelen = 100
)

// parsePagination reads Bitbucket's page/pagelen query params, defaulting to
// page=1, pagelen=50 and clamping pagelen to maxPagelen. Non-positive or unparseable
// values fall back to the defaults.
func parsePagination(r *http.Request) (page, pagelen int) {
	page, pagelen = defaultPage, defaultPagelen
	// Atoi returns 0 on an absent/garbage param, so the >0 guard keeps the default;
	// the parse error carries no more information than that and is intentionally dropped.
	if v, _ := strconv.Atoi(r.URL.Query().Get("page")); v > 0 { //nolint:errcheck // absent/garbage → 0 → default kept
		page = v
	}
	if v, _ := strconv.Atoi(r.URL.Query().Get("pagelen")); v > 0 { //nolint:errcheck // absent/garbage → 0 → default kept
		pagelen = min(v, maxPagelen)
	}
	return page, pagelen
}

// writeEnvelope emits the Bitbucket paginated list envelope
// {"pagelen","values","page","size","next"}. `next` is present only when the page
// was full (len(values)==pagelen), signalling more rows may follow. The eight
// BaseURL-routed operations only test `next` for emptiness and rebuild the URL from
// their own base, so a syntactically valid same-request URL with page+1 suffices.
// Callers must pass exactly one page of values (the DAO windows list routes; per-PR
// handlers window the extracted array via windowSlice).
func writeEnvelope(w http.ResponseWriter, r *http.Request, values []json.RawMessage, page, pagelen int) {
	env := map[string]any{
		"pagelen": pagelen,
		"values":  values,
		"page":    page,
		"size":    len(values),
	}
	if len(values) == pagelen {
		env["next"] = nextPageURL(r, page+1)
	}
	writeJSON(w, env)
}

// emptyListEnvelope writes the sim-empty body for a paginated list route:
// {"pagelen":50,"values":[],"page":1,"size":0} with no `next`.
func emptyListEnvelope(w http.ResponseWriter) {
	writeJSON(w, map[string]any{
		"pagelen": defaultPagelen,
		"values":  []json.RawMessage{},
		"page":    defaultPage,
		"size":    0,
	})
}

// nextPageURL returns the request URL with its page query param set to nextPage,
// as an absolute URL on the request's own host. Absolute (not relative) is required
// by the two operations that follow `next` verbatim — fetch_branches and
// fetch_repo_tree's src walk — whose connector-side SSRF guard compares the next
// URL's host against the configured REST host; a relative URL parses to an empty
// host and is rejected. Server-side r.URL carries only path+query, so the host and
// scheme are taken from the request (r.Host, r.TLS). This also matches real
// Bitbucket, which returns absolute next URLs; the list routes that merely test
// `next` for emptiness are unaffected.
func nextPageURL(r *http.Request, nextPage int) string {
	u := *r.URL
	u.Host = r.Host
	u.Scheme = "http"
	if r.TLS != nil {
		u.Scheme = "https"
	}
	q := u.Query()
	q.Set("page", strconv.Itoa(nextPage))
	u.RawQuery = q.Encode()
	return u.String()
}

// windowSlice returns the [(page-1)*pagelen, +pagelen) window of values, or an empty
// slice past the end. Used by per-PR handlers to paginate an array extracted from a
// single stored payload (list routes are windowed in SQL by the DAO instead).
func windowSlice(values []json.RawMessage, page, pagelen int) []json.RawMessage {
	start := (page - 1) * pagelen
	if start < 0 || start >= len(values) {
		return []json.RawMessage{}
	}
	// pagelen is clamped to maxPagelen upstream, so end = start+pagelen stays in
	// range and cannot overflow past a start already bounded to [0,len).
	end := min(start+pagelen, len(values))
	return values[start:end]
}

// payloads projects DAO rows to their raw JSON payloads for enveloping.
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

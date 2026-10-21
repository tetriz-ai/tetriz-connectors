// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// handleMRList serves GET /projects/{id}/merge_requests. fetch_prs sends
// updated_after plus order_by=updated_at&sort=asc and emits every MR returned
// — it does not discard old MRs client-side — so the simulator must filter
// and sort here or every 15/30m tick re-emits the full backfill.
func (s *Service) handleMRList(w http.ResponseWriter, r *http.Request) {
	org, projectID, emptyMatch, ok := s.orgFromProjectPath(w, r)
	if !ok {
		return
	}
	if emptyMatch {
		emptyList(w)
		return
	}

	q := r.URL.Query()
	rows, err := s.dao.ListPageWindowByProjectID(r.Context(), s.vendor, org, "pr", projectID, 0, 0)
	if err != nil {
		writeDBError(r.Context(), w, "handleMRList", err)
		return
	}
	values := filterAndSortMRs(payloads(rows), q.Get("updated_after"), q.Get("sort"))
	page, perPage := parsePagination(r)
	writeList(w, windowSlice(values, page, perPage), page, perPage)
}

// filterAndSortMRs keeps MRs with updated_at >= updated_after (inclusive, GitLab
// semantics) and sorts by updated_at ascending when sort=asc. An unparseable
// cutoff is ignored so a garbage query param cannot 500 the list.
func filterAndSortMRs(values []json.RawMessage, updatedAfter, sortDir string) []json.RawMessage {
	cutoff, hasCutoff := parseGitLabTime(updatedAfter)
	type keyed struct {
		raw json.RawMessage
		at  time.Time
	}
	kept := make([]keyed, 0, len(values))
	for _, raw := range values {
		at := payloadUpdatedAt(raw)
		if hasCutoff && at.Before(cutoff) {
			continue
		}
		kept = append(kept, keyed{raw: raw, at: at})
	}
	if sortDir == "asc" {
		sort.SliceStable(kept, func(i, j int) bool { return kept[i].at.Before(kept[j].at) })
	}
	out := make([]json.RawMessage, len(kept))
	for i, k := range kept {
		out[i] = k.raw
	}
	return out
}

func payloadUpdatedAt(payload []byte) time.Time {
	var p struct {
		UpdatedAt string `json:"updated_at"`
	}
	_ = json.Unmarshal(payload, &p) //nolint:errcheck // missing/garbage → zero time, sorts first
	t, ok := parseGitLabTime(p.UpdatedAt)
	if !ok {
		return time.Time{}
	}
	return t
}

func parseGitLabTime(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		logger.DebugNoCtx("simulator: gitlab ignoring unparseable time", zap.Error(err))
		return time.Time{}, false
	}
	return t, true
}

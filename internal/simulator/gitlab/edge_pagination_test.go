// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
)

// offsetRecordingDAO records the window each SQL-windowed list route asks for.
type offsetRecordingDAO struct {
	mockDAO
	offsets []int
}

func (d *offsetRecordingDAO) ListPageWindow(ctx context.Context, v, org, et, repo string, limit, offset int) ([]dao.Row, error) {
	d.offsets = append(d.offsets, offset)
	return d.mockDAO.ListPageWindow(ctx, v, org, et, repo, limit, offset)
}

func (d *offsetRecordingDAO) ListPageWindowByProjectID(ctx context.Context, v, org, et string, pid int64, limit, offset int) ([]dao.Row, error) {
	d.offsets = append(d.offsets, offset)
	return d.mockDAO.ListPageWindowByProjectID(ctx, v, org, et, pid, limit, offset)
}

// TestEdge_HugePageNeverReachesTheDAOAsANegativeOffset covers a crafted page number.
// pagination.go clamps per_page so "(page-1)*per_page" stays bounded, but page itself
// is unbounded: page=MaxInt64 with per_page=100 wraps to offset -200. Postgres rejects
// a negative OFFSET, so the route would 500 where GitLab answers an empty page.
func TestEdge_HugePageNeverReachesTheDAOAsANegativeOffset(t *testing.T) {
	d := &offsetRecordingDAO{mockDAO: mockDAO{rows: []dao.Row{row("repo", `{"id":9100101,"path":"ledger"}`)}}}
	srv := newTestServer(d)
	defer srv.Close()

	for _, route := range []string{
		"/gitlab/api/v4/groups/91001/projects",
		"/gitlab/api/v4/projects/9100101/repository/commits",
	} {
		resp, body := getJSON(t, srv.URL+route+"?page=9223372036854775807&per_page=100")
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", route, body)
		assert.Empty(t, decodeArray(t, body), route)
		assert.Empty(t, resp.Header.Get("X-Next-Page"), route)
	}
	for _, off := range d.offsets {
		assert.GreaterOrEqual(t, off, 0, "the DAO must never be asked for a negative offset")
	}
}

// TestEdge_ExactMultipleOfPerPageEndsOnAnEmptyPage covers a listing whose size is an
// exact multiple of per_page. The last full page advertises a next page; that page
// must be an empty array without X-Next-Page so a client following the header stops.
func TestEdge_ExactMultipleOfPerPageEndsOnAnEmptyPage(t *testing.T) {
	srv := newTestServer(&mockDAO{rows: []dao.Row{
		row("repo", `{"id":9100101}`), row("repo", `{"id":9100102}`),
		row("repo", `{"id":9100103}`), row("repo", `{"id":9100104}`),
	}})
	defer srv.Close()

	pages := 0
	next := "1"
	total := 0
	for next != "" {
		pages++
		require.LessOrEqual(t, pages, 10, "X-Next-Page must eventually clear")
		resp, body := getJSON(t, srv.URL+"/gitlab/api/v4/groups/91001/projects?per_page=2&page="+next)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		total += len(decodeArray(t, body))
		next = resp.Header.Get("X-Next-Page")
	}
	assert.Equal(t, 4, total)
	assert.Equal(t, 3, pages, "two full pages, then one empty page with no X-Next-Page")
}

// TestEdge_PerPageOutOfRangeFallsBackOrClamps covers zero, negative, garbage and
// oversized per_page and page values.
func TestEdge_PerPageOutOfRangeFallsBackOrClamps(t *testing.T) {
	tests := []struct {
		query           string
		wantPage, wantN int
	}{
		{"", 1, 20},
		{"page=0&per_page=0", 1, 20},
		{"page=-3&per_page=-5", 1, 20},
		{"page=abc&per_page=1e3", 1, 20},
		{"page=2&per_page=1000", 2, 100},
		{"page=2&per_page=100", 2, 100},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/x?"+tt.query, nil)
			page, perPage := parsePagination(r)
			assert.Equal(t, tt.wantPage, page)
			assert.Equal(t, tt.wantN, perPage)
		})
	}
}

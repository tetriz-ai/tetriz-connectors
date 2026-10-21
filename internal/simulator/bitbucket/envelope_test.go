// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestParsePagination(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantPage    int
		wantPagelen int
	}{
		{name: "defaults", query: "", wantPage: 1, wantPagelen: 50},
		{name: "both set", query: "?page=3&pagelen=10", wantPage: 3, wantPagelen: 10},
		{name: "zero falls back", query: "?page=0&pagelen=0", wantPage: 1, wantPagelen: 50},
		{name: "negative falls back", query: "?page=-2&pagelen=-1", wantPage: 1, wantPagelen: 50},
		{name: "garbage falls back", query: "?page=abc&pagelen=xyz", wantPage: 1, wantPagelen: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/x"+tt.query, nil)
			page, pagelen := parsePagination(r)
			if page != tt.wantPage || pagelen != tt.wantPagelen {
				t.Errorf("page,pagelen = %d,%d want %d,%d", page, pagelen, tt.wantPage, tt.wantPagelen)
			}
		})
	}
}

func TestWindowSlice(t *testing.T) {
	vals := rawSlice("a", "b", "c", "d", "e")
	tests := []struct {
		name    string
		page    int
		pagelen int
		want    []string
	}{
		{name: "first full page", page: 1, pagelen: 2, want: []string{"a", "b"}},
		{name: "second page offset", page: 2, pagelen: 2, want: []string{"c", "d"}},
		{name: "last partial page", page: 3, pagelen: 2, want: []string{"e"}},
		{name: "past the end is empty", page: 4, pagelen: 2, want: []string{}},
		{name: "pagelen larger than data", page: 1, pagelen: 50, want: []string{"a", "b", "c", "d", "e"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := windowSlice(vals, tt.page, tt.pagelen)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				var s string
				_ = json.Unmarshal(got[i], &s)
				if s != tt.want[i] {
					t.Errorf("[%d] = %q, want %q", i, s, tt.want[i])
				}
			}
		})
	}
}

func TestWriteEnvelope_NextOnlyOnFullPage(t *testing.T) {
	tests := []struct {
		name     string
		values   []json.RawMessage
		pagelen  int
		wantNext bool
		wantSize int
	}{
		{name: "full page has next", values: rawSlice("a", "b"), pagelen: 2, wantNext: true, wantSize: 2},
		{name: "partial page has no next", values: rawSlice("a"), pagelen: 2, wantNext: false, wantSize: 1},
		{name: "empty page has no next", values: rawSlice(), pagelen: 2, wantNext: false, wantSize: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/x?page=1&pagelen="+strconv.Itoa(tt.pagelen), nil)
			writeEnvelope(w, r, tt.values, 1, tt.pagelen)

			var env struct {
				Pagelen int               `json:"pagelen"`
				Values  []json.RawMessage `json:"values"`
				Page    int               `json:"page"`
				Size    int               `json:"size"`
				Next    string            `json:"next"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if env.Size != tt.wantSize {
				t.Errorf("size = %d, want %d", env.Size, tt.wantSize)
			}
			if (env.Next != "") != tt.wantNext {
				t.Errorf("next = %q, wantPresent = %v", env.Next, tt.wantNext)
			}
			if tt.wantNext && !strings.Contains(env.Next, "page=2") {
				t.Errorf("next = %q, want page=2", env.Next)
			}
		})
	}
}

// --- helpers ---

func rawSlice(ss ...string) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(ss))
	for _, s := range ss {
		b, _ := json.Marshal(s)
		out = append(out, b)
	}
	return out
}

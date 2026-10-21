// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package fakevendor is a scriptable vendor API and a connector that pulls
// from it. Tests drive the runner through the scenarios a real vendor
// produces (pagination, rate limits, server errors, dead credentials, shape
// changes, duplicates, slow pages) with no network and no account.
//
// The API is one endpoint:
//
//	GET /items?updated_since=<RFC 3339>&page_token=<token>&limit=<n>
//
// It returns items with updated_at >= updated_since, oldest first, as
// {"items":[...],"next_page_token":"..."}; an empty token ends the listing.
package fakevendor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Item is one vendor record.
type Item struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Fault is what the server does instead of answering one call.
type Fault struct {
	// Status, when non-zero, is returned with Body instead of a page.
	Status int
	Body   string
	// RetryAfter sets the Retry-After header, in seconds.
	RetryAfter int
	// Delay holds the response back this long.
	Delay time.Duration
	// StuckToken returns the requested page token as the next token.
	StuckToken bool
	// DropID removes "id" from every item on the page.
	DropID bool
	// Duplicate repeats the page's first item at its end.
	Duplicate bool
	// Before runs before the call reads the items, for example to change an
	// item between two pages of one listing.
	Before func()
}

// Server is the fake vendor. Its zero value is not usable; call New.
type Server struct {
	*httptest.Server

	mu     sync.Mutex
	items  []Item
	faults map[int]Fault
	sticky *Fault
	calls  int
	tokens []string
}

// New starts a server holding items.
func New(items ...Item) *Server {
	s := &Server{faults: map[int]Fault{}}
	s.SetItems(items...)
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// SetItems replaces the server's items.
func (s *Server) SetItems(items ...Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append([]Item(nil), items...)
	sort.SliceStable(s.items, func(i, j int) bool { return s.items[i].UpdatedAt.Before(s.items[j].UpdatedAt) })
}

// FailCall makes the nth call (1-based, counting from now) return f.
func (s *Server) FailCall(n int, f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults[s.calls+n] = f
}

// SetFault makes every call from now on wait delay (or until the request is
// abandoned) and then, when status is non-zero, answer status, with a
// Retry-After of retryAfter seconds when retryAfter > 0. A fault FailCall
// set for a call takes precedence. SetFault(0, 0, 0) restores normal
// answers. It implements testkit.FaultInjector, so the conformance suite can
// make this vendor fail.
func (s *Server) SetFault(status, retryAfter int, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if status == 0 && delay == 0 {
		s.sticky = nil
		return
	}
	s.sticky = &Fault{Status: status, RetryAfter: retryAfter, Delay: delay, Body: http.StatusText(status)}
}

// Calls reports how many requests the server has answered.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// AuthHeaders returns the Authorization header of every request, in order.
func (s *Server) AuthHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tokens...)
}

type page struct {
	Items         []map[string]any `json:"items"`
	NextPageToken string           `json:"next_page_token"`
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	f, ok := s.faults[s.calls]
	if !ok && s.sticky != nil {
		f = *s.sticky
	}
	s.tokens = append(s.tokens, r.Header.Get("Authorization"))
	s.mu.Unlock()
	if f.Before != nil {
		f.Before()
	}
	s.mu.Lock()
	items := append([]Item(nil), s.items...)
	s.mu.Unlock()

	if f.Delay > 0 {
		select {
		case <-time.After(f.Delay):
		case <-r.Context().Done():
			return
		}
	}
	if f.Status != 0 {
		if f.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(f.RetryAfter))
		}
		http.Error(w, f.Body, f.Status)
		return
	}
	if r.URL.Path != "/items" {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	var since time.Time
	if v := q.Get("updated_since"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			http.Error(w, "bad updated_since", http.StatusBadRequest)
			return
		}
		since = t
	}
	limit := 2
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			http.Error(w, "bad limit", http.StatusBadRequest)
			return
		}
		limit = n
	}
	// The page token is the (updated_at, id) of the last item served, so a
	// page continues after that item in (updated_at, id) order. An item
	// updated mid-listing moves to the end of the listing rather than
	// shifting the items after it.
	var after *Item
	if v := q.Get("page_token"); v != "" {
		ts, id, ok := strings.Cut(v, "|")
		t, err := time.Parse(time.RFC3339Nano, ts)
		if !ok || err != nil || id == "" {
			http.Error(w, "bad page_token", http.StatusBadRequest)
			return
		}
		after = &Item{ID: id, UpdatedAt: t}
	}
	sort.SliceStable(items, func(i, j int) bool { return itemLess(items[i], items[j]) })
	var matched []Item
	for _, it := range items {
		if it.UpdatedAt.Before(since) {
			continue
		}
		if after != nil && !itemLess(*after, it) {
			continue
		}
		matched = append(matched, it)
	}
	end := limit
	if end > len(matched) {
		end = len(matched)
	}
	var out page
	for _, it := range matched[:end] {
		m := map[string]any{"id": it.ID, "title": it.Title, "updated_at": it.UpdatedAt.Format(time.RFC3339Nano)}
		if f.DropID {
			delete(m, "id")
		}
		out.Items = append(out.Items, m)
	}
	if f.Duplicate && len(out.Items) > 0 {
		out.Items = append(out.Items, out.Items[0])
	}
	if end < len(matched) {
		last := matched[end-1]
		out.NextPageToken = last.UpdatedAt.Format(time.RFC3339Nano) + "|" + last.ID
	}
	if f.StuckToken {
		out.NextPageToken = q.Get("page_token")
		if out.NextPageToken == "" {
			out.NextPageToken = "stuck"
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		panic(fmt.Sprintf("fakevendor: encode page: %v", err))
	}
}

// itemLess orders items by (updated_at, id), the listing's keyset order.
func itemLess(a, b Item) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.Before(b.UpdatedAt)
	}
	return a.ID < b.ID
}

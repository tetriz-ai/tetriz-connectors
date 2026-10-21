// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package memory implements the auth token cache and lock in process, for
// tests and single-process runners. One Store serves as both
// auth.RefreshDeps.Cache and auth.RefreshDeps.Locker; locks then exclude only
// within this process.
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth"
)

type entry struct {
	value   string
	expires time.Time
}

// sweepInterval bounds how often a write also drops every expired entry, so
// entries nobody reads again do not stay in memory.
const sweepInterval = time.Minute

// Store is both an auth.TokenCache and an auth.Locker. The zero value is
// ready to use. Now, when set, replaces the wall clock. Expired entries are
// dropped when read and by a sweep, at most once a minute, on write.
type Store struct {
	mu        sync.Mutex
	entries   map[string]entry
	seq       uint64
	lastSweep time.Time
	Now       func() time.Time
}

var (
	_ auth.TokenCache = (*Store)(nil)
	_ auth.Locker     = (*Store)(nil)
)

// New returns an empty Store on the wall clock, the same as &Store{}.
func New() *Store { return &Store{} }

// Len is the number of entries held, expired ones not yet dropped included.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// write prepares the map for a write and sweeps expired entries when due.
func (s *Store) write() {
	if s.entries == nil {
		s.entries = map[string]entry{}
	}
	now := s.now()
	if now.Sub(s.lastSweep) < sweepInterval {
		return
	}
	for k, e := range s.entries {
		if !e.expires.IsZero() && !now.Before(e.expires) {
			delete(s.entries, k)
		}
	}
	s.lastSweep = now
}

func (s *Store) live(key string) (entry, bool) {
	e, ok := s.entries[key]
	if !ok {
		return entry{}, false
	}
	if !e.expires.IsZero() && !s.now().Before(e.expires) {
		delete(s.entries, key)
		return entry{}, false
	}
	return e, true
}

// Get implements auth.TokenCache.
func (s *Store) Get(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.live(key)
	if !ok {
		return "", auth.ErrCacheMiss
	}
	return e.value, nil
}

// Set implements auth.TokenCache. A ttl of zero never expires.
func (s *Store) Set(_ context.Context, key, value string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.write()
	e := entry{value: value}
	if ttl > 0 {
		e.expires = s.now().Add(ttl)
	}
	s.entries[key] = e
	return nil
}

// Delete implements auth.TokenCache.
func (s *Store) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

// TryLock implements auth.Locker. A ttl of zero never expires, as with
// Redis SET NX.
func (s *Store) TryLock(_ context.Context, key string, ttl time.Duration) (auth.Lock, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.live(key); held {
		return nil, false, nil
	}
	s.write()
	s.seq++
	token := time.Duration(s.seq).String()
	e := entry{value: token}
	if ttl > 0 {
		e.expires = s.now().Add(ttl)
	}
	s.entries[key] = e
	return &lock{s: s, key: key, token: token}, true, nil
}

type lock struct {
	s     *Store
	key   string
	token string
}

func (l *lock) Unlock(_ context.Context) error {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	if e, ok := l.s.live(l.key); ok && e.value == l.token {
		delete(l.s.entries, l.key)
	}
	return nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scope: the simulator serves GitHub, GitLab, Bitbucket and Jira, the
// vendors the quick start and the acceptance sync run. The other connectors'
// tests use testkit/fakevendor or their own fixtures (internal/simulator/README.md, Scope).
func TestSimulatorScope_ServesFourVendors(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "host=127.0.0.1 port=1 dbname=unused sslmode=disable")
	require.NoError(t, err) // the pool connects lazily; no route is called
	t.Cleanup(pool.Close)
	router, ok := Handler(pool).(*mux.Router)
	require.True(t, ok)

	vendors := map[string]bool{}
	require.NoError(t, router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		tpl, err := route.GetPathTemplate()
		if err != nil {
			return nil //nolint:nilerr // a route without a path template names no vendor
		}
		if first := strings.SplitN(strings.TrimPrefix(tpl, "/"), "/", 2)[0]; first != "" && first != "_health" {
			vendors[first] = true
		}
		return nil
	}))
	var got []string
	for v := range vendors {
		got = append(got, v)
	}
	sort.Strings(got)
	assert.Equal(t, []string{"bitbucket", "github", "gitlab", "jira"}, got)
}

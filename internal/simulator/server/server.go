// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package server runs the vendor simulator: an HTTP service that answers the
// GitHub, GitLab, Bitbucket and Jira API calls the connectors make, from
// seeded rows in its own Postgres database.
package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/tetriz-ai/tetriz-connectors/internal/migrate"
	simbitbucket "github.com/tetriz-ai/tetriz-connectors/internal/simulator/bitbucket"
	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/dao"
	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/demoseed"
	simgithub "github.com/tetriz-ai/tetriz-connectors/internal/simulator/github"
	simgitlab "github.com/tetriz-ai/tetriz-connectors/internal/simulator/gitlab"
	simjira "github.com/tetriz-ai/tetriz-connectors/internal/simulator/jira"
	"github.com/tetriz-ai/tetriz-connectors/internal/simulator/migrations"
)

// Migrate applies the simulator's migrations to dsn.
func Migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("simulator: open db: %w", err)
	}
	defer db.Close() //nolint:errcheck // best-effort close after migrating
	if _, err := migrate.UpTable(ctx, db, migrations.FS, "simulator_schema_migrations"); err != nil {
		return fmt.Errorf("simulator: %w", err)
	}
	return nil
}

// Seed writes the demo catalog's vendor data. It is safe to run repeatedly.
func Seed(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("simulator: connect db: %w", err)
	}
	defer pool.Close()
	if err := demoseed.SeedAll(ctx, pool); err != nil {
		return fmt.Errorf("simulator: seed: %w", err)
	}
	return nil
}

// Handler returns the simulator's HTTP routes over pool.
func Handler(pool *pgxpool.Pool) http.Handler {
	simDAO := dao.New(pool)
	r := mux.NewRouter()
	simgithub.New(simDAO).RegisterRoutes(r)
	simbitbucket.New(simDAO).RegisterRoutes(r)
	simgitlab.New(simDAO).RegisterRoutes(r)
	simjira.New(simDAO).RegisterRoutes(r)
	r.HandleFunc("/_health", func(w http.ResponseWriter, req *http.Request) {
		if err := pool.Ping(req.Context()); err != nil {
			http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	return r
}

// Serve listens on addr until ctx is cancelled.
func Serve(ctx context.Context, dsn, addr string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("simulator: connect db: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("simulator: ping db: %w", err)
	}
	srv := &http.Server{Addr: addr, Handler: Handler(pool), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("simulator: serve: %w", err)
	}
}

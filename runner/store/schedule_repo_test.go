// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// testDB is the shared connection pool, initialised once in TestMain.
var testDB *gorm.DB

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("connector dao", err)
		os.Exit(0)
	}

	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connector dao test schema load failed: %v\n", err)
		os.Exit(1)
	}

	testDB = db
	code := testguard.Run(m)
	cleanup()
	os.Exit(code)
}

// newTestTx begins a transaction that is rolled back at test cleanup,
// giving each test full isolation without leaving data behind.
func newTestTx(t *testing.T) *gorm.DB {
	t.Helper()

	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin transaction: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

// seedOverride inserts a ScheduleOverride via the repo and returns it.
func seedOverride(t *testing.T, repo *store.ScheduleRepo, orgID string, connID sdk.ConnectorID, opID sdk.OperationID, cron string, enabled bool) *model.ScheduleOverride {
	t.Helper()
	o := &model.ScheduleOverride{
		OrganizationID: orgID,
		ConnectorID:    connID,
		OperationID:    opID,
		CronExpression: cron,
		Enabled:        enabled,
	}
	if err := repo.Upsert(context.Background(), o); err != nil {
		t.Fatalf("seed override: %v", err)
	}
	return o
}

func TestUpsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo)
	}{
		{
			name: "insert new override",
			run: func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo) {
				o := &model.ScheduleOverride{
					OrganizationID: "org-new",
					ConnectorID:    "github",
					OperationID:    "fetch_prs",
					CronExpression: "*/5 * * * *",
					Enabled:        true,
				}
				if err := repo.Upsert(context.Background(), o); err != nil {
					t.Fatalf("Upsert() insert error: %v", err)
				}
				if o.CreatedAt.IsZero() {
					t.Error("expected CreatedAt to be set")
				}
				if o.UpdatedAt.IsZero() {
					t.Error("expected UpdatedAt to be set")
				}
			},
		},
		{
			name: "update existing override on conflict",
			run: func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo) {
				seedOverride(t, repo, "org-upsert", "github", "fetch_prs", "*/5 * * * *", true)

				updated := &model.ScheduleOverride{
					OrganizationID: "org-upsert",
					ConnectorID:    "github",
					OperationID:    "fetch_prs",
					CronExpression: "0 */2 * * *",
					Enabled:        false,
				}
				if err := repo.Upsert(context.Background(), updated); err != nil {
					t.Fatalf("Upsert() update error: %v", err)
				}

				// Verify the row was updated, not duplicated.
				var count int64
				db.Raw(
					"SELECT count(*) FROM connector_schedules WHERE organization_id = ? AND connector_id = ? AND operation_id = ?",
					"org-upsert", "github", "fetch_prs",
				).Scan(&count)
				if count != 1 {
					t.Errorf("expected 1 row after upsert, got %d", count)
				}

				var row model.ScheduleOverride
				db.Where("organization_id = ? AND connector_id = ? AND operation_id = ?",
					"org-upsert", "github", "fetch_prs",
				).First(&row)
				if row.CronExpression != "0 */2 * * *" {
					t.Errorf("cron_expression = %q, want %q", row.CronExpression, "0 */2 * * *")
				}
				if row.Enabled != false {
					t.Error("expected enabled = false after upsert")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			repo := store.NewScheduleRepo(tx)
			tt.run(t, tx, repo)
		})
	}
}

func TestUpdateRunTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo)
	}{
		{
			name: "updates last_run_at and next_run_at",
			run: func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo) {
				seedOverride(t, repo, "org-ts", "github", "fetch_prs", "*/5 * * * *", true)

				lastRun := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
				nextRun := time.Date(2026, 2, 18, 10, 5, 0, 0, time.UTC)

				err := repo.UpdateRunTimestamps(
					context.Background(),
					"org-ts", "github", "fetch_prs",
					lastRun, nextRun,
				)
				if err != nil {
					t.Fatalf("UpdateRunTimestamps() error: %v", err)
				}

				var row model.ScheduleOverride
				db.Where("organization_id = ? AND connector_id = ? AND operation_id = ?",
					"org-ts", "github", "fetch_prs",
				).First(&row)

				if row.LastRunAt == nil || row.LastRunAt.UTC().Truncate(time.Second) != lastRun.Truncate(time.Second) {
					t.Errorf("last_run_at = %v, want %v", row.LastRunAt, lastRun)
				}
				if row.NextRunAt == nil || row.NextRunAt.UTC().Truncate(time.Second) != nextRun.Truncate(time.Second) {
					t.Errorf("next_run_at = %v, want %v", row.NextRunAt, nextRun)
				}
			},
		},
		{
			name: "no-op when row does not exist",
			run: func(t *testing.T, db *gorm.DB, repo *store.ScheduleRepo) {
				err := repo.UpdateRunTimestamps(
					context.Background(),
					"org-nonexistent", "github", "fetch_prs",
					time.Now(), time.Now(),
				)
				if err != nil {
					t.Fatalf("UpdateRunTimestamps() on missing row error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			repo := store.NewScheduleRepo(tx)
			tt.run(t, tx, repo)
		})
	}
}

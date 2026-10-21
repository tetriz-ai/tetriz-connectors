// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func newSyncRun(orgID string, connID sdk.ConnectorID, opID sdk.OperationID, status model.SyncRunStatus, startedAt time.Time) *model.SyncRun {
	run := &model.SyncRun{
		ID:             uuid.NewString(),
		OrganizationID: orgID,
		ConnectorID:    connID,
		OperationID:    opID,
		Status:         status,
		StartedAt:      startedAt,
		Trigger:        "test",
	}
	if status == model.SyncRunStatusCompleted || status == model.SyncRunStatusFailed {
		completedAt := startedAt.Add(10 * time.Second)
		run.CompletedAt = &completedAt
	}
	return run
}

func seedSyncRun(t *testing.T, db *gorm.DB, run *model.SyncRun) {
	t.Helper()
	d := store.NewSyncRunRepo(db)
	require.NoError(t, d.Create(context.Background(), run))
}

func TestSyncRunRepo_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		build    func(t *testing.T) *model.SyncRun
		validate func(t *testing.T, tx *gorm.DB, d store.ISyncRunDAO, run *model.SyncRun)
	}{
		{
			name: "inserts a new sync run",
			build: func(t *testing.T) *model.SyncRun {
				t.Helper()
				return newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusRunning, time.Now().UTC())
			},
			validate: func(t *testing.T, tx *gorm.DB, d store.ISyncRunDAO, run *model.SyncRun) {
				t.Helper()
				var count int64
				tx.Raw("SELECT count(*) FROM connector_sync_runs WHERE id = ?", run.ID).Scan(&count)
				assert.Equal(t, int64(1), count)
			},
		},
		{
			name: "stores cursor_before as JSONB",
			build: func(t *testing.T) *model.SyncRun {
				t.Helper()
				cursor := sdk.NewTimestampCursor(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
				cursorJSON, err := json.Marshal(cursor)
				require.NoError(t, err)

				run := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusRunning, time.Now().UTC())
				run.CursorBefore = cursorJSON
				return run
			},
			validate: func(t *testing.T, tx *gorm.DB, d store.ISyncRunDAO, run *model.SyncRun) {
				t.Helper()
				got, err := d.GetLatest(context.Background(), "org-1", "github", "fetch_prs")
				require.NoError(t, err)
				assert.JSONEq(t, string(run.CursorBefore), string(got.CursorBefore))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			d := store.NewSyncRunRepo(tx)

			run := tt.build(t)
			require.NoError(t, d.Create(context.Background(), run))
			tt.validate(t, tx, d, run)
		})
	}
}

func TestSyncRunRepo_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(t *testing.T, run *model.SyncRun)
		validate func(t *testing.T, got *model.SyncRun)
	}{
		{
			name: "updates status and cursor_after on completion",
			mutate: func(t *testing.T, run *model.SyncRun) {
				t.Helper()
				completedAt := time.Now().UTC()
				run.Status = model.SyncRunStatusCompleted
				run.CompletedAt = &completedAt

				newCursor := sdk.NewTimestampCursor(time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC))
				cursorAfterJSON, err := json.Marshal(newCursor)
				require.NoError(t, err)
				run.CursorAfter = cursorAfterJSON
			},
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				assert.Equal(t, model.SyncRunStatusCompleted, got.Status)
				assert.NotNil(t, got.CompletedAt)

				// Compare only the cursor payload; Cursor.UpdatedAt is wall-clock
				// stamped at NewTimestampCursor time, so rebuilding an "expected"
				// cursor here would drift from the one persisted in mutate().
				var gotCursor sdk.Cursor
				require.NoError(t, json.Unmarshal(got.CursorAfter, &gotCursor))
				gotTS, err := sdk.TimestampFromCursor(&gotCursor)
				require.NoError(t, err)
				assert.Equal(t, time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC), gotTS.UTC())
			},
		},
		{
			name: "updates error fields on failure",
			mutate: func(t *testing.T, run *model.SyncRun) {
				t.Helper()
				completedAt := time.Now().UTC()
				run.Status = model.SyncRunStatusFailed
				run.CompletedAt = &completedAt
				run.ErrorKind = "vendor_error"
				run.ErrorMessage = "500 internal server error"
			},
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				assert.Equal(t, model.SyncRunStatusFailed, got.Status)
				assert.Equal(t, "vendor_error", got.ErrorKind)
				assert.Equal(t, "500 internal server error", got.ErrorMessage)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			d := store.NewSyncRunRepo(tx)

			run := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusRunning, time.Now().UTC())
			require.NoError(t, d.Create(context.Background(), run))

			tt.mutate(t, run)
			require.NoError(t, d.Update(context.Background(), run))

			got, err := d.GetLatest(context.Background(), "org-1", "github", "fetch_prs")
			require.NoError(t, err)
			tt.validate(t, got)
		})
	}
}

func TestSyncRunRepo_GetLatest(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		seed     func(t *testing.T, tx *gorm.DB)
		orgID    string
		connID   sdk.ConnectorID
		opID     sdk.OperationID
		wantErr  error
		validate func(t *testing.T, got *model.SyncRun)
	}{
		{
			name:    "returns ErrSyncRunNotFound when no runs exist",
			seed:    func(t *testing.T, tx *gorm.DB) { t.Helper() },
			orgID:   "org-1",
			connID:  "github",
			opID:    "fetch_prs",
			wantErr: store.ErrSyncRunNotFound,
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				assert.Nil(t, got)
			},
		},
		{
			name: "returns the most recent run by started_at",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base.Add(time.Hour)))
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				require.NotNil(t, got)
				assert.Equal(t, base.Add(time.Hour).Truncate(time.Second), got.StartedAt.UTC().Truncate(time.Second))
			},
		},
		{
			name: "scopes by org, connector, and operation",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				// Same org+connector, different operation.
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_commits", model.SyncRunStatusCompleted, base.Add(time.Hour)))
				// Different org, same connector+operation.
				seedSyncRun(t, tx, newSyncRun("org-2", "github", "fetch_prs", model.SyncRunStatusCompleted, base.Add(2*time.Hour)))
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				require.NotNil(t, got)
				assert.Equal(t, base.Truncate(time.Second), got.StartedAt.UTC().Truncate(time.Second),
					"should return only the run matching org+connector+operation")
			},
		},
		{
			name: "returns failed run as latest when it is most recent",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusFailed, base.Add(time.Hour)))
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			validate: func(t *testing.T, got *model.SyncRun) {
				t.Helper()
				require.NotNil(t, got)
				assert.Equal(t, model.SyncRunStatusFailed, got.Status)
				assert.Equal(t, base.Add(time.Hour).Truncate(time.Second), got.StartedAt.UTC().Truncate(time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			tt.seed(t, tx)

			d := store.NewSyncRunRepo(tx)
			got, err := d.GetLatest(context.Background(), tt.orgID, tt.connID, tt.opID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			tt.validate(t, got)
		})
	}
}

func TestSyncRunRepo_GetConnectorSyncStatusByOrg(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		seed     func(t *testing.T, tx *gorm.DB)
		orgID    string
		validate func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus)
	}{
		{
			name:  "returns an empty map when no runs exist",
			seed:  func(t *testing.T, tx *gorm.DB) { t.Helper() },
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				assert.Empty(t, got)
			},
		},
		{
			name: "a completed run with records emitted counts as synced",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				run := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base)
				run.RecordsEmitted = 5
				seedSyncRun(t, tx, run)
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["github"]
				require.True(t, ok)
				require.NotNil(t, st.LastSuccessfulAt)
				assert.Equal(t, base.Add(10*time.Second).Truncate(time.Second), st.LastSuccessfulAt.UTC().Truncate(time.Second))
				assert.Equal(t, store.ConnectorSyncStateSynced, st.State)
			},
		},
		{
			name: "a completed run with zero records emitted still counts as synced — idle-but-healthy, not stalled",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				run := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base)
				run.RecordsEmitted = 0
				seedSyncRun(t, tx, run)
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["github"]
				require.True(t, ok)
				require.NotNil(t, st.LastSuccessfulAt, "a genuine vendor failure is marked Failed by the framework — a Completed run means the vendor answered, even with nothing new")
				assert.Equal(t, base.Add(10*time.Second).Truncate(time.Second), st.LastSuccessfulAt.UTC().Truncate(time.Second))
				assert.Equal(t, store.ConnectorSyncStateSynced, st.State)
			},
		},
		{
			name: "a connector with only a running run is still on its first sync",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "gitlab", "fetch_prs", model.SyncRunStatusRunning, base))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["gitlab"]
				require.True(t, ok)
				assert.Nil(t, st.LastSuccessfulAt)
				assert.Equal(t, store.ConnectorSyncStateStillSyncing, st.State)
			},
		},
		{
			name: "a connector with only failed runs is still on its first sync, reported as FAILED not STILL_SYNCING",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "jira", "fetch_issues", model.SyncRunStatusFailed, base))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["jira"]
				require.True(t, ok)
				assert.Nil(t, st.LastSuccessfulAt)
				assert.Equal(t, store.ConnectorSyncStateFailed, st.State, "a genuine failure is a distinct state from a first sync in progress")
			},
		},
		{
			name: "aggregates the MAX successful completion across a connector's operations",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				prs := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base)
				prs.RecordsEmitted = 3
				commits := newSyncRun("org-1", "github", "fetch_commits", model.SyncRunStatusCompleted, base.Add(2*time.Hour))
				commits.RecordsEmitted = 7
				seedSyncRun(t, tx, prs)
				seedSyncRun(t, tx, commits)
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["github"]
				require.True(t, ok)
				require.NotNil(t, st.LastSuccessfulAt)
				assert.Equal(t, base.Add(2*time.Hour).Add(10*time.Second).Truncate(time.Second),
					st.LastSuccessfulAt.UTC().Truncate(time.Second), "must take the LATEST successful op, not the first")
				assert.Equal(t, store.ConnectorSyncStateSynced, st.State)
			},
		},
		{
			name: "scopes by org",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				ours := newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base)
				ours.RecordsEmitted = 1
				theirs := newSyncRun("org-2", "github", "fetch_prs", model.SyncRunStatusCompleted, base)
				theirs.RecordsEmitted = 1
				seedSyncRun(t, tx, ours)
				seedSyncRun(t, tx, theirs)
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				assert.Len(t, got, 1)
			},
		},
		{
			name: "a VCS connector's completed metadata-only run (fetch_repos) does not count as synced — and is filtered out entirely, same as no history",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_repos", model.SyncRunStatusCompleted, base))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				_, ok := got["github"]
				assert.False(t, ok, "a repo listing succeeding is not evidence the PR/commit data is fresh — the map treats this the same as no history at all")
			},
		},
		{
			name: "a VCS connector's failing data op with a completed metadata op alongside it reads FAILED, not synced",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_repos", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusFailed, base))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["github"]
				require.True(t, ok)
				assert.Nil(t, st.LastSuccessfulAt)
				assert.Equal(t, store.ConnectorSyncStateFailed, st.State, "fetch_repos succeeding must not mask fetch_prs failing")
			},
		},
		{
			name: "a VCS data op that failed and later completed reads SYNCED — the latest attempt wins, not the stale failure",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusFailed, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base.Add(time.Hour)))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["github"]
				require.True(t, ok)
				require.NotNil(t, st.LastSuccessfulAt)
				assert.Equal(t, store.ConnectorSyncStateSynced, st.State)
			},
		},
		{
			name: "a non-VCS connector's operation set is not narrowed — an op absent from vcsDataOperationIDs still counts",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				// "fetch_comments" is a genuine Jira data op but is not in
				// vcsDataOperationIDs — narrowing is scoped to VCS connectors only.
				seedSyncRun(t, tx, newSyncRun("org-1", "jira", "fetch_comments", model.SyncRunStatusCompleted, base))
			},
			orgID: "org-1",
			validate: func(t *testing.T, got map[sdk.ConnectorID]store.ConnectorSyncStatus) {
				t.Helper()
				st, ok := got["jira"]
				require.True(t, ok)
				require.NotNil(t, st.LastSuccessfulAt)
				assert.Equal(t, store.ConnectorSyncStateSynced, st.State)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			tt.seed(t, tx)

			d := store.NewSyncRunRepo(tx)
			got, err := d.GetConnectorSyncStatusByOrg(context.Background(), tt.orgID)
			require.NoError(t, err)
			tt.validate(t, got)
		})
	}
}

func TestSyncRunRepo_ListByOperation(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		seed     func(t *testing.T, tx *gorm.DB)
		orgID    string
		connID   sdk.ConnectorID
		opID     sdk.OperationID
		limit    int
		validate func(t *testing.T, got []*model.SyncRun)
	}{
		{
			name:   "returns empty slice when no runs exist",
			seed:   func(t *testing.T, tx *gorm.DB) { t.Helper() },
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			limit:  10,
			validate: func(t *testing.T, got []*model.SyncRun) {
				t.Helper()
				assert.Empty(t, got)
			},
		},
		{
			name: "returns runs ordered by started_at DESC",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base.Add(time.Hour)))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusFailed, base.Add(2*time.Hour)))
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			limit:  10,
			validate: func(t *testing.T, got []*model.SyncRun) {
				t.Helper()
				require.Len(t, got, 3)
				assert.True(t, got[0].StartedAt.After(got[1].StartedAt), "newest first")
				assert.True(t, got[1].StartedAt.After(got[2].StartedAt), "oldest last")
			},
		},
		{
			name: "respects limit",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				for i := range 5 {
					seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base.Add(time.Duration(i)*time.Hour)))
				}
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			limit:  3,
			validate: func(t *testing.T, got []*model.SyncRun) {
				t.Helper()
				assert.Len(t, got, 3)
			},
		},
		{
			name: "scopes by org, connector, and operation",
			seed: func(t *testing.T, tx *gorm.DB) {
				t.Helper()
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-1", "github", "fetch_commits", model.SyncRunStatusCompleted, base))
				seedSyncRun(t, tx, newSyncRun("org-2", "github", "fetch_prs", model.SyncRunStatusCompleted, base))
			},
			orgID:  "org-1",
			connID: "github",
			opID:   "fetch_prs",
			limit:  10,
			validate: func(t *testing.T, got []*model.SyncRun) {
				t.Helper()
				assert.Len(t, got, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx := newTestTx(t)
			tt.seed(t, tx)

			d := store.NewSyncRunRepo(tx)
			got, err := d.ListByOperation(context.Background(), tt.orgID, tt.connID, tt.opID, tt.limit)
			require.NoError(t, err)
			tt.validate(t, got)
		})
	}
}

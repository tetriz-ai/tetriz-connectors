// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// SyncRunRepo implements ISyncRunDAO using PostgreSQL via GORM.
type SyncRunRepo struct {
	db *gorm.DB
}

// NewSyncRunRepo creates a new SyncRunRepo.
func NewSyncRunRepo(db *gorm.DB) *SyncRunRepo {
	return &SyncRunRepo{db: db}
}

func (r *SyncRunRepo) Create(ctx context.Context, run *model.SyncRun) error {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "Create", time.Now())
	db := tenantdb.DB(ctx, r.db)
	return db.Create(run).Error
}

// Update implements ISyncRunDAO: every column but the id and the lease, and
// only while the stored row is running.
func (r *SyncRunRepo) Update(ctx context.Context, run *model.SyncRun) error {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "Update", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Model(&model.SyncRun{}).
		Where("id = ? AND status = ? AND deleted_at IS NULL", run.ID, model.SyncRunStatusRunning).
		Select("*").Omit("id", "lease_expires_at").
		Updates(run)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrRunClosed
	}
	return nil
}

// Get returns one run of orgID by its id, or ErrSyncRunNotFound.
func (r *SyncRunRepo) Get(ctx context.Context, orgID, runID string) (*model.SyncRun, error) {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "Get", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var run model.SyncRun
	err := db.Where("id = ? AND organization_id = ? AND deleted_at IS NULL", runID, orgID).First(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSyncRunNotFound
		}
		return nil, err
	}
	return &run, nil
}

func (r *SyncRunRepo) GetLatest(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*model.SyncRun, error) {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "GetLatest", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var run model.SyncRun
	err := db.Where("organization_id = ? AND connector_id = ? AND operation_id = ? AND deleted_at IS NULL", orgID, connectorID, opID).
		Order("started_at DESC").
		First(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSyncRunNotFound
		}
		return nil, err
	}
	return &run, nil
}

// RenewLease implements ISyncRunDAO, on the database's clock.
func (r *SyncRunRepo) RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "RenewLease", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Exec(`UPDATE connector_sync_runs SET lease_expires_at = now() + make_interval(secs => ?)
WHERE id = ? AND organization_id = ? AND status = ? AND deleted_at IS NULL`,
		lease.Seconds(), runID, orgID, model.SyncRunStatusRunning)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrRunClosed
	}
	return nil
}

// CloseAbandoned implements ISyncRunDAO, on the database's clock.
func (r *SyncRunRepo) CloseAbandoned(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (int64, error) {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "CloseAbandoned", time.Now())
	db := tenantdb.DB(ctx, r.db)
	q := `UPDATE connector_sync_runs
SET status = ?, completed_at = now(), error_kind = ?,
    error_message = 'the run''s lease expired at ' || to_char(lease_expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || ': the process running it stopped without closing it'
WHERE status = ? AND lease_expires_at < now() AND deleted_at IS NULL`
	args := []any{model.SyncRunStatusFailed, sdk.ErrKindAbandoned.String(), model.SyncRunStatusRunning}
	if orgID != "" {
		q += " AND organization_id = ?"
		args = append(args, orgID)
	}
	if connectorID != "" {
		q += " AND connector_id = ?"
		args = append(args, connectorID)
	}
	if opID != "" {
		q += " AND operation_id = ?"
		args = append(args, opID)
	}
	res := db.Exec(q, args...)
	return res.RowsAffected, res.Error
}

func (r *SyncRunRepo) DeleteRunsByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "DeleteRunsByConnector", time.Now())
	db := tenantdb.DB(ctx, r.db)
	return db.Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
		Delete(&model.SyncRun{}).Error
}

// GetConnectorSyncStatusByOrg aggregates, per connector_id, the freshness
// signal derived from that connector's data-bearing operations for orgID
// (vcsDataOperationIDs). A run counts toward LastSuccessfulAt as soon as
// Status=Completed — a genuine vendor failure is marked Failed by the
// connector framework, so a Completed run is trustworthy regardless of
// records emitted.
func (r *SyncRunRepo) GetConnectorSyncStatusByOrg(ctx context.Context, orgID string) (map[sdk.ConnectorID]ConnectorSyncStatus, error) {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "GetConnectorSyncStatusByOrg", time.Now())
	return queryConnectorSyncStatusByOrg(tenantdb.DB(ctx, r.db), orgID) //nolint:wrapcheck // queryConnectorSyncStatusByOrg's own error already carries its cause; wrapping here would double-wrap
}

func (r *SyncRunRepo) ListByOperation(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, limit int) ([]*model.SyncRun, error) {
	defer metricutil.TrackDuration("runner/store", "SyncRunRepo", "ListByOperation", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var runs []*model.SyncRun
	err := db.Where("organization_id = ? AND connector_id = ? AND operation_id = ? AND deleted_at IS NULL", orgID, connectorID, opID).
		Order("started_at DESC").
		Limit(limit).
		Find(&runs).Error
	if err != nil {
		return nil, err
	}
	return runs, nil
}

// vcsDataOperationIDs is the operation set whose completion backs a VCS
// connector's (github/gitlab/bitbucket) sync-status signal — the ops that
// emit pull request, commit, issue and review records.
// Verified against each connector's own Operations() registry: all three
// vendors name these ops with the identical literal operation_id. Deliberately
// excludes metadata/discovery ops (fetch_repos, fetch_repo_tree,
// fetch_branches, fetch_config, org/workspace member fetches) and
// content-enrichment ops (fetch_pr_diffs/fetch_mr_diffs, fetch_prs_data/
// fetch_issues_data, fetch_pr_comments, fetch_copilot_usage, fetch_projects,
// webhook_events) — their success says a listing or an enrichment call
// worked, not that the connector's core activity data is fresh. Scoped to VCS
// connectors only (vendorids.VCSConnectorIDs): every other connector's full
// operation set still counts, unchanged, since narrowing it needs the same
// per-connector audit this comment records for VCS and has not been done.
var vcsDataOperationIDs = []string{"fetch_prs", "fetch_commits", "fetch_issues", "fetch_reviews"}

// queryConnectorSyncStatusByOrg is SyncRunRepo's sync-status query.
//
// filtered narrows a VCS connector's rows to vcsDataOperationIDs and leaves
// every other connector's rows untouched (its operation_id never collides
// with a VCS op — e.g. Jira and Linear also run a literal "fetch_issues", but
// their connector_id is never in vendorids.VCSConnectorIDs, so the OR's left arm
// passes their rows through unfiltered). last_successful_at is the historical
// high-water mark across those rows — it can stay set even once a later run
// fails, naming the last time the connector's data genuinely synced.
// has_failed_latest asks a narrower question: is the MOST RECENT attempt of
// any of those operations currently Failed — computed via latest_per_op
// (DISTINCT ON, one row per operation, its newest attempt) so a since-retried
// failure does not haunt the connector forever.
func queryConnectorSyncStatusByOrg(db *gorm.DB, orgID string) (map[sdk.ConnectorID]ConnectorSyncStatus, error) {
	var rows []struct {
		ConnectorID      sdk.ConnectorID
		LastSuccessfulAt *time.Time
		HasFailedLatest  bool
	}
	err := db.Raw(`
		WITH filtered AS (
			SELECT connector_id, operation_id, status, completed_at, started_at
			FROM connector_sync_runs
			WHERE organization_id = ?
				AND deleted_at IS NULL
				AND (connector_id NOT IN (?) OR operation_id IN (?))
		),
		latest_per_op AS (
			SELECT DISTINCT ON (connector_id, operation_id)
				connector_id, operation_id, status
			FROM filtered
			ORDER BY connector_id, operation_id, started_at DESC
		),
		success AS (
			SELECT connector_id, MAX(completed_at) FILTER (WHERE status = ?) AS last_successful_at
			FROM filtered
			GROUP BY connector_id
		),
		failing AS (
			SELECT connector_id, BOOL_OR(status = ?) AS has_failed_latest
			FROM latest_per_op
			GROUP BY connector_id
		)
		SELECT success.connector_id AS connector_id, success.last_successful_at, failing.has_failed_latest
		FROM success JOIN failing ON failing.connector_id = success.connector_id
	`, orgID, vendorids.VCSConnectorIDs, vcsDataOperationIDs, model.SyncRunStatusCompleted, model.SyncRunStatusFailed).
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("get connector sync status by org: %w", err)
	}
	out := make(map[sdk.ConnectorID]ConnectorSyncStatus, len(rows))
	for _, r := range rows {
		state := ConnectorSyncStateStillSyncing
		switch {
		case r.HasFailedLatest:
			state = ConnectorSyncStateFailed
		case r.LastSuccessfulAt != nil:
			state = ConnectorSyncStateSynced
		}
		out[r.ConnectorID] = ConnectorSyncStatus{LastSuccessfulAt: r.LastSuccessfulAt, State: state}
	}
	return out, nil
}

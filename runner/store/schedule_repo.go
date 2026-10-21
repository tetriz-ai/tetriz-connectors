// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ScheduleRepo implements IScheduleDAO using PostgreSQL via GORM.
type ScheduleRepo struct {
	db *gorm.DB
}

// NewScheduleRepo creates a new ScheduleRepo.
func NewScheduleRepo(db *gorm.DB) *ScheduleRepo {
	return &ScheduleRepo{db: db}
}

// ListAll returns every live schedule override, enabled or not.
func (r *ScheduleRepo) ListAll(ctx context.Context) ([]*model.ScheduleOverride, error) {
	defer metricutil.TrackDuration("runner/store", "ScheduleRepo", "ListAll", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var overrides []*model.ScheduleOverride
	if err := db.Where("deleted_at IS NULL").Find(&overrides).Error; err != nil {
		return nil, err
	}
	return overrides, nil
}

func (r *ScheduleRepo) Upsert(ctx context.Context, override *model.ScheduleOverride) error {
	defer metricutil.TrackDuration("runner/store", "ScheduleRepo", "Upsert", time.Now())
	db := tenantdb.DB(ctx, r.db)
	now := time.Now().UTC()
	override.UpdatedAt = now
	if override.CreatedAt.IsZero() {
		override.CreatedAt = now
	}
	// Use a map so GORM includes zero-value fields (e.g. Enabled=false)
	// in the INSERT. Without this, EXCLUDED.enabled would use the column
	// default (true) instead of the intended value.
	row := map[string]interface{}{
		"organization_id": override.OrganizationID,
		"connector_id":    override.ConnectorID,
		"operation_id":    override.OperationID,
		"cron_expression": override.CronExpression,
		"enabled":         override.Enabled,
		"created_at":      override.CreatedAt,
		"updated_at":      override.UpdatedAt,
	}
	return db.Model(&model.ScheduleOverride{}).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}, {Name: "operation_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"cron_expression", "enabled", "updated_at"}),
		}).Create(row).Error
}

func (r *ScheduleRepo) UpdateRunTimestamps(
	ctx context.Context,
	orgID string,
	connectorID sdk.ConnectorID,
	opID sdk.OperationID,
	lastRunAt time.Time,
	nextRunAt time.Time,
) error {
	defer metricutil.TrackDuration("runner/store", "ScheduleRepo", "UpdateRunTimestamps", time.Now())
	db := tenantdb.DB(ctx, r.db)
	return db.Model(&model.ScheduleOverride{}).
		Where("organization_id = ? AND connector_id = ? AND operation_id = ?", orgID, connectorID, opID).
		Updates(map[string]interface{}{
			"last_run_at": lastRunAt,
			"next_run_at": nextRunAt,
			"updated_at":  time.Now().UTC(),
		}).Error
}

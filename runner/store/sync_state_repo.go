// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// SyncStateRepo implements ISyncStateDAO using PostgreSQL via GORM.
type SyncStateRepo struct {
	db *gorm.DB
}

// NewSyncStateRepo creates a new SyncStateRepo.
func NewSyncStateRepo(db *gorm.DB) *SyncStateRepo {
	return &SyncStateRepo{db: db}
}

func (r *SyncStateRepo) GetCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*sdk.Cursor, error) {
	defer metricutil.TrackDuration("runner/store", "SyncStateRepo", "GetCursor", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var m model.SyncStateModel
	err := db.Where("organization_id = ? AND connector_id = ? AND operation_id = ? AND deleted_at IS NULL", orgID, connectorID, opID).
		First(&m).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrCursorNotFound // first run
		}
		return nil, err
	}

	if len(m.CursorData) == 0 {
		return nil, ErrCursorNotFound
	}

	var cursor sdk.Cursor
	if err := json.Unmarshal(m.CursorData, &cursor); err != nil {
		return nil, fmt.Errorf("unmarshalling cursor data: %w", err)
	}
	cursor.Version = m.Version
	return &cursor, nil
}

func (r *SyncStateRepo) DeleteByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	defer metricutil.TrackDuration("runner/store", "SyncStateRepo", "DeleteByConnector", time.Now())
	db := tenantdb.DB(ctx, r.db)
	return db.Where("organization_id = ? AND connector_id = ?", orgID, connectorID).
		Delete(&model.SyncStateModel{}).Error
}

func (r *SyncStateRepo) SaveCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, cursor *sdk.Cursor) error {
	defer metricutil.TrackDuration("runner/store", "SyncStateRepo", "SaveCursor", time.Now())
	db := tenantdb.DB(ctx, r.db)

	cursorData, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("marshalling cursor: %w", err)
	}

	// A compare-and-swap on the version the caller read. Version zero means
	// no cursor was read: insert, or save over a row GetCursor reads as none
	// (soft-deleted, which the save revives, or live at version zero, as a row
	// written outside SaveCursor is). Otherwise update only the live row still
	// at that version. No row returned is a conflict.
	now := time.Now().UTC()
	var newVersion []int64
	var res *gorm.DB
	if cursor.Version == 0 {
		res = db.Raw(`INSERT INTO connector_sync_states AS s (organization_id, connector_id, operation_id, cursor_data, updated_at, version)
VALUES (?, ?, ?, ?, ?, 1)
ON CONFLICT (organization_id, connector_id, operation_id) DO UPDATE
SET cursor_data = EXCLUDED.cursor_data, updated_at = EXCLUDED.updated_at, version = s.version + 1, deleted_at = NULL
WHERE s.deleted_at IS NOT NULL OR s.version = 0
RETURNING version`, orgID, connectorID, opID, cursorData, now).Scan(&newVersion)
	} else {
		res = db.Raw(`UPDATE connector_sync_states
SET cursor_data = ?, updated_at = ?, version = version + 1
WHERE organization_id = ? AND connector_id = ? AND operation_id = ? AND version = ? AND deleted_at IS NULL
RETURNING version`, cursorData, now, orgID, connectorID, opID, cursor.Version).Scan(&newVersion)
	}
	if res.Error != nil {
		return res.Error
	}
	if len(newVersion) == 0 {
		return &CursorConflictError{OrganizationID: orgID, ConnectorID: connectorID, OperationID: opID, ReadVersion: cursor.Version}
	}
	cursor.Version = newVersion[0]
	return nil
}

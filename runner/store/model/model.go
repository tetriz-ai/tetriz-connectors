// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// SyncRunStatus represents the current state of a sync run.
type SyncRunStatus string

const (
	SyncRunStatusRunning   SyncRunStatus = "running"
	SyncRunStatusCompleted SyncRunStatus = "completed"
	SyncRunStatusFailed    SyncRunStatus = "failed"
)

// SyncRun records a single execution of an operation for audit and debugging.
// DeletedAt is a plain column, not gorm.DeletedAt, so each DAO query filters
// "deleted_at IS NULL" itself.
type SyncRun struct {
	ID             string          `gorm:"type:uuid;default:uuid_generate_v4();primaryKey"`
	OrganizationID string          `gorm:"column:organization_id;not null"`
	ConnectorID    sdk.ConnectorID `gorm:"column:connector_id;not null"`
	OperationID    sdk.OperationID `gorm:"column:operation_id;not null"`
	Status         SyncRunStatus   `gorm:"column:status;not null"`
	StartedAt      time.Time       `gorm:"column:started_at;not null"`
	CompletedAt    *time.Time      `gorm:"column:completed_at"`
	RecordsEmitted int64           `gorm:"column:records_emitted;default:0"`
	ErrorMessage   string          `gorm:"column:error_message"`
	ErrorKind      string          `gorm:"column:error_kind"`
	// Raw JSON: opaque vendor cursor; preserve unknown fields
	CursorBefore json.RawMessage `gorm:"column:cursor_before;type:jsonb"`
	// Raw JSON: opaque vendor cursor; preserve unknown fields
	CursorAfter json.RawMessage `gorm:"column:cursor_after;type:jsonb"`
	Trigger     string          `gorm:"column:trigger;not null"`
	// LeaseExpiresAt is when a running run counts as abandoned unless its
	// heartbeat renews it first. Nil: no lease, never reconciled.
	LeaseExpiresAt *time.Time        `gorm:"column:lease_expires_at"`
	Metadata       map[string]string `gorm:"-"`
}

// TableName returns the database table name for GORM.
func (SyncRun) TableName() string {
	return "connector_sync_runs"
}

// ScheduleOverride represents an org-level schedule override from the connector_schedules table.
type ScheduleOverride struct {
	OrganizationID string          `gorm:"column:organization_id;primaryKey"`
	ConnectorID    sdk.ConnectorID `gorm:"column:connector_id;primaryKey"`
	OperationID    sdk.OperationID `gorm:"column:operation_id;primaryKey"`
	CronExpression string          `gorm:"column:cron_expression;not null"`
	Enabled        bool            `gorm:"column:enabled;not null;default:true"`
	LastRunAt      *time.Time      `gorm:"column:last_run_at"`
	NextRunAt      *time.Time      `gorm:"column:next_run_at"`
	DeletedAt      *time.Time      `gorm:"column:deleted_at"`
	CreatedAt      time.Time       `gorm:"column:created_at"`
	UpdatedAt      time.Time       `gorm:"column:updated_at"`
}

// TableName returns the database table name for GORM.
func (ScheduleOverride) TableName() string {
	return "connector_schedules"
}

// SyncStateModel is the GORM model for the connector_sync_states table.
type SyncStateModel struct {
	OrganizationID string          `gorm:"column:organization_id;primaryKey"`
	ConnectorID    sdk.ConnectorID `gorm:"column:connector_id;primaryKey"`
	OperationID    sdk.OperationID `gorm:"column:operation_id;primaryKey"`
	// Raw JSON: opaque vendor cursor; preserve unknown fields
	CursorData json.RawMessage `gorm:"column:cursor_data;type:jsonb"`
	// Version counts the saves of this cursor; SaveCursor compares it.
	Version   int64      `gorm:"column:version;not null;default:0"`
	UpdatedAt time.Time  `gorm:"column:updated_at"`
	DeletedAt *time.Time `gorm:"column:deleted_at"`
}

// TableName returns the database table name for GORM.
func (SyncStateModel) TableName() string { return "connector_sync_states" }

// FetchCache is the GORM model for the connector_fetch_cache table — one
// row per memoization cache entry, keyed (org, connector, cache_type, entry_key).
// The relocation target for the Bitbucket memo caches.
type FetchCache struct {
	OrganizationID string          `gorm:"column:organization_id;primaryKey"`
	ConnectorID    sdk.ConnectorID `gorm:"column:connector_id;primaryKey"`
	CacheType      string          `gorm:"column:cache_type;primaryKey"`
	EntryKey       string          `gorm:"column:entry_key;primaryKey"`
	// Raw JSON: connector-private, lossy, re-derivable memoization value;
	// shape varies per cache_type.
	Value          json.RawMessage `gorm:"column:value;type:jsonb;not null"`
	CreatedAt      time.Time       `gorm:"column:created_at"`
	UpdatedAt      time.Time       `gorm:"column:updated_at"`
	LastAccessedAt *time.Time      `gorm:"column:last_accessed_at"`
}

// TableName returns the database table name for GORM.
func (FetchCache) TableName() string { return "connector_fetch_cache" }

// ConnectorConfigModel is the GORM model for the connector_configs table.
type ConnectorConfigModel struct {
	ConnectorID    sdk.ConnectorID `gorm:"column:connector_id;primaryKey"`
	OrganizationID string          `gorm:"column:organization_id;primaryKey"`
	// Raw JSON: vendor auth blob; shape varies per connector
	Credentials json.RawMessage `gorm:"column:credentials;type:jsonb;not null;default:'{}'"`
	// Raw JSON: vendor-specific config shape; preserve unknown fields
	Settings json.RawMessage `gorm:"column:settings;type:jsonb;not null;default:'{}'"`
	// Raw JSON: per-operation override map; vendor-specific shapes
	OperationOverrides   json.RawMessage `gorm:"column:operation_overrides;type:jsonb;not null;default:'{}'"`
	Enabled              bool            `gorm:"column:enabled;not null;default:true"`
	HealthState          sdk.HealthState `gorm:"column:health_state;not null;default:healthy"`
	HealthStateUpdatedAt time.Time       `gorm:"column:health_state_updated_at"`
	// CredentialsUpdatedAt advances only on a credential write (every statement
	// that writes the credentials, and ResetHealthState) — unlike UpdatedAt,
	// which every settings/cache write on this row also advances mid-run.
	CredentialsUpdatedAt time.Time  `gorm:"column:credentials_updated_at"`
	CreatedAt            time.Time  `gorm:"column:created_at"`
	UpdatedAt            time.Time  `gorm:"column:updated_at"`
	DeletedAt            *time.Time `gorm:"column:deleted_at"`
}

// TableName returns the database table name for GORM.
func (ConnectorConfigModel) TableName() string {
	return "connector_configs"
}

// ToFramework converts the GORM model to a sdk.ConnectorConfig.
func (m *ConnectorConfigModel) ToFramework() (*sdk.ConnectorConfig, error) {
	cfg := &sdk.ConnectorConfig{
		ConnectorID:          m.ConnectorID,
		OrganizationID:       m.OrganizationID,
		Credentials:          m.Credentials,
		Settings:             m.Settings,
		Enabled:              m.Enabled,
		HealthState:          m.HealthState,
		CredentialsUpdatedAt: m.CredentialsUpdatedAt,
	}

	if len(m.OperationOverrides) > 0 {
		var overrides map[sdk.OperationID]json.RawMessage
		if err := json.Unmarshal(m.OperationOverrides, &overrides); err != nil {
			return nil, fmt.Errorf("unmarshal operation_overrides: %w", err)
		}
		cfg.OperationOverrides = overrides
	}

	return cfg, nil
}

// ConnectorConfigModelFromFramework converts a sdk.ConnectorConfig to a GORM model.
func ConnectorConfigModelFromFramework(cfg *sdk.ConnectorConfig) (*ConnectorConfigModel, error) {
	m := &ConnectorConfigModel{
		ConnectorID:        cfg.ConnectorID,
		OrganizationID:     cfg.OrganizationID,
		Credentials:        cfg.Credentials,
		Settings:           cfg.Settings,
		Enabled:            cfg.Enabled,
		HealthState:        cfg.HealthState,
		OperationOverrides: json.RawMessage("{}"),
	}

	if len(cfg.OperationOverrides) > 0 {
		overrides, err := json.Marshal(cfg.OperationOverrides)
		if err != nil {
			return nil, fmt.Errorf("marshal operation_overrides: %w", err)
		}
		m.OperationOverrides = overrides
	} else {
		m.OperationOverrides = json.RawMessage("{}")
	}

	return m, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package scopedstore wraps the runner stores so each call runs in a tenant
// scope. Row-level security is forced on the store tables, so a call made
// with no scope sees no rows and writes none. A wrapped call runs in the
// organization scope of the organization it names, or in the system scope for
// a list that spans organizations. A call made with a context that is already
// scoped runs in that scope, so a caller can group several calls in one
// transaction. Organization ids are canonicalized first (see package orgid).
// A nil store stays nil, and a nil runner leaves calls unscoped.
package scopedstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func run(ctx context.Context, tx store.TxRunner, orgID string, scope store.Scope, fn func(context.Context) error) error {
	if tx == nil || tenantdb.InScope(ctx) {
		return fn(ctx)
	}
	return tx.RunInScope(ctx, orgID, scope, fn) //nolint:wrapcheck // the store's own error, unchanged
}

func value[T any](ctx context.Context, tx store.TxRunner, orgID string, scope store.Scope, fn func(context.Context) (T, error)) (T, error) {
	var out T
	err := run(ctx, tx, orgID, scope, func(sctx context.Context) error {
		var err error
		out, err = fn(sctx)
		return err
	})
	return out, err
}

// Configs scopes every call of an IConnectorConfigDAO.
func Configs(tx store.TxRunner, inner store.IConnectorConfigDAO) store.IConnectorConfigDAO {
	if inner == nil {
		return nil
	}
	return configs{tx: tx, inner: inner}
}

type configs struct {
	tx    store.TxRunner
	inner store.IConnectorConfigDAO
}

func (c configs) Get(ctx context.Context, orgID string, id sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (*sdk.ConnectorConfig, error) { return c.inner.Get(ctx, orgID, id) })
}

func (c configs) ListByOrg(ctx context.Context, orgID string) ([]*sdk.ConnectorConfig, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) ([]*sdk.ConnectorConfig, error) { return c.inner.ListByOrg(ctx, orgID) })
}

func (c configs) ListByConnector(ctx context.Context, id sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	return value(ctx, c.tx, "", store.ScopeSystem, func(ctx context.Context) ([]*sdk.ConnectorConfig, error) { return c.inner.ListByConnector(ctx, id) })
}

func (c configs) ListEnabledByConnector(ctx context.Context, id sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	return value(ctx, c.tx, "", store.ScopeSystem, func(ctx context.Context) ([]*sdk.ConnectorConfig, error) {
		return c.inner.ListEnabledByConnector(ctx, id)
	})
}

func (c configs) ListEnabledOrganizations(ctx context.Context, id sdk.ConnectorID) ([]string, error) {
	return value(ctx, c.tx, "", store.ScopeSystem, func(ctx context.Context) ([]string, error) {
		return c.inner.ListEnabledOrganizations(ctx, id)
	})
}

func (c configs) SaveKeepingUnset(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	cfg.OrganizationID = orgid.Canonical(cfg.OrganizationID)
	return run(ctx, c.tx, cfg.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.SaveKeepingUnset(ctx, cfg) })
}

func (c configs) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	cfg.OrganizationID = orgid.Canonical(cfg.OrganizationID)
	return run(ctx, c.tx, cfg.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.Save(ctx, cfg) })
}

func (c configs) SaveCredentialsPreserveEnabled(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	cfg.OrganizationID = orgid.Canonical(cfg.OrganizationID)
	return run(ctx, c.tx, cfg.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.SaveCredentialsPreserveEnabled(ctx, cfg) })
}

func (c configs) SaveCredentialsEnablePreservingSettings(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	cfg.OrganizationID = orgid.Canonical(cfg.OrganizationID)
	return run(ctx, c.tx, cfg.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error {
		return c.inner.SaveCredentialsEnablePreservingSettings(ctx, cfg)
	})
}

func (c configs) EnableConnectorWithSettings(ctx context.Context, orgID string, id sdk.ConnectorID, mutator func(json.RawMessage) (json.RawMessage, error)) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.EnableConnectorWithSettings(ctx, orgID, id, mutator) })
}

func (c configs) SaveCredentialsEnableWithSettings(ctx context.Context, cfg *sdk.ConnectorConfig, mutator func(json.RawMessage) (json.RawMessage, error)) error {
	cfg.OrganizationID = orgid.Canonical(cfg.OrganizationID)
	return run(ctx, c.tx, cfg.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error {
		return c.inner.SaveCredentialsEnableWithSettings(ctx, cfg, mutator)
	})
}

func (c configs) Delete(ctx context.Context, orgID string, id sdk.ConnectorID) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.Delete(ctx, orgID, id) })
}

func (c configs) PatchSettings(ctx context.Context, orgID string, id sdk.ConnectorID, mutator func(json.RawMessage) (json.RawMessage, error)) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.PatchSettings(ctx, orgID, id, mutator) })
}

func (c configs) DisableConfigsForOrg(ctx context.Context, orgID string) (int64, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (int64, error) { return c.inner.DisableConfigsForOrg(ctx, orgID) })
}

func (c configs) MarkAuthRevoked(ctx context.Context, orgID string, id sdk.ConnectorID, expected time.Time) (bool, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (bool, error) { return c.inner.MarkAuthRevoked(ctx, orgID, id, expected) })
}

func (c configs) DisableIfCredentialsUnchanged(ctx context.Context, orgID string, id sdk.ConnectorID, credentialsUpdatedAt time.Time) (bool, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (bool, error) {
		return c.inner.DisableIfCredentialsUnchanged(ctx, orgID, id, credentialsUpdatedAt)
	})
}

func (c configs) SetEnabled(ctx context.Context, orgID string, id sdk.ConnectorID, enabled bool) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return c.inner.SetEnabled(ctx, orgID, id, enabled) })
}

func (c configs) ResetHealthState(ctx context.Context, orgID string, id sdk.ConnectorID) (bool, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (bool, error) { return c.inner.ResetHealthState(ctx, orgID, id) })
}

func (c configs) CompareAndSwapCredentials(ctx context.Context, orgID string, id sdk.ConnectorID, expected time.Time, creds json.RawMessage) (bool, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, c.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (bool, error) {
		return c.inner.CompareAndSwapCredentials(ctx, orgID, id, expected, creds)
	})
}

// States scopes every call of an ISyncStateDAO.
func States(tx store.TxRunner, inner store.ISyncStateDAO) store.ISyncStateDAO {
	if inner == nil {
		return nil
	}
	return states{tx: tx, inner: inner}
}

type states struct {
	tx    store.TxRunner
	inner store.ISyncStateDAO
}

func (s states) GetCursor(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID) (*sdk.Cursor, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, s.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (*sdk.Cursor, error) { return s.inner.GetCursor(ctx, orgID, id, op) })
}

func (s states) SaveCursor(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID, c *sdk.Cursor) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, s.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return s.inner.SaveCursor(ctx, orgID, id, op, c) })
}

func (s states) DeleteByConnector(ctx context.Context, orgID string, id sdk.ConnectorID) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, s.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return s.inner.DeleteByConnector(ctx, orgID, id) })
}

// Runs scopes every call of an ISyncRunDAO.
func Runs(tx store.TxRunner, inner store.ISyncRunDAO) store.ISyncRunDAO {
	if inner == nil {
		return nil
	}
	return runs{tx: tx, inner: inner}
}

type runs struct {
	tx    store.TxRunner
	inner store.ISyncRunDAO
}

func (r runs) Create(ctx context.Context, sr *model.SyncRun) error {
	sr.OrganizationID = orgid.Canonical(sr.OrganizationID)
	return run(ctx, r.tx, sr.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return r.inner.Create(ctx, sr) })
}

func (r runs) Update(ctx context.Context, sr *model.SyncRun) error {
	sr.OrganizationID = orgid.Canonical(sr.OrganizationID)
	return run(ctx, r.tx, sr.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return r.inner.Update(ctx, sr) })
}

func (r runs) GetLatest(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID) (*model.SyncRun, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (*model.SyncRun, error) { return r.inner.GetLatest(ctx, orgID, id, op) })
}

func (r runs) ListByOperation(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID, limit int) ([]*model.SyncRun, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) ([]*model.SyncRun, error) {
		return r.inner.ListByOperation(ctx, orgID, id, op, limit)
	})
}

func (r runs) DeleteRunsByConnector(ctx context.Context, orgID string, id sdk.ConnectorID) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return r.inner.DeleteRunsByConnector(ctx, orgID, id) })
}

func (r runs) RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error { return r.inner.RenewLease(ctx, orgID, runID, lease) })
}

// CloseAbandoned runs in the organization's scope, or in the system scope
// when orgID is empty, which spans every organization.
func (r runs) CloseAbandoned(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID) (int64, error) {
	if orgID == "" {
		return value(ctx, r.tx, tenantdb.SystemTenantID, store.ScopeSystem, func(ctx context.Context) (int64, error) {
			return r.inner.CloseAbandoned(ctx, "", id, op)
		})
	}
	orgID = orgid.Canonical(orgID)
	return value(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (int64, error) {
		return r.inner.CloseAbandoned(ctx, orgID, id, op)
	})
}

func (r runs) GetConnectorSyncStatusByOrg(ctx context.Context, orgID string) (map[sdk.ConnectorID]store.ConnectorSyncStatus, error) {
	orgID = orgid.Canonical(orgID)
	return value(ctx, r.tx, orgID, store.ScopeEnforced, func(ctx context.Context) (map[sdk.ConnectorID]store.ConnectorSyncStatus, error) {
		return r.inner.GetConnectorSyncStatusByOrg(ctx, orgID)
	})
}

// Schedules scopes every call of an IScheduleDAO.
func Schedules(tx store.TxRunner, inner store.IScheduleDAO) store.IScheduleDAO {
	if inner == nil {
		return nil
	}
	return schedules{tx: tx, inner: inner}
}

type schedules struct {
	tx    store.TxRunner
	inner store.IScheduleDAO
}

func (s schedules) ListAll(ctx context.Context) ([]*model.ScheduleOverride, error) {
	return value(ctx, s.tx, "", store.ScopeSystem, func(ctx context.Context) ([]*model.ScheduleOverride, error) { return s.inner.ListAll(ctx) })
}

func (s schedules) Upsert(ctx context.Context, o *model.ScheduleOverride) error {
	o.OrganizationID = orgid.Canonical(o.OrganizationID)
	return run(ctx, s.tx, o.OrganizationID, store.ScopeEnforced, func(ctx context.Context) error { return s.inner.Upsert(ctx, o) })
}

func (s schedules) UpdateRunTimestamps(ctx context.Context, orgID string, id sdk.ConnectorID, op sdk.OperationID, last, next time.Time) error {
	orgID = orgid.Canonical(orgID)
	return run(ctx, s.tx, orgID, store.ScopeEnforced, func(ctx context.Context) error {
		return s.inner.UpdateRunTimestamps(ctx, orgID, id, op, last, next)
	})
}

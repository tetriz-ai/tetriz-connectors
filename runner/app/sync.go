// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/tokencache"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// SyncResult is what one run left in the store.
type SyncResult struct {
	RunID     string          `json:"run_id"`
	Status    string          `json:"status"`
	Records   int64           `json:"records_emitted"`
	ErrorKind string          `json:"error_kind,omitempty"`
	Error     string          `json:"error,omitempty"`
	Cursor    json.RawMessage `json:"cursor,omitempty"`
}

// SyncOnce runs one operation for one organization now, in this process,
// and reports the run row and the cursor it left. A paused connector
// (disabled or auth_revoked) runs nothing and reports status "skipped". A
// failed run returns its error with the result; if the row could not be
// closed, the result reads "failed" with that error.
//
// The operation runs under the deadline `serve` gives it: its
// ExecutionTimeout, or schedule.DefaultTaskTimeout. Without a deadline a paced
// operation cannot wait out a rejection from the local rate limiter, so it
// would fail on the first one a scheduled run absorbs.
func (a *App) SyncOnce(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, kind sdk.TenantKind) (SyncResult, error) {
	tx := store.PostgresTxRunner{}
	runCtx, cancel := context.WithTimeout(ctx, a.runTimeout(connectorID, opID))
	runID, runErr := a.Orchestrator.RunOperationWithID(runCtx, orgID, connectorID, opID, kind, "manual")
	cancel()

	// Report this run's own row, by id: a scheduled run of the same operation
	// that starts meanwhile is a different row.
	var res SyncResult
	if err := tx.RunInScope(ctx, orgID, store.ScopeEnforced, func(sctx context.Context) error {
		var r *model.SyncRun
		if runID != "" {
			var err error
			if r, err = a.Runs.Get(sctx, orgID, runID); err != nil && !store.IsNotFound(err) {
				return fmt.Errorf("read run %s: %w", runID, err)
			}
		}
		if r == nil {
			// No row of this run: the connector is paused, or the run failed
			// before its row was written. Either way the stored cursor is
			// another run's, so none is reported.
			res.Status = "skipped"
			if runErr != nil {
				res.Status, res.Error = string(model.SyncRunStatusFailed), runErr.Error()
			}
			return nil
		}
		res.RunID, res.Status, res.Records = r.ID, string(r.Status), r.RecordsEmitted
		res.ErrorKind, res.Error = r.ErrorKind, r.ErrorMessage
		c, err := a.States.GetCursor(sctx, orgID, connectorID, opID)
		if err != nil && !store.IsNotFound(err) {
			return fmt.Errorf("read cursor: %w", err)
		}
		if c != nil {
			res.Cursor = c.Data
		}
		return nil
	}); err != nil {
		return res, err
	}
	if runErr != nil {
		// The run failed. When its row could not be closed it still reads
		// running, so report the failure the store did not record.
		if res.Status == string(model.SyncRunStatusRunning) {
			res.Status, res.Error = string(model.SyncRunStatusFailed), runErr.Error()
		}
		return res, runErr
	}
	return res, nil
}

// runTimeout is the deadline the scheduler gives one run of the operation. An
// unknown operation gets the default; RunOperation then reports it unknown.
func (a *App) runTimeout(connectorID sdk.ConnectorID, opID sdk.OperationID) time.Duration {
	if op, err := a.Registry.GetOperation(connectorID, opID); err == nil && op.ExecutionTimeout > 0 {
		return op.ExecutionTimeout
	}
	return schedule.DefaultTaskTimeout
}

// PutConfig creates or replaces an organization's connector config. The
// credentials are encrypted on write when the runner has a key. It then
// forgets the connector's cached access token, so the next request carries the
// new credentials.
func (a *App) PutConfig(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if _, err := a.Registry.Get(cfg.ConnectorID); err != nil {
		return fmt.Errorf("unknown connector %q: %w", cfg.ConnectorID, err)
	}
	if err := WriteConfig(ctx, store.PostgresTxRunner{}, a.Configs, cfg); err != nil {
		return err
	}
	if a.tokens == nil {
		return nil
	}
	if err := tokencache.ForgetFromCache(ctx, a.tokens, cfg.ConnectorID, cfg.OrganizationID); err != nil {
		return fmt.Errorf("config stored, but the next request may still carry the replaced token: %w", err)
	}
	return nil
}

// ErrNoStoredConfig reports that PutSettings found no config to update.
var ErrNoStoredConfig = errors.New("no stored config")

// PutSettings updates the settings and the enabled bit of a stored config and
// keeps its credentials, operation overrides, health state and
// credentials_updated_at. Nil settings keep the stored ones too. A disconnected
// (soft-deleted) config is ErrNoStoredConfig and stays disconnected.
func (a *App) PutSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, settings json.RawMessage, enabled bool) error {
	if _, err := a.Registry.Get(connectorID); err != nil {
		return fmt.Errorf("unknown connector %q: %w", connectorID, err)
	}
	return UpdateSettings(ctx, store.PostgresTxRunner{}, a.Configs, orgID, connectorID, settings, enabled)
}

// UpdateSettings is PutSettings over any config store.
func UpdateSettings(ctx context.Context, tx store.TxRunner, configs store.IConnectorConfigDAO, orgID string, connectorID sdk.ConnectorID, settings json.RawMessage, enabled bool) error {
	return tx.RunInScope(ctx, orgID, store.ScopeEnforced, func(sctx context.Context) error {
		// Two column-scoped writes, never a read-then-Save: a concurrent
		// credential write or disconnect stands, and a soft-deleted row is
		// never revived.
		if settings != nil {
			err := configs.PatchSettings(sctx, orgID, connectorID, func(json.RawMessage) (json.RawMessage, error) { return settings, nil })
			if store.IsNotFound(err) {
				return ErrNoStoredConfig
			}
			if err != nil {
				return fmt.Errorf("save settings: %w", err)
			}
		}
		err := configs.SetEnabled(sctx, orgID, connectorID, enabled)
		if store.IsNotFound(err) {
			return ErrNoStoredConfig
		}
		if err != nil {
			return fmt.Errorf("save enabled: %w", err)
		}
		return nil
	})
}

// WriteConfig stores an organization's connector config as a credential
// write. It saves the config, then resets the health state and advances
// credentials_updated_at, so a connector paused as auth_revoked runs again and
// a run that started before the write cannot re-revoke it. The save never
// touches the health state. Nil Settings or nil OperationOverrides keep the
// stored values inside the save's own statement, so a settings write that
// lands meanwhile is not lost; with no stored config they are {}.
func WriteConfig(ctx context.Context, tx store.TxRunner, configs store.IConnectorConfigDAO, cfg *sdk.ConnectorConfig) error {
	return tx.RunInScope(ctx, cfg.OrganizationID, store.ScopeEnforced, func(sctx context.Context) error {
		if err := configs.SaveKeepingUnset(sctx, cfg); err != nil {
			return fmt.Errorf("save config: %w", err)
		}
		if _, err := configs.ResetHealthState(sctx, cfg.OrganizationID, cfg.ConnectorID); err != nil {
			return fmt.Errorf("reset health state: %w", err)
		}
		return nil
	})
}

// Cursor returns the stored cursor of one operation, or nil when none.
func (a *App) Cursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*sdk.Cursor, error) {
	var c *sdk.Cursor
	err := store.PostgresTxRunner{}.RunInScope(ctx, orgID, store.ScopeEnforced, func(sctx context.Context) error {
		got, err := a.States.GetCursor(sctx, orgID, connectorID, opID)
		if err != nil && !store.IsNotFound(err) {
			return fmt.Errorf("read cursor: %w", err)
		}
		c = got
		return nil
	})
	return c, err
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package memstore is an in-memory implementation of the runner's store
// interfaces, for tests, the conformance suite and single-process runs that
// need no database. It follows the Postgres store's contract: the same
// not-found errors, the same upsert and fence rules. A store from New keeps
// nothing across restarts; OpenFile returns one persisted to a JSON file.
// Neither applies tenant isolation; use store.NoopTxRunner with them.
package memstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

type configKey struct {
	org  string
	conn sdk.ConnectorID
}

type opKey struct {
	org  string
	conn sdk.ConnectorID
	op   sdk.OperationID
}

type cacheKey struct {
	org   string
	conn  sdk.ConnectorID
	ctype string
	entry string
}

type configRow struct {
	m model.ConnectorConfigModel
}

// Store holds every table. The zero value is not ready; use New.
type Store struct {
	mu        sync.Mutex
	now       func() time.Time
	configs   map[configKey]*configRow
	cursors   map[opKey]*model.SyncStateModel
	runs      map[string]*model.SyncRun
	schedules map[opKey]*model.ScheduleOverride
	cache     map[cacheKey]*model.FetchCache
}

var (
	_ store.IConnectorConfigDAO = (*Store)(nil)
	_ store.ISyncStateDAO       = (*Store)(nil)
	_ store.ISyncRunDAO         = (*Store)(nil)
	_ store.IScheduleDAO        = (*Store)(nil)
	_ store.IFetchCacheDAO      = (*Store)(nil)
)

// New returns an empty store.
func New() *Store {
	return &Store{
		now:       func() time.Time { return time.Now().UTC() },
		configs:   map[configKey]*configRow{},
		cursors:   map[opKey]*model.SyncStateModel{},
		runs:      map[string]*model.SyncRun{},
		schedules: map[opKey]*model.ScheduleOverride{},
		cache:     map[cacheKey]*model.FetchCache{},
	}
}

// SetClock replaces the clock the store stamps rows with.
func (s *Store) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func clone[T any](v *T) *T {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func (s *Store) live(org string, conn sdk.ConnectorID) (*configRow, bool) {
	r, ok := s.configs[configKey{org, conn}]
	if !ok || r.m.DeletedAt != nil {
		return nil, false
	}
	return r, true
}

// --- connector configs ---

// Get implements store.IConnectorConfigDAO.
func (s *Store) Get(_ context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok {
		return nil, gorm.ErrRecordNotFound
	}
	return toFramework(r.m)
}

func toFramework(m model.ConnectorConfigModel) (*sdk.ConnectorConfig, error) {
	cfg, err := m.ToFramework()
	if err != nil {
		return nil, fmt.Errorf("converting connector config model: %w", err)
	}
	cfg.Credentials = append(json.RawMessage(nil), cfg.Credentials...)
	cfg.Settings = append(json.RawMessage(nil), cfg.Settings...)
	return cfg, nil
}

func (s *Store) list(pred func(m model.ConnectorConfigModel) bool) ([]*sdk.ConnectorConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]configKey, 0, len(s.configs))
	for k := range s.configs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].org != keys[j].org {
			return keys[i].org < keys[j].org
		}
		return keys[i].conn < keys[j].conn
	})
	out := []*sdk.ConnectorConfig{}
	var unreadable *store.UndecryptableRowsError
	for _, k := range keys {
		r := s.configs[k]
		if r.m.DeletedAt != nil || !pred(r.m) {
			continue
		}
		cfg, err := toFramework(r.m)
		if err != nil {
			// One row that does not decode is left out and named, as the Postgres
			// store does, so it cannot fail the list for every organization.
			if unreadable == nil {
				unreadable = &store.UndecryptableRowsError{Err: err}
			}
			unreadable.OrgIDs = append(unreadable.OrgIDs, r.m.OrganizationID)
			unreadable.Rows = append(unreadable.Rows, store.UnreadableRow{OrganizationID: r.m.OrganizationID, ConnectorID: r.m.ConnectorID})
			continue
		}
		out = append(out, cfg)
	}
	if unreadable != nil {
		return out, fmt.Errorf("reading connector configs: %w", unreadable)
	}
	return out, nil
}

// ListByOrg implements store.IConnectorConfigDAO.
func (s *Store) ListByOrg(_ context.Context, orgID string) ([]*sdk.ConnectorConfig, error) {
	return s.list(func(m model.ConnectorConfigModel) bool { return m.OrganizationID == orgID })
}

// ListByConnector implements store.IConnectorConfigDAO.
func (s *Store) ListByConnector(_ context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	return s.list(func(m model.ConnectorConfigModel) bool { return m.ConnectorID == connectorID })
}

// ListEnabledByConnector implements store.IConnectorConfigDAO.
func (s *Store) ListEnabledByConnector(_ context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	return s.list(func(m model.ConnectorConfigModel) bool { return m.ConnectorID == connectorID && m.Enabled })
}

// ListEnabledOrganizations implements store.IConnectorConfigDAO.
func (s *Store) ListEnabledOrganizations(ctx context.Context, connectorID sdk.ConnectorID) ([]string, error) {
	cfgs, err := s.ListEnabledByConnector(ctx, connectorID)
	if err != nil {
		return nil, err
	}
	orgIDs := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		orgIDs = append(orgIDs, c.OrganizationID)
	}
	sort.Strings(orgIDs)
	return orgIDs, nil
}

// upsert writes the insert row, or on an existing row applies update.
func (s *Store) upsert(cfg *sdk.ConnectorConfig, insertEnabled bool, update func(r *configRow, in model.ConnectorConfigModel)) error {
	in, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	now := s.now()
	k := configKey{in.OrganizationID, in.ConnectorID}
	if r, ok := s.configs[k]; ok {
		update(r, *in)
		r.m.UpdatedAt = now
		return nil
	}
	in.Enabled = insertEnabled
	in.CreatedAt, in.UpdatedAt = now, now
	in.HealthState = sdk.HealthStateHealthy
	in.HealthStateUpdatedAt, in.CredentialsUpdatedAt = now, now
	in.DeletedAt = nil
	s.configs[k] = &configRow{m: *in}
	return nil
}

// Save implements store.IConnectorConfigDAO: an upsert that rewrites
// credentials, settings, overrides and enabled, and clears deleted_at.
func (s *Store) Save(_ context.Context, cfg *sdk.ConnectorConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(cfg, cfg.Enabled, func(r *configRow, in model.ConnectorConfigModel) {
		if !bytes.Equal(r.m.Credentials, in.Credentials) {
			r.m.CredentialsUpdatedAt = s.now()
		}
		r.m.Credentials, r.m.Settings, r.m.OperationOverrides = in.Credentials, in.Settings, in.OperationOverrides
		r.m.Enabled, r.m.DeletedAt = in.Enabled, nil
	})
}

// SaveKeepingUnset implements store.IConnectorConfigDAO.
func (s *Store) SaveKeepingUnset(_ context.Context, cfg *sdk.ConnectorConfig) error {
	keepSettings, keepOverrides := cfg.Settings == nil, cfg.OperationOverrides == nil
	c := *cfg
	if keepSettings {
		c.Settings = json.RawMessage("{}")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(&c, c.Enabled, func(r *configRow, in model.ConnectorConfigModel) {
		revived := r.m.DeletedAt != nil
		if !bytes.Equal(r.m.Credentials, in.Credentials) {
			r.m.CredentialsUpdatedAt = s.now()
		}
		r.m.Credentials, r.m.Enabled, r.m.DeletedAt = in.Credentials, in.Enabled, nil
		if !keepSettings || revived {
			r.m.Settings = in.Settings
		}
		if !keepOverrides || revived {
			r.m.OperationOverrides = in.OperationOverrides
		}
	})
}

// SaveCredentialsPreserveEnabled implements store.IConnectorConfigDAO.
func (s *Store) SaveCredentialsPreserveEnabled(_ context.Context, cfg *sdk.ConnectorConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(cfg, false, func(r *configRow, in model.ConnectorConfigModel) {
		r.m.Credentials, r.m.Settings, r.m.DeletedAt = in.Credentials, in.Settings, nil
		r.m.CredentialsUpdatedAt = s.now()
	})
}

// SaveCredentialsEnablePreservingSettings implements store.IConnectorConfigDAO.
func (s *Store) SaveCredentialsEnablePreservingSettings(_ context.Context, cfg *sdk.ConnectorConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upsert(cfg, true, func(r *configRow, in model.ConnectorConfigModel) {
		revived := r.m.DeletedAt != nil
		r.m.Credentials, r.m.Enabled, r.m.DeletedAt = in.Credentials, true, nil
		r.m.CredentialsUpdatedAt = s.now()
		if revived {
			r.m.Settings, r.m.OperationOverrides = in.Settings, in.OperationOverrides
		}
	})
}

// EnableConnectorWithSettings implements store.IConnectorConfigDAO.
func (s *Store) EnableConnectorWithSettings(_ context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if mutator == nil {
		return errors.New("EnableConnectorWithSettings: mutator is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok {
		return fmt.Errorf("enabling connector with settings: EnableConnectorWithSettings: load row for update: %w", gorm.ErrRecordNotFound)
	}
	next, err := mutator(append(json.RawMessage(nil), r.m.Settings...))
	if err != nil {
		return fmt.Errorf("enabling connector with settings: EnableConnectorWithSettings: mutator: %w", err)
	}
	if len(next) == 0 {
		return errors.New("enabling connector with settings: EnableConnectorWithSettings: refusing to enable with empty settings")
	}
	r.m.Settings, r.m.Enabled, r.m.UpdatedAt = next, true, s.now()
	return nil
}

// SaveCredentialsEnableWithSettings implements store.IConnectorConfigDAO.
func (s *Store) SaveCredentialsEnableWithSettings(_ context.Context, cfg *sdk.ConnectorConfig, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if mutator == nil {
		return errors.New("SaveCredentialsEnableWithSettings: mutator is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var current json.RawMessage
	r, exists := s.live(cfg.OrganizationID, cfg.ConnectorID)
	if exists {
		current = append(json.RawMessage(nil), r.m.Settings...)
	}
	next, err := mutator(current)
	if err != nil {
		return fmt.Errorf("saving credentials and settings: SaveCredentialsEnableWithSettings: mutator: %w", err)
	}
	if len(next) == 0 {
		return errors.New("saving credentials and settings: SaveCredentialsEnableWithSettings: refusing to enable with empty settings")
	}
	if exists {
		now := s.now()
		r.m.Credentials, r.m.Settings, r.m.Enabled, r.m.UpdatedAt = cfg.Credentials, next, true, now
		r.m.CredentialsUpdatedAt = now
		return nil
	}
	in := *cfg
	in.Settings, in.OperationOverrides = next, nil
	return s.upsert(&in, true, func(r *configRow, in model.ConnectorConfigModel) {
		r.m.Credentials, r.m.Settings, r.m.Enabled = in.Credentials, next, true
		r.m.OperationOverrides, r.m.DeletedAt = json.RawMessage("{}"), nil
		r.m.CredentialsUpdatedAt = s.now()
	})
}

// Delete implements store.IConnectorConfigDAO: a soft delete.
func (s *Store) Delete(_ context.Context, orgID string, connectorID sdk.ConnectorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.live(orgID, connectorID); ok {
		now := s.now()
		r.m.DeletedAt = &now
	}
	return nil
}

// PatchSettings implements store.IConnectorConfigDAO. An empty result from
// the mutator writes nothing.
func (s *Store) PatchSettings(_ context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if mutator == nil {
		return errors.New("PatchSettings: mutator is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok {
		return fmt.Errorf("patching connector settings: PatchSettings: load row for update: %w", gorm.ErrRecordNotFound)
	}
	next, err := mutator(append(json.RawMessage(nil), r.m.Settings...))
	if err != nil {
		return fmt.Errorf("patching connector settings: PatchSettings: mutator: %w", err)
	}
	if len(next) == 0 {
		return nil
	}
	r.m.Settings, r.m.UpdatedAt = next, s.now()
	return nil
}

// DisableConfigsForOrg implements store.IConnectorConfigDAO.
func (s *Store) DisableConfigsForOrg(_ context.Context, orgID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, r := range s.configs {
		if r.m.OrganizationID == orgID && r.m.Enabled && r.m.DeletedAt == nil {
			r.m.Enabled, r.m.UpdatedAt = false, s.now()
			n++
		}
	}
	return n, nil
}

// DisableIfCredentialsUnchanged implements store.IConnectorConfigDAO.
func (s *Store) DisableIfCredentialsUnchanged(_ context.Context, orgID string, connectorID sdk.ConnectorID, credentialsUpdatedAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok || !r.m.CredentialsUpdatedAt.Equal(credentialsUpdatedAt) {
		return false, nil
	}
	r.m.Enabled, r.m.UpdatedAt = false, s.now()
	return true, nil
}

// SetEnabled implements store.IConnectorConfigDAO.
func (s *Store) SetEnabled(_ context.Context, orgID string, connectorID sdk.ConnectorID, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok {
		return gorm.ErrRecordNotFound
	}
	r.m.Enabled, r.m.UpdatedAt = enabled, s.now()
	return nil
}

// MarkAuthRevoked implements store.IConnectorConfigDAO. Like the Postgres
// store it does not look at deleted_at.
func (s *Store) MarkAuthRevoked(_ context.Context, orgID string, connectorID sdk.ConnectorID, expected time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.configs[configKey{orgID, connectorID}]
	if !ok || r.m.HealthState == sdk.HealthStateAuthRevoked || !r.m.CredentialsUpdatedAt.Equal(expected) {
		return false, nil
	}
	r.m.HealthState, r.m.HealthStateUpdatedAt = sdk.HealthStateAuthRevoked, s.now()
	return true, nil
}

// ResetHealthState implements store.IConnectorConfigDAO: it always advances
// credentials_updated_at and reports whether the health state changed.
func (s *Store) ResetHealthState(_ context.Context, orgID string, connectorID sdk.ConnectorID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.configs[configKey{orgID, connectorID}]
	if !ok {
		return false, nil
	}
	now := s.now()
	r.m.CredentialsUpdatedAt = now
	if r.m.HealthState == sdk.HealthStateHealthy {
		return false, nil
	}
	r.m.HealthState, r.m.HealthStateUpdatedAt = sdk.HealthStateHealthy, now
	return true, nil
}

// --- cursors ---

// GetCursor implements store.ISyncStateDAO.
func (s *Store) GetCursor(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*sdk.Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.cursors[opKey{orgID, connectorID, opID}]
	if !ok || m.DeletedAt != nil || len(m.CursorData) == 0 {
		return nil, store.ErrCursorNotFound
	}
	var c sdk.Cursor
	if err := json.Unmarshal(m.CursorData, &c); err != nil {
		return nil, fmt.Errorf("unmarshalling cursor data: %w", err)
	}
	c.Version = m.Version
	return &c, nil
}

// SaveCursor implements store.ISyncStateDAO: a compare-and-swap on
// cursor.Version. A soft-deleted row reads as no cursor (version zero), and a
// save over it revives it like a fresh cursor.
func (s *Store) SaveCursor(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, cursor *sdk.Cursor) error {
	b, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("marshalling cursor: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := opKey{orgID, connectorID, opID}
	m, ok := s.cursors[k]
	stored := int64(0)
	if ok && m.DeletedAt == nil {
		stored = m.Version
	}
	if cursor.Version != stored {
		return &store.CursorConflictError{OrganizationID: orgID, ConnectorID: connectorID, OperationID: opID, ReadVersion: cursor.Version}
	}
	if !ok {
		m = &model.SyncStateModel{OrganizationID: orgID, ConnectorID: connectorID, OperationID: opID}
		s.cursors[k] = m
	}
	m.Version++
	m.CursorData, m.UpdatedAt, m.DeletedAt = b, s.now(), nil
	cursor.Version = m.Version
	return nil
}

// DeleteByConnector implements store.ISyncStateDAO: a hard delete.
func (s *Store) DeleteByConnector(_ context.Context, orgID string, connectorID sdk.ConnectorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.cursors {
		if k.org == orgID && k.conn == connectorID {
			delete(s.cursors, k)
		}
	}
	return nil
}

// --- sync runs ---

// Create implements store.ISyncRunDAO. An empty id gets a new UUID.
func (s *Store) Create(_ context.Context, run *model.SyncRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run.ID == "" {
		run.ID = uuid.NewString()
	}
	if _, dup := s.runs[run.ID]; dup {
		return fmt.Errorf("sync run %s already exists", run.ID)
	}
	s.runs[run.ID] = clone(run)
	return nil
}

// Update implements store.ISyncRunDAO: it writes the whole row but its
// lease, only while the stored row is running.
func (s *Store) Update(_ context.Context, run *model.SyncRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.runs[run.ID]
	if !ok || cur.Status != model.SyncRunStatusRunning {
		return store.ErrRunClosed
	}
	next := clone(run)
	next.LeaseExpiresAt = cur.LeaseExpiresAt
	s.runs[run.ID] = next
	return nil
}

func (s *Store) runsFor(orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) []*model.SyncRun {
	var out []*model.SyncRun
	for _, r := range s.runs {
		if r.OrganizationID == orgID && r.ConnectorID == connectorID && r.OperationID == opID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

// GetLatest implements store.ISyncRunDAO.
func (s *Store) GetLatest(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*model.SyncRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := s.runsFor(orgID, connectorID, opID)
	if len(runs) == 0 {
		return nil, store.ErrSyncRunNotFound
	}
	return clone(runs[0]), nil
}

// ListByOperation implements store.ISyncRunDAO, newest first.
func (s *Store) ListByOperation(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, limit int) ([]*model.SyncRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := s.runsFor(orgID, connectorID, opID)
	if limit >= 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	out := make([]*model.SyncRun, len(runs))
	for i, r := range runs {
		out[i] = clone(r)
	}
	return out, nil
}

// RenewLease implements store.ISyncRunDAO.
// The lease is measured on the store's clock (SetClock).
func (s *Store) RenewLease(_ context.Context, orgID, runID string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || r.OrganizationID != orgID || r.Status != model.SyncRunStatusRunning {
		return store.ErrRunClosed
	}
	u := s.now().Add(lease)
	r.LeaseExpiresAt = &u
	return nil
}

// CloseAbandoned implements store.ISyncRunDAO, on the store's clock.
func (s *Store) CloseAbandoned(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var n int64
	for _, r := range s.runs {
		if r.Status != model.SyncRunStatusRunning || r.LeaseExpiresAt == nil || !r.LeaseExpiresAt.Before(now) ||
			(orgID != "" && r.OrganizationID != orgID) || (connectorID != "" && r.ConnectorID != connectorID) || (opID != "" && r.OperationID != opID) {
			continue
		}
		at := now
		r.Status, r.CompletedAt = model.SyncRunStatusFailed, &at
		r.ErrorKind, r.ErrorMessage = sdk.ErrKindAbandoned.String(), store.AbandonedRunMessage(*r.LeaseExpiresAt)
		n++
	}
	return n, nil
}

// DeleteRunsByConnector implements store.ISyncRunDAO. Like the Postgres
// store, it removes the rows.
func (s *Store) DeleteRunsByConnector(_ context.Context, orgID string, connectorID sdk.ConnectorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.runs {
		if r.OrganizationID == orgID && r.ConnectorID == connectorID {
			delete(s.runs, id)
		}
	}
	return nil
}

// dataOperations are the VCS operations whose runs back a VCS connector's
// sync status; every operation of any other connector counts.
var dataOperations = map[sdk.OperationID]bool{"fetch_prs": true, "fetch_commits": true, "fetch_issues": true, "fetch_reviews": true}

// GetConnectorSyncStatusByOrg implements store.ISyncRunDAO with the Postgres
// store's aggregation.
func (s *Store) GetConnectorSyncStatusByOrg(_ context.Context, orgID string) (map[sdk.ConnectorID]store.ConnectorSyncStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vcs := map[sdk.ConnectorID]bool{}
	for _, id := range vendorids.VCSConnectorIDs {
		vcs[sdk.ConnectorID(id)] = true
	}
	type latest struct {
		at     time.Time
		status model.SyncRunStatus
	}
	lastOK := map[sdk.ConnectorID]*time.Time{}
	seen := map[sdk.ConnectorID]bool{}
	latestPerOp := map[opKey]latest{}
	for _, r := range s.runs {
		if r.OrganizationID != orgID || (vcs[r.ConnectorID] && !dataOperations[r.OperationID]) {
			continue
		}
		seen[r.ConnectorID] = true
		if r.Status == model.SyncRunStatusCompleted && r.CompletedAt != nil {
			if cur := lastOK[r.ConnectorID]; cur == nil || r.CompletedAt.After(*cur) {
				t := *r.CompletedAt
				lastOK[r.ConnectorID] = &t
			}
		}
		k := opKey{orgID, r.ConnectorID, r.OperationID}
		if l, ok := latestPerOp[k]; !ok || r.StartedAt.After(l.at) {
			latestPerOp[k] = latest{r.StartedAt, r.Status}
		}
	}
	out := make(map[sdk.ConnectorID]store.ConnectorSyncStatus, len(seen))
	for conn := range seen {
		failed := false
		for k, l := range latestPerOp {
			if k.conn == conn && l.status == model.SyncRunStatusFailed {
				failed = true
			}
		}
		state := store.ConnectorSyncStateStillSyncing
		switch {
		case failed:
			state = store.ConnectorSyncStateFailed
		case lastOK[conn] != nil:
			state = store.ConnectorSyncStateSynced
		}
		out[conn] = store.ConnectorSyncStatus{LastSuccessfulAt: lastOK[conn], State: state}
	}
	return out, nil
}

// --- schedules ---

// ListAll implements store.IScheduleDAO: every live override, enabled or not.
func (s *Store) ListAll(_ context.Context) ([]*model.ScheduleOverride, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.ScheduleOverride
	for _, o := range s.schedules {
		if o.DeletedAt == nil {
			out = append(out, clone(o))
		}
	}
	return out, nil
}

// Upsert implements store.IScheduleDAO.
func (s *Store) Upsert(_ context.Context, o *model.ScheduleOverride) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	o.UpdatedAt = now
	if o.CreatedAt.IsZero() {
		o.CreatedAt = now
	}
	k := opKey{o.OrganizationID, o.ConnectorID, o.OperationID}
	if cur, ok := s.schedules[k]; ok {
		cur.CronExpression, cur.Enabled, cur.UpdatedAt = o.CronExpression, o.Enabled, now
		return nil
	}
	c := clone(o)
	c.LastRunAt, c.NextRunAt, c.DeletedAt = nil, nil, nil
	s.schedules[k] = c
	return nil
}

// UpdateRunTimestamps implements store.IScheduleDAO.
func (s *Store) UpdateRunTimestamps(_ context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, lastRunAt, nextRunAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.schedules[opKey{orgID, connectorID, opID}]; ok {
		cur.LastRunAt, cur.NextRunAt, cur.UpdatedAt = &lastRunAt, &nextRunAt, s.now()
	}
	return nil
}

// --- fetch cache ---

func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(x, y)
}

// PutMany implements store.IFetchCacheDAO: a row counts as written only when
// its value changed, so updated_at moves only on a real change.
func (s *Store) PutMany(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entries map[string]json.RawMessage) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var n int64
	for key, v := range entries {
		k := cacheKey{orgID, connectorID, cacheType, key}
		if cur, ok := s.cache[k]; ok {
			if sameJSON(cur.Value, v) {
				continue
			}
			cur.Value, cur.UpdatedAt = append(json.RawMessage(nil), v...), now
			n++
			continue
		}
		s.cache[k] = &model.FetchCache{OrganizationID: orgID, ConnectorID: connectorID, CacheType: cacheType, EntryKey: key, Value: append(json.RawMessage(nil), v...), CreatedAt: now, UpdatedAt: now}
		n++
	}
	return n, nil
}

// GetMany implements store.IFetchCacheDAO.
func (s *Store) GetMany(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) (map[string]json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]json.RawMessage, len(entryKeys))
	for _, key := range entryKeys {
		if cur, ok := s.cache[cacheKey{orgID, connectorID, cacheType, key}]; ok {
			out[key] = append(json.RawMessage(nil), cur.Value...)
		}
	}
	return out, nil
}

// ScanForEviction implements store.IFetchCacheDAO, oldest first.
func (s *Store) ScanForEviction(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, updatedBefore time.Time) ([]*model.FetchCache, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.FetchCache
	for k, v := range s.cache {
		if k.org == orgID && k.conn == connectorID && k.ctype == cacheType && v.UpdatedAt.Before(updatedBefore) {
			c := clone(v)
			c.Value = nil
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	return out, nil
}

// DeleteMany implements store.IFetchCacheDAO.
func (s *Store) DeleteMany(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range entryKeys {
		delete(s.cache, cacheKey{orgID, connectorID, cacheType, key})
	}
	return nil
}

// DeleteManyOlderThan implements store.IFetchCacheDAO.
func (s *Store) DeleteManyOlderThan(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string, olderThan time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, key := range entryKeys {
		k := cacheKey{orgID, connectorID, cacheType, key}
		if v, ok := s.cache[k]; ok && v.UpdatedAt.Before(olderThan) {
			delete(s.cache, k)
			n++
		}
	}
	return n, nil
}

// CountByType implements store.IFetchCacheDAO.
func (s *Store) CountByType(_ context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k := range s.cache {
		if k.org == orgID && k.conn == connectorID && k.ctype == cacheType {
			n++
		}
	}
	return n, nil
}

// CompareAndSwapCredentials implements store.IConnectorConfigDAO.
func (s *Store) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expected time.Time, credentials json.RawMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.live(orgID, connectorID)
	if !ok || !r.m.CredentialsUpdatedAt.Equal(expected) {
		return false, nil
	}
	now := s.now()
	r.m.Credentials = append(json.RawMessage(nil), credentials...)
	r.m.CredentialsUpdatedAt, r.m.UpdatedAt = now, now
	store.RecordCredentialSwap(ctx, orgID, connectorID, now)
	return true, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Encryptor seals credentials before they reach the file. The auth/cryptutil
// encryptors satisfy it.
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// FileStore is the in-memory store persisted to one JSON file, for
// single-process runs that must survive a restart without a database. Every
// write rewrites the whole file (write to a temporary file, then rename), so
// it suits small deployments: tens of connectors, not a fleet. It applies no
// tenant isolation; use store.NoopTxRunner with it. With a nil Encryptor the
// credentials are stored as given, in a file created with mode 0600.
type FileStore struct {
	*Store
	path string
	enc  Encryptor
	wmu  sync.Mutex
}

var (
	_ store.IConnectorConfigDAO = (*FileStore)(nil)
	_ store.ISyncStateDAO       = (*FileStore)(nil)
	_ store.ISyncRunDAO         = (*FileStore)(nil)
	_ store.IScheduleDAO        = (*FileStore)(nil)
	_ store.IFetchCacheDAO      = (*FileStore)(nil)
)

type snapshot struct {
	Version   int                          `json:"version"`
	Configs   []model.ConnectorConfigModel `json:"configs"`
	Cursors   []model.SyncStateModel       `json:"cursors"`
	Runs      []model.SyncRun              `json:"runs"`
	Schedules []model.ScheduleOverride     `json:"schedules"`
	Cache     []model.FetchCache           `json:"fetch_cache"`
}

const snapshotVersion = 1

// OpenFile loads the store at path, or starts an empty one when the file
// does not exist yet.
func OpenFile(path string, enc Encryptor) (*FileStore, error) {
	f := &FileStore{Store: New(), path: path, enc: enc}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return f, nil
	}
	if err != nil {
		return nil, fmt.Errorf("memstore: reading %s: %w", path, err)
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, fmt.Errorf("memstore: decoding %s: %w", path, err)
	}
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("memstore: %s has version %d, want %d", path, snap.Version, snapshotVersion)
	}
	for i := range snap.Configs {
		m := snap.Configs[i]
		if enc == nil && cryptutil.IsEnvelope(m.Credentials) {
			return nil, fmt.Errorf("memstore: credentials for %s/%s: %w", m.OrganizationID, m.ConnectorID, store.ErrCredentialsEncrypted)
		}
		if enc != nil && len(m.Credentials) > 0 {
			pt, err := enc.Decrypt(m.Credentials)
			if err != nil {
				return nil, fmt.Errorf("memstore: decrypting credentials for %s/%s: %w", m.OrganizationID, m.ConnectorID, err)
			}
			m.Credentials = pt
		}
		f.configs[configKey{m.OrganizationID, m.ConnectorID}] = &configRow{m: m}
	}
	for i := range snap.Cursors {
		c := snap.Cursors[i]
		f.cursors[opKey{c.OrganizationID, c.ConnectorID, c.OperationID}] = &c
	}
	for i := range snap.Runs {
		r := snap.Runs[i]
		f.runs[r.ID] = &r
	}
	for i := range snap.Schedules {
		o := snap.Schedules[i]
		f.schedules[opKey{o.OrganizationID, o.ConnectorID, o.OperationID}] = &o
	}
	for i := range snap.Cache {
		c := snap.Cache[i]
		f.cache[cacheKey{c.OrganizationID, c.ConnectorID, c.CacheType, c.EntryKey}] = &c
	}
	return f, nil
}

func (f *FileStore) flush() error {
	f.wmu.Lock()
	defer f.wmu.Unlock()
	f.mu.Lock()
	snap := snapshot{Version: snapshotVersion}
	for _, r := range f.configs {
		snap.Configs = append(snap.Configs, r.m)
	}
	for _, c := range f.cursors {
		snap.Cursors = append(snap.Cursors, *c)
	}
	for _, r := range f.runs {
		snap.Runs = append(snap.Runs, *r)
	}
	for _, o := range f.schedules {
		snap.Schedules = append(snap.Schedules, *o)
	}
	for _, c := range f.cache {
		snap.Cache = append(snap.Cache, *c)
	}
	f.mu.Unlock()
	if f.enc != nil {
		for i := range snap.Configs {
			if len(snap.Configs[i].Credentials) == 0 {
				continue
			}
			ct, err := f.enc.Encrypt(snap.Configs[i].Credentials)
			if err != nil {
				return fmt.Errorf("memstore: encrypting credentials: %w", err)
			}
			snap.Configs[i].Credentials = ct
		}
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("memstore: encoding snapshot: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), filepath.Base(f.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("memstore: writing snapshot: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("memstore: writing snapshot: %w", err)
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("memstore: writing snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("memstore: writing snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("memstore: writing snapshot: %w", err)
	}
	if err := os.Rename(tmp.Name(), f.path); err != nil {
		return fmt.Errorf("memstore: replacing snapshot: %w", err)
	}
	return nil
}

// Save writes through to the file.
func (f *FileStore) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if err := f.Store.Save(ctx, cfg); err != nil {
		return err
	}
	return f.flush()
}

// SaveKeepingUnset writes through to the file.
func (f *FileStore) SaveKeepingUnset(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if err := f.Store.SaveKeepingUnset(ctx, cfg); err != nil {
		return err
	}
	return f.flush()
}

// SaveCredentialsPreserveEnabled writes through to the file.
func (f *FileStore) SaveCredentialsPreserveEnabled(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if err := f.Store.SaveCredentialsPreserveEnabled(ctx, cfg); err != nil {
		return err
	}
	return f.flush()
}

// SaveCredentialsEnablePreservingSettings writes through to the file.
func (f *FileStore) SaveCredentialsEnablePreservingSettings(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	if err := f.Store.SaveCredentialsEnablePreservingSettings(ctx, cfg); err != nil {
		return err
	}
	return f.flush()
}

// EnableConnectorWithSettings writes through to the file.
func (f *FileStore) EnableConnectorWithSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if err := f.Store.EnableConnectorWithSettings(ctx, orgID, connectorID, mutator); err != nil {
		return err
	}
	return f.flush()
}

// SaveCredentialsEnableWithSettings writes through to the file.
func (f *FileStore) SaveCredentialsEnableWithSettings(ctx context.Context, cfg *sdk.ConnectorConfig, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if err := f.Store.SaveCredentialsEnableWithSettings(ctx, cfg, mutator); err != nil {
		return err
	}
	return f.flush()
}

// Delete writes through to the file.
func (f *FileStore) Delete(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	if err := f.Store.Delete(ctx, orgID, connectorID); err != nil {
		return err
	}
	return f.flush()
}

// PatchSettings writes through to the file.
func (f *FileStore) PatchSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if err := f.Store.PatchSettings(ctx, orgID, connectorID, mutator); err != nil {
		return err
	}
	return f.flush()
}

// DisableConfigsForOrg writes through to the file.
func (f *FileStore) DisableConfigsForOrg(ctx context.Context, orgID string) (int64, error) {
	v, err := f.Store.DisableConfigsForOrg(ctx, orgID)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

// DisableIfCredentialsUnchanged writes through to the file.
func (f *FileStore) DisableIfCredentialsUnchanged(ctx context.Context, orgID string, connectorID sdk.ConnectorID, credentialsUpdatedAt time.Time) (bool, error) {
	v, err := f.Store.DisableIfCredentialsUnchanged(ctx, orgID, connectorID, credentialsUpdatedAt)
	if err != nil || !v {
		return v, err
	}
	return v, f.flush()
}

// SetEnabled writes through to the file.
func (f *FileStore) SetEnabled(ctx context.Context, orgID string, connectorID sdk.ConnectorID, enabled bool) error {
	if err := f.Store.SetEnabled(ctx, orgID, connectorID, enabled); err != nil {
		return err
	}
	return f.flush()
}

// MarkAuthRevoked writes through to the file.
func (f *FileStore) MarkAuthRevoked(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expected time.Time) (bool, error) {
	v, err := f.Store.MarkAuthRevoked(ctx, orgID, connectorID, expected)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

// ResetHealthState writes through to the file.
func (f *FileStore) ResetHealthState(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (bool, error) {
	v, err := f.Store.ResetHealthState(ctx, orgID, connectorID)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

// SaveCursor writes through to the file.
func (f *FileStore) SaveCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, cursor *sdk.Cursor) error {
	if err := f.Store.SaveCursor(ctx, orgID, connectorID, opID, cursor); err != nil {
		return err
	}
	return f.flush()
}

// DeleteByConnector writes through to the file.
func (f *FileStore) DeleteByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	if err := f.Store.DeleteByConnector(ctx, orgID, connectorID); err != nil {
		return err
	}
	return f.flush()
}

// Create writes through to the file.
func (f *FileStore) Create(ctx context.Context, run *model.SyncRun) error {
	if err := f.Store.Create(ctx, run); err != nil {
		return err
	}
	return f.flush()
}

// Update writes through to the file.
func (f *FileStore) Update(ctx context.Context, run *model.SyncRun) error {
	if err := f.Store.Update(ctx, run); err != nil {
		return err
	}
	return f.flush()
}

// DeleteRunsByConnector writes through to the file.
func (f *FileStore) DeleteRunsByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	if err := f.Store.DeleteRunsByConnector(ctx, orgID, connectorID); err != nil {
		return err
	}
	return f.flush()
}

// RenewLease writes through to the file.
func (f *FileStore) RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error {
	if err := f.Store.RenewLease(ctx, orgID, runID, lease); err != nil {
		return err
	}
	return f.flush()
}

// CloseAbandoned writes through to the file.
func (f *FileStore) CloseAbandoned(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (int64, error) {
	n, err := f.Store.CloseAbandoned(ctx, orgID, connectorID, opID)
	if err != nil || n == 0 {
		return n, err
	}
	return n, f.flush()
}

// Upsert writes through to the file.
func (f *FileStore) Upsert(ctx context.Context, o *model.ScheduleOverride) error {
	if err := f.Store.Upsert(ctx, o); err != nil {
		return err
	}
	return f.flush()
}

// UpdateRunTimestamps writes through to the file.
func (f *FileStore) UpdateRunTimestamps(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, lastRunAt, nextRunAt time.Time) error {
	if err := f.Store.UpdateRunTimestamps(ctx, orgID, connectorID, opID, lastRunAt, nextRunAt); err != nil {
		return err
	}
	return f.flush()
}

// PutMany writes through to the file.
func (f *FileStore) PutMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entries map[string]json.RawMessage) (int64, error) {
	v, err := f.Store.PutMany(ctx, orgID, connectorID, cacheType, entries)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

// DeleteMany writes through to the file.
func (f *FileStore) DeleteMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) error {
	if err := f.Store.DeleteMany(ctx, orgID, connectorID, cacheType, entryKeys); err != nil {
		return err
	}
	return f.flush()
}

// DeleteManyOlderThan writes through to the file.
func (f *FileStore) DeleteManyOlderThan(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string, olderThan time.Time) (int64, error) {
	v, err := f.Store.DeleteManyOlderThan(ctx, orgID, connectorID, cacheType, entryKeys, olderThan)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

// CompareAndSwapCredentials writes through to the file.
func (f *FileStore) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expected time.Time, credentials json.RawMessage) (bool, error) {
	v, err := f.Store.CompareAndSwapCredentials(ctx, orgID, connectorID, expected, credentials)
	if err != nil {
		return v, err
	}
	return v, f.flush()
}

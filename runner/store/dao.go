// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

//go:generate go tool mockgen -source=dao.go -destination=../../internal/mocks/runner/store/mock_dao.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// UndecryptableRowsError reports connector config rows that could not be read:
// their credentials did not decrypt (for example, written under another key)
// or their stored JSON did not decode. ListByOrg,
// ListByConnector and ListEnabledByConnector return it wrapped, together with
// the rows that did decrypt, so a caller that can act on the rest finds it with
// errors.As; a caller that treats any error as a failed list is unchanged.
type UndecryptableRowsError struct {
	OrgIDs []string // the organizations whose rows were left out
	Err    error    // the first decryption error
	// Rows names each row left out, with its settings when the row decoded
	// and only its credentials did not decrypt: settings are stored in plain
	// text, so a caller can still match a row on them.
	Rows []UnreadableRow
}

// UnreadableRow is a connector config row a list left out.
type UnreadableRow struct {
	OrganizationID string
	ConnectorID    sdk.ConnectorID
	Settings       json.RawMessage // nil when the row did not decode
}

func (e *UndecryptableRowsError) Error() string {
	return fmt.Sprintf("%d row(s) could not be read (first: %v)", len(e.OrgIDs), e.Err)
}

func (e *UndecryptableRowsError) Unwrap() error { return e.Err }

// IsNotFound reports whether err is a "row not found" outcome from a DAO Get.
// Callers outside this package should not import gorm directly; use
// this helper to discriminate genuine NotFound from other DB failures.
// It also recognizes the package sentinels (ErrCursorNotFound, ErrSyncRunNotFound).
func IsNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound) ||
		errors.Is(err, ErrCursorNotFound) ||
		errors.Is(err, ErrSyncRunNotFound)
}

// ErrCursorNotFound is returned by GetCursor when no cursor has been persisted
// yet for an operation (first run, or an empty cursor payload).
var ErrCursorNotFound = errors.New("connector: cursor not found")

// ErrCursorConflict is matched (errors.Is) by the *CursorConflictError
// SaveCursor returns when another writer saved the cursor after it was read.
var ErrCursorConflict = errors.New("connector: cursor conflict")

// CursorConflictError is SaveCursor's refusal to overwrite a cursor another
// writer saved after the caller read it: the stored version is no longer the
// one the caller's cursor carries. The stored cursor is left as it is.
type CursorConflictError struct {
	OrganizationID string
	ConnectorID    sdk.ConnectorID
	OperationID    sdk.OperationID
	// ReadVersion is the version the caller read and tried to save over.
	ReadVersion int64
}

func (e *CursorConflictError) Error() string {
	return fmt.Sprintf("connector: cursor of %s/%s for organization %s changed since version %d was read; not overwriting it",
		e.ConnectorID, e.OperationID, e.OrganizationID, e.ReadVersion)
}

// Is makes errors.Is(err, ErrCursorConflict) match.
func (e *CursorConflictError) Is(target error) bool { return target == ErrCursorConflict }

// ErrSyncRunNotFound is returned by GetLatest when no sync run exists yet for an operation.
var ErrSyncRunNotFound = errors.New("connector: sync run not found")

// ConnectorSyncState is a connector's data-freshness state for an organization.
// Only data-bearing operations count toward it. A discovery operation such as a
// repository listing does not.
type ConnectorSyncState int

const (
	// ConnectorSyncStateStillSyncing: no data operation has ever completed —
	// either one is currently running (first sync in progress) or none has
	// run yet.
	ConnectorSyncStateStillSyncing ConnectorSyncState = iota
	// ConnectorSyncStateSynced: at least one data operation has completed, and
	// none is currently failing.
	ConnectorSyncStateSynced
	// ConnectorSyncStateFailed: a data operation's most recent attempt is
	// Failed — a genuine problem (e.g. a revoked token, a rate limit), distinct
	// from a first sync still in progress. LastSuccessfulAt may still be set,
	// naming the last time that data synced before the failure.
	ConnectorSyncStateFailed
)

// ConnectorSyncStatus is one connector's true vendor-sync freshness for an
// org, aggregated across its data-bearing operations only. LastSuccessfulAt
// only advances on a run that is Status=Completed — a genuinely failed vendor
// fetch is marked Failed (with ErrorKind set) by the connector framework, so a
// Completed run is trustworthy evidence of a live sync regardless of how many
// records it emitted: zero is a legitimate answer for an idle-but-healthy
// connector, not a stall. Nil means the connector has never had a data
// operation complete.
type ConnectorSyncStatus struct {
	LastSuccessfulAt *time.Time
	State            ConnectorSyncState
}

// IConnectorConfigDAO manages per-organization connector configurations.
type IConnectorConfigDAO interface {
	Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error)
	// The three list methods below leave out a row that cannot be read (its
	// credentials do not decrypt or its JSON does not decode) and report it in
	// a wrapped *UndecryptableRowsError, returned with the rest.
	ListByOrg(ctx context.Context, orgID string) ([]*sdk.ConnectorConfig, error)
	// ListByConnector lists every organization's live row for the connector.
	ListByConnector(ctx context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error)
	ListEnabledByConnector(ctx context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error)
	// ListEnabledOrganizations returns the organizations with a live, enabled
	// row for the connector, sorted. It reads no credentials.
	ListEnabledOrganizations(ctx context.Context, connectorID sdk.ConnectorID) ([]string, error)
	// SaveKeepingUnset is Save, except that nil Settings or nil
	// OperationOverrides keep the live row's stored value in the same
	// statement (a first insert or a revival gets {}).
	SaveKeepingUnset(ctx context.Context, cfg *sdk.ConnectorConfig) error
	// The four methods below all write a connector_configs row and differ only in which
	// columns they leave alone, so pick by what the caller owns rather than by name
	// alone: Save writes everything; SaveCredentialsPreserveEnabled keeps `enabled`;
	// SaveCredentialsEnablePreservingSettings keeps `settings` and
	// `operation_overrides`; EnableConnectorWithSettings keeps `credentials`.
	//
	// DisableIfCredentialsUnchanged and SetEnabled, at the end of this
	// interface, write only `enabled`.
	//
	// credentials_updated_at is the credential fence. Every statement that writes
	// credentials advances it in the same statement: the three SaveCredentials*
	// methods always, Save when the credentials column changes (an encrypted value
	// always does), and CompareAndSwapCredentials on a swap. So a CAS or
	// MarkAuthRevoked fenced on a value read before a concurrent credential write
	// fails. None of these writes health_state: a connector paused as auth_revoked
	// stays paused after any of them; the unpause path is a credential write
	// followed by ResetHealthState, as auth/integrations and runner/app.WriteConfig
	// (the `config put` command) do.
	Save(ctx context.Context, cfg *sdk.ConnectorConfig) error
	// SaveCredentialsPreserveEnabled upserts credentials + settings without ever
	// overwriting an existing row's `enabled` column (first insert lands disabled).
	// Use instead of Get + Save on the credential-rotation path so a concurrent
	// enable (ConfirmConnectorRepos) is never clobbered back to disabled.
	SaveCredentialsPreserveEnabled(ctx context.Context, cfg *sdk.ConnectorConfig) error
	// SaveCredentialsEnablePreservingSettings upserts credentials and marks the connector enabled,
	// omitting settings and operation_overrides from the conflict update so a
	// credential rotation cannot lose a value it does not own. Use instead of
	// Get + Save on a one-step connect flow: there is no read, so there is no
	// window for a concurrent writer of those columns to be clobbered.
	SaveCredentialsEnablePreservingSettings(ctx context.Context, cfg *sdk.ConnectorConfig) error
	// EnableConnectorWithSettings writes mutated settings and flips enabled
	// false→true in one row-locked transaction, never touching `credentials`.
	// The enable-path analogue of SaveCredentialsPreserveEnabled: use instead of
	// Get + Save so a concurrent credential rotation cannot be clobbered. The
	// mutator returns the FULL settings JSON; empty bytes are an error, not a no-op.
	EnableConnectorWithSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error
	// SaveCredentialsEnableWithSettings writes credentials, mutated settings and
	// enabled=true in ONE row-locked transaction. Use it when the credential and the
	// settings must agree with each other: composing two of the calls above leaves a
	// window two concurrent connects can interleave through, ending with a pair neither
	// caller submitted while both are told they succeeded. The mutator sees the live
	// settings (nil on a first insert) and returns the FULL settings JSON.
	SaveCredentialsEnableWithSettings(ctx context.Context, cfg *sdk.ConnectorConfig, mutator func(current json.RawMessage) (json.RawMessage, error)) error
	Delete(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error

	// PatchSettings reads the live settings JSON inside a row-locked
	// transaction, hands the bytes to the mutator, and writes the result back.
	// Use instead of `Get` + `Save` whenever two operations might persist
	// disjoint parts of the same settings row concurrently — `fetch_prs` cache
	// + `fetch_commits` cache + observed rate-limit caps are the canonical
	// offenders. The mutator must return the FULL settings JSON (it sees the
	// current bytes, decodes, mutates, re-encodes).
	PatchSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error

	// DisableConfigsForOrg flips enabled true→false on every live config row
	// owned by orgID (deleted_at IS NULL), returning the rows changed.
	// Scheduling builds work only from enabled configs, so this stops the org's
	// scheduled syncs. Idempotent (0 rows when none are enabled); never touches
	// credentials or settings.
	DisableConfigsForOrg(ctx context.Context, orgID string) (int64, error)

	// MarkAuthRevoked atomically transitions health_state to auth_revoked, unless
	// the row is already in that state (returns transitioned=false, a no-op).
	// expectedCredentialsUpdatedAt fences the write against a concurrent
	// credential rewrite: the row must still carry this exact
	// credentials_updated_at, so a long-tail sync failure from before a
	// reconnect can never override the reconnect's fresh row. Callers use the
	// transitioned flag to fire a health-state-change signal only once per
	// actual transition.
	MarkAuthRevoked(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time) (transitioned bool, err error)

	// ResetHealthState sets health_state back to healthy and advances
	// credentials_updated_at unconditionally. Call on any event that proves the
	// connector's credentials are live again (a fresh OAuth credential write, an
	// explicit disconnect); transitioned reports only whether health_state
	// itself changed, not whether credentials_updated_at advanced (it always does).
	ResetHealthState(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (transitioned bool, err error)

	// CompareAndSwapCredentials replaces only the credentials of a live
	// (not soft-deleted) row, and only while the row still carries
	// expectedCredentialsUpdatedAt. On a swap it advances
	// credentials_updated_at. It never writes enabled, settings or
	// deleted_at, so a refresh that raced a disconnect or a reconnect loses
	// (swapped=false) instead of overwriting the newer row.
	CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time, credentials json.RawMessage) (swapped bool, err error)

	// DisableIfCredentialsUnchanged sets only enabled=false on a live row, and
	// only while the row still carries credentialsUpdatedAt, the fence
	// MarkAuthRevoked uses. It never writes credentials, settings,
	// operation_overrides or health_state, so a reconnect that landed after the
	// caller's read wins (disabled=false) and its fresh grant stays enabled.
	DisableIfCredentialsUnchanged(ctx context.Context, orgID string, connectorID sdk.ConnectorID, credentialsUpdatedAt time.Time) (disabled bool, err error)

	// SetEnabled writes only the enabled column of a live row, leaving
	// credentials, settings, operation_overrides and health_state alone, so a
	// concurrent credential or settings write survives. A missing row is
	// gorm.ErrRecordNotFound.
	SetEnabled(ctx context.Context, orgID string, connectorID sdk.ConnectorID, enabled bool) error
}

// ISyncStateDAO manages cursor persistence for incremental sync.
type ISyncStateDAO interface {
	// GetCursor returns the last-saved cursor for an operation.
	// Returns ErrCursorNotFound if no cursor exists (first run).
	// The returned cursor carries the stored Version.
	GetCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*sdk.Cursor, error)
	// SaveCursor is a compare-and-swap on cursor.Version, the version the
	// caller read (zero when GetCursor found none). It saves the cursor only
	// while the stored version still equals it, and then sets cursor.Version
	// to the new version. Otherwise it saves nothing and returns a
	// *CursorConflictError, so an older run cannot overwrite a newer cursor.
	SaveCursor(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, cursor *sdk.Cursor) error
	// DeleteByConnector removes all cursor state for a connector, triggering a fresh full sync.
	DeleteByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error
}

// ISyncRunDAO records execution history for audit and debugging.
type ISyncRunDAO interface {
	Create(ctx context.Context, run *model.SyncRun) error
	// Update writes a run's state while its stored row is still running.
	// When the row is gone or already closed (as abandoned, after its lease
	// expired), it writes nothing and returns ErrRunClosed, so a run that
	// lost its lease never overwrites the closed row.
	Update(ctx context.Context, run *model.SyncRun) error
	// GetLatest returns the most recent sync run for an operation.
	// Returns ErrSyncRunNotFound if no runs exist.
	GetLatest(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (*model.SyncRun, error)
	ListByOperation(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, limit int) ([]*model.SyncRun, error)
	// DeleteRunsByConnector deletes every sync run of a connector, including
	// stuck "running" ones. It removes the rows outright.
	DeleteRunsByConnector(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error
	// GetConnectorSyncStatusByOrg returns the per-connector sync-freshness
	// signal for orgID, keyed by connector_id, covering every connector with
	// at least one connector_sync_runs row for this org among its data-bearing
	// operations (see vcsDataOperationIDs) — a VCS connector's metadata/
	// discovery runs (e.g. a repo listing) never contribute. A connector
	// absent from the map has no such history at all; callers treat that the
	// same as ConnectorSyncStateStillSyncing with a nil LastSuccessfulAt.
	GetConnectorSyncStatusByOrg(ctx context.Context, orgID string) (map[sdk.ConnectorID]ConnectorSyncStatus, error)
	// RenewLease sets the lease of a run that is still running to expire
	// lease from now, by the store's clock (the database's, for Postgres), so
	// every process compares leases on one clock. A run that is gone or no
	// longer running is left alone and the call returns ErrRunClosed.
	RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error
	// CloseAbandoned closes every running run whose lease expired before the
	// store's clock now as failed, with error kind abandoned, and returns how
	// many it closed. An empty orgID spans every organization (it needs the
	// system scope); an empty connectorID or opID matches any. A run without
	// a lease is left.
	CloseAbandoned(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) (int64, error)
}

// ErrRunClosed is returned by Update and RenewLease when the run's row is
// gone or no longer running: it was closed as abandoned after its lease
// expired, so the run lost its lease.
var ErrRunClosed = errors.New("connector: the run's row is no longer running; its lease expired and it was closed as abandoned")

// AbandonedRunMessage is the error message CloseAbandoned records on a run
// whose lease expired at leaseExpiredAt.
func AbandonedRunMessage(leaseExpiredAt time.Time) string {
	return "the run's lease expired at " + leaseExpiredAt.UTC().Format(time.RFC3339) +
		": the process running it stopped without closing it"
}

// IFetchCacheDAO manages per-entity memoization rows in connector_fetch_cache.
// It is the raw storage layer; org/connector scoping, degrade-on-error, and the
// per-row size guard live in the framework FetchCache capability, not here.
type IFetchCacheDAO interface {
	// PutMany upserts one batch of entries for a single cache_type, moving
	// updated_at ONLY on a genuine value change (the eviction-safety invariant):
	// the ON CONFLICT DO UPDATE is guarded by
	// `value IS DISTINCT FROM EXCLUDED.value`, so re-writing an unchanged entry
	// is a no-op that preserves updated_at. Returns rows actually written
	// (RowsAffected — inserts + genuine updates, excluding value-guarded no-ops),
	// the churn signal the rows-written metric reports. Empty entries is a no-op.
	PutMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entries map[string]json.RawMessage) (int64, error)
	// GetMany returns the entries present for the given keys (a point lookup on
	// the PK); absent keys are simply missing from the result map (a cache miss).
	GetMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) (map[string]json.RawMessage, error)
	// ScanForEviction returns the rows of one cache_type whose updated_at predates
	// the cutoff, oldest-first — the age-basis candidate set a host's eviction job
	// deletes. Projects the key columns only (entry_key + updated_at, NOT the JSONB
	// value): callers evict by key, so value is never materialized.
	ScanForEviction(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, updatedBefore time.Time) ([]*model.FetchCache, error)
	// DeleteMany removes the given keys of one cache_type — it tombstones a key
	// whose fresh value was skipped as oversized, so GetMany forces a re-fetch rather
	// than returning the stale prior row. Absent keys are a no-op.
	DeleteMany(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string) error
	// DeleteManyOlderThan removes the given keys of one cache_type but ONLY the rows
	// still older than olderThan (the same cutoff used by the eviction scan). This
	// closes the eviction scan→delete TOCTOU: a row that was refreshed (its updated_at
	// bumped by a PutMany) between the scan and the delete is NOT deleted, so eviction
	// never drops a row that became hot after it was listed as a candidate. Returns
	// the count of rows actually deleted (RowsAffected). Absent keys are a no-op.
	DeleteManyOlderThan(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string, entryKeys []string, olderThan time.Time) (int64, error)
	// CountByType returns the rows currently held for one (org, connector, cache_type)
	// — the table-growth gauge's source. Zero rows returns (0, nil).
	CountByType(ctx context.Context, orgID string, connectorID sdk.ConnectorID, cacheType string) (int64, error)
}

// IScheduleDAO manages schedule overrides in the connector_schedules table.
type IScheduleDAO interface {

	// ListAll returns every live schedule override across all orgs, enabled
	// or not. The schedule source reads it: a disabled override pauses its
	// operation.
	ListAll(ctx context.Context) ([]*model.ScheduleOverride, error)

	// Upsert creates or updates a schedule override.
	Upsert(ctx context.Context, override *model.ScheduleOverride) error

	// UpdateRunTimestamps updates last_run_at and next_run_at.
	UpdateRunTimestamps(ctx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID, lastRunAt time.Time, nextRunAt time.Time) error
}

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
	"gorm.io/gorm/clause"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/tenantdb"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ConnectorConfigRepo implements IConnectorConfigDAO using PostgreSQL via GORM.
type ConnectorConfigRepo struct {
	db  *gorm.DB
	enc cryptutil.Encryptor // nil = no encryption (dev without key configured)
}

// NewConnectorConfigRepo creates a new ConnectorConfigRepo.
func NewConnectorConfigRepo(db *gorm.DB, enc cryptutil.Encryptor) *ConnectorConfigRepo {
	return &ConnectorConfigRepo{db: db, enc: enc}
}

func (r *ConnectorConfigRepo) encryptCreds(creds json.RawMessage) (json.RawMessage, error) {
	if r.enc == nil || len(creds) == 0 {
		return creds, nil
	}
	encrypted, err := r.enc.Encrypt(creds)
	if err != nil {
		return nil, fmt.Errorf("encrypt credentials: %w", err)
	}
	return encrypted, nil
}

// ErrCredentialsEncrypted is returned when a repo with no encryptor reads a
// row whose credentials are a ciphertext envelope: handing the envelope on as
// plaintext would run the connector with no token.
var ErrCredentialsEncrypted = errors.New("credentials are encrypted; set TETRIZ_CREDENTIALS_KEY")

func (r *ConnectorConfigRepo) decryptCreds(creds json.RawMessage) (json.RawMessage, error) {
	if len(creds) == 0 {
		return creds, nil
	}
	if r.enc == nil {
		if cryptutil.IsEnvelope(creds) {
			return nil, ErrCredentialsEncrypted
		}
		return creds, nil
	}
	decrypted, err := r.enc.Decrypt(creds)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	return decrypted, nil
}

func (r *ConnectorConfigRepo) Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "Get", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var m model.ConnectorConfigModel
	err := db.Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
		First(&m).Error
	if err != nil {
		return nil, err
	}
	cfg, err := m.ToFramework()
	if err != nil {
		return nil, fmt.Errorf("converting connector config model: %w", err)
	}
	cfg.Credentials, err = r.decryptCreds(cfg.Credentials)
	if err != nil {
		return nil, fmt.Errorf("decrypting credentials: %w", err)
	}
	return cfg, nil
}

func (r *ConnectorConfigRepo) ListByOrg(ctx context.Context, orgID string) ([]*sdk.ConnectorConfig, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "ListByOrg", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var models []model.ConnectorConfigModel
	err := db.Where("organization_id = ? AND deleted_at IS NULL", orgID).Find(&models).Error
	if err != nil {
		return nil, err
	}
	return r.readRows(models)
}

func (r *ConnectorConfigRepo) ListByConnector(ctx context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "ListByConnector", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var models []model.ConnectorConfigModel
	err := db.Where("connector_id = ? AND deleted_at IS NULL", connectorID).Find(&models).Error
	if err != nil {
		return nil, err
	}
	return r.readRows(models)
}

// readRows decodes each row and decrypts its credentials, row by row. A row
// whose stored JSON does not decode or whose credentials do not decrypt is left
// out and named in an *UndecryptableRowsError, returned wrapped with the rows
// that could be read, so one tenant's row cannot fail a list for the rest.
func (r *ConnectorConfigRepo) readRows(models []model.ConnectorConfigModel) ([]*sdk.ConnectorConfig, error) {
	out := make([]*sdk.ConnectorConfig, 0, len(models))
	var unreadable *UndecryptableRowsError
	skip := func(row UnreadableRow, err error) {
		if unreadable == nil {
			unreadable = &UndecryptableRowsError{Err: err}
		}
		unreadable.OrgIDs = append(unreadable.OrgIDs, row.OrganizationID)
		unreadable.Rows = append(unreadable.Rows, row)
	}
	for i := range models {
		cfg, err := models[i].ToFramework()
		if err != nil {
			skip(UnreadableRow{OrganizationID: models[i].OrganizationID, ConnectorID: models[i].ConnectorID}, fmt.Errorf("converting connector config model: %w", err))
			continue
		}
		creds, decErr := r.decryptCreds(cfg.Credentials)
		if decErr != nil {
			skip(UnreadableRow{OrganizationID: cfg.OrganizationID, ConnectorID: cfg.ConnectorID, Settings: cfg.Settings}, fmt.Errorf("decrypting credentials: %w", decErr))
			continue
		}
		cfg.Credentials = creds
		out = append(out, cfg)
	}
	if unreadable != nil {
		return out, fmt.Errorf("reading connector configs: %w", unreadable)
	}
	return out, nil
}

// ListEnabledOrganizations returns the organizations whose row for the
// connector is live and enabled. It reads no credentials, so no row's
// encryption can fail it.
func (r *ConnectorConfigRepo) ListEnabledOrganizations(ctx context.Context, connectorID sdk.ConnectorID) ([]string, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "ListEnabledOrganizations", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var orgIDs []string
	err := db.Model(&model.ConnectorConfigModel{}).
		Where("connector_id = ? AND enabled = true AND deleted_at IS NULL", connectorID).
		Order("organization_id").
		Pluck("organization_id", &orgIDs).Error
	if err != nil {
		return nil, fmt.Errorf("list enabled organizations: %w", err)
	}
	return orgIDs, nil
}

func (r *ConnectorConfigRepo) ListEnabledByConnector(ctx context.Context, connectorID sdk.ConnectorID) ([]*sdk.ConnectorConfig, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "ListEnabledByConnector", time.Now())
	db := tenantdb.DB(ctx, r.db)
	var models []model.ConnectorConfigModel
	err := db.Where("connector_id = ? AND enabled = true AND deleted_at IS NULL", connectorID).Find(&models).Error
	if err != nil {
		return nil, err
	}
	return r.readRows(models)
}

func (r *ConnectorConfigRepo) Save(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "Save", time.Now())
	db := tenantdb.DB(ctx, r.db)
	m, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	encryptedCreds, err := r.encryptCreds(m.Credentials)
	if err != nil {
		return fmt.Errorf("encrypting credentials: %w", err)
	}
	now := time.Now().UTC()
	m.UpdatedAt = now
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	row := map[string]any{
		"connector_id":        m.ConnectorID,
		"organization_id":     m.OrganizationID,
		"credentials":         encryptedCreds,
		"settings":            m.Settings,
		"operation_overrides": m.OperationOverrides,
		"enabled":             m.Enabled,
		"created_at":          m.CreatedAt,
		"updated_at":          m.UpdatedAt,
		"deleted_at":          nil,
		// credentials_updated_at is the credential fence: every statement that writes
		// the credentials advances it, so a CAS or MarkAuthRevoked fenced on an earlier
		// read fails. An encrypted value carries a fresh nonce, so an encrypted write
		// always changes the column.
		"credentials_updated_at": now,
	}
	return db.Model(&model.ConnectorConfigModel{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"credentials":         encryptedCreds,
				"settings":            m.Settings,
				"operation_overrides": m.OperationOverrides,
				"enabled":             m.Enabled,
				"updated_at":          m.UpdatedAt,
				"deleted_at":          nil,
				"credentials_updated_at": gorm.Expr(
					"CASE WHEN connector_configs.credentials IS DISTINCT FROM EXCLUDED.credentials THEN ?::timestamptz ELSE connector_configs.credentials_updated_at END", now),
			}),
		}).Create(row).Error
}

// SaveKeepingUnset is Save, except that nil Settings or nil OperationOverrides
// keep the stored value of a live row, inside the same statement: no read
// precedes the write, so a concurrent settings write is not lost. A first
// insert, or a row revived from a disconnect, gets {} for an unset column.
func (r *ConnectorConfigRepo) SaveKeepingUnset(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "SaveKeepingUnset", time.Now())
	db := tenantdb.DB(ctx, r.db)
	keepSettings, keepOverrides := cfg.Settings == nil, cfg.OperationOverrides == nil
	m, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	if keepSettings {
		m.Settings = json.RawMessage("{}")
	}
	encryptedCreds, err := r.encryptCreds(m.Credentials)
	if err != nil {
		return fmt.Errorf("encrypting credentials: %w", err)
	}
	now := time.Now().UTC()
	m.UpdatedAt = now
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	row := map[string]any{
		"connector_id":           m.ConnectorID,
		"organization_id":        m.OrganizationID,
		"credentials":            encryptedCreds,
		"settings":               m.Settings,
		"operation_overrides":    m.OperationOverrides,
		"enabled":                m.Enabled,
		"created_at":             m.CreatedAt,
		"updated_at":             m.UpdatedAt,
		"deleted_at":             nil,
		"credentials_updated_at": now,
	}
	// SET expressions see the row as it was before this statement, so the CASE
	// on deleted_at tells a live row (keep) from a revived one (take {}).
	column := func(name string, keep bool, incoming any) any {
		if !keep {
			return incoming
		}
		return gorm.Expr("CASE WHEN connector_configs.deleted_at IS NOT NULL THEN ? ELSE connector_configs."+name+" END", incoming)
	}
	return db.Model(&model.ConnectorConfigModel{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"credentials":         encryptedCreds,
				"settings":            column("settings", keepSettings, m.Settings),
				"operation_overrides": column("operation_overrides", keepOverrides, m.OperationOverrides),
				"enabled":             m.Enabled,
				"updated_at":          m.UpdatedAt,
				"deleted_at":          nil,
				"credentials_updated_at": gorm.Expr(
					"CASE WHEN connector_configs.credentials IS DISTINCT FROM EXCLUDED.credentials THEN ?::timestamptz ELSE connector_configs.credentials_updated_at END", now),
			}),
		}).Create(row).Error
}

// SaveCredentialsPreserveEnabled upserts a connector's credentials + settings
// without ever overwriting the live `enabled` column on an existing row. On a
// first insert (no existing row) the connector lands disabled — a later
// ConfirmConnectorRepos enables it. On a re-submit (PAT rotation) the row's
// current `enabled` is preserved verbatim because the conflict-update set omits
// it, so the whole credential swap is one atomic statement.
//
// It is one statement, so a concurrent ConfirmConnectorRepos that sets
// enabled=true is never overwritten.
func (r *ConnectorConfigRepo) SaveCredentialsPreserveEnabled(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "SaveCredentialsPreserveEnabled", time.Now())
	db := tenantdb.DB(ctx, r.db)
	m, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	encryptedCreds, err := r.encryptCreds(m.Credentials)
	if err != nil {
		return fmt.Errorf("encrypting credentials: %w", err)
	}
	now := time.Now().UTC()
	m.UpdatedAt = now
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	// First-insert value for `enabled`: a freshly-pasted credential is disabled
	// until confirmed. On conflict this column is left untouched (not in DoUpdates),
	// so an already-enabled connector stays enabled.
	row := map[string]any{
		"connector_id":           m.ConnectorID,
		"organization_id":        m.OrganizationID,
		"credentials":            encryptedCreds,
		"settings":               m.Settings,
		"enabled":                false,
		"created_at":             m.CreatedAt,
		"updated_at":             m.UpdatedAt,
		"deleted_at":             nil,
		"credentials_updated_at": now,
	}
	return db.Model(&model.ConnectorConfigModel{}).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"credentials", "settings", "updated_at", "deleted_at", "credentials_updated_at"}),
		}).Create(row).Error
}

// SaveCredentialsEnablePreservingSettings upserts a connector's credentials and marks it
// enabled. It keeps `settings` and `operation_overrides` on a rotation and takes the
// caller's values on a revival, in one statement. Use it for a connect flow that completes
// in one step; a flow with a later election step uses SaveCredentialsPreserveEnabled,
// which leaves `enabled` alone.
func (r *ConnectorConfigRepo) SaveCredentialsEnablePreservingSettings(ctx context.Context, cfg *sdk.ConnectorConfig) error {
	// A rotation keeps settings and overrides without reading them first: a read then
	// write back would lose whatever else wrote the row in between. A revival takes the
	// caller's values, because a disconnect tore the configuration down; a stale schedule
	// override carried across it would change how often the vendor is called. The
	// conflict target is the primary key with no `deleted_at` predicate, so both cases
	// land here and the column expressions tell them apart.
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "SaveCredentialsEnablePreservingSettings", time.Now())
	db := tenantdb.DB(ctx, r.db)
	m, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	encryptedCreds, err := r.encryptCreds(m.Credentials)
	if err != nil {
		return fmt.Errorf("encrypting credentials: %w", err)
	}
	now := time.Now().UTC()
	m.UpdatedAt = now
	if m.CreatedAt.IsZero() {
		m.CreatedAt = now
	}
	row := map[string]any{
		"connector_id":           m.ConnectorID,
		"organization_id":        m.OrganizationID,
		"credentials":            encryptedCreds,
		"settings":               m.Settings,
		"operation_overrides":    m.OperationOverrides,
		"enabled":                true,
		"created_at":             m.CreatedAt,
		"updated_at":             m.UpdatedAt,
		"deleted_at":             nil,
		"credentials_updated_at": now,
	}
	// Postgres evaluates every SET expression against the row as it was before this
	// statement, so a CASE on deleted_at still sees the pre-revival value even though
	// the same UPDATE clears it.
	keepUnlessRevived := func(column string, incoming any) clause.Expr {
		return gorm.Expr(
			"CASE WHEN connector_configs.deleted_at IS NOT NULL THEN ? ELSE connector_configs."+column+" END",
			incoming)
	}
	return db.Model(&model.ConnectorConfigModel{}).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"credentials":            encryptedCreds,
				"credentials_updated_at": now,
				"enabled":                true,
				"updated_at":             m.UpdatedAt,
				"deleted_at":             nil,
				"settings":               keepUnlessRevived("settings", m.Settings),
				"operation_overrides":    keepUnlessRevived("operation_overrides", m.OperationOverrides),
			}),
		}).Create(row).Error
}

// PatchSettings mutates only the `settings` JSONB column. The row is locked
// with `FOR UPDATE` for the transaction, so concurrent callers serialize and
// each mutator sees the previous caller's settings.
//
// The mutator MUST be deterministic and side-effect-free aside from the
// returned bytes — it may be invoked under a held DB lock, so blocking IO
// or fan-out from inside the callback will hold the lock and starve peers.
func (r *ConnectorConfigRepo) PatchSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "PatchSettings", time.Now())
	if mutator == nil {
		return fmt.Errorf("PatchSettings: mutator is nil")
	}
	db := tenantdb.DB(ctx, r.db)
	if err := db.Transaction(func(tx *gorm.DB) error {
		var m model.ConnectorConfigModel
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
			First(&m).Error
		if err != nil {
			return fmt.Errorf("PatchSettings: load row for update: %w", err)
		}
		newSettings, err := mutator(m.Settings)
		if err != nil {
			return fmt.Errorf("PatchSettings: mutator: %w", err)
		}
		if len(newSettings) == 0 {
			// Empty settings would null out the column unintentionally.
			// Treat as a no-op so callers can short-circuit cheaply.
			return nil
		}
		return tx.Model(&model.ConnectorConfigModel{}).
			Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
			Updates(map[string]any{
				"settings":   newSettings,
				"updated_at": time.Now().UTC(),
			}).Error
	}); err != nil {
		return fmt.Errorf("patching connector settings: %w", err)
	}
	return nil
}

// EnableConnectorWithSettings is the enable-path analogue of
// SaveCredentialsPreserveEnabled: it writes the mutated `settings` JSONB and
// flips `enabled` false→true in one row-locked transaction, and never touches
// the `credentials` column. The settings write and the enable land in one
// row-locked transaction, so a concurrent credential rotation is never
// overwritten.
//
// The mutator receives the live settings bytes and must return the FULL
// settings JSON (decode, mutate, re-encode). It MUST be deterministic and
// side-effect-free aside from the returned bytes — it runs under a held DB lock,
// so blocking IO inside it starves peers. Returning empty bytes is an error, not
// a no-op: enable must persist, so refusing keeps a connector from being enabled
// with a nulled-out settings column.
func (r *ConnectorConfigRepo) EnableConnectorWithSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "EnableConnectorWithSettings", time.Now())
	if mutator == nil {
		return fmt.Errorf("EnableConnectorWithSettings: mutator is nil")
	}
	db := tenantdb.DB(ctx, r.db)
	if err := db.Transaction(func(tx *gorm.DB) error {
		var m model.ConnectorConfigModel
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
			First(&m).Error
		if err != nil {
			return fmt.Errorf("EnableConnectorWithSettings: load row for update: %w", err)
		}
		newSettings, err := mutator(m.Settings)
		if err != nil {
			return fmt.Errorf("EnableConnectorWithSettings: mutator: %w", err)
		}
		if len(newSettings) == 0 {
			return fmt.Errorf("EnableConnectorWithSettings: refusing to enable with empty settings")
		}
		return tx.Model(&model.ConnectorConfigModel{}).
			Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
			Updates(map[string]any{
				"settings":   newSettings,
				"enabled":    true,
				"updated_at": time.Now().UTC(),
			}).Error
	}); err != nil {
		return fmt.Errorf("enabling connector with settings: %w", err)
	}
	return nil
}

// SaveCredentialsEnableWithSettings writes credentials, mutated settings and enabled=true
// in ONE row-locked transaction.
//
// It holds a row lock, so the stored credentials and settings always come from
// the same caller.
//
// Use it for a connect flow whose credential and settings must agree with each other. A
// rotation that touches only the credential still wants SaveCredentialsEnablePreservingSettings,
// which needs no lock at all.
//
// The mutator receives the live settings bytes (nil on a first insert) and must return the
// FULL settings JSON. It runs under a held row lock, so it must be deterministic and free of
// blocking IO. Returning empty bytes is an error rather than a no-op: enable must persist,
// and a nulled-out settings column would leave the connector enabled with nothing to sync.
func (r *ConnectorConfigRepo) SaveCredentialsEnableWithSettings(ctx context.Context, cfg *sdk.ConnectorConfig, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "SaveCredentialsEnableWithSettings", time.Now())
	if mutator == nil {
		return fmt.Errorf("SaveCredentialsEnableWithSettings: mutator is nil")
	}
	m, err := model.ConnectorConfigModelFromFramework(cfg)
	if err != nil {
		return fmt.Errorf("building connector config model: %w", err)
	}
	encryptedCreds, err := r.encryptCreds(m.Credentials)
	if err != nil {
		return fmt.Errorf("encrypting credentials: %w", err)
	}

	db := tenantdb.DB(ctx, r.db)
	if err := db.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		var existing model.ConnectorConfigModel
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", m.OrganizationID, m.ConnectorID).
			First(&existing).Error
		switch {
		case err == nil:
			// A live row: the mutator sees what is stored, so a setting this caller does not
			// own survives.
			newSettings, mErr := mutator(existing.Settings)
			if mErr != nil {
				return fmt.Errorf("SaveCredentialsEnableWithSettings: mutator: %w", mErr)
			}
			if len(newSettings) == 0 {
				return fmt.Errorf("SaveCredentialsEnableWithSettings: refusing to enable with empty settings")
			}
			return tx.Model(&model.ConnectorConfigModel{}).
				Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", m.OrganizationID, m.ConnectorID).
				Updates(map[string]any{
					"credentials":            encryptedCreds,
					"credentials_updated_at": now,
					"settings":               newSettings,
					"enabled":                true,
					"updated_at":             now,
				}).Error
		case errors.Is(err, gorm.ErrRecordNotFound):
			// No live row: a first connect, or a revival after a disconnect. The mutator is
			// handed nil so it builds the settings from scratch rather than inheriting a
			// torn-down configuration's.
			newSettings, mErr := mutator(nil)
			if mErr != nil {
				return fmt.Errorf("SaveCredentialsEnableWithSettings: mutator: %w", mErr)
			}
			if len(newSettings) == 0 {
				return fmt.Errorf("SaveCredentialsEnableWithSettings: refusing to enable with empty settings")
			}
			row := map[string]any{
				"connector_id":           m.ConnectorID,
				"organization_id":        m.OrganizationID,
				"credentials":            encryptedCreds,
				"settings":               newSettings,
				"enabled":                true,
				"operation_overrides":    json.RawMessage("{}"),
				"created_at":             now,
				"updated_at":             now,
				"deleted_at":             nil,
				"credentials_updated_at": now,
			}
			// The conflict target carries no deleted_at predicate, so a soft-deleted row is
			// revived by this branch rather than colliding with it — and a revival must CLEAR
			// operation_overrides rather than inherit them.
			//
			// A disconnect tore the configuration down, so a reconnect is a fresh start. An
			// override left behind wins over the descriptor's default: a stale
			// {"enabled": false} would survive the disconnect, this call would return success
			// with the row globally enabled, and no schedule would ever be emitted. Connected,
			// and never syncing, with nothing failing. The credential-save path
			// beside this one resets the column for the same reason.
			return tx.Model(&model.ConnectorConfigModel{}).
				Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "organization_id"}, {Name: "connector_id"}},
					DoUpdates: clause.AssignmentColumns([]string{
						"credentials", "settings", "enabled", "operation_overrides", "updated_at", "deleted_at", "credentials_updated_at",
					}),
				}).Create(row).Error
		default:
			return fmt.Errorf("SaveCredentialsEnableWithSettings: load row for update: %w", err)
		}
	}); err != nil {
		return fmt.Errorf("saving credentials and settings: %w", err)
	}
	return nil
}

// DisableConfigsForOrg flips enabled true→false on every live connector config
// row owned by orgID, returning the number of rows changed. Scheduling builds
// per-tenant work only from enabled configs, so this stops all of the org's
// scheduled syncs. Idempotent: an org with no enabled rows is a no-op (0 rows). Never
// touches credentials or settings — the credential stays intact so a future
// re-activation needs no re-connect.
func (r *ConnectorConfigRepo) DisableConfigsForOrg(ctx context.Context, orgID string) (int64, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "DisableConfigsForOrg", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND enabled = true AND deleted_at IS NULL", orgID).
		Updates(map[string]any{
			"enabled":    false,
			"updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return 0, fmt.Errorf("disabling connector configs for org: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// DisableIfCredentialsUnchanged sets enabled=false on the live row only while
// it still carries credentialsUpdatedAt, the fence MarkAuthRevoked uses. One
// UPDATE, so nothing another writer stored after the caller's read is undone.
func (r *ConnectorConfigRepo) DisableIfCredentialsUnchanged(ctx context.Context, orgID string, connectorID sdk.ConnectorID, credentialsUpdatedAt time.Time) (bool, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "DisableIfCredentialsUnchanged", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL AND credentials_updated_at = ?",
			orgID, connectorID, credentialsUpdatedAt).
		Updates(map[string]any{
			"enabled":    false,
			"updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return false, fmt.Errorf("disabling connector config: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// SetEnabled writes only enabled on the live row. A missing row is
// gorm.ErrRecordNotFound.
func (r *ConnectorConfigRepo) SetEnabled(ctx context.Context, orgID string, connectorID sdk.ConnectorID, enabled bool) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "SetEnabled", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
		Updates(map[string]any{
			"enabled":    enabled,
			"updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return fmt.Errorf("setting connector enabled: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// MarkAuthRevoked transitions health_state to auth_revoked, scoped so the
// WHERE clause excludes rows already in that state — a second call after the
// orchestrator gate has already started skipping the connector is a genuine
// no-op (transitioned=false), not a repeat write. expectedCredentialsUpdatedAt
// additionally fences the write to the exact credential version the caller
// loaded: a sync run that started before a concurrent credential rewrite
// (reconnect) must not be able to re-revoke the rewritten row once its own
// stale failure is classified. The fence is credentials_updated_at, so a
// settings write earlier in the same run does not block the revocation.
func (r *ConnectorConfigRepo) MarkAuthRevoked(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time) (bool, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "MarkAuthRevoked", time.Now())
	db := tenantdb.DB(ctx, r.db)
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND health_state != ? AND credentials_updated_at = ?",
			orgID, connectorID, sdk.HealthStateAuthRevoked, expectedCredentialsUpdatedAt).
		Updates(map[string]any{
			"health_state":            sdk.HealthStateAuthRevoked,
			"health_state_updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return false, fmt.Errorf("marking connector auth-revoked: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// ResetHealthState restores health_state to healthy and advances
// credentials_updated_at unconditionally. Call it right after a credential
// write. It reports whether health_state changed.
func (r *ConnectorConfigRepo) ResetHealthState(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (bool, error) {
	// The two columns are updated in separate statements because their conditions
	// differ: health_state only when not already healthy, to report a genuine
	// transition; credentials_updated_at always, so the freshness fence advances.
	// Each statement is atomic, and a MarkAuthRevoked landing between them only
	// sees the result of whichever ran first.
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "ResetHealthState", time.Now())
	db := tenantdb.DB(ctx, r.db)
	if err := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ?", orgID, connectorID).
		Update("credentials_updated_at", time.Now().UTC()).Error; err != nil {
		return false, fmt.Errorf("bumping connector credentials_updated_at: %w", err)
	}
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND health_state != ?", orgID, connectorID, sdk.HealthStateHealthy).
		Updates(map[string]any{
			"health_state":            sdk.HealthStateHealthy,
			"health_state_updated_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return false, fmt.Errorf("resetting connector health state: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

func (r *ConnectorConfigRepo) Delete(ctx context.Context, orgID string, connectorID sdk.ConnectorID) error {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "Delete", time.Now())
	db := tenantdb.DB(ctx, r.db)
	return db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL", orgID, connectorID).
		Update("deleted_at", time.Now().UTC()).Error
}

// CompareAndSwapCredentials implements IConnectorConfigDAO. The fence is
// credentials_updated_at, the column only credential writes advance.
func (r *ConnectorConfigRepo) CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time, credentials json.RawMessage) (bool, error) {
	defer metricutil.TrackDuration("runner/store", "ConnectorConfigRepo", "CompareAndSwapCredentials", time.Now())
	db := tenantdb.DB(ctx, r.db)
	encrypted, err := r.encryptCreds(credentials)
	if err != nil {
		return false, fmt.Errorf("encrypting credentials: %w", err)
	}
	// Microseconds, the column's precision, so the fence recorded below is
	// exactly the stored one.
	now := time.Now().UTC().Truncate(time.Microsecond)
	res := db.Model(&model.ConnectorConfigModel{}).
		Where("organization_id = ? AND connector_id = ? AND deleted_at IS NULL AND credentials_updated_at = ?",
			orgID, connectorID, expectedCredentialsUpdatedAt).
		Updates(map[string]any{
			"credentials":            encrypted,
			"credentials_updated_at": now,
			"updated_at":             now,
		})
	if res.Error != nil {
		return false, fmt.Errorf("swapping connector credentials: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return false, nil
	}
	RecordCredentialSwap(ctx, orgID, connectorID, now)
	return true, nil
}

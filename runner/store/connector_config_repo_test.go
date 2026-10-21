// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func newTestConnectorConfig(orgID string, connectorID sdk.ConnectorID) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		OrganizationID: orgID,
		ConnectorID:    connectorID,
		Credentials:    json.RawMessage(`{"token":"secret-token","refresh":"refresh-token"}`),
		Settings:       json.RawMessage(`{"workspace":"engineering"}`),
		Enabled:        true,
	}
}

func fetchStoredCredentials(t *testing.T, db *gorm.DB, orgID string, connectorID sdk.ConnectorID) string {
	t.Helper()

	var stored string
	err := db.Raw(
		"SELECT credentials::text FROM connector_configs WHERE organization_id = ? AND connector_id = ?",
		orgID,
		connectorID,
	).Row().Scan(&stored)
	require.NoError(t, err)
	return stored
}

func fetchStoredEnabled(t *testing.T, db *gorm.DB, orgID string, connectorID sdk.ConnectorID) bool {
	t.Helper()

	var enabled bool
	err := db.Raw(
		"SELECT enabled FROM connector_configs WHERE organization_id = ? AND connector_id = ?",
		orgID,
		connectorID,
	).Row().Scan(&enabled)
	require.NoError(t, err)
	return enabled
}

func fetchStoredHealthState(t *testing.T, db *gorm.DB, orgID string, connectorID sdk.ConnectorID) string {
	t.Helper()

	var healthState string
	err := db.Raw(
		"SELECT health_state FROM connector_configs WHERE organization_id = ? AND connector_id = ?",
		orgID,
		connectorID,
	).Row().Scan(&healthState)
	require.NoError(t, err)
	return healthState
}

func fetchStoredCredentialsUpdatedAt(t *testing.T, db *gorm.DB, orgID string, connectorID sdk.ConnectorID) time.Time {
	t.Helper()

	var credentialsUpdatedAt time.Time
	err := db.Raw(
		"SELECT credentials_updated_at FROM connector_configs WHERE organization_id = ? AND connector_id = ?",
		orgID,
		connectorID,
	).Row().Scan(&credentialsUpdatedAt)
	require.NoError(t, err)
	return credentialsUpdatedAt
}

func TestConnectorConfigRepo_SaveAndReadCredentials(t *testing.T) {
	t.Parallel()

	encryptor, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)

	tests := []struct {
		name              string
		encryptor         cryptutil.Encryptor
		assertStoredValue func(t *testing.T, stored string)
	}{
		{
			name:      "stores plaintext when encryptor is nil",
			encryptor: nil,
			assertStoredValue: func(t *testing.T, stored string) {
				t.Helper()
				assert.JSONEq(t, `{"token":"secret-token","refresh":"refresh-token"}`, stored)
			},
		},
		{
			name:      "stores ciphertext envelope when encryptor is configured",
			encryptor: encryptor,
			assertStoredValue: func(t *testing.T, stored string) {
				t.Helper()
				assert.NotEqual(t, `{"token":"secret-token","refresh":"refresh-token"}`, stored)

				var env map[string]any
				require.NoError(t, json.Unmarshal([]byte(stored), &env))
				assert.Equal(t, float64(1), env["v"])
				assert.NotEmpty(t, env["ct"])
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx := newTestTx(t)
			repo := store.NewConnectorConfigRepo(tx, tt.encryptor)
			cfg := newTestConnectorConfig("org-save-read", "github")

			require.NoError(t, repo.Save(context.Background(), cfg))

			stored := fetchStoredCredentials(t, tx, cfg.OrganizationID, cfg.ConnectorID)
			tt.assertStoredValue(t, stored)

			got, err := repo.Get(context.Background(), cfg.OrganizationID, cfg.ConnectorID)
			require.NoError(t, err)
			assert.JSONEq(t, string(cfg.Credentials), string(got.Credentials))
			assert.JSONEq(t, string(cfg.Settings), string(got.Settings))
		})
	}
}

func TestConnectorConfigRepo_ListMethodsDecryptCredentials(t *testing.T) {
	t.Parallel()

	encryptor, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)

	type fetchFn func(ctx context.Context, repo *store.ConnectorConfigRepo) ([]*sdk.ConnectorConfig, error)

	tests := []struct {
		name         string
		seed         func(t *testing.T, repo *store.ConnectorConfigRepo) map[string]json.RawMessage
		fetch        fetchFn
		expectedOrgs []string
	}{
		{
			name: "ListByOrg",
			seed: func(t *testing.T, repo *store.ConnectorConfigRepo) map[string]json.RawMessage {
				t.Helper()

				first := newTestConnectorConfig("org-list", "github")
				second := newTestConnectorConfig("org-list", "gitlab")
				second.Credentials = json.RawMessage(`{"token":"gitlab-token","refresh":"gitlab-refresh"}`)

				require.NoError(t, repo.Save(context.Background(), first))
				require.NoError(t, repo.Save(context.Background(), second))

				return map[string]json.RawMessage{
					string(first.ConnectorID):  first.Credentials,
					string(second.ConnectorID): second.Credentials,
				}
			},
			fetch: func(ctx context.Context, repo *store.ConnectorConfigRepo) ([]*sdk.ConnectorConfig, error) {
				return repo.ListByOrg(ctx, "org-list")
			},
			expectedOrgs: []string{"org-list", "org-list"},
		},
		{
			name: "ListByConnector",
			seed: func(t *testing.T, repo *store.ConnectorConfigRepo) map[string]json.RawMessage {
				t.Helper()

				first := newTestConnectorConfig("org-list-a", "github")
				second := newTestConnectorConfig("org-list-b", "github")
				second.Credentials = json.RawMessage(`{"token":"other-token","refresh":"other-refresh"}`)

				require.NoError(t, repo.Save(context.Background(), first))
				require.NoError(t, repo.Save(context.Background(), second))

				return map[string]json.RawMessage{
					first.OrganizationID:  first.Credentials,
					second.OrganizationID: second.Credentials,
				}
			},
			fetch: func(ctx context.Context, repo *store.ConnectorConfigRepo) ([]*sdk.ConnectorConfig, error) {
				return repo.ListByConnector(ctx, "github")
			},
			expectedOrgs: []string{"org-list-a", "org-list-b"},
		},
		{
			name: "ListEnabledByConnector",
			seed: func(t *testing.T, repo *store.ConnectorConfigRepo) map[string]json.RawMessage {
				t.Helper()

				enabled := newTestConnectorConfig("org-enabled", "github")
				disabled := newTestConnectorConfig("org-disabled", "github")
				disabled.Credentials = json.RawMessage(`{"token":"disabled-token","refresh":"disabled-refresh"}`)
				disabled.Enabled = false

				require.NoError(t, repo.Save(context.Background(), enabled))
				require.NoError(t, repo.Save(context.Background(), disabled))

				return map[string]json.RawMessage{
					enabled.OrganizationID: enabled.Credentials,
				}
			},
			fetch: func(ctx context.Context, repo *store.ConnectorConfigRepo) ([]*sdk.ConnectorConfig, error) {
				return repo.ListEnabledByConnector(ctx, "github")
			},
			expectedOrgs: []string{"org-enabled"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tx := newTestTx(t)
			repo := store.NewConnectorConfigRepo(tx, encryptor)
			expectedCreds := tt.seed(t, repo)

			got, err := tt.fetch(context.Background(), repo)
			require.NoError(t, err)
			require.Len(t, got, len(tt.expectedOrgs))
			for _, cfg := range got {
				key := cfg.OrganizationID
				if tt.name == "ListByOrg" {
					key = string(cfg.ConnectorID)
				}
				expected, ok := expectedCreds[key]
				require.Truef(t, ok, "unexpected row returned: org=%s connector=%s", cfg.OrganizationID, cfg.ConnectorID)
				assert.JSONEq(t, string(expected), string(cfg.Credentials))
			}
		})
	}
}

// TestConnectorConfigRepo_SaveCredentialsPreserveEnabled pins that
// rotating a credential must never flip a live
// (enabled=true) connector back to disabled. The DAO method must leave the
// `enabled` column untouched on conflict and land a first insert disabled.
func TestConnectorConfigRepo_SaveCredentialsPreserveEnabled(t *testing.T) {
	t.Parallel()

	t.Run("first connect lands disabled", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)

		cfg := newTestConnectorConfig("org-pe-first", "github")
		// Caller leaves Enabled at its zero value; the method itself decides the
		// first-insert value (disabled), not the passed config.
		cfg.Enabled = true // even an enabled-looking config must not enable on first insert.
		require.NoError(t, repo.SaveCredentialsPreserveEnabled(context.Background(), cfg))

		assert.False(t, fetchStoredEnabled(t, tx, cfg.OrganizationID, cfg.ConnectorID),
			"a freshly-pasted credential must land disabled until ConfirmConnectorRepos enables it")
	})

	t.Run("re-submit preserves a concurrently-enabled connector", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		orgID, connID := "org-pe-rotate", sdk.ConnectorID("github")

		// Existing live connector: an earlier ConfirmConnectorRepos enabled it.
		seed := newTestConnectorConfig(orgID, connID)
		seed.Enabled = true
		require.NoError(t, repo.Save(context.Background(), seed))
		require.True(t, fetchStoredEnabled(t, tx, orgID, connID))

		// PAT rotation re-submit with brand-new credentials. The enabled bit must
		// survive — this is exactly the column a Get-then-Save would have clobbered
		// had a concurrent enable landed between the read and the write.
		rotated := &sdk.ConnectorConfig{
			OrganizationID: orgID,
			ConnectorID:    connID,
			Credentials:    json.RawMessage(`{"token":"rotated-token","refresh":"rotated-refresh"}`),
			Settings:       json.RawMessage(`{"workspace":"engineering"}`),
		}
		require.NoError(t, repo.SaveCredentialsPreserveEnabled(context.Background(), rotated))

		assert.True(t, fetchStoredEnabled(t, tx, orgID, connID),
			"re-submit must not disable a live connector")

		got, err := repo.Get(context.Background(), orgID, connID)
		require.NoError(t, err)
		assert.JSONEq(t, string(rotated.Credentials), string(got.Credentials),
			"rotated credentials must be persisted")
	})
}

// TestConnectorConfigRepo_DisableConfigsForOrg: disabling an org's configs must
// flip every enabled row of THAT org to disabled, leave other orgs untouched,
// and be idempotent (a second run changes nothing).
func TestConnectorConfigRepo_DisableConfigsForOrg(t *testing.T) {
	t.Parallel()

	t.Run("disables only the target org's enabled rows, leaves others", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()

		// Target org: two enabled connectors.
		target := "org-disable-target"
		gh := newTestConnectorConfig(target, "github")
		gh.Enabled = true
		require.NoError(t, repo.Save(ctx, gh))
		sl := newTestConnectorConfig(target, "slack")
		sl.Enabled = true
		require.NoError(t, repo.Save(ctx, sl))

		// A different org's enabled connector must be untouched.
		other := "org-disable-other"
		oth := newTestConnectorConfig(other, "github")
		oth.Enabled = true
		require.NoError(t, repo.Save(ctx, oth))

		n, err := repo.DisableConfigsForOrg(ctx, target)
		require.NoError(t, err)
		assert.Equal(t, int64(2), n, "both of the target org's enabled rows are disabled")

		assert.False(t, fetchStoredEnabled(t, tx, target, "github"), "target github disabled")
		assert.False(t, fetchStoredEnabled(t, tx, target, "slack"), "target slack disabled")
		assert.True(t, fetchStoredEnabled(t, tx, other, "github"), "another org's row is untouched")
	})

	t.Run("idempotent: no enabled rows is a zero-row no-op", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-disable-idem"

		cfg := newTestConnectorConfig(orgID, "github")
		cfg.Enabled = true
		require.NoError(t, repo.Save(ctx, cfg))

		first, err := repo.DisableConfigsForOrg(ctx, orgID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), first)

		// Second run finds nothing enabled — must report 0 and not error.
		second, err := repo.DisableConfigsForOrg(ctx, orgID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), second, "re-running on an already-disabled org changes nothing")
	})

	t.Run("never touches credentials or settings", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-disable-creds"

		cfg := newTestConnectorConfig(orgID, "github")
		cfg.Enabled = true
		require.NoError(t, repo.Save(ctx, cfg))
		credsBefore := fetchStoredCredentials(t, tx, orgID, "github")

		_, err := repo.DisableConfigsForOrg(ctx, orgID)
		require.NoError(t, err)

		assert.False(t, fetchStoredEnabled(t, tx, orgID, "github"))
		assert.Equal(t, credsBefore, fetchStoredCredentials(t, tx, orgID, "github"),
			"credentials column must be byte-for-byte untouched by disable")
		got, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)
		assert.JSONEq(t, string(cfg.Settings), string(got.Settings), "settings untouched by disable")
	})
}

func TestConnectorConfigRepo_MarkAuthRevoked(t *testing.T) {
	t.Parallel()

	t.Run("transitions a healthy row to auth_revoked", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-mark-revoked"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, orgID, "github"))
		saved, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)

		transitioned, err := repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)
		assert.True(t, transitioned)
		assert.Equal(t, string(sdk.HealthStateAuthRevoked), fetchStoredHealthState(t, tx, orgID, "github"))
	})

	t.Run("no-ops on a row already auth_revoked", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-mark-revoked-noop"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		saved, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)

		first, err := repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)
		assert.True(t, first)

		second, err := repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)
		assert.False(t, second, "already-revoked row is not re-transitioned")
	})

	t.Run("scoped to (org, connector): does not affect other rows", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-mark-revoked-scope"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "slack")))
		require.NoError(t, repo.Save(ctx, newTestConnectorConfig("org-mark-revoked-other", "github")))
		saved, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)

		_, err = repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)

		assert.Equal(t, string(sdk.HealthStateAuthRevoked), fetchStoredHealthState(t, tx, orgID, "github"))
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, orgID, "slack"), "sibling connector untouched")
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, "org-mark-revoked-other", "github"), "other org untouched")
	})

	t.Run("stale snapshot is a no-op: a concurrent reconnect wins", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-mark-revoked-stale"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		staleSnapshot, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)

		// Simulate a concurrent reconnect: ResetHealthState (called right after a
		// fresh credential Save on every real reconnect path) bumps
		// credentials_updated_at after the stale run's snapshot was taken.
		_, err = repo.ResetHealthState(ctx, orgID, "github")
		require.NoError(t, err)

		transitioned, err := repo.MarkAuthRevoked(ctx, orgID, "github", staleSnapshot.CredentialsUpdatedAt)
		require.NoError(t, err)
		assert.False(t, transitioned, "a stale credentials_updated_at snapshot must not override the newer row")
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, orgID, "github"),
			"the reconnect's healthy state must survive the stale run's late failure")
	})

	t.Run("an unrelated settings write does not block a genuine revocation", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-mark-revoked-settings-write"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		saved, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)

		// Simulate the run's own mid-Execute settings/cache persist (e.g.
		// saveDiscoveredRepos), which bumps the generic updated_at column but is
		// NOT a credential rewrite — MarkAuthRevoked fences on credentials_updated_at
		// specifically so this must not block the transition.
		require.NoError(t, repo.PatchSettings(ctx, orgID, "github", func(current json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"workspace":"engineering","discovered_repos":["a","b"]}`), nil
		}))

		transitioned, err := repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)
		assert.True(t, transitioned, "an unrelated settings write must not defeat a genuine revocation")
	})
}

func TestConnectorConfigRepo_ResetHealthState(t *testing.T) {
	t.Parallel()

	t.Run("restores an auth_revoked row to healthy", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-reset-health"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		saved, err := repo.Get(ctx, orgID, "github")
		require.NoError(t, err)
		_, err = repo.MarkAuthRevoked(ctx, orgID, "github", saved.CredentialsUpdatedAt)
		require.NoError(t, err)
		require.Equal(t, string(sdk.HealthStateAuthRevoked), fetchStoredHealthState(t, tx, orgID, "github"))

		transitioned, err := repo.ResetHealthState(ctx, orgID, "github")
		require.NoError(t, err)
		assert.True(t, transitioned)
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, orgID, "github"))
	})

	t.Run("health_state no-ops on an already-healthy row, but credentials_updated_at still advances", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		ctx := context.Background()
		orgID := "org-reset-health-idem"

		require.NoError(t, repo.Save(ctx, newTestConnectorConfig(orgID, "github")))
		// The insert stamps credentials_updated_at with the database clock and
		// ResetHealthState with the application clock. Restamp the row from the
		// application clock first, so the comparison below uses one clock and a
		// skew between the two cannot decide it.
		before := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
		require.NoError(t, tx.Exec("UPDATE connector_configs SET credentials_updated_at = ? WHERE organization_id = ? AND connector_id = ?", before, orgID, "github").Error)
		require.True(t, fetchStoredCredentialsUpdatedAt(t, tx, orgID, "github").Equal(before))

		transitioned, err := repo.ResetHealthState(ctx, orgID, "github")
		require.NoError(t, err)
		assert.False(t, transitioned, "an already-healthy row is not re-transitioned")
		assert.Equal(t, string(sdk.HealthStateHealthy), fetchStoredHealthState(t, tx, orgID, "github"))
		assert.True(t, fetchStoredCredentialsUpdatedAt(t, tx, orgID, "github").After(before),
			"credentials_updated_at must still advance — a reconnect into an already-healthy connector is a genuine fresh credential, and a later stale run must fence against it")
	})
}

func TestConnectorConfigRepo_GetReturnsNotFound(t *testing.T) {
	t.Parallel()

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)

	_, err := repo.Get(context.Background(), "missing-org", "github")
	require.Error(t, err)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	assert.NotErrorIs(t, err, sql.ErrNoRows)
}

// TestConnectorConfigRepo_PatchSettings_MutatesAndReturnsNewBytes verifies the
// happy path: existing row, mutator transforms settings, the row is updated
// in place, credentials/other columns are untouched.
func TestConnectorConfigRepo_PatchSettings_MutatesAndReturnsNewBytes(t *testing.T) {
	t.Parallel()

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)
	cfg := newTestConnectorConfig("org-patch-1", "bitbucket")
	cfg.Settings = json.RawMessage(`{"workspace":"acme","disable_commit_diffstat":false}`)
	require.NoError(t, repo.Save(context.Background(), cfg))

	err := repo.PatchSettings(context.Background(), cfg.OrganizationID, cfg.ConnectorID, func(current json.RawMessage) (json.RawMessage, error) {
		var s map[string]any
		require.NoError(t, json.Unmarshal(current, &s))
		s["disable_commit_diffstat"] = true
		s["new_field"] = "from-mutator"
		return json.Marshal(s)
	})
	require.NoError(t, err)

	got, err := repo.Get(context.Background(), cfg.OrganizationID, cfg.ConnectorID)
	require.NoError(t, err)
	var gotSettings map[string]any
	require.NoError(t, json.Unmarshal(got.Settings, &gotSettings))
	assert.Equal(t, true, gotSettings["disable_commit_diffstat"], "mutator-written value persisted")
	assert.Equal(t, "from-mutator", gotSettings["new_field"], "mutator-added key persisted")
	assert.Equal(t, "acme", gotSettings["workspace"], "untouched key preserved")
	assert.JSONEq(t, string(cfg.Credentials), string(got.Credentials), "credentials untouched by PatchSettings")
}

// TestConnectorConfigRepo_PatchSettings_NilMutatorReturnsError guards
// against silently no-oping when callers forget to pass a mutator.
func TestConnectorConfigRepo_PatchSettings_NilMutatorReturnsError(t *testing.T) {
	t.Parallel()

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)
	err := repo.PatchSettings(context.Background(), "org", "bitbucket", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutator")
}

// TestConnectorConfigRepo_PatchSettings_MutatorError surfaces the mutator's
// error wrapped so callers can distinguish "mutator failed" from "DB
// failed" — important for the persist-helper log message.
func TestConnectorConfigRepo_PatchSettings_MutatorError(t *testing.T) {
	t.Parallel()

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)
	cfg := newTestConnectorConfig("org-patch-err", "bitbucket")
	require.NoError(t, repo.Save(context.Background(), cfg))

	wantErr := errors.New("mutator boom")
	err := repo.PatchSettings(context.Background(), cfg.OrganizationID, cfg.ConnectorID, func(_ json.RawMessage) (json.RawMessage, error) {
		return nil, wantErr
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

// TestConnectorConfigRepo_PatchSettings_RowMissingReturnsNotFound makes sure
// callers can detect a soft-deleted/non-existent row instead of silently
// creating a fresh one with a partial settings blob.
func TestConnectorConfigRepo_PatchSettings_RowMissingReturnsNotFound(t *testing.T) {
	t.Parallel()

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)
	err := repo.PatchSettings(context.Background(), "missing-org", "bitbucket", func(c json.RawMessage) (json.RawMessage, error) {
		return c, nil
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

// TestConnectorConfigRepo_EnableConnectorWithSettings proves the enable-with-settings
// contract at the DB layer — what a gomock cannot: that the UPDATE flips `enabled`
// and writes the mutated `settings` while leaving the `credentials` column
// byte-for-byte untouched, and that the method refuses to enable when the row is
// missing or the mutator yields empty bytes.
func TestConnectorConfigRepo_EnableConnectorWithSettings(t *testing.T) {
	t.Parallel()

	t.Run("enables, sets repos, never touches credentials, preserves other settings", func(t *testing.T) {
		t.Parallel()
		// Encryptor on: credentials land as a ciphertext envelope, so the
		// byte-for-byte assertion also proves the enable path never re-encrypts
		// or rewrites the stored credential bytes.
		encryptor, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
		require.NoError(t, err)

		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, encryptor)
		orgID, connID := "org-enable-1", sdk.ConnectorID("github")

		seed := newTestConnectorConfig(orgID, connID)
		// Settings is opaque JSON; seed a pre-existing key alongside an empty
		// repositories list so we can prove the mutator merges, not clobbers.
		seed.Settings = json.RawMessage(`{"include_private":true,"repositories":[]}`)
		seed.Enabled = false
		require.NoError(t, repo.Save(context.Background(), seed))
		require.False(t, fetchStoredEnabled(t, tx, orgID, connID), "seed must start disabled")

		credsBefore := fetchStoredCredentials(t, tx, orgID, connID)

		err = repo.EnableConnectorWithSettings(context.Background(), orgID, connID, func(current json.RawMessage) (json.RawMessage, error) {
			var s map[string]any
			if err := json.Unmarshal(current, &s); err != nil {
				return nil, err
			}
			s["repositories"] = []string{"acme/a", "acme/b"}
			return json.Marshal(s)
		})
		require.NoError(t, err)

		assert.True(t, fetchStoredEnabled(t, tx, orgID, connID), "connector must be enabled")

		credsAfter := fetchStoredCredentials(t, tx, orgID, connID)
		assert.Equal(t, credsBefore, credsAfter,
			"credentials column must be byte-for-byte untouched by enable (the Critical proof)")

		got, err := repo.Get(context.Background(), orgID, connID)
		require.NoError(t, err)
		var gotSettings map[string]any
		require.NoError(t, json.Unmarshal(got.Settings, &gotSettings))
		assert.Equal(t, []any{"acme/a", "acme/b"}, gotSettings["repositories"],
			"mutator-written repositories persisted")
		assert.Equal(t, true, gotSettings["include_private"],
			"pre-existing settings key preserved through the mutate")
	})

	t.Run("row missing returns NotFound", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)

		err := repo.EnableConnectorWithSettings(context.Background(), "missing-org", "github", func(c json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"repositories":["x/y"]}`), nil
		})
		require.Error(t, err)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})

	t.Run("empty mutator output refuses to enable", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		orgID, connID := "org-enable-empty", sdk.ConnectorID("github")

		seed := newTestConnectorConfig(orgID, connID)
		seed.Enabled = false
		require.NoError(t, repo.Save(context.Background(), seed))

		err := repo.EnableConnectorWithSettings(context.Background(), orgID, connID, func(_ json.RawMessage) (json.RawMessage, error) {
			return nil, nil
		})
		require.Error(t, err, "empty mutator output must be an error, not a silent no-op")
		assert.False(t, fetchStoredEnabled(t, tx, orgID, connID),
			"a refused enable must leave the connector disabled")
	})

	t.Run("rotation interleave: enable does not revert a freshly-rotated credential", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		orgID, connID := "org-enable-rotate", sdk.ConnectorID("github")

		// Disabled connector with cred-A.
		seed := newTestConnectorConfig(orgID, connID)
		seed.Credentials = json.RawMessage(`{"token":"cred-A","refresh":"refresh-A"}`)
		seed.Enabled = false
		require.NoError(t, repo.Save(context.Background(), seed))

		// A PAT rotation lands cred-B (preserving the disabled bit), THEN the
		// confirm-repos enable runs. The enable must not resurrect cred-A — it
		// only touches settings + enabled, never credentials. This is the
		// real-world TOCTOU rotation/enable race, expressed sequentially.
		rotated := &sdk.ConnectorConfig{
			OrganizationID: orgID,
			ConnectorID:    connID,
			Credentials:    json.RawMessage(`{"token":"cred-B","refresh":"refresh-B"}`),
			Settings:       json.RawMessage(`{"workspace":"engineering"}`),
		}
		require.NoError(t, repo.SaveCredentialsPreserveEnabled(context.Background(), rotated))

		err := repo.EnableConnectorWithSettings(context.Background(), orgID, connID, func(current json.RawMessage) (json.RawMessage, error) {
			var s map[string]any
			if err := json.Unmarshal(current, &s); err != nil {
				return nil, err
			}
			s["repositories"] = []string{"acme/a"}
			return json.Marshal(s)
		})
		require.NoError(t, err)

		assert.True(t, fetchStoredEnabled(t, tx, orgID, connID))
		got, err := repo.Get(context.Background(), orgID, connID)
		require.NoError(t, err)
		assert.JSONEq(t, string(rotated.Credentials), string(got.Credentials),
			"enable must keep the rotated cred-B, never revert to cred-A")
	})
}

// TestConnectorConfigRepo_SaveCredentialsEnablePreservingSettings covers the column
// contract stated on the method: a first insert seeds settings and overrides, a rotation
// leaves them alone, and a revival takes the caller's.
func TestConnectorConfigRepo_SaveCredentialsEnablePreservingSettings(t *testing.T) {
	t.Parallel()

	t.Run("first connect lands enabled and seeds settings", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)

		cfg := newTestConnectorConfig("org-sce-first", "cursor")
		cfg.Settings = json.RawMessage(`{"lookback_days":30}`)
		require.NoError(t, repo.SaveCredentialsEnablePreservingSettings(context.Background(), cfg))

		assert.True(t, fetchStoredEnabled(t, tx, cfg.OrganizationID, cfg.ConnectorID),
			"a one-step connect flow enables on save")

		got, err := repo.Get(context.Background(), cfg.OrganizationID, cfg.ConnectorID)
		require.NoError(t, err)
		assert.JSONEq(t, `{"lookback_days":30}`, string(got.Settings),
			"there was nothing to keep, so the caller's settings seed the row")
	})

	t.Run("rotation keeps the stored settings and overrides", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		orgID, connID := "org-sce-rotate", sdk.ConnectorID("cursor")

		seed := newTestConnectorConfig(orgID, connID)
		seed.Settings = json.RawMessage(`{"lookback_days":45}`)
		require.NoError(t, repo.Save(context.Background(), seed))

		// A rotation carrying the defaults, which is what a connect handler builds when
		// it does not read the row first. The stored window must survive it.
		rotated := &sdk.ConnectorConfig{
			OrganizationID: orgID,
			ConnectorID:    connID,
			Credentials:    json.RawMessage(`{"api_key":"rotated-key"}`),
			Settings:       json.RawMessage(`{}`),
		}
		require.NoError(t, repo.SaveCredentialsEnablePreservingSettings(context.Background(), rotated))

		got, err := repo.Get(context.Background(), orgID, connID)
		require.NoError(t, err)
		assert.JSONEq(t, `{"lookback_days":45}`, string(got.Settings),
			"a credential rotation must not overwrite settings it does not own")
		assert.JSONEq(t, string(rotated.Credentials), string(got.Credentials),
			"the rotated credential must be persisted")
		assert.True(t, fetchStoredEnabled(t, tx, orgID, connID))
	})

	t.Run("rotation enables a suspended connector", func(t *testing.T) {
		t.Parallel()
		tx := newTestTx(t)
		repo := store.NewConnectorConfigRepo(tx, nil)
		orgID, connID := "org-sce-suspended", sdk.ConnectorID("cursor")

		// A revoked credential leaves the row disabled; re-pasting a working key is how
		// the customer recovers, so the save must flip it back on.
		seed := newTestConnectorConfig(orgID, connID)
		seed.Enabled = false
		require.NoError(t, repo.Save(context.Background(), seed))
		require.False(t, fetchStoredEnabled(t, tx, orgID, connID))

		require.NoError(t, repo.SaveCredentialsEnablePreservingSettings(context.Background(), &sdk.ConnectorConfig{
			OrganizationID: orgID,
			ConnectorID:    connID,
			Credentials:    json.RawMessage(`{"api_key":"fresh-key"}`),
			Settings:       json.RawMessage(`{}`),
		}))
		assert.True(t, fetchStoredEnabled(t, tx, orgID, connID),
			"re-pasting a key must recover a suspended connector")
	})
}

// TestConnectorConfigRepo_SaveCredentialsEnablePreservingSettings_Revival covers the
// half of the column contract a live-row rotation cannot show. The conflict target is
// the primary key with no `deleted_at` predicate, so a disconnected row is matched and
// revived by the same statement — and it must come back with the caller's settings, not
// the ones it carried before the disconnect.
func TestConnectorConfigRepo_SaveCredentialsEnablePreservingSettings_Revival(t *testing.T) {
	t.Parallel()
	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, nil)
	orgID, connID := "org-sce-revive", sdk.ConnectorID("cursor")

	seed := newTestConnectorConfig(orgID, connID)
	seed.Settings = json.RawMessage(`{"lookback_days":90}`)
	require.NoError(t, repo.Save(context.Background(), seed))
	require.NoError(t, repo.Delete(context.Background(), orgID, connID))

	// Delete only stamps deleted_at, so the row and its settings are still there.
	_, err := repo.Get(context.Background(), orgID, connID)
	require.Error(t, err, "a soft-deleted row must not be readable")

	require.NoError(t, repo.SaveCredentialsEnablePreservingSettings(context.Background(), &sdk.ConnectorConfig{
		OrganizationID: orgID,
		ConnectorID:    connID,
		Credentials:    json.RawMessage(`{"api_key":"reconnect-key"}`),
		Settings:       json.RawMessage(`{}`),
	}))

	got, err := repo.Get(context.Background(), orgID, connID)
	require.NoError(t, err, "the reconnect must revive the row")
	assert.JSONEq(t, `{}`, string(got.Settings),
		"a reconnect after a disconnect starts fresh; carrying the old window across it "+
			"would quietly restore configuration the administrator tore down")
	assert.True(t, fetchStoredEnabled(t, tx, orgID, connID))
}

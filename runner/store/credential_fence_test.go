// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Every statement that writes credentials advances credentials_updated_at, so
// a swap or a revocation fenced on the value read before it fails. As the
// table owner, with and without an encryptor.
func TestEveryCredentialWriteAdvancesTheFence(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("credential fence as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	enc, err := cryptutil.NewAESGCMEncryptor(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)

	for _, variant := range []struct {
		name string
		repo *store.ConnectorConfigRepo
		base string
	}{
		{"plaintext", store.NewConnectorConfigRepo(db, nil), "00000000-0000-4000-8000-0000000f0a"},
		{"encrypted", store.NewConnectorConfigRepo(db, enc), "00000000-0000-4000-8000-0000000f0b"},
	} {
		repo := variant.repo
		writes := []struct {
			name  string
			write func(s context.Context, org string) error
		}{
			{"Save", func(s context.Context, org string) error {
				return repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira", Enabled: true,
					Credentials: json.RawMessage(`{"refresh_token":"new"}`), Settings: json.RawMessage(`{}`)})
			}},
			{"SaveCredentialsPreserveEnabled", func(s context.Context, org string) error {
				return repo.SaveCredentialsPreserveEnabled(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
					Credentials: json.RawMessage(`{"refresh_token":"new"}`), Settings: json.RawMessage(`{}`)})
			}},
			{"SaveCredentialsEnablePreservingSettings", func(s context.Context, org string) error {
				return repo.SaveCredentialsEnablePreservingSettings(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
					Credentials: json.RawMessage(`{"refresh_token":"new"}`), Settings: json.RawMessage(`{}`)})
			}},
			{"SaveCredentialsEnableWithSettings", func(s context.Context, org string) error {
				return repo.SaveCredentialsEnableWithSettings(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
					Credentials: json.RawMessage(`{"refresh_token":"new"}`)}, func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"cloud_id":"b"}`), nil })
			}},
		}
		for i, w := range writes {
			t.Run(variant.name+"/"+w.name, func(t *testing.T) {
				org := variant.base + string(rune('0'+i)) + "0"
				in := func(fn func(context.Context) error) {
					require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, fn))
				}
				var before *sdk.ConnectorConfig
				in(func(s context.Context) error {
					if err := repo.Save(s, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira", Enabled: true,
						Credentials: json.RawMessage(`{"refresh_token":"old"}`), Settings: json.RawMessage(`{}`)}); err != nil {
						return err
					}
					var err error
					before, err = repo.Get(s, org, "jira")
					return err
				})
				time.Sleep(2 * time.Millisecond)
				in(func(s context.Context) error { return w.write(s, org) })

				var after *sdk.ConnectorConfig
				in(func(s context.Context) (err error) { after, err = repo.Get(s, org, "jira"); return err })
				assert.True(t, after.CredentialsUpdatedAt.After(before.CredentialsUpdatedAt), "the fence advanced: before %v after %v", before.CredentialsUpdatedAt, after.CredentialsUpdatedAt)

				var swapped, revoked bool
				in(func(s context.Context) (err error) {
					swapped, err = repo.CompareAndSwapCredentials(s, org, "jira", before.CredentialsUpdatedAt, json.RawMessage(`{"refresh_token":"stale-rotation"}`))
					if err != nil {
						return err
					}
					revoked, err = repo.MarkAuthRevoked(s, org, "jira", before.CredentialsUpdatedAt)
					return err
				})
				assert.False(t, swapped, "a swap fenced on the earlier read fails")
				assert.False(t, revoked, "a revocation fenced on the earlier read fails")
			})
		}
	}
}

// A Save that rewrites the same plaintext credentials without an encryptor
// leaves the fence where it was: only a changed credentials column moves it.
func TestSaveWithUnchangedPlaintextCredentialsKeepsTheFence(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("credential fence as the table owner", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	db, _, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	repo := store.NewConnectorConfigRepo(db, nil)
	const org = "00000000-0000-4000-8000-0000000f0c01"
	cfg := &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira", Enabled: true,
		Credentials: json.RawMessage(`{"refresh_token":"same"}`), Settings: json.RawMessage(`{}`)}
	var before, after *sdk.ConnectorConfig
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) (err error) {
		if err = repo.Save(s, cfg); err != nil {
			return err
		}
		before, err = repo.Get(s, org, "jira")
		return err
	}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(s context.Context) (err error) {
		cfg.Settings = json.RawMessage(`{"cloud_id":"a"}`)
		if err = repo.Save(s, cfg); err != nil {
			return err
		}
		after, err = repo.Get(s, org, "jira")
		return err
	}))
	assert.True(t, after.CredentialsUpdatedAt.Equal(before.CredentialsUpdatedAt))
}

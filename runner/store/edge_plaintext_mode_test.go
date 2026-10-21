// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A repo with no encryptor (plaintext mode) that reads a row written under a
// key must refuse it with a clear error, not hand the ciphertext envelope to
// the connector as if it were the credentials. Plaintext rows, including ones
// that happen to have a "v" member, still read unchanged.
func TestEdge_PlaintextModeRefusesEncryptedCredentials(t *testing.T) {
	if testDB == nil {
		t.Skip("no test database")
	}
	tx := newTestTx(t)
	ctx := context.Background()
	enc, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)
	const org = "00000000-0000-4000-8000-0000000e1c01"

	keyed := store.NewConnectorConfigRepo(tx, enc)
	require.NoError(t, keyed.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github", Enabled: true, Credentials: json.RawMessage(`{"token":"secret"}`), Settings: json.RawMessage(`{}`)}))

	plain := store.NewConnectorConfigRepo(tx, nil)
	_, err = plain.Get(ctx, org, "github")
	require.Error(t, err, "an encrypted row read without a key is an error")
	assert.ErrorIs(t, err, store.ErrCredentialsEncrypted)
	assert.Contains(t, err.Error(), "TETRIZ_CREDENTIALS_KEY")

	list, err := plain.ListByOrg(ctx, org)
	var unreadable *store.UndecryptableRowsError
	require.True(t, errors.As(err, &unreadable), "the list reports the row as unreadable: %v", err)
	assert.Equal(t, []string{org}, unreadable.OrgIDs)
	assert.Empty(t, list)

	// Plaintext-mode writes and reads are unchanged, even for credentials that
	// look partly like an envelope.
	for _, creds := range []string{`{"token":"t"}`, `{"v":1,"token":"t"}`, `{"v":1,"ct":"x","user":"u"}`, `{"v":0,"ct":"x"}`, `{"v":1,"ct":null}`} {
		const plainOrg = "00000000-0000-4000-8000-0000000e1c02"
		require.NoError(t, plain.Save(ctx, &sdk.ConnectorConfig{OrganizationID: plainOrg, ConnectorID: "github", Enabled: true, Credentials: json.RawMessage(creds), Settings: json.RawMessage(`{}`)}))
		got, err := plain.Get(ctx, plainOrg, "github")
		require.NoError(t, err, creds)
		assert.JSONEq(t, creds, string(got.Credentials), creds)
	}
}

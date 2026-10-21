// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memstore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
)

// A file written with an encryptor and reopened without one refuses to load,
// rather than handing the ciphertext envelope to a connector as credentials.
func TestEdge_FileStoreWithoutKeyRefusesEncryptedCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	f, err := memstore.OpenFile(path, testEncryptor(t))
	require.NoError(t, err)
	require.NoError(t, f.Save(context.Background(), cfg(org(), true, `{"a":1}`)))

	_, err = memstore.OpenFile(path, nil)
	assert.ErrorIs(t, err, store.ErrCredentialsEncrypted)
}

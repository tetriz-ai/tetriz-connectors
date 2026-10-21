// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth"
	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/auth/memory"
)

func testKey(b byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = b
	}
	return k
}

// A token is stored as an envelope and read back in the clear.
func TestEncryptedTokenCacheStoresAnEnvelope(t *testing.T) {
	inner := memory.New()
	enc, err := cryptutil.NewAESGCMEncryptor(testKey(1))
	require.NoError(t, err)
	c := auth.NewEncryptedTokenCache(inner, enc)
	require.NoError(t, c.Set(context.Background(), "k", "access-token", time.Minute))

	raw, err := inner.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, cryptutil.IsEnvelope([]byte(raw)))
	assert.NotContains(t, raw, "access-token")
	got, err := c.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, "access-token", got)
}

// A plaintext entry under a key (one written before the key was set, or by a
// writer without one) is a miss, never handed on as a token, though the
// envelope decrypter passes plain JSON through.
func TestEncryptedTokenCacheReadsAPlaintextEntryAsAMiss(t *testing.T) {
	inner := memory.New()
	require.NoError(t, inner.Set(context.Background(), "k", "plain-token", time.Minute))
	enc, err := cryptutil.NewAESGCMEncryptor(testKey(1))
	require.NoError(t, err)
	_, err = auth.NewEncryptedTokenCache(inner, enc).Get(context.Background(), "k")
	require.ErrorIs(t, err, auth.ErrCacheMiss)
}

// An envelope under another key id, or another key, is a miss: the caller
// refreshes and overwrites it.
func TestEncryptedTokenCacheReadsAnotherKeysEnvelopeAsAMiss(t *testing.T) {
	ctx := context.Background()
	mine, err := cryptutil.NewAESGCMEncryptorWithKeyID("k1", testKey(1))
	require.NoError(t, err)
	for name, other := range map[string]func() (cryptutil.Encryptor, error){
		"another key id": func() (cryptutil.Encryptor, error) { return cryptutil.NewAESGCMEncryptorWithKeyID("k2", testKey(1)) },
		"another key":    func() (cryptutil.Encryptor, error) { return cryptutil.NewAESGCMEncryptorWithKeyID("k1", testKey(2)) },
	} {
		t.Run(name, func(t *testing.T) {
			theirs, err := other()
			require.NoError(t, err)
			inner := memory.New()
			require.NoError(t, auth.NewEncryptedTokenCache(inner, theirs).Set(ctx, "k", "their-token", time.Minute))
			_, err = auth.NewEncryptedTokenCache(inner, mine).Get(ctx, "k")
			require.ErrorIs(t, err, auth.ErrCacheMiss)
		})
	}
}

// Delete removes the stored entry.
func TestEncryptedTokenCacheDelete(t *testing.T) {
	ctx := context.Background()
	inner := memory.New()
	enc, err := cryptutil.NewAESGCMEncryptor(testKey(1))
	require.NoError(t, err)
	c := auth.NewEncryptedTokenCache(inner, enc)
	require.NoError(t, c.Set(ctx, "k", "access-token", time.Minute))
	require.NoError(t, c.Delete(ctx, "k"))
	_, err = inner.Get(ctx, "k")
	require.ErrorIs(t, err, auth.ErrCacheMiss)
}

// Without an encryptor the cache is the inner cache itself.
func TestEncryptedTokenCacheWithoutAKeyIsPlain(t *testing.T) {
	inner := memory.New()
	assert.Same(t, auth.TokenCache(inner), auth.NewEncryptedTokenCache(inner, nil))
}

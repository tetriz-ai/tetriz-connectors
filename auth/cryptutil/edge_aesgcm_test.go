// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cryptutil_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
)

func key(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func sealedOf(t *testing.T, env []byte) []byte {
	t.Helper()
	var e struct {
		CT string `json:"ct"`
	}
	require.NoError(t, json.Unmarshal(env, &e))
	raw, err := base64.StdEncoding.DecodeString(e.CT)
	require.NoError(t, err)
	return raw
}

func envelope(t *testing.T, v int, sealed []byte) []byte {
	t.Helper()
	out, err := json.Marshal(map[string]any{"v": v, "ct": base64.StdEncoding.EncodeToString(sealed)})
	require.NoError(t, err)
	return out
}

// A ciphertext opened under a different key (both without a key id, so the
// kid check cannot catch it) fails authentication instead of returning bytes.
func TestEdge_WrongKeyWithoutKeyIDFails(t *testing.T) {
	a, _ := cryptutil.NewAESGCMEncryptor(key(1))
	b, _ := cryptutil.NewAESGCMEncryptor(key(2))
	ct, err := a.Encrypt([]byte(`{"t":"x"}`))
	require.NoError(t, err)
	got, err := b.Decrypt(ct)
	assert.Error(t, err)
	assert.Nil(t, got)
}

// Truncation, a flipped nonce byte and a flipped tag byte all fail.
func TestEdge_TamperedEnvelopesFail(t *testing.T) {
	enc, _ := cryptutil.NewAESGCMEncryptor(key(3))
	ct, err := enc.Encrypt([]byte(`{"t":"x"}`))
	require.NoError(t, err)
	sealed := sealedOf(t, ct)

	cases := map[string][]byte{
		"shorter than a nonce": sealed[:5],
		"nonce only":           sealed[:12],
		"tag cut short":        sealed[:len(sealed)-1],
	}
	nonce := append([]byte(nil), sealed...)
	nonce[0] ^= 0xff
	cases["flipped nonce byte"] = nonce
	tag := append([]byte(nil), sealed...)
	tag[len(tag)-1] ^= 0x01
	cases["flipped tag byte"] = tag

	for name, s := range cases {
		_, err := enc.Decrypt(envelope(t, 1, s))
		assert.Error(t, err, name)
	}
	_, err = enc.Decrypt([]byte(`{"v":1,"ct":"%%%not-base64"}`))
	assert.Error(t, err, "bad base64")
}

// Empty and nil plaintexts round-trip and still produce an authenticated
// envelope, not a pass-through.
func TestEdge_EmptyPlaintextRoundTrips(t *testing.T) {
	enc, _ := cryptutil.NewAESGCMEncryptor(key(4))
	for _, p := range [][]byte{nil, {}} {
		ct, err := enc.Encrypt(p)
		require.NoError(t, err)
		assert.Len(t, sealedOf(t, ct), 12+16, "nonce + tag")
		got, err := enc.Decrypt(ct)
		require.NoError(t, err)
		assert.Empty(t, got)
	}
}

// An envelope naming a format version this build does not know is refused,
// not opened as version 1: the version field exists so a future algorithm
// change cannot be misread by an older reader.
func TestEdge_UnknownEnvelopeVersionIsRefused(t *testing.T) {
	enc, _ := cryptutil.NewAESGCMEncryptor(key(5))
	ct, err := enc.Encrypt([]byte("secret"))
	require.NoError(t, err)
	for _, v := range []int{2, -1} {
		got, err := enc.Decrypt(envelope(t, v, sealedOf(t, ct)))
		assert.Error(t, err, "envelope version %d was opened (got %q)", v, got)
	}
}

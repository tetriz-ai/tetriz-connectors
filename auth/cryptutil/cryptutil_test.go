// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cryptutil_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
)

func TestRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	enc, err := cryptutil.NewAESGCMEncryptor(key)
	require.NoError(t, err)

	plaintext := []byte(`{"oauth_refresh_token":"secret-value","webhook_secret":"abc123"}`)
	ciphertext, err := enc.Encrypt(plaintext)
	require.NoError(t, err)

	got, err := enc.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestEncryptNonDeterministic(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := cryptutil.NewAESGCMEncryptor(key)
	plaintext := []byte(`{"test":"value"}`)

	c1, _ := enc.Encrypt(plaintext)
	c2, _ := enc.Encrypt(plaintext)
	// Different nonces → different ciphertext
	assert.NotEqual(t, c1, c2)
}

func TestEncryptProducesVersionedEnvelope(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := cryptutil.NewAESGCMEncryptor(key)
	ciphertext, _ := enc.Encrypt([]byte(`{"x":"y"}`))

	var env map[string]any
	require.NoError(t, json.Unmarshal(ciphertext, &env))
	assert.Equal(t, float64(1), env["v"])
	assert.NotEmpty(t, env["ct"])
}

func TestDecryptBackwardCompatPlainJSON(t *testing.T) {
	// Old rows: plain JSON — must pass through unchanged.
	key := make([]byte, 32)
	enc, _ := cryptutil.NewAESGCMEncryptor(key)
	plain := []byte(`{"oauth_refresh_token":"plaintext-token"}`)

	got, err := enc.Decrypt(plain)
	require.NoError(t, err)
	assert.Equal(t, plain, got)
}

func TestNewAESGCMEncryptorRejectsWrongKeySize(t *testing.T) {
	_, err := cryptutil.NewAESGCMEncryptor([]byte("tooshort"))
	assert.Error(t, err)
}

func TestDecryptTamperedCiphertext(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := cryptutil.NewAESGCMEncryptor(key)
	ct, _ := enc.Encrypt([]byte(`{"x":"y"}`))

	// Flip a byte in the enc field to simulate tampering.
	var env map[string]any
	_ = json.Unmarshal(ct, &env)
	env["ct"] = "aGVsbG8=" // valid base64 but not a real ciphertext
	tampered, _ := json.Marshal(env)

	_, err := enc.Decrypt(tampered)
	assert.Error(t, err)
}

// TestEnvelopeWithoutKeyIDIsUnchanged pins the default envelope: an encryptor
// built without a key id writes exactly {"v":1,"ct":...}, so rows written
// before key ids existed and rows written after read the same.
func TestEnvelopeWithoutKeyIDIsUnchanged(t *testing.T) {
	key := make([]byte, 32)
	enc, err := cryptutil.NewAESGCMEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	ct, err := enc.Encrypt([]byte(`{"token":"t"}`))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(ct, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 || fields["v"] == nil || fields["ct"] == nil {
		t.Fatalf("default envelope = %s, want exactly v and ct", ct)
	}
}

// TestKeyIDRoundTripAndMismatch pins the key id contract: an envelope names
// its key, the same key decrypts it, a different key id refuses it, and a
// key-id-less envelope still decrypts under a keyed encryptor.
func TestKeyIDRoundTripAndMismatch(t *testing.T) {
	key := make([]byte, 32)
	a, _ := cryptutil.NewAESGCMEncryptorWithKeyID("k1", key)
	b, _ := cryptutil.NewAESGCMEncryptorWithKeyID("k2", key)
	plain, _ := cryptutil.NewAESGCMEncryptor(key)

	ct, err := a.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ct), `"kid":"k1"`) {
		t.Fatalf("envelope %s does not name its key", ct)
	}
	if got, err := a.Decrypt(ct); err != nil || string(got) != "secret" {
		t.Fatalf("same key: got %q, %v", got, err)
	}
	if _, err := b.Decrypt(ct); err == nil {
		t.Fatal("a different key id must refuse the envelope")
	}
	plainRow, _ := plain.Encrypt([]byte("old"))
	if got, err := a.Decrypt(plainRow); err != nil || string(got) != "old" {
		t.Fatalf("key-id-less envelope under a keyed encryptor: got %q, %v", got, err)
	}
}

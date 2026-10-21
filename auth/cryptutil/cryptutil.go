// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package cryptutil encrypts connector credentials at rest with AES-256-GCM.
package cryptutil

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// Encryptor encrypts and decrypts credential blobs.
// Plaintext is always []byte (raw JSON from the DB model).
// Ciphertext is a versioned JSON envelope: {"v":1,"ct":"<base64(nonce+ciphertext)>"},
// with a "kid" member naming the key when the encryptor has a key id.
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
	// KeyID names the key; "" for an encryptor built without one.
	KeyID() string
}

type aesgcmEncryptor struct {
	aead  cipher.AEAD
	keyID string
}

// NewAESGCMEncryptor creates an Encryptor from a 32-byte AES-256 key. Its
// envelopes carry no key id.
func NewAESGCMEncryptor(key []byte) (Encryptor, error) {
	return NewAESGCMEncryptorWithKeyID("", key)
}

// NewAESGCMEncryptorWithKeyID creates an Encryptor whose envelopes record
// keyID, so a store holding rows under several keys can tell them apart.
// Decrypt rejects an envelope that names a different key; an envelope with
// no key id is decrypted with this key.
func NewAESGCMEncryptorWithKeyID(keyID string, key []byte) (Encryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("cryptutil: key must be exactly 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cryptutil: create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cryptutil: create GCM: %w", err)
	}
	return &aesgcmEncryptor{aead: aead, keyID: keyID}, nil
}

// KeyID implements Encryptor.
func (e *aesgcmEncryptor) KeyID() string { return e.keyID }

// envelope is the JSON structure stored in the DB.
// v: format version, which tells encrypted rows from plain-JSON rows and
// leaves room for algorithm changes or key rotation.
//
// ct: ciphertext — base64(nonce || AES-256-GCM ciphertext), where nonce is 12 random bytes prepended.
type envelope struct {
	V   int    `json:"v"`
	CT  string `json:"ct"`
	KID string `json:"kid,omitempty"`
}

func (e *aesgcmEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("cryptutil: generate nonce: %w", err)
	}
	sealed := e.aead.Seal(nonce, nonce, plaintext, nil)
	env := envelope{
		V:   1,
		CT:  base64.StdEncoding.EncodeToString(sealed),
		KID: e.keyID,
	}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("cryptutil: marshal envelope: %w", err)
	}
	return out, nil
}

func (e *aesgcmEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	// A row that is not a v1 envelope is plain JSON, which TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS
	// writes. It passes through unchanged, whether it does not parse as an envelope or
	// parses but carries no "v" key.
	var env envelope
	if err := json.Unmarshal(ciphertext, &env); err != nil {
		// Log the cause, so a corrupt blob that reads as plaintext is still observable.
		logger.InfoNoCtx("cryptutil: ciphertext not a v1 envelope, passing through as plaintext", zap.Error(err))
		return ciphertext, nil
	}
	if env.V == 0 {
		return ciphertext, nil
	}
	if env.V != 1 {
		return nil, fmt.Errorf("cryptutil: unsupported envelope version %d", env.V)
	}
	if env.KID != "" && env.KID != e.keyID {
		return nil, fmt.Errorf("cryptutil: ciphertext is under key %q, not %q", env.KID, e.keyID)
	}
	sealed, err := base64.StdEncoding.DecodeString(env.CT)
	if err != nil {
		return nil, fmt.Errorf("cryptutil: decode base64: %w", err)
	}
	ns := e.aead.NonceSize()
	if len(sealed) < ns {
		return nil, fmt.Errorf("cryptutil: ciphertext too short")
	}
	nonce, ct := sealed[:ns], sealed[ns:]
	plaintext, err := e.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("cryptutil: decrypt: %w", err)
	}
	return plaintext, nil
}

// IsEnvelope reports whether b is a ciphertext envelope as Encrypt writes it:
// a JSON object with a non-zero number "v", a string "ct", an optional string
// "kid" and no other members. A store reading without a key uses it to refuse
// an encrypted row instead of handing the envelope on as plaintext.
func IsEnvelope(b []byte) bool {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(b, &members); err != nil {
		return false
	}
	var v float64
	var ct string
	if json.Unmarshal(members["v"], &v) != nil || v == 0 || json.Unmarshal(members["ct"], &ct) != nil {
		return false
	}
	// A JSON null unmarshals into a string without error; ct must be a string.
	if len(members["ct"]) == 0 || members["ct"][0] != '"' {
		return false
	}
	for k, raw := range members {
		switch k {
		case "v", "ct":
		case "kid":
			if len(raw) == 0 || raw[0] != '"' {
				return false
			}
		default:
			return false
		}
	}
	return true
}

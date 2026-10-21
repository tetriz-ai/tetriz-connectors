// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // building a wrong-algorithm signature on purpose
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Signature verification accepts only a full-length sha256 HMAC over the exact
// body, under the configured secret.
func TestEdge_VerifySignatureRejectsNearMisses(t *testing.T) {
	const secret = "s3cret"
	body := []byte(`{"action":"suspend"}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	good := hex.EncodeToString(mac.Sum(nil))

	sha1mac := hmac.New(sha1.New, []byte(secret))
	sha1mac.Write(body)

	empty := hmac.New(sha256.New, []byte(secret))

	assert.True(t, verifySignature(secret, body, "sha256="+good))
	assert.True(t, verifySignature(secret, body, "sha256="+strings.ToUpper(good)), "hex is case-insensitive")
	assert.True(t, verifySignature(secret, nil, "sha256="+hex.EncodeToString(empty.Sum(nil))), "an empty body signed correctly verifies")

	for name, sig := range map[string]string{
		"sha1 prefix with sha1 mac":    "sha1=" + hex.EncodeToString(sha1mac.Sum(nil)),
		"sha1 mac under sha256 prefix": "sha256=" + hex.EncodeToString(sha1mac.Sum(nil)),
		"upper-case algorithm prefix":  "SHA256=" + good,
		"truncated mac":                "sha256=" + good[:32],
		"empty mac":                    "sha256=",
		"leading whitespace":           " sha256=" + good,
		"odd-length hex":               "sha256=" + good[1:],
		"mac with trailing byte":       "sha256=" + good + "00",
	} {
		assert.False(t, verifySignature(secret, body, sig), name)
	}
	assert.False(t, verifySignature(secret, append(body, ' '), "sha256="+good), "a body changed by one byte")
	assert.False(t, verifySignature("", body, "sha256="+good), "no secret configured")
}

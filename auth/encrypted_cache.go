// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
)

// NewEncryptedTokenCache returns a TokenCache that stores each value in
// inner as the envelope enc writes, the one stored credentials use, so a
// cached access token is not readable from the cache's store. Get decrypts
// it. A stored value that is not an envelope, or does not decrypt, reads as
// ErrCacheMiss, so the caller refreshes and overwrites it encrypted.
//
// A nil enc returns inner itself: values are cached in the clear. Pass nil
// only where stored credentials are plaintext too, which the runner allows
// only with TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS=1.
func NewEncryptedTokenCache(inner TokenCache, enc cryptutil.Encryptor) TokenCache {
	if enc == nil {
		return inner
	}
	return encryptedTokenCache{inner: inner, enc: enc}
}

type encryptedTokenCache struct {
	inner TokenCache
	enc   cryptutil.Encryptor
}

// Get implements TokenCache.
func (c encryptedTokenCache) Get(ctx context.Context, key string) (string, error) {
	stored, err := c.inner.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if !cryptutil.IsEnvelope([]byte(stored)) {
		return "", ErrCacheMiss
	}
	plain, err := c.enc.Decrypt([]byte(stored))
	if err != nil {
		return "", ErrCacheMiss
	}
	return string(plain), nil
}

// Set implements TokenCache.
func (c encryptedTokenCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	sealed, err := c.enc.Encrypt([]byte(value))
	if err != nil {
		return fmt.Errorf("token cache encrypt: %w", err)
	}
	return c.inner.Set(ctx, key, string(sealed), ttl)
}

// Delete implements TokenCache.
func (c encryptedTokenCache) Delete(ctx context.Context, key string) error {
	return c.inner.Delete(ctx, key)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package auth declares what the OAuth gateways need to keep an access token
// fresh: a store for the connector's credentials, a cache for the short-lived
// access token, and a lock so only one worker refreshes a rotating refresh
// token at a time. Package internal/auth/redis implements the cache and lock
// over Redis, package auth/memory in process. The runner's connector store
// (package runner/store) is the CredentialStore.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// RefreshDeps are the collaborators of an OAuth gateway's token refresher.
// Tx scopes the credential reads and writes to the organization; use
// store.PostgresTxRunner with the Postgres store.
//
// Encryptor, when set, encrypts the access tokens the GitLab, Bitbucket and
// Jira refreshers cache, with the envelope stored credentials
// use (see NewEncryptedTokenCache). Nil caches them in the clear, which the
// runner allows only where credentials are plaintext too
// (TETRIZ_ALLOW_PLAINTEXT_CREDENTIALS=1). Linear takes its encryptor
// separately.
type RefreshDeps struct {
	Cache     TokenCache
	Locker    Locker
	Creds     CredentialStore
	Tx        store.TxRunner
	Encryptor cryptutil.Encryptor
}

// ErrCacheMiss is returned by TokenCache.Get when the key holds no value.
var ErrCacheMiss = errors.New("auth: token cache miss")

// CredentialStore reads and writes a connector's stored config, whose
// Credentials field holds the refresh token. Save writes the whole config,
// including enabled, settings and deleted_at, from the caller's snapshot.
// CompareAndSwapCredentials writes only the credentials, and only while the
// row is live and still at the CredentialsUpdatedAt the caller read; a
// refresher that persists through it cannot resurrect a disconnected
// connector. The runner store's IConnectorConfigDAO satisfies it.
type CredentialStore interface {
	Get(ctx context.Context, orgID string, connectorID sdk.ConnectorID) (*sdk.ConnectorConfig, error)
	Save(ctx context.Context, cfg *sdk.ConnectorConfig) error
	CompareAndSwapCredentials(ctx context.Context, orgID string, connectorID sdk.ConnectorID, expectedCredentialsUpdatedAt time.Time, credentials json.RawMessage) (swapped bool, err error)
}

// TokenCache holds access tokens between runs. Get returns ErrCacheMiss for
// a missing key; any other error means the cache could not be read.
type TokenCache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// Locker hands out short exclusive locks by key.
type Locker interface {
	// TryLock takes key for ttl. ok is false when another holder has it; an
	// error means the lock service could not be reached.
	TryLock(ctx context.Context, key string, ttl time.Duration) (lock Lock, ok bool, err error)
}

// Lock is one held lock.
type Lock interface {
	// Unlock releases the lock only if this holder still owns it: a lock
	// that expired and was taken by another holder is left alone.
	Unlock(ctx context.Context) error
}

// PersistTimeout bounds the write of a rotated refresh token, and the
// re-read that checks a cached access token against the credential fence.
const PersistTimeout = 10 * time.Second

// RefreshLockSlack is what a refresh lock's TTL adds to its token exchange
// timeout. The swap of the rotated token and the cache check run under the
// lock after the exchange, each bounded by PersistTimeout, and the rest is
// margin. A lock that expired while its holder still exchanged or wrote would
// let a second worker refresh with the refresh token the first one rotated.
const RefreshLockSlack = 2*PersistTimeout + 5*time.Second

// SwapRefreshedCredentials stores credentials minted by a token refresh,
// replacing only the credentials and only while the row still carries
// expected, the credentials_updated_at the refresh read. It reports false
// when another credential write (a reconnect, a disconnect) landed in
// between, and that write stands. On a swap it returns the fence the swap
// wrote, read back in the same scope. It runs on a context detached from
// ctx's cancellation, bounded at ten seconds, because the vendor has already
// rotated the refresh token: a write lost to a run's deadline would leave a
// dead token stored.
func SwapRefreshedCredentials(ctx context.Context, tx store.TxRunner, creds CredentialStore, orgID string, connectorID sdk.ConnectorID, expected time.Time, credentials json.RawMessage) (swapped bool, fence time.Time, err error) {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PersistTimeout)
	defer cancel()
	err = tx.RunInScope(saveCtx, orgID, store.ScopeEnforced, func(sctx context.Context) error {
		var err error
		swapped, err = creds.CompareAndSwapCredentials(sctx, orgID, connectorID, expected, credentials)
		if err != nil || !swapped {
			return err //nolint:wrapcheck // the caller wraps it
		}
		cfg, err := creds.Get(sctx, orgID, connectorID)
		if err != nil {
			return err //nolint:wrapcheck // the caller wraps it
		}
		fence = cfg.CredentialsUpdatedAt
		return nil
	})
	return swapped, fence, err //nolint:wrapcheck // the caller wraps it
}

// CacheIfCredentialsUnchanged caches an access token minted from the
// credentials whose credential fence was fence, and keeps it only while the
// stored row still carries that fence. It sets the entry first and then
// re-reads the row: a reconnect or disconnect writes the credentials and then
// forgets the cache, so a write that lands before the re-read is seen and the
// entry is deleted, and one that lands after it forgets the entry itself. It
// reports whether the entry was kept. The read runs on a detached context,
// bounded like the swap.
func CacheIfCredentialsUnchanged(ctx context.Context, tx store.TxRunner, creds CredentialStore, cache TokenCache, orgID string, connectorID sdk.ConnectorID, fence time.Time, key, value string, ttl time.Duration) (bool, error) {
	if err := cache.Set(ctx, key, value, ttl); err != nil {
		return false, fmt.Errorf("cache access token: %w", err)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), PersistTimeout)
	defer cancel()
	var current time.Time
	err := tx.RunInScope(readCtx, orgID, store.ScopeEnforced, func(sctx context.Context) error {
		cfg, err := creds.Get(sctx, orgID, connectorID)
		if err != nil {
			return err //nolint:wrapcheck // wrapped below
		}
		current = cfg.CredentialsUpdatedAt
		return nil
	})
	if err != nil || !current.Equal(fence) {
		_ = cache.Delete(readCtx, key)
		if err != nil {
			return false, fmt.Errorf("re-read credentials after caching: %w", err)
		}
		return false, nil
	}
	return true, nil
}

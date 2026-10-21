// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// One Kiro reports bucket belongs to one organisation, and the database is what says so.
//
// These assert the refusal comes from the index rather than from a read-then-write in
// application code, because the two are not equivalent: two submissions naming one bucket
// can both read "unclaimed" before either writes, and an application check would let both
// through. The consequence of losing that race is one organisation's reports collected
// into another's tenant, so the guarantee has to hold where the race cannot happen.

const kiroBucketTestBucket = "acme-kiro-reports"

func kiroConfig(orgID, bucket, prefix string) *sdk.ConnectorConfig {
	return &sdk.ConnectorConfig{
		OrganizationID: orgID,
		ConnectorID:    "kiro",
		// A real secret, so the encryptor has something to hide. Nothing reads it here;
		// it is the contrast that matters — see the legibility assertion below.
		Credentials: json.RawMessage(`{"role_arn":"arn:aws:iam::123456789012:role/Read"}`),
		Settings: json.RawMessage(fmt.Sprintf(
			`{"bucket":%q,"prefix":%q,"region":"us-east-1","account_id":"123456789012"}`, bucket, prefix)),
		Enabled: true,
	}
}

func storedSettingsBucket(t *testing.T, db *gorm.DB, orgID string) string {
	t.Helper()
	var bucket string
	err := db.Raw(
		"SELECT settings ->> 'bucket' FROM connector_configs WHERE organization_id = ? AND connector_id = 'kiro'",
		orgID,
	).Row().Scan(&bucket)
	require.NoError(t, err)
	return bucket
}

// realEncryptor matters because the repository treats a missing key as no encryption: a
// test on the keyless path stores credentials in the clear, which is not the shape the
// index was designed around. The store migration states why that shape decides where the
// bucket lives.
func realEncryptor(t *testing.T) cryptutil.Encryptor {
	t.Helper()
	enc, err := cryptutil.NewAESGCMEncryptor(make([]byte, 32))
	require.NoError(t, err)
	return enc
}

func TestKiroBucketIsClaimedByOneOrganisation(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, kiroConfig("org-kiro-first", kiroBucketTestBucket, "kiro")))

	// A failed statement aborts the enclosing transaction, so the conflict runs inside a
	// savepoint — otherwise every assertion after it fails with SQLSTATE 25P02 and the
	// test would report the abort rather than the refusal.
	require.NoError(t, tx.SavePoint("before_conflict").Error)
	err := repo.Save(ctx, kiroConfig("org-kiro-second", kiroBucketTestBucket, "kiro"))
	require.Error(t, err, "a second organisation must not be able to claim a bucket already claimed")
	assert.Contains(t, err.Error(), "uniq_kiro_bucket_per_org",
		"the refusal must name the index, which is what proves it came from the database rather than from application code")
	require.NoError(t, tx.RollbackTo("before_conflict").Error)

	// The claim is the organisation's to keep: reconnecting, or correcting the prefix,
	// must not be refused by the organisation's own earlier claim.
	require.NoError(t, repo.Save(ctx, kiroConfig("org-kiro-first", kiroBucketTestBucket, "kiro/reports")),
		"the owning organisation must be able to re-save its own bucket")

	// And the guarantee rests on the bucket being legible to an index while the
	// credentials are not. Both halves are asserted together because either one alone
	// would pass in a setup that has no key.
	assert.Equal(t, kiroBucketTestBucket, storedSettingsBucket(t, tx, "org-kiro-first"),
		"the bucket must be readable as stored text")
	assert.NotContains(t, fetchStoredCredentials(t, tx, "org-kiro-first", "kiro"), "arn:aws:iam",
		"credentials must be ciphertext")
}

// The index is scoped to this connector, so it must not constrain a value another
// connector happens to store under the same key.
func TestAnotherConnectorMayStoreTheSameBucketString(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, kiroConfig("org-scope-a", kiroBucketTestBucket, "kiro")))

	other := kiroConfig("org-scope-b", kiroBucketTestBucket, "kiro")
	other.ConnectorID = "cursor"
	require.NoError(t, repo.Save(ctx, other),
		"the rule is about Kiro's buckets; another connector's settings must be untouched by it")
}

// A configuration carrying no bucket is the common case for every other connector, and
// several of them must be able to coexist. An absent JSON key and a cleared one read back
// differently — null and empty string — and both shapes occur, so both are excluded from
// the index rather than only the one that would obviously collide.
func TestConfigurationsWithoutABucketDoNotCollide(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	absent := kiroConfig("org-nobucket-a", "", "kiro")
	absent.Settings = json.RawMessage(`{"prefix":"kiro"}`)
	require.NoError(t, repo.Save(ctx, absent))

	alsoAbsent := kiroConfig("org-nobucket-b", "", "kiro")
	alsoAbsent.Settings = json.RawMessage(`{"prefix":"kiro"}`)
	require.NoError(t, repo.Save(ctx, alsoAbsent), "two configurations with no bucket must not collide")

	cleared := kiroConfig("org-nobucket-c", "", "kiro")
	require.NoError(t, repo.Save(ctx, cleared), "an empty bucket string must not collide with an absent one")

	alsoCleared := kiroConfig("org-nobucket-d", "", "kiro")
	require.NoError(t, repo.Save(ctx, alsoCleared), "nor two empty ones with each other")
}

// A disconnected organisation stops holding its claim, or a bucket handed from one
// organisation to another could never be connected again.
func TestASoftDeletedConfigurationReleasesItsBucket(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, kiroConfig("org-release-a", kiroBucketTestBucket, "kiro")))
	require.NoError(t, tx.Exec(
		"UPDATE connector_configs SET deleted_at = now() WHERE organization_id = ? AND connector_id = 'kiro'",
		"org-release-a").Error)

	require.NoError(t, repo.Save(ctx, kiroConfig("org-release-b", kiroBucketTestBucket, "kiro")),
		"a released bucket must be claimable by the organisation that now owns it")
}

// Disabling is the lever an operator actually has, so it has to be the one that releases.
//
// An organisation that offboards has its connectors DISABLED, not disconnected — nobody
// calls the disconnect endpoint on the way out. Keying the claim on the soft delete alone
// meant that bucket was held forever by a row no operator could reach, for exactly the
// case the rule was written to unblock. Only an enabled connector collects, so an enabled
// one is all the claim needs to protect.
func TestADisabledConfigurationReleasesItsBucket(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, kiroConfig("org-disabled-a", kiroBucketTestBucket, "kiro")))

	// What DisableConfigsForOrg does: enabled false, deleted_at untouched.
	require.NoError(t, tx.Exec(
		"UPDATE connector_configs SET enabled = false WHERE organization_id = ? AND connector_id = 'kiro'",
		"org-disabled-a").Error)

	require.NoError(t, repo.Save(ctx, kiroConfig("org-disabled-b", kiroBucketTestBucket, "kiro")),
		"a disabled organisation must stop holding its claim")
}

// And the claim still binds where it matters: two organisations may each hold a disabled
// row for one bucket, but only one of them can be collecting from it.
func TestOnlyOneDisabledClaimantMayEnable(t *testing.T) {
	t.Parallel()
	if testDB == nil {
		t.Skip("no test database")
	}

	tx := newTestTx(t)
	repo := store.NewConnectorConfigRepo(tx, realEncryptor(t))
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, kiroConfig("org-enable-a", kiroBucketTestBucket, "kiro")))
	require.NoError(t, tx.Exec(
		"UPDATE connector_configs SET enabled = false WHERE organization_id = ? AND connector_id = 'kiro'",
		"org-enable-a").Error)
	require.NoError(t, repo.Save(ctx, kiroConfig("org-enable-b", kiroBucketTestBucket, "kiro")))

	// B now holds it enabled. A re-enabling would put two enabled rows on one bucket,
	// which is the state the index exists to refuse — so widening the predicate must not
	// have widened that.
	require.NoError(t, tx.SavePoint("before_reenable").Error)
	err := repo.Save(ctx, kiroConfig("org-enable-a", kiroBucketTestBucket, "kiro"))
	require.Error(t, err, "re-enabling must not give two organisations one bucket")
	assert.Contains(t, err.Error(), "uniq_kiro_bucket_per_org",
		"the refusal must come from the database")
	require.NoError(t, tx.RollbackTo("before_reenable").Error)
}

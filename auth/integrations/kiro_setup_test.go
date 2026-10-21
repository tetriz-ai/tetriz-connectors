// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorconfig "github.com/tetriz-ai/tetriz-connectors/connectors/config"
	connectorkiro "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	connectordao "github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ---- a bucket that exists only in memory -------------------------------------------

const (
	kiroTestBucket  = "acme-kiro-reports"
	kiroTestPrefix  = "kiro"
	kiroTestAccount = "123456789012"
	kiroTestRegion  = "us-east-1"
)

// fakeBucket answers the three questions the connect flow asks of an object store, from a
// set of keys. Keys are the whole fixture: the flow's job is to work out what a bucket
// holds from its key hierarchy, so a fake built on keys exercises exactly that.
type fakeBucket struct {
	keys       []string
	unreadable bool // lists cleanly, refuses every read: the half-granted policy
	listErr    error
	probes     int // how many times the read grant was actually put to the test
}

func (f *fakeBucket) ListPrefixes(_ context.Context, _, prefix string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	seen := map[string]struct{}{}
	for _, k := range f.keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		seg, _, found := strings.Cut(rest, "/")
		if !found || seg == "" {
			continue
		}
		seen[seg] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out, nil
}

func (f *fakeBucket) ListDay(_ context.Context, _, prefix string) ([]connectorkiro.SourceObject, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []connectorkiro.SourceObject
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, connectorkiro.SourceObject{Key: k, LastModified: time.Now().UTC(), Size: 128})
		}
	}
	return out, nil
}

func (f *fakeBucket) ProbeReadable(_ context.Context, _, key string) error {
	f.probes++
	if f.unreadable {
		return fmt.Errorf("reading %s: %w", key, connectorkiro.ErrObjectUnreadable)
	}
	return nil
}

// kiroKey builds one vendor key in the documented layout.
func kiroKey(clientType string, d time.Time) string {
	return fmt.Sprintf("%s/AWSLogs/%s/KiroLogs/user_report/%s/%04d/%02d/%02d/00/%s_%s_user_report_1717171717.csv",
		kiroTestPrefix, kiroTestAccount, kiroTestRegion, d.Year(), int(d.Month()), d.Day(), clientType, kiroTestAccount)
}

// kiroUndatedKey builds a key that resolves an account and a region but no year, so the
// boundary walk finds no day. The segment where a year belongs is deliberately not the
// vendor's fixed-width numeric shape.
func kiroUndatedKey() string {
	return fmt.Sprintf("%s/AWSLogs/%s/KiroLogs/user_report/%s/pending/placeholder.csv",
		kiroTestPrefix, kiroTestAccount, kiroTestRegion)
}

// serviceWithBucket wires the connect handlers to an in-memory bucket, so what they decide
// can be asserted without an object store. Built through NewService like every other
// connector's setup test, so the seams are exercised the way production supplies them.
func serviceWithBucket(b *fakeBucket, dao connectordao.IConnectorConfigDAO) *Service {
	return NewService(Deps{
		// The environment-wide switch is on for these; the case where it is off has its
		// own test, because a off switch that is never exercised is a off switch nobody
		// knows the state of.
		Cfg:             &connectorconfig.Config{EnabledConnectors: map[string]bool{connectorIDKiro: true}},
		ConnectorDAO:    dao,
		SyncStateDAO:    noopSyncStateDAO{},
		KiroRegionOf:    func(context.Context, string) (string, error) { return kiroTestRegion, nil },
		KiroReaderFor:   func(context.Context, string) (KiroBucketReader, error) { return b, nil },
		KiroPrincipalOf: func(context.Context) (string, error) { return "arn:aws:iam::999:role/Reader", nil },
	})
}

func validateKiro(t *testing.T, s *Service, path string) (*connintv1.ValidateKiroBucketResponse, error) {
	t.Helper()
	return s.ValidateKiroBucket(context.Background(),
		&connintv1.ValidateKiroBucketRequest{OrgId: "org-kiro", BucketPath: path})
}

// ---- what the dialogue decides ------------------------------------------------------

// The path is the only thing the administrator types, so the shapes it can take are the
// first thing that has to hold.
func TestKiroBucketPathAcceptsWhatAnAdministratorWouldType(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"s3://acme-kiro-reports/kiro",
		"acme-kiro-reports/kiro",
		"  s3://acme-kiro-reports/kiro/  ",
	} {
		bucket, prefix, err := parseKiroBucketPath(in)
		require.NoError(t, err, "input %q", in)
		assert.Equal(t, "acme-kiro-reports", bucket)
		assert.Equal(t, "kiro", prefix)
	}

	// A bucket root is refused rather than accepted and left to fail later: the vendor
	// will not write reports there, so a path without a prefix describes a layout that
	// cannot exist.
	_, _, err := parseKiroBucketPath("s3://acme-kiro-reports")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prefix is required")
}

// A grant that lists but cannot read is the failure this flow exists to catch: it passes
// every listing and fails every fetch, so without the single probe the connector would
// save and then fail on its first scheduled run with nobody watching.
func TestAListOnlyGrantIsRefused(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	bucket := &fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}, unreadable: true}
	s := serviceWithBucket(bucket, nil)

	_, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.Error(t, err, "a bucket we cannot read from must not validate")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "s3:GetObject",
		"the message must name the missing permission, or the administrator cannot act on it")
	assert.NotZero(t, bucket.probes,
		"the refusal must come from an attempted read, not from inspecting keys")
}

// A bucket that lists cleanly and holds nothing cannot have its read grant proven at all.
// Saying "verified" would be false confidence in a different place, so it saves as
// provisional and says so.
func TestAnEmptyBucketIsProvisionalRatherThanVerified(t *testing.T) {
	t.Parallel()

	s := serviceWithBucket(&fakeBucket{}, nil)
	resp, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.NoError(t, err, "an empty bucket is not an error: reporting may simply not be switched on yet")
	assert.Equal(t, connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL, resp.GetState())
	assert.Nil(t, resp.GetInventory(), "there is nothing to report an inventory about")
}

// Reading is UNPROVEN in the provisional state, so the screen may claim listing and no more.
//
// The bucket is READABLE here deliberately. Against an unreadable one the probe count proves
// nothing — a stray probe would return an error and fail the assertion above it, so the count
// would never be what caught the regression. Readable inverts that: a stray probe returns nil
// and slips past every other assertion, leaving the count as the only check that fires.
//
// Two of the three provisional returns are covered. The third — a day that lists no object —
// cannot be built on this fake, which derives its prefixes FROM its keys: a day prefix exists
// only because a key under it does, and such a key always satisfies ListDay.
func TestProvisionalLeavesReadingUnproven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		keys []string
	}{
		{name: "no account holds reports", keys: nil},
		{name: "an account holds no year", keys: []string{kiroUndatedKey()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bucket := &fakeBucket{keys: tt.keys}
			s := serviceWithBucket(bucket, nil)
			resp, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
			require.NoError(t, err, "a bucket holding no report is not an error")
			assert.Equal(t, connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL, resp.GetState())
			assert.Zero(t, bucket.probes,
				"no object exists to fetch, so reading is untested here and the copy must not claim it")
		})
	}
}

// Proof that the undated fixture above resolves an account and a region, rather than being a
// second spelling of the empty-bucket path — which would make the two-path claim false while
// both subtests still passed.
//
// The region cross-check is reachable ONLY once an account has resolved, so the same key one
// region away from the bucket's own must be refused. An unresolved account would return
// provisional instead and this would not error at all.
func TestTheUndatedFixtureResolvesAnAccount(t *testing.T) {
	t.Parallel()

	elsewhere := strings.Replace(kiroUndatedKey(), kiroTestRegion, "eu-west-1", 1)
	s := serviceWithBucket(&fakeBucket{keys: []string{elsewhere}}, nil)

	_, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.Error(t, err, "a resolved account whose reports sit in another region must be refused")
	assert.Contains(t, err.Error(), "eu-west-1",
		"the refusal names the region read off the key, which only a resolved account provides")
}

// Before the grant is added, the call that cannot reach the bucket is the one whose answer
// the administrator most needs — the policy they have yet to paste.
func TestABucketWeCannotListStillReturnsThePolicy(t *testing.T) {
	t.Parallel()

	s := serviceWithBucket(&fakeBucket{listErr: errors.New("AccessDenied")}, nil)
	resp, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.NoError(t, err, "not yet granted is the expected middle of the dialogue, not a failure")
	assert.Equal(t, connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_NEEDS_GRANT, resp.GetState())
	assert.Contains(t, resp.GetReadPolicy(), "s3:ListBucket", "the policy is what the administrator needs next")
}

// The inventory's exact half comes from the key hierarchy's boundaries, so it must span the
// whole history rather than the sampled window — a bucket older than that window is exactly
// where a sampled range would understate the depth.
func TestTheInventoryReportsTheFullRangeAndTheSampledWindowApart(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)
	recent := now.AddDate(0, 0, -2)
	s := serviceWithBucket(&fakeBucket{keys: []string{
		kiroKey("KIRO_IDE", old),
		kiroKey("KIRO_IDE", recent),
		kiroKey("KIRO_CLI", recent),
	}}, nil)

	resp, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.NoError(t, err)
	require.Equal(t, connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_CONFIRMED, resp.GetState())

	inv := resp.GetInventory()
	require.NotNil(t, inv)
	assert.Equal(t, old.Format("2006-01-02"), inv.GetFirstDay(),
		"the range must reach the oldest day in the bucket, not the oldest in the sampled window")
	assert.Equal(t, recent.Format("2006-01-02"), inv.GetLastDay())
	assert.Greater(t, inv.GetCollectableDays(), int32(300), "a year of history is a year of depth")

	// The sampled half covers the recent window only, and says which window that was.
	assert.Equal(t, int32(kiroRecentWindowDays), inv.GetRecentWindowDays())
	assert.Equal(t, int32(2), inv.GetRecentFileCount(), "the year-old file is outside the sampled window")
	assert.Equal(t, []string{"KIRO_CLI", "KIRO_IDE"}, inv.GetRecentClientTypes())
	assert.Contains(t, inv.GetOldestObjectKey(), old.Format("2006/01/02"),
		"the oldest key is the honest statement of how far back collection can reach")
}

// The vendor requires the bucket's region to match the region of the profile writing into
// it. Both halves are observed independently — one from the object store, one from the
// vendor's own key path — which is what makes the comparison mean anything.
func TestAMismatchedRegionIsRefused(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	s := NewService(Deps{
		Cfg:          &connectorconfig.Config{EnabledConnectors: map[string]bool{connectorIDKiro: true}},
		KiroRegionOf: func(context.Context, string) (string, error) { return "eu-west-1", nil },
		KiroReaderFor: func(context.Context, string) (KiroBucketReader, error) {
			return &fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, nil
		},
		KiroPrincipalOf: func(context.Context) (string, error) { return "arn:aws:iam::999:role/Reader", nil },
	})

	_, err := validateKiro(t, s, "s3://"+kiroTestBucket+"/"+kiroTestPrefix)
	require.Error(t, err, "a bucket in one region holding another region's reports is not the right bucket")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "eu-west-1")
	assert.Contains(t, err.Error(), kiroTestRegion)
}

// ---- what reaches storage -----------------------------------------------------------

// The submitted path is the only client-supplied value that is persisted. Everything else
// stored is a value this call observed for itself, which is what makes the configuration
// something the server saw rather than something it was told.
func TestSubmitPersistsOnlyWhatItObserved(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	saved := &savingConnectorDAO{}
	s := serviceWithBucket(&fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, saved)

	resp, err := s.SubmitKiroBucket(context.Background(), &connintv1.SubmitKiroBucketRequest{
		OrgId: "org-kiro", BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix,
	})
	require.NoError(t, err)
	assert.Equal(t, connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_CONFIRMED, resp.GetState())

	require.NotNil(t, saved.cfg, "a confirmed bucket must be saved")
	var settings connectorkiro.Settings
	require.NoError(t, json.Unmarshal(saved.cfg.Settings, &settings))

	assert.Equal(t, kiroTestBucket, settings.Bucket, "the path is the one value taken from the request")
	assert.Equal(t, kiroTestPrefix, settings.Prefix)
	// Neither of these was in the request. The region came from the object store and the
	// account from the vendor's own key path, so a caller cannot choose either.
	assert.Equal(t, kiroTestRegion, settings.Region)
	assert.Equal(t, kiroTestAccount, settings.AccountID)
	assert.Empty(t, saved.cfg.Credentials, "the default grant carries no secret to store")
}

// Submit re-runs the walk rather than trusting the earlier call, so a bucket that stopped
// being readable between the two steps is refused at the point of saving.
func TestSubmitRefusesABucketThatIsNoLongerReadable(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	saved := &savingConnectorDAO{}
	s := serviceWithBucket(&fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}, unreadable: true}, saved)

	_, err := s.SubmitKiroBucket(context.Background(), &connintv1.SubmitKiroBucketRequest{
		OrgId: "org-kiro", BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix,
	})
	require.Error(t, err)
	assert.Nil(t, saved.cfg, "nothing may be stored for a bucket whose grant could not be proven")
}

// A bucket already connected elsewhere is refused by the database, and the handler has to
// say so in terms an administrator can act on rather than as an internal failure.
func TestAnAlreadyClaimedBucketIsReportedAsSuch(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	s := serviceWithBucket(&fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, &savingConnectorDAO{
		err: errors.New(`ERROR: duplicate key value violates unique constraint "uniq_kiro_bucket_per_org"`),
	})

	_, err := s.SubmitKiroBucket(context.Background(), &connintv1.SubmitKiroBucketRequest{
		OrgId: "org-kiro", BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix,
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Contains(t, err.Error(), "another organisation")
}

// ---- the policy ---------------------------------------------------------------------

// The policy is the artifact the customer grants from, so both halves have to be present
// and scoped: listing is a permission on the bucket, reading a permission on the objects,
// and a policy naming only one produces exactly the half-working grant the probe catches.
func TestTheReadPolicyGrantsBothHalvesAndScopesThem(t *testing.T) {
	t.Parallel()

	policy := kiroReadPolicy("acme-kiro-reports", "kiro", "arn:aws:iam::999:role/Reader", testOrgID)

	var parsed struct {
		Statement []struct {
			Action    []string       `json:"Action"`
			Resource  string         `json:"Resource"`
			Principal map[string]any `json:"Principal"`
			Condition map[string]any `json:"Condition"`
		} `json:"Statement"`
	}
	require.NoError(t, json.Unmarshal([]byte(policy), &parsed), "the policy must be valid JSON to be pasteable")
	require.Len(t, parsed.Statement, 2)

	byAction := map[string]int{}
	for i, st := range parsed.Statement {
		byAction[st.Action[0]] = i
		assert.Equal(t, "arn:aws:iam::999:role/Reader", st.Principal["AWS"],
			"every statement must name the principal that will actually read")
	}
	list, listOK := byAction["s3:ListBucket"]
	read, readOK := byAction["s3:GetObject"]
	require.True(t, listOK && readOK, "both permissions are required; one alone is the half-working grant")

	assert.Equal(t, "arn:aws:s3:::acme-kiro-reports", parsed.Statement[list].Resource,
		"listing is a permission on the bucket itself")
	assert.Equal(t, "arn:aws:s3:::acme-kiro-reports/kiro/*", parsed.Statement[read].Resource,
		"reading is scoped to the prefix, so the grant reaches the reports and nothing else")
	assert.NotNil(t, parsed.Statement[list].Condition,
		"s3:ListBucket is bucket-wide however the resource is written, so it needs a prefix condition")
}

// A running process reports a transient session identity, and a policy naming it would stop
// matching the moment that session rotated — a grant that works when pasted and fails later.
func TestThePolicyNamesTheDurableRoleRatherThanASession(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "arn:aws:iam::123456789012:role/ConnectorReader",
		kiroRolePrincipal("arn:aws:sts::123456789012:assumed-role/ConnectorReader/i-0abc123"))

	// Shapes that are already durable pass through untouched.
	for _, arn := range []string{
		"arn:aws:iam::123456789012:role/ConnectorReader",
		"arn:aws:iam::123456789012:user/deploy",
	} {
		assert.Equal(t, arn, kiroRolePrincipal(arn))
	}
}

// savingConnectorDAO captures what the handler tried to persist. It implements only the
// methods this flow calls; the embedded interface makes any other call a loud nil panic
// rather than a silent success.
type savingConnectorDAO struct {
	connectordao.IConnectorConfigDAO
	cfg *sdk.ConnectorConfig
	err error
	// current is what the row already holds, so the mutator sees a reconnect rather than
	// a first insert.
	current json.RawMessage
}

func (d *savingConnectorDAO) SaveCredentialsEnableWithSettings(
	_ context.Context, cfg *sdk.ConnectorConfig, mutator func(json.RawMessage) (json.RawMessage, error),
) error {
	if d.err != nil {
		return d.err
	}
	settings, err := mutator(d.current)
	if err != nil {
		return err
	}
	stored := *cfg
	stored.Settings = settings
	d.cfg = &stored
	return nil
}

func (d *savingConnectorDAO) ResetHealthState(
	context.Context, string, sdk.ConnectorID,
) (bool, error) {
	return false, nil
}

// noopSyncStateDAO stands in for cursor state. Only the path-change reset touches it, and
// that case asserts the call rather than its effect.
type noopSyncStateDAO struct{}

func (noopSyncStateDAO) GetCursor(context.Context, string, sdk.ConnectorID, sdk.OperationID) (*sdk.Cursor, error) {
	return nil, nil
}

func (noopSyncStateDAO) SaveCursor(context.Context, string, sdk.ConnectorID, sdk.OperationID, *sdk.Cursor) error {
	return nil
}

func (noopSyncStateDAO) DeleteByConnector(context.Context, string, sdk.ConnectorID) error {
	return nil
}

// countingSyncStateDAO records whether the reconnect cleared the cursor.
type countingSyncStateDAO struct {
	noopSyncStateDAO
	deletes int
}

func (d *countingSyncStateDAO) DeleteByConnector(context.Context, string, sdk.ConnectorID) error {
	d.deletes++
	return nil
}

// The environment-wide switch refuses every organisation or none, and it is a different
// gate from any per-organization gate a host applies. Without this check an
// organisation a host lets through could connect while an operator believed the switch
// still held the connector back — the one direction a off switch must never fail.
func TestTheEnvironmentSwitchRefusesBothCalls(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	off := NewService(Deps{
		Cfg:          &connectorconfig.Config{EnabledConnectors: map[string]bool{connectorIDKiro: false}},
		KiroRegionOf: func(context.Context, string) (string, error) { return kiroTestRegion, nil },
		KiroReaderFor: func(context.Context, string) (KiroBucketReader, error) {
			return &fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, nil
		},
		KiroPrincipalOf: func(context.Context) (string, error) { return "arn:aws:iam::999:role/Reader", nil },
	})
	path := "s3://" + kiroTestBucket + "/" + kiroTestPrefix

	_, err := validateKiro(t, off, path)
	require.Error(t, err, "validate must refuse while the environment switch is off")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = off.SubmitKiroBucket(context.Background(),
		&connintv1.SubmitKiroBucketRequest{OrgId: "org-kiro", BucketPath: path})
	require.Error(t, err, "submit must refuse while the environment switch is off")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// A bucket with no reports yields no account, and the scheduled run refuses a configuration
// without one — so saving would record a connection that cannot run until someone
// reconnects. Refusing and saying why is the better answer.
func TestAnEmptyBucketCannotBeSaved(t *testing.T) {
	t.Parallel()

	saved := &savingConnectorDAO{}
	s := serviceWithBucket(&fakeBucket{}, saved)

	_, err := s.SubmitKiroBucket(context.Background(), &connintv1.SubmitKiroBucketRequest{
		OrgId: "org-kiro", BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Kiro reports yet")
	assert.Nil(t, saved.cfg, "nothing may be stored for a bucket that cannot be configured from")
}

// The tuning dials are an operator's answers about read cost and backfill depth. Nothing in
// a connect call observes them, so reconnecting to fix a typo must not silently return an
// organisation to defaults.
func TestReconnectKeepsTheOperatorsTuningAndResetsTheCursorOnANewPath(t *testing.T) {
	t.Parallel()

	day := time.Now().UTC().AddDate(0, 0, -2)
	cursors := &countingSyncStateDAO{}
	saved := &savingConnectorDAO{
		current: []byte(`{"bucket":"old-bucket","prefix":"kiro","region":"us-east-1",` +
			`"account_id":"123456789012","trailing_overlap_days":7,"first_run_window_days":90}`),
	}
	s := NewService(Deps{
		Cfg:          &connectorconfig.Config{EnabledConnectors: map[string]bool{connectorIDKiro: true}},
		ConnectorDAO: saved,
		SyncStateDAO: cursors,
		KiroRegionOf: func(context.Context, string) (string, error) { return kiroTestRegion, nil },
		KiroReaderFor: func(context.Context, string) (KiroBucketReader, error) {
			return &fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, nil
		},
		KiroPrincipalOf: func(context.Context) (string, error) { return "arn:aws:iam::999:role/Reader", nil },
	})

	_, err := s.SubmitKiroBucket(context.Background(), &connintv1.SubmitKiroBucketRequest{
		OrgId: "org-kiro", BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix,
	})
	require.NoError(t, err)

	var stored connectorkiro.Settings
	require.NoError(t, json.Unmarshal(saved.cfg.Settings, &stored))
	assert.Equal(t, kiroTestBucket, stored.Bucket, "the corrected path is taken")
	assert.Equal(t, 7, stored.TrailingOverlapDays, "the operator's overlap survives a reconnect")
	assert.Equal(t, 90, stored.FirstRunWindowDays, "so does their backfill depth")

	assert.Equal(t, 1, cursors.deletes,
		"a new path is a new history: the old high-water mark would skip every report older than it")
}

// A prefix containing an IAM wildcard would generate a grant over far more of the customer's
// bucket than the screen describes, so it is refused before a policy is ever built.
func TestAWildcardInThePathIsRefused(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"s3://acme-kiro-reports/kiro*",
		"s3://acme-kiro-reports/ki?ro",
		"s3://acme-kiro-reports/kiro/../other",
	} {
		_, _, err := parseKiroBucketPath(path)
		require.Error(t, err, "input %q must be refused before it reaches a policy", path)
	}

	// And a bucket name that is not one.
	_, _, err := parseKiroBucketPath("s3://Not_A_Bucket/kiro")
	require.Error(t, err)
}

func TestOneBucketLocationHasOneStoredSpelling(t *testing.T) {
	t.Parallel()

	// The stored path is compared as a string to decide whether the path changed, and a
	// path change clears the cursor. So a stray slash in a retyped path would reconnect to
	// the same location and restart a backfill of up to four hundred days.
	for _, raw := range []string{
		"s3://acme-kiro-reports/kiro",
		"s3://acme-kiro-reports//kiro",
		"s3://acme-kiro-reports/kiro/",
		"acme-kiro-reports/kiro",
		"  s3://acme-kiro-reports///kiro//  ",
	} {
		bucket, prefix, err := parseKiroBucketPath(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, "acme-kiro-reports", bucket, raw)
		assert.Equal(t, "kiro", prefix, "every spelling of one location must store the same prefix: %q", raw)
	}

	// A path whose prefix is only separators is still a bucket root, which the vendor
	// refuses to write to — so it must be refused rather than normalised into nothing.
	_, _, err := parseKiroBucketPath("s3://acme-kiro-reports///")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a prefix is required")
}

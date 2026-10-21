// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/auth/cryptutil"
	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
	kirogw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/kiro"
	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// An App built with TETRIZ_CREDENTIALS_KEY gives its OAuth gateways the
// credentials' encryptor, so they read a cached access token as that key's
// envelope: the token the request carries is the decrypted one. The stored
// credentials hold no refresh token, so a cache miss fails before any token
// exchange and no request leaves the test.
func TestAppReadsCachedTokensWithItsCredentialsKey(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("app reads cached tokens with its key", err)
		t.Skip("no test database")
	}
	ctx := context.Background()
	_, dsn, cleanup, err := testdb.SetupAsOwner(ctx, testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	mr := miniredis.RunT(t)
	var factory sdk.VendorGatewayFactory
	a, err := New(ctx, Config{PostgresDSN: dsn, RedisAddr: mr.Addr(), CredentialsKey: base64.StdEncoding.EncodeToString(key),
		SinkPath: "log", BlobDir: t.TempDir(), Concurrency: 1, DefaultTenantKind: sdk.TenantKindOrganization,
		OAuth: map[string]OAuthClient{"jira": {ClientID: "id", ClientSecret: "secret"}}},
		WithGatewayWrapper(func(f sdk.VendorGatewayFactory) sdk.VendorGatewayFactory { factory = f; return f }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	const org = "00000000-0000-4000-8000-0000000c0c55"
	require.NoError(t, WriteConfig(ctx, store.PostgresTxRunner{}, a.Configs, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "jira",
		Settings: []byte(`{"cloud_id":"c"}`), Credentials: []byte(`{}`), Enabled: true, HealthState: sdk.HealthStateHealthy}))
	enc, err := cryptutil.NewAESGCMEncryptor(key)
	require.NoError(t, err)
	sealed, err := enc.Encrypt([]byte("cached-access-token"))
	require.NoError(t, err)
	require.NoError(t, mr.Set(jiraconfig.AccessTokenKey(org), string(sealed)))

	var mu sync.Mutex
	var authz string
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authz = r.Header.Get("Authorization")
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer vendor.Close()

	var cfg *sdk.ConnectorConfig
	require.NoError(t, store.PostgresTxRunner{}.RunInScope(ctx, org, store.ScopeEnforced, func(sctx context.Context) error {
		var gerr error
		cfg, gerr = a.Configs.Get(sctx, org, "jira")
		return gerr
	}))
	require.NotNil(t, factory)
	gw, err := factory(ctx, cfg)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, vendor.URL, nil)
	require.NoError(t, err)
	resp, err := gw.Do(ctx, req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "Bearer cached-access-token", authz, "the cached envelope is decrypted with the App's key")
}

// The gateway factory builds a Kiro gateway for the run's organization: every
// request carries the canonical organization's Referer, which the bucket
// policy the connect flow generates requires.
func TestKiroGatewayCarriesTheOrganizationsReferer(t *testing.T) {
	const org = "0a1b2c3d-0000-4000-8000-00000000abcd"
	var mu sync.Mutex
	var referers []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		referers = append(referers, r.Header.Get("Referer"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer srv.Close()
	prev := awscreds.Load
	awscreds.Load = func(_ context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		var o awsconfig.LoadOptions
		for _, fn := range optFns {
			if err := fn(&o); err != nil {
				return aws.Config{}, err
			}
		}
		return aws.Config{Region: o.Region, BaseEndpoint: aws.String(srv.URL),
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake-secret", Source: "test"}, nil
			})}, nil
	}
	t.Cleanup(func() { awscreds.Load = prev })

	factory := newGatewayFactory(gatewayDeps{})
	gw, err := factory(context.Background(), &sdk.ConnectorConfig{OrganizationID: strings.ToUpper(org), ConnectorID: "kiro",
		Settings: []byte(`{"bucket":"bucket","prefix":"reports","region":"eu-west-1","account_id":"000000000000"}`)})
	require.NoError(t, err)
	kg, isKiro := gw.(*kirogw.Gateway)
	require.True(t, isKiro, "the factory builds a Kiro gateway, got %T", gw)
	_, err = kg.ListDay(context.Background(), "bucket", "reports/day/")
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{kirogw.TenantReferer(org)}, referers)
	assert.Equal(t, "tetriz-connectors/org/"+org, referers[0])
}

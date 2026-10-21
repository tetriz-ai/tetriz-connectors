// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
)

// fakeS3 is an S3 endpoint that records the Referer of every request. A
// request without want's Referer is refused 403 with the bucket's region
// header, as a bucket policy conditioned on aws:Referer refuses it.
type fakeS3 struct {
	want string

	mu       sync.Mutex
	referers map[string][]string // by "METHOD query-kind"
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	kind := r.Method
	if r.URL.Query().Has("list-type") {
		kind += " list"
	}
	f.mu.Lock()
	if f.referers == nil {
		f.referers = map[string][]string{}
	}
	f.referers[kind] = append(f.referers[kind], r.Header.Get("Referer"))
	f.mu.Unlock()
	w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
	if r.Header.Get("Referer") != f.want {
		w.WriteHeader(http.StatusForbidden)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`))
		}
		return
	}
	switch {
	case r.URL.Query().Has("list-type"):
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>bucket</Name><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>` +
			`<Contents><Key>day/report.csv</Key><Size>5</Size></Contents></ListBucketResult>`))
	case r.Method == http.MethodGet:
		_, _ = w.Write([]byte("hello"))
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// useFakeS3 points every client this package builds at srv, with static fake
// credentials, for the test's duration.
func useFakeS3(t *testing.T, srv *httptest.Server) {
	t.Helper()
	prev := awscreds.Load
	awscreds.Load = func(_ context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		var o awsconfig.LoadOptions
		for _, fn := range optFns {
			if err := fn(&o); err != nil {
				return aws.Config{}, err
			}
		}
		return aws.Config{
			Region:       o.Region,
			BaseEndpoint: aws.String(srv.URL),
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake-secret", Source: "test"}, nil
			}),
		}, nil
	}
	t.Cleanup(func() { awscreds.Load = prev })
}

// The organization's id is canonicalized in the Referer: an upper-case
// spelling of the id yields the same marker the policy names.
func TestTenantRefererIsCanonical(t *testing.T) {
	const lower = "0a1b2c3d-0000-4000-8000-00000000abcd"
	assert.Equal(t, "tetriz-connectors/org/"+lower, TenantReferer(strings.ToUpper(lower)))
	assert.Equal(t, TenantReferer(lower), TenantReferer(strings.ToUpper(lower)))
}

// A gateway built for an organization sends that organization's Referer on
// every list and read, so a bucket policy conditioned on it admits the run,
// whatever spelling of the id the caller used.
func TestGatewayForOrganizationSendsTheTenantReferer(t *testing.T) {
	const org = "0a1b2c3d-0000-4000-8000-00000000abcd"
	fake := &fakeS3{want: TenantReferer(org)}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	useFakeS3(t, srv)

	g, err := NewGatewayForOrganization(context.Background(), "eu-west-1", strings.ToUpper(org))
	require.NoError(t, err)
	objs, err := g.ListDay(context.Background(), "bucket", "day/")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	_, err = g.ListPrefixes(context.Background(), "bucket", "day/")
	require.NoError(t, err)
	body, err := g.Get(context.Background(), "bucket", "day/report.csv")
	require.NoError(t, err)
	assert.Equal(t, "hello", string(body))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Equal(t, []string{TenantReferer(org), TenantReferer(org)}, fake.referers["GET list"])
	assert.Equal(t, []string{TenantReferer(org)}, fake.referers["GET"])
}

// A gateway built without an organization sends no Referer, so the
// conditioned policy refuses it: the marker is what admits a run.
func TestGatewayWithoutAnOrganizationIsRefusedByTheConditionedPolicy(t *testing.T) {
	fake := &fakeS3{want: TenantReferer("0a1b2c3d-0000-4000-8000-00000000abcd")}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	useFakeS3(t, srv)

	g, err := NewGateway(context.Background(), "eu-west-1")
	require.NoError(t, err)
	_, err = g.ListDay(context.Background(), "bucket", "day/")
	require.Error(t, err)
}

// Region discovery sends no Referer, by design, and still works under the
// conditioned policy: the HEAD is refused, and S3 names the bucket's region on
// the refusal, which is all the lookup reads.
func TestDiscoverRegionWorksUnderTheConditionedPolicy(t *testing.T) {
	fake := &fakeS3{want: TenantReferer("0a1b2c3d-0000-4000-8000-00000000abcd")}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	useFakeS3(t, srv)

	region, err := DiscoverRegion(context.Background(), "bucket")
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", region)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	assert.Equal(t, []string{""}, fake.referers["HEAD"], "the region lookup was refused and still answered")
}

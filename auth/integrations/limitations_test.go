// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

// Tests of the connect-side guards: OAuth state, installation events and the
// Kiro grant.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jiraconfig "github.com/tetriz-ai/tetriz-connectors/connectors/jira"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	kirogateway "github.com/tetriz-ai/tetriz-connectors/internal/gateway/kiro"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The OAuth state is consumed with GETDEL before the org check, so a request
// whose org does not match burns it, like the GitHub install state. The
// concurrent-replay case is TestEdge_JiraOAuthStateCannotBeConsumedTwiceConcurrently.
func TestOAuthStateMismatchConsumesState(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	stateData, _ := json.Marshal(map[string]string{"org_id": "different-org"})
	key := jiraconfig.OAuthStateKey("lim-state")
	require.NoError(t, mr.Set(key, string(stateData)))

	svc := NewService(Deps{Cfg: jiraTestConfig(), ConnectorDAO: memstore.New(), Redis: rc, Registry: testRegistry()})
	_, err := svc.ValidateAndSaveJiraConnection(context.Background(), &connintv1.ValidateAndSaveJiraConnectionRequest{OrgId: testOrgID, AuthCode: "code", State: "lim-state"})
	require.Error(t, err)
	assert.False(t, mr.Exists(key))
}

type tenantKinds map[string]sdk.TenantKind

func (k tenantKinds) TenantType(_ context.Context, orgID string) (sdk.TenantKind, error) {
	return k[orgID], nil
}

// allOrganizations is a tenant directory in which every organization is an
// organization tenant, the one tenant kind an unsuspend re-enables.
type allOrganizations struct{}

func (allOrganizations) TenantType(context.Context, string) (sdk.TenantKind, error) {
	return sdk.TenantKindOrganization, nil
}

type failingTenants struct{}

func (failingTenants) TenantType(context.Context, string) (sdk.TenantKind, error) {
	return sdk.TenantKindUnspecified, errors.New("directory down")
}

// A GitHub App "unsuspend" event applies the connect path's tenant gate: it
// re-enables an organization tenant's connector, leaves an individual or
// unknown tenant's disabled until it confirms its repositories again, and
// refuses the event when the tenant cannot be resolved.
func TestUnsuspendAppliesTheTenantGate(t *testing.T) {
	const org = "00000000-0000-4000-8000-00000000eeee"
	body := []byte(`{"action":"unsuspend","installation":{"id":4242,"account":{"login":"acme"}}}`)
	for _, c := range []struct {
		name        string
		tenants     sdk.TenantDirectory
		wantEnabled bool
		wantCode    codes.Code
	}{
		{"organization", tenantKinds{org: sdk.TenantKindOrganization}, true, codes.OK},
		{"individual", tenantKinds{org: sdk.TenantKindIndividual}, false, codes.OK},
		{"unknown intent", tenantKinds{}, false, codes.OK},
		{"no directory", nil, false, codes.OK},
		{"lookup fails", failingTenants{}, false, codes.Unavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			mem := memstore.New()
			require.NoError(t, mem.Save(context.Background(), &sdk.ConnectorConfig{
				OrganizationID: org, ConnectorID: connectorIDGitHub, Enabled: false,
				Credentials: json.RawMessage(`{"installation_id":"4242"}`),
				Settings:    json.RawMessage(`{"installation_id":"4242"}`),
			}))
			svc := NewService(Deps{Cfg: testConfig(), ConnectorDAO: mem, Registry: testRegistry(), Tenants: c.tenants})
			err := svc.handleInstallationEvent(context.Background(), body)
			assert.Equal(t, c.wantCode, status.Code(err))
			cfg, err := mem.Get(context.Background(), org, connectorIDGitHub)
			require.NoError(t, err)
			assert.Equal(t, c.wantEnabled, cfg.Enabled)
		})
	}
}

// The Kiro bucket policy carries a per-tenant condition on both statements:
// the reader principal is shared by every tenant, so the grant is bound to the
// organization it was generated for by the marker the runner sends for it.
func TestKiroPolicyIsBoundToTheTenant(t *testing.T) {
	var p struct {
		Statement []struct {
			Sid       string
			Condition map[string]map[string][]string
		}
	}
	require.NoError(t, json.Unmarshal([]byte(kiroReadPolicy("bucket", "reports/", "arn:aws:iam::000000000000:role/reader", "org-a")), &p))
	require.Len(t, p.Statement, 2)
	assert.ElementsMatch(t, []string{"StringEquals", "StringLike"}, keys2(p.Statement[0].Condition))
	assert.Equal(t, []string{"s3:prefix"}, keys(p.Statement[0].Condition["StringLike"]))
	for _, st := range p.Statement {
		assert.Equal(t, []string{kirogateway.TenantReferer("org-a")}, st.Condition["StringEquals"]["aws:Referer"], st.Sid)
	}
	other := kiroReadPolicy("bucket", "reports/", "arn:aws:iam::000000000000:role/reader", "org-b")
	assert.NotContains(t, other, kirogateway.TenantReferer("org-a"), "another tenant's policy does not admit org-a's runs")
}

func keys2(m map[string]map[string][]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keys(m map[string][]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The Kiro bucket policy's statements are named TetrizConnectorsKiroReports*.
func TestKiroPolicyStatementIDs(t *testing.T) {
	var p struct{ Statement []struct{ Sid string } }
	require.NoError(t, json.Unmarshal([]byte(kiroReadPolicy("bucket", "reports/", "arn:aws:iam::000000000000:role/reader", "org-a")), &p))
	require.Len(t, p.Statement, 2)
	assert.Equal(t, "TetrizConnectorsKiroReportsList", p.Statement[0].Sid)
	assert.Equal(t, "TetrizConnectorsKiroReportsRead", p.Statement[1].Sid)
}

// The policy the connect flow returns names the organization's canonical id,
// the one the runner sends: an upper-case spelling of the id in the request
// does not produce a policy every scheduled run is refused by.
func TestKiroPolicyNamesTheCanonicalOrganization(t *testing.T) {
	const lower = "0a1b2c3d-0000-4000-8000-00000000abcd"
	day := time.Now().UTC().AddDate(0, 0, -2)
	s := serviceWithBucket(&fakeBucket{keys: []string{kiroKey("KIRO_IDE", day)}}, nil)
	res, err := s.ValidateKiroBucket(context.Background(),
		&connintv1.ValidateKiroBucketRequest{OrgId: strings.ToUpper(lower), BucketPath: "s3://" + kiroTestBucket + "/" + kiroTestPrefix})
	require.NoError(t, err)
	assert.Contains(t, res.GetReadPolicy(), `"tetriz-connectors/org/`+lower+`"`)
	assert.NotContains(t, res.GetReadPolicy(), strings.ToUpper(lower))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_dao "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	sdkmock "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newMockConnector creates a MockConnector with ID and Operations stubbed.
func newMockConnector(ctrl *gomock.Controller, id sdk.ConnectorID, ops []sdk.OperationDescriptor) *sdkmock.MockConnector {
	mc := sdkmock.NewMockConnector(ctrl)
	mc.EXPECT().ID().Return(id).AnyTimes()
	mc.EXPECT().Operations().Return(ops).AnyTimes()
	return mc
}

// pullOp creates an OperationDescriptor with Pull IngestionType.
func pullOp(id sdk.OperationID, schedule string) sdk.OperationDescriptor {
	return sdk.OperationDescriptor{ID: id, IngestionType: sdk.IngestionPull, DefaultSchedule: schedule}
}

// multiUserPullOp creates a Pull OperationDescriptor flagged RequiresMultiUser.
func multiUserPullOp(id sdk.OperationID, schedule string) sdk.OperationDescriptor {
	op := pullOp(id, schedule)
	op.RequiresMultiUser = true
	return op
}

// groupPullOp creates a Pull OperationDescriptor tagged into a stagger group at a fixed offset.
func groupPullOp(id sdk.OperationID, schedule, group string, offset int) sdk.OperationDescriptor {
	op := pullOp(id, schedule)
	op.StaggerGroup = group
	op.StaggerOffsetMinutes = offset
	return op
}

// individualSetOf builds a live-individual org-ID set from the given org IDs.
func individualSetOf(orgIDs ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(orgIDs))
	for _, id := range orgIDs {
		set[id] = struct{}{}
	}
	return set
}

// ---------------------------------------------------------------------------
// Tests: resolveSchedule
// ---------------------------------------------------------------------------

func TestResolveSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]*model.ScheduleOverride
		key       string
		cfg       *sdk.ConnectorConfig
		op        sdk.OperationDescriptor
		want      string
		// wantFromDefault is whether the value came from op.DefaultSchedule — the only tier
		// the emit site per-org staggers. An override (tier 1/2) must report false so it is
		// passed through verbatim.
		wantFromDefault bool
	}{
		{
			name: "priority 1: connector_schedules override wins",
			overrides: map[string]*model.ScheduleOverride{
				"org-1/github/fetch_prs": {CronExpression: "0 */2 * * *", Enabled: true},
			},
			key: "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "0 */2 * * *",
			wantFromDefault: false,
		},
		{
			name:      "priority 2: operation override from config",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{
				OperationOverrides: map[sdk.OperationID]json.RawMessage{
					"fetch_prs": json.RawMessage(`{"schedule":"*/30 * * * *"}`),
				},
			},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "*/30 * * * *",
			wantFromDefault: false,
		},
		{
			name:      "priority 3: default schedule from descriptor",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg:       &sdk.ConnectorConfig{},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "*/5 * * * *",
			wantFromDefault: true,
		},
		{
			name:      "priority 4: empty when no schedule configured",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg:       &sdk.ConnectorConfig{},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "",
			},
			want:            "",
			wantFromDefault: true,
		},
		{
			name: "override priority: tier 1 beats tier 2",
			overrides: map[string]*model.ScheduleOverride{
				"org-1/github/fetch_prs": {CronExpression: "0 */2 * * *", Enabled: true},
			},
			key: "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{
				OperationOverrides: map[sdk.OperationID]json.RawMessage{
					"fetch_prs": json.RawMessage(`{"schedule":"*/30 * * * *"}`),
				},
			},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "0 */2 * * *",
			wantFromDefault: false,
		},
		{
			name:      "operation override with invalid JSON falls through to default",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{
				OperationOverrides: map[sdk.OperationID]json.RawMessage{
					"fetch_prs": json.RawMessage(`{invalid`),
				},
			},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "*/5 * * * *",
			wantFromDefault: true,
		},
		{
			name:      "operation override with empty schedule falls through to default",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{
				OperationOverrides: map[sdk.OperationID]json.RawMessage{
					"fetch_prs": json.RawMessage(`{"schedule":""}`),
				},
			},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "*/5 * * * *",
			wantFromDefault: true,
		},
		{
			name:      "operation override disabled returns empty, not from default",
			overrides: map[string]*model.ScheduleOverride{},
			key:       "org-1/github/fetch_prs",
			cfg: &sdk.ConnectorConfig{
				OperationOverrides: map[sdk.OperationID]json.RawMessage{
					"fetch_prs": json.RawMessage(`{"enabled":false}`),
				},
			},
			op: sdk.OperationDescriptor{
				ID:              "fetch_prs",
				DefaultSchedule: "*/5 * * * *",
			},
			want:            "",
			wantFromDefault: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, gotFromDefault := resolveSchedule(tt.overrides, tt.key, tt.cfg, tt.op)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantFromDefault, gotFromDefault)
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: skipForIndividualTenant
// ---------------------------------------------------------------------------

func TestSkipForIndividualTenant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind sdk.TenantKind
		op   sdk.OperationDescriptor
		want bool
	}{
		{"multi-user op + individual tenant: skip", sdk.TenantKindIndividual, multiUserPullOp("fetch_org_members", "0 */6 * * *"), true},
		{"multi-user op + organization: keep", sdk.TenantKindOrganization, multiUserPullOp("fetch_org_members", "0 */6 * * *"), false},
		{"repo-scoped op + individual tenant: keep", sdk.TenantKindIndividual, pullOp("fetch_prs", "*/15 * * * *"), false},
		{"fail-open: an unresolved kind never skips a multi-user op", sdk.TenantKindUnspecified, multiUserPullOp("fetch_org_members", "0 */6 * * *"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, skipForIndividualTenant(tt.kind, tt.op))
		})
	}
}

// TestTenantTypeFor exercises the cache-hit fast path and the authoritative
// cache-miss lookup: the live-individual set is trusted on a hit (in-set →
// INDIVIDUAL, no RPC); a miss resolves organizations.tenant_type via
// the tenant directory rather than assuming ORGANIZATION.
func TestTenantTypeFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		individualSet map[string]struct{}
		orgID         string
		// authoritative, when set, is the tenant_type the tenant directory returns for
		// a cache-miss lookup; expectRPC asserts whether the RPC is expected.
		authoritative    sdk.TenantKind
		authoritativeErr bool
		expectRPC        bool
		want             sdk.TenantKind
	}{
		{
			name:          "org in set: INDIVIDUAL, no RPC (cache hit on the fast path)",
			individualSet: individualSetOf("org-individual"),
			orgID:         "org-individual",
			expectRPC:     false,
			want:          sdk.TenantKindIndividual,
		},
		{
			// A freshly-provisioned individual absent from the lagging set must
			// resolve INDIVIDUAL authoritatively, not be assumed ORGANIZATION.
			name:          "absent + authoritative says INDIVIDUAL: INDIVIDUAL",
			individualSet: individualSetOf("org-other-individual"),
			orgID:         "org-new-individual",
			authoritative: sdk.TenantKindIndividual,
			expectRPC:     true,
			want:          sdk.TenantKindIndividual,
		},
		{
			name:          "absent + authoritative says ORGANIZATION: ORGANIZATION",
			individualSet: individualSetOf("org-individual"),
			orgID:         "org-real",
			authoritative: sdk.TenantKindOrganization,
			expectRPC:     true,
			want:          sdk.TenantKindOrganization,
		},
		{
			// A successful lookup of UNSPECIFIED is a pre-classification org
			// (the tenant directory contract), stamped ORGANIZATION — never the
			// poison-pill UNSPECIFIED that would terminal-fail the org forever.
			name:          "absent + authoritative says UNSPECIFIED: ORGANIZATION (pre-classification org, normalized)",
			individualSet: map[string]struct{}{},
			orgID:         "org-unclassified",
			authoritative: sdk.TenantKindUnspecified,
			expectRPC:     true,
			want:          sdk.TenantKindOrganization,
		},
		{
			// A nil (cold/never-published) set is not an UNSPECIFIED shortcut: the
			// absent org is resolved authoritatively like any other miss.
			name:          "nil set + authoritative says ORGANIZATION: ORGANIZATION (no cold-start shortcut)",
			individualSet: nil,
			orgID:         "org-real",
			authoritative: sdk.TenantKindOrganization,
			expectRPC:     true,
			want:          sdk.TenantKindOrganization,
		},
		{
			// Fail-safe: on an RPC error stamp UNSPECIFIED, not a guessed
			// ORGANIZATION. The orchestrator's RequiresTenantType guard fails the
			// run loud on UNSPECIFIED rather than silently hitting the org endpoint.
			name:             "absent + RPC error: UNSPECIFIED (fail-safe, not guessed ORGANIZATION)",
			individualSet:    map[string]struct{}{},
			orgID:            "org-real",
			authoritativeErr: true,
			expectRPC:        true,
			want:             sdk.TenantKindUnspecified,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			mgmt := sdkmock.NewMockTenantDirectory(ctrl)
			if tt.expectRPC {
				if tt.authoritativeErr {
					mgmt.EXPECT().TenantType(gomock.Any(), tt.orgID).
						Return(sdk.TenantKindUnspecified, errors.New("iam down")).Times(1)
				} else {
					mgmt.EXPECT().TenantType(gomock.Any(), tt.orgID).
						Return(tt.authoritative, nil).Times(1)
				}
			}

			src := NewConnectorScheduleSource(nil, nil, nil, mgmt, nil, nil)
			assert.Equal(t, tt.want, src.tenantTypeFor(context.Background(), tt.individualSet, tt.orgID))
		})
	}
}

// TestTenantTypeFor_CacheBounded asserts the per-org cache fires the
// authoritative RPC at most once per org: a second resolution of the same
// org serves the cached value with no further RPC (Times(1) total). This is
// what keeps the authoritative lookup from hammering IAM on every tick.
func TestTenantTypeFor_CacheBounded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mgmt := sdkmock.NewMockTenantDirectory(ctrl)
	mgmt.EXPECT().TenantType(gomock.Any(), "org-real").
		Return((sdk.TenantKindOrganization), nil).
		Times(1)

	src := NewConnectorScheduleSource(nil, nil, nil, mgmt, nil, nil)
	empty := map[string]struct{}{}

	first := src.tenantTypeFor(context.Background(), empty, "org-real")
	second := src.tenantTypeFor(context.Background(), empty, "org-real")
	assert.Equal(t, sdk.TenantKindOrganization, first)
	assert.Equal(t, sdk.TenantKindOrganization, second,
		"second call must serve the cached resolution, not a second RPC")
}

// TestTenantTypeFor_NormalizedValueCached pins that the *normalized*
// ORGANIZATION (not the raw UNSPECIFIED the RPC returned) is what lands in the
// cache: a second resolution serves ORGANIZATION with no further RPC. Returning
// ORGANIZATION on the first call while caching UNSPECIFIED would re-poison the
// org on every later tick, so the assertion is on the second (cache-served) call.
func TestTenantTypeFor_NormalizedValueCached(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mgmt := sdkmock.NewMockTenantDirectory(ctrl)
	mgmt.EXPECT().TenantType(gomock.Any(), "org-unclassified").
		Return((sdk.TenantKindUnspecified), nil).
		Times(1)

	src := NewConnectorScheduleSource(nil, nil, nil, mgmt, nil, nil)
	empty := map[string]struct{}{}

	first := src.tenantTypeFor(context.Background(), empty, "org-unclassified")
	second := src.tenantTypeFor(context.Background(), empty, "org-unclassified")
	assert.Equal(t, sdk.TenantKindOrganization, first)
	assert.Equal(t, sdk.TenantKindOrganization, second,
		"cache must hold the normalized ORGANIZATION, not the raw UNSPECIFIED")
}

// TestTenantTypeFor_NilClient asserts the nil-client degradation path stamps
// UNSPECIFIED (fail-safe) rather than panicking on a cache miss.
func TestTenantTypeFor_NilClient(t *testing.T) {
	t.Parallel()
	src := NewConnectorScheduleSource(nil, nil, nil, nil, nil, nil)
	got := src.tenantTypeFor(context.Background(), map[string]struct{}{}, "org-real")
	assert.Equal(t, sdk.TenantKindUnspecified, got)
}

// TestTenantTypeFor_BoundsTheLookup pins that the authoritative lookup is asked
// for the org being stamped, under a deadline no longer than
// orgTenantTypeRPCTimeout, so one slow directory cannot starve the rest of a
// refresh.
func TestTenantTypeFor_BoundsTheLookup(t *testing.T) {
	t.Parallel()

	for _, orgID := range []string{"org-real", "11111111-2222-3333-4444-555555555555"} {
		t.Run(orgID, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			dir := sdkmock.NewMockTenantDirectory(ctrl)

			var deadline time.Time
			var hadDeadline bool
			dir.EXPECT().
				TenantType(gomock.Any(), orgID).
				DoAndReturn(func(ctx context.Context, _ string) (sdk.TenantKind, error) {
					deadline, hadDeadline = ctx.Deadline()
					return sdk.TenantKindOrganization, nil
				}).
				Times(1)

			src := NewConnectorScheduleSource(nil, nil, nil, dir, nil, nil)
			start := time.Now()
			got := src.tenantTypeFor(context.Background(), map[string]struct{}{}, orgID)

			assert.Equal(t, sdk.TenantKindOrganization, got)
			require.True(t, hadDeadline, "the lookup must run under a deadline")
			assert.LessOrEqual(t, deadline.Sub(start), orgTenantTypeRPCTimeout+time.Second)
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: jobKey
// ---------------------------------------------------------------------------

func TestJobKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		orgID       string
		connectorID sdk.ConnectorID
		opID        sdk.OperationID
		want        string
	}{
		{"org-1", "github", "fetch_prs", "org-1/github/fetch_prs"},
		{"acme-corp", "jira", "fetch_issues", "acme-corp/jira/fetch_issues"},
		{"", "x", "y", "/x/y"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, jobKey(tt.orgID, tt.connectorID, tt.opID))
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: ConnectorScheduleSource.Name
// ---------------------------------------------------------------------------

func TestConnectorScheduleSource_Name(t *testing.T) {
	t.Parallel()
	src := NewConnectorScheduleSource(nil, nil, nil, nil, nil, nil)
	assert.Equal(t, "connector", src.Name())
}

// ---------------------------------------------------------------------------
// Tests: FetchSchedules
// ---------------------------------------------------------------------------

func TestFetchSchedules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// individualSet, when non-nil, overrides the source's live-individual
		// org-ID set so the RequiresMultiUser skip can be exercised without
		// touching the metrictenant package global. nil leaves the default
		// (empty/fail-open) set, i.e. no tenant is treated as individual.
		individualSet map[string]struct{}
		setup         func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO)
		wantIDs       []string
		wantErr       bool
	}{
		{
			name: "single connector single pull op",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name: "multiple orgs same connector",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
					{ConnectorID: "github", OrganizationID: "org-2", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs", "org-2/github/fetch_prs"},
		},
		{
			name: "multiple connectors",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/5 * * * *")}))
				reg.Register(newMockConnector(ctrl, "jira", []sdk.OperationDescriptor{pullOp("fetch_tickets", "0 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("jira")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "jira", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs", "org-1/jira/fetch_tickets"},
		},
		{
			name: "skip non-pull operations",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "slack", []sdk.OperationDescriptor{
					pullOp("fetch_messages", "*/5 * * * *"),
					{ID: "receive_events", IngestionType: sdk.IngestionWebhook},
				}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("slack")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "slack", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/slack/fetch_messages"},
		},
		{
			name: "skip disabled configs",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/5 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name: "schedule override tier 1",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return([]*model.ScheduleOverride{
					{OrganizationID: "org-1", ConnectorID: "github", OperationID: "fetch_prs", CronExpression: "*/5 * * * *", Enabled: true},
				}, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name: "operation override tier 2",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{
						ConnectorID: "github", OrganizationID: "org-1", Enabled: true,
						OperationOverrides: map[sdk.OperationID]json.RawMessage{
							"fetch_prs": json.RawMessage(`{"schedule":"*/10 * * * *"}`),
						},
					},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name: "default schedule tier 3",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name: "no schedule tier 4 skips operation",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: nil,
		},
		{
			name: "DB error on override load propagates",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, errors.New("db down"))
				return sdk.NewConnectorRegistry(), configDAO, scheduleDAO
			},
			wantErr: true,
		},
		{
			name: "DB error on config load skips connector gracefully",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/5 * * * *")}))
				reg.Register(newMockConnector(ctrl, "jira", []sdk.OperationDescriptor{pullOp("fetch_tickets", "0 * * * *")}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return(nil, errors.New("timeout"))
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("jira")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "jira", OrganizationID: "org-1", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-1/jira/fetch_tickets"},
		},
		{
			name: "empty registry returns 0 definitions",
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				return sdk.NewConnectorRegistry(), configDAO, scheduleDAO
			},
			wantIDs: nil,
		},
		{
			name:          "multi-user op skipped for individual tenant, repo-scoped op kept",
			individualSet: individualSetOf("org-individual"),
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{
					pullOp("fetch_prs", "*/15 * * * *"),
					multiUserPullOp("fetch_org_members", "0 */6 * * *"),
				}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-individual", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-individual/github/fetch_prs"},
		},
		{
			name:          "multi-user op kept for org tenant not in individual set",
			individualSet: individualSetOf("org-individual"),
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{
					pullOp("fetch_prs", "*/15 * * * *"),
					multiUserPullOp("fetch_org_members", "0 */6 * * *"),
				}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-real", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-real/github/fetch_prs", "org-real/github/fetch_org_members"},
		},
		{
			name:          "fail-open: empty individual set keeps multi-user op for every tenant",
			individualSet: map[string]struct{}{},
			setup: func(ctrl *gomock.Controller) (*sdk.ConnectorRegistry, *mock_dao.MockIConnectorConfigDAO, *mock_dao.MockIScheduleDAO) {
				reg := sdk.NewConnectorRegistry()
				reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{
					multiUserPullOp("fetch_org_members", "0 */6 * * *"),
				}))

				configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
				scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
				scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
					{ConnectorID: "github", OrganizationID: "org-individual", Enabled: true},
				}, nil)
				return reg, configDAO, scheduleDAO
			},
			wantIDs: []string{"org-individual/github/fetch_org_members"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			reg, configDAO, scheduleDAO := tt.setup(ctrl)
			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
			if tt.individualSet != nil {
				set := tt.individualSet
				src.liveIndividualSet = func() map[string]struct{} { return set }
			}

			defs, err := src.FetchSchedules(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(defs))
			for _, d := range defs {
				gotIDs = append(gotIDs, d.ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// ---------------------------------------------------------------------------
// Tests: FetchSchedules field validation
// ---------------------------------------------------------------------------

func TestFetchSchedules_PayloadFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	// org-1 is in the live-individual set, so the stamped payload must carry
	// INDIVIDUAL — proving the stamp reaches the enqueued payload end-to-end.
	src.liveIndividualSet = func() map[string]struct{} { return individualSetOf("org-1") }
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)

	var payload ConnectorSyncPayload
	require.NoError(t, json.Unmarshal(defs[0].Payload, &payload))
	assert.Equal(t, "org-1", payload.OrgID)
	assert.Equal(t, sdk.ConnectorID("github"), payload.ConnectorID)
	assert.Equal(t, sdk.OperationID("fetch_prs"), payload.OperationID)
	assert.Equal(t, sdk.TenantKindIndividual, payload.TenantType)
}

// An org absent from the live-individual set is stamped from the authoritative
// the tenant directory lookup, end-to-end onto the enqueued payload — not from the
// set's absence. A freshly-provisioned individual missing from the lagging set
// is stamped INDIVIDUAL (so its fetch_repos hits the individual endpoint), and a
// real org is stamped ORGANIZATION. The op is always scheduled, never dropped.
func TestFetchSchedules_AbsentOrgStampsFromAuthoritativeLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		orgID         string
		authoritative sdk.TenantKind
		want          sdk.TenantKind
	}{
		{
			name:          "freshly-provisioned individual absent from set: INDIVIDUAL",
			orgID:         "org-new-individual",
			authoritative: sdk.TenantKindIndividual,
			want:          sdk.TenantKindIndividual,
		},
		{
			name:          "real org absent from set: ORGANIZATION",
			orgID:         "org-real",
			authoritative: sdk.TenantKindOrganization,
			want:          sdk.TenantKindOrganization,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_repos", "*/15 * * * *")}))

			configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
			scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
			scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
			configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: tt.orgID, Enabled: true},
			}, nil)

			mgmt := sdkmock.NewMockTenantDirectory(ctrl)
			mgmt.EXPECT().TenantType(gomock.Any(), tt.orgID).
				Return(tt.authoritative, nil).Times(1)

			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, mgmt, nil, nil)
			// Non-nil empty set: a successful publish of zero individuals, so the
			// org is absent and must be resolved authoritatively.
			src.liveIndividualSet = func() map[string]struct{} { return map[string]struct{}{} }
			defs, err := src.FetchSchedules(context.Background())
			require.NoError(t, err)
			require.Len(t, defs, 1, "the op must be scheduled, not dropped")

			var payload ConnectorSyncPayload
			require.NoError(t, json.Unmarshal(defs[0].Payload, &payload))
			assert.Equal(t, tt.want, payload.TenantType)
		})
	}
}

func TestFetchSchedules_JobIDFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("pull_commits", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "org1/github/pull_commits", defs[0].ID)
}

func TestFetchSchedules_QueueAssignment(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, schedule.QueueConnectorSync, defs[0].Queue)
}

func TestFetchSchedules_QueueRoutingByTenant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		individualSet map[string]struct{}
		orgID         string
		wantQueue     string
	}{
		{
			name:          "individual config routes to the individual lane",
			individualSet: individualSetOf("org-indiv"),
			orgID:         "org-indiv",
			wantQueue:     schedule.QueueIndividualConnectorSync,
		},
		{
			name:          "org config routes to the standard lane",
			individualSet: individualSetOf("org-indiv"),
			orgID:         "org-real",
			wantQueue:     schedule.QueueConnectorSync,
		},
		{
			name:          "fail-open: empty set routes every config to the standard lane",
			individualSet: map[string]struct{}{},
			orgID:         "org-indiv",
			wantQueue:     schedule.QueueConnectorSync,
		},
		{
			name:          "fail-open: nil set routes every config to the standard lane",
			individualSet: nil,
			orgID:         "org-indiv",
			wantQueue:     schedule.QueueConnectorSync,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

			configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
			scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
			scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
			configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: tt.orgID, Enabled: true},
			}, nil)

			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
			set := tt.individualSet
			src.liveIndividualSet = func() map[string]struct{} { return set }

			defs, err := src.FetchSchedules(context.Background())
			require.NoError(t, err)
			require.Len(t, defs, 1)
			assert.Equal(t, tt.wantQueue, defs[0].Queue)
		})
	}
}

func TestFetchSchedules_JobType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, schedule.JobTypeConnectorSync, defs[0].JobType)
}

func TestFetchSchedules_ScheduleOverrideCronUsed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return([]*model.ScheduleOverride{
		{OrganizationID: "org-1", ConnectorID: "github", OperationID: "fetch_prs", CronExpression: "*/5 * * * *", Enabled: true},
	}, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "*/5 * * * *", defs[0].CronExpression)
}

// TestFetchSchedules_DefaultScheduleStaggeredPerOrg proves the descriptor default is emitted
// with a per-org minute-phase offset — the cadence preserved (still four fires an hour for
// `*/15`), the expression no longer the raw default, and two orgs landing on distinct phases —
// so the fleet's ticks spread instead of all firing on the same wall-clock minute.
func TestFetchSchedules_DefaultScheduleStaggeredPerOrg(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
		{ConnectorID: "github", OrganizationID: "org-2", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 2)

	byID := make(map[string]string, len(defs))
	for _, d := range defs {
		byID[d.ID] = d.CronExpression
	}
	e1 := byID["org-1/github/fetch_prs"]
	e2 := byID["org-2/github/fetch_prs"]
	assert.Regexp(t, `^\d+-59/15 \* \* \* \*$`, e1, "the 15-minute cadence must be preserved as a phase-shifted step")
	assert.Regexp(t, `^\d+-59/15 \* \* \* \*$`, e2)
	assert.NotEqual(t, "*/15 * * * *", e1, "the default must be phase-shifted, not emitted verbatim")
	assert.NotEqual(t, e1, e2, "distinct orgs must land on distinct phases")
}

// TestFetchSchedules_StaggerGroupSharesPhasePerOrg proves the group wiring at the emit site:
// members of one stagger group resolve to a shared per-org phase (so each member sits at its
// fixed offset from that phase and members never share a minute within an org), while distinct
// orgs land on distinct phases — the whole group shifts as a unit per org rather than firing
// fleet-wide in lockstep.
func TestFetchSchedules_StaggerGroupSharesPhasePerOrg(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "bitbucket", []sdk.OperationDescriptor{
		groupPullOp("fetch_prs", "*/15 * * * *", "bitbucket-pr-pull", 0),
		groupPullOp("fetch_prs_data", "3-59/15 * * * *", "bitbucket-pr-pull", 3),
	}))

	orgs := []string{"org-1", "org-2", "org-3", "org-4"}
	configs := make([]*sdk.ConnectorConfig, len(orgs))
	for i, org := range orgs {
		configs[i] = &sdk.ConnectorConfig{ConnectorID: "bitbucket", OrganizationID: org, Enabled: true}
	}

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("bitbucket")).Return(configs, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)

	byID := make(map[string]string, len(defs))
	for _, d := range defs {
		byID[d.ID] = d.CronExpression
	}

	bases := map[int]bool{}
	for _, org := range orgs {
		base := groupPhaseMinute(t, byID[org+"/bitbucket/fetch_prs"])
		data := groupPhaseMinute(t, byID[org+"/bitbucket/fetch_prs_data"])
		assert.Equalf(t, (base+3)%15, data, "%s: the offset-3 member must sit 3 minutes past the shared phase", org)
		assert.NotEqualf(t, base, data, "%s: members must not share a firing minute", org)
		bases[base] = true
	}
	assert.Greater(t, len(bases), 1, "distinct orgs must land on distinct shared phases, not one minute")
}

// TestFetchSchedules_StaggerGroupFallsBackWhenNotPlaceable proves the graceful-degradation branch:
// a group-tagged op whose schedule the group primitive cannot place (here an hour-restricted
// `22 */6 * * *`) is emitted on its own per-op stagger, not a group entry — so a misconfigured
// member still schedules (and roams per-op) rather than being dropped or silently lockstepped.
func TestFetchSchedules_StaggerGroupFallsBackWhenNotPlaceable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "bitbucket", []sdk.OperationDescriptor{
		groupPullOp("fetch_members", "22 */6 * * *", "bitbucket-pr-pull", 0),
	}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("bitbucket")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "bitbucket", OrganizationID: "org-1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)

	entry := defs[0].CronExpression
	// A non-placeable member takes the per-op slot: `22 */6 * * *` resolves to an hour-phase
	// stagger (`<min> <h>-23/6 * * *`), NOT the group's minute-phase `<m>-59/15` form.
	assert.Regexp(t, `^\d+ \d+-23/6 \* \* \*$`, entry, "a non-placeable group member must fall back to its per-op stagger")
	assert.NotRegexp(t, `-59/`, entry, "it must not be emitted as a group entry")
}

// groupPhaseMinute extracts the phase minute from a `<m>-59/15 * * * *` group entry, asserting
// the cadence is preserved as a phase-shifted 15-minute step.
func groupPhaseMinute(t *testing.T, entry string) int {
	t.Helper()
	require.Regexpf(t, `^\d+-59/15 \* \* \* \*$`, entry, "expected a phase-shifted 15-minute step, got %q", entry)
	minute, _, _ := strings.Cut(entry, "-")
	n, err := strconv.Atoi(minute)
	require.NoError(t, err)
	return n
}

func TestFetchSchedules_OperationOverrideCronUsed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
		{
			ConnectorID: "github", OrganizationID: "org-1", Enabled: true,
			OperationOverrides: map[sdk.OperationID]json.RawMessage{
				"fetch_prs": json.RawMessage(`{"schedule":"*/10 * * * *"}`),
			},
		},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "*/10 * * * *", defs[0].CronExpression)
}

// TestFetchSchedules_VCSContentFlagGate proves the fetch_file_content schedule is
// emitted only when VCSContentEnabledFunc is true for the org, so turning it off
// cannot re-arm the pull.
func TestFetchSchedules_VCSContentFlagGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		gate    VCSContentEnabledFunc
		wantIDs []string
	}{
		{name: "flag on: op scheduled", gate: func(string) bool { return true }, wantIDs: []string{"org-1/github/fetch_file_content"}},
		{name: "flag off: op skipped", gate: func(string) bool { return false }, wantIDs: nil},
		{name: "nil gate: op skipped", gate: nil, wantIDs: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, "github", []sdk.OperationDescriptor{pullOp("fetch_file_content", "0 0 * * 0")}))

			configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
			scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
			scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
			configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).Return([]*sdk.ConnectorConfig{
				{ConnectorID: "github", OrganizationID: "org-1", Enabled: true},
			}, nil)

			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, tt.gate, nil)
			src.liveIndividualSet = func() map[string]struct{} { return nil }

			defs, err := src.FetchSchedules(context.Background())
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(defs))
			for _, d := range defs {
				gotIDs = append(gotIDs, d.ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// TestFetchSchedules_CarriesExecutionTimeout pins the per-operation deadline
// through to the schedule definition.
//
// An operation that paces itself against a vendor rate limit has a runtime set
// by that budget, not by its own speed, so the scheduler default can cancel it
// mid-run — and an operation that skips work on cancellation never advances
// past it. Dropping this field silently reinstates that, so it is asserted
// rather than assumed. Zero must stay zero so every other op keeps the default.
func TestFetchSchedules_CarriesExecutionTimeout(t *testing.T) {
	t.Parallel()

	paced := pullOp("fetch_repo_tree", "37 3,15 * * *")
	paced.ExecutionTimeout = 30 * time.Minute

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "bitbucket", []sdk.OperationDescriptor{
		paced,
		pullOp("fetch_prs", "*/15 * * * *"),
	}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("bitbucket")).Return([]*sdk.ConnectorConfig{
		{ConnectorID: "bitbucket", OrganizationID: "org1", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 2)

	byID := make(map[string]time.Duration, len(defs))
	for _, d := range defs {
		byID[d.ID] = d.Timeout
	}
	assert.Equal(t, 30*time.Minute, byID["org1/bitbucket/fetch_repo_tree"],
		"a paced operation's timeout must reach the schedule definition")
	assert.Zero(t, byID["org1/bitbucket/fetch_prs"],
		"an op that sets no timeout must keep the scheduler default")
}

// TestFetchSchedules_CursorRolloutFlagGate is the assertion that makes closing the gate real.
// The connect gate stops a *new* organisation getting a configuration row, and the
// source selects on that row alone — so for an organisation that has already connected,
// this gate is the only thing that can stop the vendor calls and the writes. It is
// connector-wide rather than per-operation, so the administrator's connect state
// survives the gate being closed.
func TestFetchSchedules_CursorRolloutFlagGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		gate    RolloutEnabledFunc
		wantIDs []string
	}{
		{
			name:    "flag on: the connector's operation is scheduled",
			gate:    func(string) bool { return true },
			wantIDs: []string{"org-1/cursor/fetch_usage_events"},
		},
		{
			// The row is still enabled and the credential still stored; only the
			// emission stops. That is the difference between a rollback and a
			// disconnect.
			name:    "flag off: nothing is scheduled for a connected organisation",
			gate:    func(string) bool { return false },
			wantIDs: nil,
		},
		{
			name:    "nil gate: off everywhere, so a dark rollout cannot emit",
			gate:    nil,
			wantIDs: nil,
		},
		{
			// Per organisation, not per environment: one organisation's open gate
			// must not schedule another's.
			name:    "the gate is per organisation",
			gate:    func(orgID string) bool { return orgID == "org-2" },
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, "cursor",
				[]sdk.OperationDescriptor{pullOp("fetch_usage_events", "0 2 * * *")}))

			configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
			scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
			scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
			configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("cursor")).
				Return([]*sdk.ConnectorConfig{
					{ConnectorID: "cursor", OrganizationID: "org-1", Enabled: true},
				}, nil)

			// tt.gate == nil exercises the registered-but-nil state: the connector is
			// gated and withheld from every organisation. That is distinct from being
			// absent from the map, which means not gated at all.
			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil,
				RolloutGates{ConnectorIDCursor: tt.gate})
			src.liveIndividualSet = func() map[string]struct{} { return nil }

			defs, err := src.FetchSchedules(context.Background())
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(defs))
			for _, d := range defs {
				gotIDs = append(gotIDs, d.ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// TestFetchSchedules_CursorGateLeavesOtherConnectors is the containment assertion: the
// gate is keyed on the connector, so disabling the Cursor gate must not stop
// anyone else's syncs.
func TestFetchSchedules_CursorGateLeavesOtherConnectors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "cursor",
		[]sdk.OperationDescriptor{pullOp("fetch_usage_events", "0 2 * * *")}))
	reg.Register(newMockConnector(ctrl, "github",
		[]sdk.OperationDescriptor{pullOp("fetch_prs", "*/15 * * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("cursor")).
		Return([]*sdk.ConnectorConfig{{ConnectorID: "cursor", OrganizationID: "org-1", Enabled: true}}, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("github")).
		Return([]*sdk.ConnectorConfig{{ConnectorID: "github", OrganizationID: "org-1", Enabled: true}}, nil)

	// Cursor off for everyone.
	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil,
		RolloutGates{ConnectorIDCursor: func(string) bool { return false }})
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)

	gotIDs := make([]string, 0, len(defs))
	for _, d := range defs {
		gotIDs = append(gotIDs, d.ID)
	}
	assert.ElementsMatch(t, []string{"org-1/github/fetch_prs"}, gotIDs)
}

// TestFetchSchedules_SuppressionCollapsesIndividualTenants guards the metric label, not
// the gate. An individual tenant is one organisation each, so a raw org ID here mints a
// series per tenant — and while a gate is closed that is every configured tenant, on
// every schedule refresh. A series already ingested cannot be recovered by a recording
// rule, which is why the collapse has to happen at the emit site.
func TestFetchSchedules_SuppressionCollapsesIndividualTenants(t *testing.T) {
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "cursor",
		[]sdk.OperationDescriptor{pullOp("fetch_usage_events", "0 2 * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("cursor")).
		Return([]*sdk.ConnectorConfig{
			{ConnectorID: "cursor", OrganizationID: "org-individual", Enabled: true},
		}, nil)

	collapsed := scheduleSuppressed.WithLabelValues("cursor", tenantlabel.MetricLabelIndividual)
	raw := scheduleSuppressed.WithLabelValues("cursor", "org-individual")
	collapsedBefore, rawBefore := testutil.ToFloat64(collapsed), testutil.ToFloat64(raw)

	// Cursor gated off, and this organisation is a live individual tenant.
	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil,
		RolloutGates{ConnectorIDCursor: func(string) bool { return false }})
	src.liveIndividualSet = func() map[string]struct{} { return individualSetOf("org-individual") }

	_, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)

	assert.Equal(t, collapsedBefore+1, testutil.ToFloat64(collapsed),
		"an individual tenant's suppression must land on the collapsed label")
	assert.Equal(t, rawBefore, testutil.ToFloat64(raw),
		"and never on a series keyed by its own organisation ID")
}

// TestFetchSchedules_RolloutGateIsKeyedByConnector is the property that makes the gate
// reusable: each gated connector is withheld on its own flag, and a connector
// with no gate registered is untouched by any of them.
//
// The last case is the one that matters most. A connector absent from the map is NOT
// gated and always emits, which is what keeps this from silently gating the
// connectors that have no gate — but it is also why a forgotten registration
// fails open, so a host must register every connector it gates.
func TestFetchSchedules_RolloutGateIsKeyedByConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		gates   RolloutGates
		wantIDs []string
	}{
		{
			name: "each connector answers to its own gate",
			gates: RolloutGates{
				ConnectorIDCursor:     func(string) bool { return true },
				ConnectorIDClaudeCode: func(string) bool { return false },
			},
			wantIDs: []string{"org-1/cursor/fetch_usage_events", "org-1/github/fetch_prs"},
		},
		{
			name: "flipping one connector off leaves the other emitting",
			gates: RolloutGates{
				ConnectorIDCursor:     func(string) bool { return false },
				ConnectorIDClaudeCode: func(string) bool { return true },
			},
			wantIDs: []string{"org-1/claude-code/fetch_analytics_users", "org-1/github/fetch_prs"},
		},
		{
			name: "a registered-but-nil gate withholds that connector everywhere",
			gates: RolloutGates{
				ConnectorIDCursor:     nil,
				ConnectorIDClaudeCode: nil,
			},
			wantIDs: []string{"org-1/github/fetch_prs"},
		},
		{
			name:  "an unregistered connector is not under rollout and still emits",
			gates: RolloutGates{},
			wantIDs: []string{
				"org-1/claude-code/fetch_analytics_users",
				"org-1/cursor/fetch_usage_events",
				"org-1/github/fetch_prs",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			reg := sdk.NewConnectorRegistry()
			reg.Register(newMockConnector(ctrl, "cursor",
				[]sdk.OperationDescriptor{pullOp("fetch_usage_events", "0 2 * * *")}))
			reg.Register(newMockConnector(ctrl, "claude-code",
				[]sdk.OperationDescriptor{pullOp("fetch_analytics_users", "0 3 * * *")}))
			reg.Register(newMockConnector(ctrl, "github",
				[]sdk.OperationDescriptor{pullOp("fetch_prs", "0 4 * * *")}))

			configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
			scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
			scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
			for _, id := range []string{"cursor", "claude-code", "github"} {
				configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID(id)).
					Return([]*sdk.ConnectorConfig{
						{ConnectorID: sdk.ConnectorID(id), OrganizationID: "org-1", Enabled: true},
					}, nil)
			}

			src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, tt.gates)
			src.liveIndividualSet = func() map[string]struct{} { return nil }

			defs, err := src.FetchSchedules(context.Background())
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(defs))
			for _, d := range defs {
				gotIDs = append(gotIDs, d.ID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// TestFetchSchedules_ClaudeCodeRollbackStopsAConnectedOrg is the rollback
// check: an organisation that has ALREADY connected stops emitting when its flag goes
// off. The fixture keeps Enabled: true across both runs, so what changes is the flag and
// nothing else.
//
// That the connect state survives is true by construction rather than asserted here —
// this source has no write path — so read this as the documented acceptance. The
// mechanism itself is covered by TestFetchSchedules_RolloutGateIsKeyedByConnector.
func TestFetchSchedules_ClaudeCodeRollbackStopsAConnectedOrg(t *testing.T) {
	t.Parallel()

	build := func(t *testing.T, on bool) []string {
		t.Helper()
		ctrl := gomock.NewController(t)
		reg := sdk.NewConnectorRegistry()
		reg.Register(newMockConnector(ctrl, "claude-code",
			[]sdk.OperationDescriptor{pullOp("fetch_analytics_users", "0 3 * * *")}))

		configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
		scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
		scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
		// Enabled: true throughout — this organisation is connected in both runs.
		configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), ConnectorIDClaudeCode).
			Return([]*sdk.ConnectorConfig{
				{ConnectorID: ConnectorIDClaudeCode, OrganizationID: "org-connected", Enabled: true},
			}, nil)

		src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil,
			RolloutGates{ConnectorIDClaudeCode: func(string) bool { return on }})
		src.liveIndividualSet = func() map[string]struct{} { return nil }

		defs, err := src.FetchSchedules(context.Background())
		require.NoError(t, err)
		ids := make([]string, 0, len(defs))
		for _, d := range defs {
			ids = append(ids, d.ID)
		}
		return ids
	}

	assert.Equal(t, []string{"org-connected/claude-code/fetch_analytics_users"}, build(t, true),
		"with the flag on, a connected organisation emits")
	assert.Empty(t, build(t, false),
		"flipping the flag off must stop a connected organisation, which the connect gate cannot do")
}

// TestFetchSchedules_NilGateMapGatesNothing pins the semantics of the map's zero value.
// A nil RolloutEnabledFunc withholds every organisation; a nil RolloutGates withholds none.
// runner/app passes nil; a host that gates connectors passes a complete map.
func TestFetchSchedules_NilGateMapGatesNothing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "cursor",
		[]sdk.OperationDescriptor{pullOp("fetch_usage_events", "0 2 * * *")}))

	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), ConnectorIDCursor).
		Return([]*sdk.ConnectorConfig{
			{ConnectorID: ConnectorIDCursor, OrganizationID: "org-1", Enabled: true},
		}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1,
		"a nil gate map means no connector is under rollout, so nothing is withheld")
	assert.Equal(t, "org-1/cursor/fetch_usage_events", defs[0].ID)
}

// TestFetchSchedules_PublishesCronEntriesMetric is the end-to-end proof that FetchSchedules
// stamps scheduler_cron_entries with the right buckets: a group member reports
// stagger_mode=group, a plain default reports its Slot mode at origin=staggered_default, and
// an operator override reports origin=operator_override (still labelled with the default's
// mode). It reads the emitted gauge back off the default registry — what a /metrics scrape
// would show.
//
// Deliberately NOT parallel: it asserts on the process-global metric that every FetchSchedules
// test writes to, and Go runs non-parallel tests in a sequential phase with no t.Parallel()
// test running, so no sibling clobbers this source's series mid-assertion.
func TestFetchSchedules_PublishesCronEntriesMetric(t *testing.T) {
	const connID = "metrics-probe-conn"

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, connID, []sdk.OperationDescriptor{
		groupPullOp("group_op", "*/15 * * * *", "probe-group", 0),
		groupPullOp("group_override_op", "*/15 * * * *", "probe-group", 5),
		pullOp("plain_op", "*/30 * * * *"),
		pullOp("override_op", "*/30 * * * *"),
	}))

	overrides := []*model.ScheduleOverride{
		{OrganizationID: "org-metrics", ConnectorID: connID, OperationID: "override_op", CronExpression: "0 * * * *", Enabled: true},
		{OrganizationID: "org-metrics", ConnectorID: connID, OperationID: "group_override_op", CronExpression: "0 * * * *", Enabled: true},
	}
	configDAO := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	scheduleDAO := mock_dao.NewMockIScheduleDAO(ctrl)
	scheduleDAO.EXPECT().ListAll(gomock.Any()).Return(overrides, nil)
	configDAO.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID(connID)).Return([]*sdk.ConnectorConfig{
		{ConnectorID: connID, OrganizationID: "org-metrics", Enabled: true},
	}, nil)

	src := NewConnectorScheduleSource(configDAO, scheduleDAO, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }
	_, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)

	// group_op default → group / staggered_default.
	assert.Equal(t, 1.0, cronEntriesGauge(t, connID, "group", "staggered_default"),
		"a stagger-group member's default must be labelled stagger_mode=group")
	// plain_op default (*/30) → minute_phase / staggered_default.
	assert.Equal(t, 1.0, cronEntriesGauge(t, connID, "minute_phase", "staggered_default"),
		"a plain */30 default must be labelled with its Slot mode at staggered_default")
	// override_op (operator override) → origin=operator_override, mode still the default's.
	assert.Equal(t, 1.0, cronEntriesGauge(t, connID, "minute_phase", "operator_override"),
		"an operator override must be labelled origin=operator_override")
	// group_override_op (grouped op an operator overrode) → stagger_mode still "group"
	// (describes the descriptor default) at origin=operator_override, even though the
	// verbatim override cron is emitted and GroupEntryFor is never called for it.
	assert.Equal(t, 1.0, cronEntriesGauge(t, connID, "group", "operator_override"),
		"a grouped op that was overridden keeps stagger_mode=group at operator_override")
}

// cronEntriesGauge reads one scheduler_cron_entries series (source=connector) off the
// default registry, matching the connector, stagger_mode, and origin labels — 0 if absent.
func cronEntriesGauge(t *testing.T, connectorID, staggerMode, origin string) float64 {
	t.Helper()
	want := map[string]string{
		"source":       "connector",
		"connector_id": connectorID,
		"stagger_mode": staggerMode,
		"origin":       origin,
	}
	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range fams {
		if mf.GetName() != "scheduler_cron_entries" {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// TestFetchSchedules_ClearsCronEntriesOnRefreshFailure pins the failure-path contract: a pass
// whose overrides load fails returns early, and the deferred Publish then clears this source's
// series rather than leaving a stale inventory — matching how GetConfigs drops the failed
// source from periodic_tasks. Not parallel, for the same global-metric reason as the sibling
// metric tests.
func TestFetchSchedules_ClearsCronEntriesOnRefreshFailure(t *testing.T) {
	const connID = "clear-on-fail-conn"

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, connID, []sdk.OperationDescriptor{
		pullOp("op", "*/30 * * * *"),
	}))

	// A successful pass populates the source's series.
	okSchedules := mock_dao.NewMockIScheduleDAO(ctrl)
	okSchedules.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	okConfigs := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	okConfigs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID(connID)).Return([]*sdk.ConnectorConfig{
		{ConnectorID: connID, OrganizationID: "org-1", Enabled: true},
	}, nil)
	okSrc := NewConnectorScheduleSource(okConfigs, okSchedules, reg, nil, nil, nil)
	okSrc.liveIndividualSet = func() map[string]struct{} { return nil }
	_, err := okSrc.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1.0, cronEntriesGauge(t, connID, "minute_phase", "staggered_default"),
		"a successful pass must populate the source's series")

	// The next pass fails its overrides load and returns early; the deferred Publish still runs
	// with an empty recorder, clearing the series.
	failSchedules := mock_dao.NewMockIScheduleDAO(ctrl)
	failSchedules.EXPECT().ListAll(gomock.Any()).Return(nil, errors.New("overrides load failed"))
	failConfigs := mock_dao.NewMockIConnectorConfigDAO(ctrl) // never reached — the early return precedes the loop
	failSrc := NewConnectorScheduleSource(failConfigs, failSchedules, reg, nil, nil, nil)
	failSrc.liveIndividualSet = func() map[string]struct{} { return nil }
	_, err = failSrc.FetchSchedules(context.Background())
	require.Error(t, err)
	assert.Equal(t, 0.0, cronEntriesGauge(t, connID, "minute_phase", "staggered_default"),
		"a failed refresh must clear the source's stale series, not leave a stale inventory")
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	mock_adapter "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/handler"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	edgeOrgA = "00000000-0000-4000-8000-0000000ed0a1"
	edgeOrgB = "00000000-0000-4000-8000-0000000ed0b2"
)

// scriptedDirectory answers TenantType from a per-call script, then from kind.
type scriptedDirectory struct {
	mu     sync.Mutex
	errs   []error
	kind   sdk.TenantKind
	calls  int
	byOrgs map[string]sdk.TenantKind
}

func (d *scriptedDirectory) TenantType(_ context.Context, orgID string) (sdk.TenantKind, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if len(d.errs) > 0 {
		err := d.errs[0]
		d.errs = d.errs[1:]
		if err != nil {
			return sdk.TenantKindUnspecified, err
		}
	}
	if k, ok := d.byOrgs[orgID]; ok {
		return k, nil
	}
	return d.kind, nil
}

func edgeSource(t *testing.T, mem *memstore.Store, dir sdk.TenantDirectory, ops ...sdk.OperationDescriptor) *ConnectorScheduleSource {
	t.Helper()
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "edge", ops))
	src := NewConnectorScheduleSource(mem, mem, reg, dir, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }
	return src
}

func defIDs(defs []schedule.ScheduleDefinition) []string {
	ids := make([]string, 0, len(defs))
	for _, d := range defs {
		ids = append(ids, d.ID)
	}
	sort.Strings(ids)
	return ids
}

// hostOp is a pull op that requires the given host services.
func hostOp(id sdk.OperationID, svcs ...sdk.HostService) sdk.OperationDescriptor {
	op := pullOp(id, "0 * * * *")
	op.RequiresHost = svcs
	return op
}

// A connector_schedules row for an operation withheld for a host service
// the host does not provide does not bring it back: withholding is checked
// before any override tier.
func TestEdge_TableOverrideDoesNotResurrectAWithheldOperation(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true,
		OperationOverrides: map[sdk.OperationID]json.RawMessage{"held": json.RawMessage(`{"schedule":"*/5 * * * *"}`)}}))
	require.NoError(t, mem.Upsert(ctx, &model.ScheduleOverride{OrganizationID: edgeOrgA, ConnectorID: "edge", OperationID: "held", CronExpression: "* * * * *", Enabled: true}))
	src := edgeSource(t, mem, nil, hostOp("held", sdk.HostWorkQueue), pullOp("kept", "0 * * * *"))

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{edgeOrgA + "/edge/kept"}, defIDs(defs))
}

// The source schedules an operation only when the host provides every
// service its RequiresHost names: none provided withholds it, some provided
// still withholds it, and all provided schedules it.
func TestSource_WithholdsByRequiresHost(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true}))
	ops := []sdk.OperationDescriptor{
		pullOp("plain", "0 * * * *"),
		hostOp("queue", sdk.HostWorkQueue),
		hostOp("lineage", sdk.HostBlameSink, sdk.HostLineageWalker),
	}
	for _, tc := range []struct {
		name     string
		provided []sdk.HostService
		want     []string
	}{
		{"none provided", nil, []string{"plain"}},
		{"some provided", []sdk.HostService{sdk.HostBlameSink}, []string{"plain"}},
		{"queue provided", []sdk.HostService{sdk.HostWorkQueue}, []string{"plain", "queue"}},
		{"all provided", []sdk.HostService{sdk.HostWorkQueue, sdk.HostBlameSink, sdk.HostLineageWalker}, []string{"lineage", "plain", "queue"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := edgeSource(t, mem, nil, ops...).WithHostServices(tc.provided...)
			defs, err := src.FetchSchedules(ctx)
			require.NoError(t, err)
			want := make([]string, len(tc.want))
			for i, id := range tc.want {
				want[i] = edgeOrgA + "/edge/" + id
			}
			assert.Equal(t, want, defIDs(defs))
		})
	}
}

// Providing a work queue does not open fetch_file_content's separate content
// gate: the op still needs the organization enabled.
func TestSource_HostServicesDoNotOpenTheContentGate(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true}))
	src := edgeSource(t, mem, nil, hostOp(opFetchFileContent, sdk.HostWorkQueue)).WithHostServices(sdk.HostWorkQueue)
	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	assert.Empty(t, defs, "the content gate is closed")

	src.isVCSContentEnabled = func(string) bool { return true }
	defs, err = src.FetchSchedules(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{edgeOrgA + "/edge/" + string(opFetchFileContent)}, defIDs(defs))
}

// Override rows that name an operation the connector does not declare, or a
// connector that is not registered, schedule nothing and fail nothing.
func TestEdge_OverridesForUnknownOperationsAreIgnored(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true,
		OperationOverrides: map[sdk.OperationID]json.RawMessage{"ghost": json.RawMessage(`{"schedule":"*/5 * * * *"}`)}}))
	require.NoError(t, mem.Upsert(ctx, &model.ScheduleOverride{OrganizationID: edgeOrgA, ConnectorID: "edge", OperationID: "ghost", CronExpression: "* * * * *", Enabled: true}))
	require.NoError(t, mem.Upsert(ctx, &model.ScheduleOverride{OrganizationID: edgeOrgA, ConnectorID: "unregistered", OperationID: "op", CronExpression: "* * * * *", Enabled: true}))
	src := edgeSource(t, mem, nil, pullOp("op", "0 * * * *"))

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{edgeOrgA + "/edge/op"}, defIDs(defs))
}

// An enabled override row for an organization whose config is disabled
// schedules nothing: the config, not the override, decides whether it runs.
func TestEdge_OverrideWithoutAnEnabledConfigSchedulesNothing(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: false}))
	require.NoError(t, mem.Upsert(ctx, &model.ScheduleOverride{OrganizationID: edgeOrgA, ConnectorID: "edge", OperationID: "op", CronExpression: "*/5 * * * *", Enabled: true}))
	src := edgeSource(t, mem, nil, pullOp("op", "0 * * * *"))

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	assert.Empty(t, defs)
}

// An operator's override is emitted verbatim for every organization, an
// @every descriptor included; only the default is per-org staggered.
func TestEdge_OverrideIsVerbatimAcrossOrganizations(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	for _, org := range []string{edgeOrgA, edgeOrgB} {
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "edge", Enabled: true,
			OperationOverrides: map[sdk.OperationID]json.RawMessage{"op": json.RawMessage(`{"schedule":"@every 30m","enabled":true}`)}}))
	}
	src := edgeSource(t, mem, nil, pullOp("op", "*/15 * * * *"))

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, defs, 2)
	for _, d := range defs {
		assert.Equal(t, "@every 30m", d.CronExpression, d.ID)
	}
}

// An individual tenant the directory resolves (not in the live set) is
// stamped individual end to end: its multi-user op is skipped, its task is
// routed to the individual lane, and the handler hands the runner
// TenantKindIndividual after the payload's JSON round trip.
func TestEdge_IndividualKindRoundTripsToTheRunner(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true}))
	dir := &scriptedDirectory{kind: sdk.TenantKindIndividual}
	src := edgeSource(t, mem, dir, pullOp("op", "*/15 * * * *"), multiUserPullOp("members", "*/15 * * * *"))

	cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{src}, 0, nil).GetConfigs()
	require.NoError(t, err)
	require.Len(t, cfgs, 1, "the multi-user op is skipped for an individual")

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, schedule.QueueIndividualConnectorSync, defs[0].Queue)

	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)
	runner.EXPECT().RunOperation(gomock.Any(), edgeOrgA, sdk.ConnectorID("edge"), sdk.OperationID("op"), sdk.TenantKindIndividual, "scheduled").Return(nil)
	require.NoError(t, NewConnectorSyncHandler(runner).ProcessTask(ctx, cfgs[0].Task))
}

// A tenant lookup that fails is not cached: that refresh fails open (standard
// lane, multi-user op kept, UNSPECIFIED stamp), and the next refresh asks the
// directory again and routes the individual correctly.
func TestEdge_FailedTenantLookupIsRetriedOnTheNextRefresh(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: edgeOrgA, ConnectorID: "edge", Enabled: true}))
	dir := &scriptedDirectory{errs: []error{errors.New("directory unavailable")}, kind: sdk.TenantKindIndividual}
	src := edgeSource(t, mem, dir, pullOp("op", "*/15 * * * *"), multiUserPullOp("members", "*/15 * * * *"))

	first, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, first, 2, "fail-open: the multi-user op is kept")
	for _, d := range first {
		assert.Equal(t, schedule.QueueConnectorSync, d.Queue)
		var p ConnectorSyncPayload
		require.NoError(t, json.Unmarshal(d.Payload, &p))
		assert.Equal(t, sdk.TenantKindUnspecified, p.TenantType)
	}

	second, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, schedule.QueueIndividualConnectorSync, second[0].Queue)
	assert.Equal(t, 2, dir.calls, "the failure was not cached")
}

// A payload from a newer producer with fields this handler does not know,
// and a tenant name it does not know, still runs: the unknown kind reads as
// UNSPECIFIED, which the orchestrator's guard handles.
func TestEdge_PayloadWithUnknownFieldsStillRuns(t *testing.T) {
	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)
	runner.EXPECT().RunOperation(gomock.Any(), edgeOrgA, sdk.ConnectorID("edge"), sdk.OperationID("op"), sdk.TenantKindUnspecified, "scheduled").Return(nil)
	payload := []byte(`{"org_id":"` + edgeOrgA + `","connector_id":"edge","operation_id":"op","tenant_type":"TENANT_TYPE_TEAM","priority":7}`)
	require.NoError(t, NewConnectorSyncHandler(runner).ProcessTask(context.Background(), asynq.NewTask(schedule.JobTypeConnectorSync, payload)))
}

// The SkipRetry a terminal failure carries survives the middleware chain
// serve installs, so asynq still stops retrying it.
func TestEdge_TerminalFailureSkipsRetryThroughTheMiddlewareChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	runner := mock_adapter.NewMockOperationRunner(ctrl)
	runner.EXPECT().RunOperation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sdk.SyncError{Kind: sdk.ErrKindAuth, Terminal: true, Cause: errors.New("401")})
	mux := asynq.NewServeMux()
	mux.Use(handler.ContextMiddleware(handler.WithTraceID()))
	mux.Use(handler.LoggingMiddleware(zap.NewNop()))
	mux.Use(handler.RecoveryMiddleware(zap.NewNop()))
	mux.Handle(schedule.JobTypeConnectorSync, NewConnectorSyncHandler(runner))

	payload, err := json.Marshal(ConnectorSyncPayload{OrgID: edgeOrgA, ConnectorID: "edge", OperationID: "op"})
	require.NoError(t, err)
	err = mux.ProcessTask(context.Background(), asynq.NewTask(schedule.JobTypeConnectorSync, payload))
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
}

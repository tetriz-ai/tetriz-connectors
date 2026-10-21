// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

// Tests of how the connector source treats failed reads, overrides and
// undecodable tasks.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mock_adapter "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/schedule/connectorsource"
	mock_dao "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// When listing one connector's configs fails (a database blip), that
// connector keeps the schedules of its last successful read, so the
// scheduler does not unregister its tasks. Before any successful read there
// is nothing to keep.
func TestConfigListFailureKeepsTheConnectorsLastGoodSchedules(t *testing.T) {
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "lim-a", []sdk.OperationDescriptor{pullOp("op", "*/30 * * * *")}))
	sched := mock_dao.NewMockIScheduleDAO(ctrl)
	sched.EXPECT().ListAll(gomock.Any()).Return(nil, nil).Times(3)
	cfgs := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	down := errors.New("database is unreachable")
	ok := &sdk.ConnectorConfig{OrganizationID: "00000000-0000-4000-8000-00000000aa01", ConnectorID: "lim-a", Enabled: true}
	gomock.InOrder(
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return(nil, down),
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return([]*sdk.ConnectorConfig{ok}, nil),
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return(nil, down),
	)
	src := NewConnectorScheduleSource(cfgs, sched, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err, "the failure is logged, not returned")
	assert.Empty(t, defs, "nothing to keep before a successful read")

	good, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, good, 1)

	kept, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	assert.Equal(t, good, kept, "the failed read keeps the last good schedules")
}

// cronEntriesFor sums scheduler_cron_entries for one connector of the
// connector source.
func cronEntriesFor(t *testing.T, connectorID string) float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(schedule.Collectors()...)
	families, err := reg.Gather()
	require.NoError(t, err)
	var sum float64
	for _, f := range families {
		if f.GetName() != "scheduler_cron_entries" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["source"] == "connector" && labels["connector_id"] == connectorID {
				sum += m.GetGauge().GetValue()
			}
		}
	}
	return sum
}

// The last good schedules a failed read keeps are counted in
// scheduler_cron_entries, both when one connector's list fails and when the
// whole source fails, so the metric shows what stays scheduled.
func TestKeptSchedulesAreCountedInCronEntries(t *testing.T) {
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "kept-metric", []sdk.OperationDescriptor{pullOp("op", "*/30 * * * *"), pullOp("op2", "0 * * * *")}))
	sched := mock_dao.NewMockIScheduleDAO(ctrl)
	cfgs := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	down := errors.New("database is unreachable")
	ok := &sdk.ConnectorConfig{OrganizationID: "00000000-0000-4000-8000-00000000ab01", ConnectorID: "kept-metric", Enabled: true}
	gomock.InOrder(
		sched.EXPECT().ListAll(gomock.Any()).Return(nil, nil),
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("kept-metric")).Return([]*sdk.ConnectorConfig{ok}, nil),
		sched.EXPECT().ListAll(gomock.Any()).Return(nil, nil),
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("kept-metric")).Return(nil, down),
		sched.EXPECT().ListAll(gomock.Any()).Return(nil, down),
	)
	src := NewConnectorScheduleSource(cfgs, sched, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	good, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, good, 2)
	assert.Equal(t, 2.0, cronEntriesFor(t, "kept-metric"))

	kept, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, kept, 2)
	assert.Equal(t, 2.0, cronEntriesFor(t, "kept-metric"), "a failed config list keeps and counts the last good schedules")

	_, err = src.FetchSchedules(context.Background())
	require.Error(t, err, "the overrides read failed: the provider keeps the source's last good schedules")
	assert.Equal(t, 2.0, cronEntriesFor(t, "kept-metric"), "and the metric still counts them")
}

// One organization's row that does not decrypt leaves only that
// organization unscheduled on a first read: the store returns the rows that
// decrypted with the error, and they are scheduled.
func TestUndecryptableRowDropsOnlyItsOrganizationsSchedules(t *testing.T) {
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "lim-a", []sdk.OperationDescriptor{pullOp("op", "*/30 * * * *")}))
	sched := mock_dao.NewMockIScheduleDAO(ctrl)
	sched.EXPECT().ListAll(gomock.Any()).Return(nil, nil)
	cfgs := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	ok := &sdk.ConnectorConfig{OrganizationID: "00000000-0000-4000-8000-00000000aa01", ConnectorID: "lim-a", Enabled: true}
	cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return([]*sdk.ConnectorConfig{ok},
		fmt.Errorf("decrypting credentials: %w", &store.UndecryptableRowsError{OrgIDs: []string{"00000000-0000-4000-8000-00000000aa02"}, Err: errors.New("message authentication failed")}))
	src := NewConnectorScheduleSource(cfgs, sched, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	defs, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 1, "the organization whose row decrypted is scheduled")
}

// An organization whose row stops decrypting keeps the schedules of its
// last successful read; the other organizations are scheduled from their
// current configs.
func TestUndecryptableRowKeepsItsOrganizationsLastGoodSchedules(t *testing.T) {
	const orgA, orgB = "00000000-0000-4000-8000-00000000aa01", "00000000-0000-4000-8000-00000000aa02"
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "lim-a", []sdk.OperationDescriptor{pullOp("op", "*/30 * * * *")}))
	sched := mock_dao.NewMockIScheduleDAO(ctrl)
	sched.EXPECT().ListAll(gomock.Any()).Return(nil, nil).Times(2)
	cfgs := mock_dao.NewMockIConnectorConfigDAO(ctrl)
	a := &sdk.ConnectorConfig{OrganizationID: orgA, ConnectorID: "lim-a", Enabled: true}
	b := &sdk.ConnectorConfig{OrganizationID: orgB, ConnectorID: "lim-a", Enabled: true}
	gomock.InOrder(
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return([]*sdk.ConnectorConfig{a, b}, nil),
		cfgs.EXPECT().ListEnabledByConnector(gomock.Any(), sdk.ConnectorID("lim-a")).Return([]*sdk.ConnectorConfig{a},
			fmt.Errorf("decrypting credentials: %w", &store.UndecryptableRowsError{OrgIDs: []string{orgB}, Err: errors.New("message authentication failed")})),
	)
	src := NewConnectorScheduleSource(cfgs, sched, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	good, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, good, 2)
	kept, err := src.FetchSchedules(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, good, kept, "the organization whose row did not decrypt keeps its schedule")
}

// A connector_schedules row with enabled=false pauses the operation: it is
// not scheduled, though its default schedule would apply without the row.
func TestDisabledOverridePausesTheOperation(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000bbbb"
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "lim-b", Enabled: true}))

	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "lim-b", []sdk.OperationDescriptor{pullOp("op", "0 * * * *"), pullOp("other", "0 * * * *")}))
	src := NewConnectorScheduleSource(mem, mem, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	defs, err := src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, defs, 2, "both operations are scheduled without an override")

	require.NoError(t, mem.Upsert(ctx, &model.ScheduleOverride{OrganizationID: org, ConnectorID: "lim-b", OperationID: "op", CronExpression: "0 0 1 1 *", Enabled: false}))
	defs, err = src.FetchSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, defs, 1, "the paused operation is not scheduled")
	assert.Equal(t, org+"/lim-b/other", defs[0].ID)
}

// A task whose payload does not decode fails with asynq.SkipRetry: the
// payload is fixed in the task, so no retry can succeed, and nothing runs.
func TestUndecodablePayloadSkipsRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := NewConnectorSyncHandler(mock_adapter.NewMockOperationRunner(ctrl))
	err := h.ProcessTask(context.Background(), asynq.NewTask("connector.sync", []byte("{not json")))
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
	assert.Contains(t, err.Error(), "unmarshal payload")
}

// A per-operation override in the config that does not parse is logged, and
// the operation keeps its default schedule.
func TestMalformedOperationOverrideKeepsTheDefaultSchedule(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000bbbc"
	schedule := func(overrides map[sdk.OperationID]json.RawMessage) string {
		mem := memstore.New()
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "lim-c", Enabled: true, OperationOverrides: overrides}))
		ctrl := gomock.NewController(t)
		reg := sdk.NewConnectorRegistry()
		reg.Register(newMockConnector(ctrl, "lim-c", []sdk.OperationDescriptor{pullOp("op", "0 * * * *")}))
		src := NewConnectorScheduleSource(mem, mem, reg, nil, nil, nil)
		src.liveIndividualSet = func() map[string]struct{} { return nil }
		defs, err := src.FetchSchedules(ctx)
		require.NoError(t, err)
		require.Len(t, defs, 1, "the operation is still scheduled")
		return defs[0].CronExpression
	}
	def := schedule(nil)
	assert.Equal(t, def, schedule(map[sdk.OperationID]json.RawMessage{"op": json.RawMessage(`"not an object"`)}),
		"a malformed override keeps the default schedule")
	assert.Equal(t, "5 4 * * *", schedule(map[sdk.OperationID]json.RawMessage{"op": json.RawMessage(`{"schedule":"5 4 * * *"}`)}),
		"a well-formed override replaces it")
}

// An override whose schedule is not a cron expression is ignored: the
// operation keeps its default schedule and is still registered.
func TestUnparseableCronOverrideKeepsTheDefaultSchedule(t *testing.T) {
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000bbbd"
	mem := memstore.New()
	require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "lim-d", Enabled: true,
		OperationOverrides: map[sdk.OperationID]json.RawMessage{"op": json.RawMessage(`{"schedule":"every hour"}`)}}))
	ctrl := gomock.NewController(t)
	reg := sdk.NewConnectorRegistry()
	reg.Register(newMockConnector(ctrl, "lim-d", []sdk.OperationDescriptor{pullOp("op", "0 * * * *")}))
	src := NewConnectorScheduleSource(mem, mem, reg, nil, nil, nil)
	src.liveIndividualSet = func() map[string]struct{} { return nil }

	cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{src}, 0, nil).GetConfigs()
	require.NoError(t, err)
	require.Len(t, cfgs, 1, "the operation keeps its default schedule")
}

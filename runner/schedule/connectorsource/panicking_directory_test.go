// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// panicsFor panics for one organization, as a host directory with a bug for
// one organization would; every other organization resolves.
type panicsFor string

func (p panicsFor) TenantType(_ context.Context, orgID string) (sdk.TenantKind, error) {
	if orgID == string(p) {
		panic("assignment to entry in nil map")
	}
	return sdk.TenantKindOrganization, nil
}

func countFor(cfgs []*asynq.PeriodicTaskConfig, org string) int {
	n := 0
	for _, c := range cfgs {
		if strings.Contains(string(c.Task.Payload()), org) {
			n++
		}
	}
	return n
}

// A tenant directory that panics for one organization costs only that
// organization's schedules.
func TestPanickingTenantDirectoryCostsOnlyThatOrganization(t *testing.T) {
	ctx := context.Background()
	mem := memstore.New()
	const a, b = "00000000-0000-4000-8000-00000000000a", "00000000-0000-4000-8000-00000000000b"
	for _, org := range []string{a, b} {
		require.NoError(t, mem.Save(ctx, &sdk.ConnectorConfig{OrganizationID: org, ConnectorID: "github", Enabled: true,
			Settings: json.RawMessage(`{"organization":"x"}`), Credentials: json.RawMessage(`{}`)}))
	}
	reg := app.NewRegistry(nil)
	healthy, err := schedule.NewConfigProvider([]schedule.ScheduleSource{
		connectorsource.NewConnectorScheduleSource(mem, mem, reg, panicsFor("none"), nil, nil)}, 0, nil).GetConfigs()
	require.NoError(t, err)
	cfgs, err := schedule.NewConfigProvider([]schedule.ScheduleSource{
		connectorsource.NewConnectorScheduleSource(mem, mem, reg, panicsFor(b), nil, nil)}, 0, nil).GetConfigs()
	require.NoError(t, err)
	require.NotZero(t, countFor(healthy, a))
	assert.Equal(t, countFor(healthy, a), countFor(cfgs, a), "organization a keeps every schedule")
}

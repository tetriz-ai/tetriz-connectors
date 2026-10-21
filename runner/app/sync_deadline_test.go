// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
	"github.com/tetriz-ai/tetriz-connectors/sink"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

const (
	pacedConnectorID sdk.ConnectorID = "pacedprobe"
	pacedOpDefault   sdk.OperationID = "fetch_default"
	pacedOpOwn       sdk.OperationID = "fetch_own_timeout"
	pacedOwnTimeout                  = 7 * time.Minute
)

// denyOnceGateway rejects its first call the way the gateways' local limiter
// does (sdk.ErrLocalRateLimit with a sized one-second wait) and answers every
// later call.
type denyOnceGateway struct {
	sdk.VendorGateway
	calls atomic.Int32
}

func (g *denyOnceGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	if g.calls.Add(1) == 1 {
		return nil, &sdk.SyncError{
			Kind:              sdk.ErrKindRateLimit,
			Cause:             fmt.Errorf("rate limit exceeded for group test: %w", sdk.ErrLocalRateLimit),
			Retryable:         true,
			RetryAfterSeconds: 1,
		}
	}
	return &emptypb.Empty{}, nil
}

// pacedOp makes one paced vendor call and records the deadline it ran under.
type pacedOp struct {
	id       sdk.OperationID
	gw       *denyOnceGateway
	deadline *time.Time
}

func (o *pacedOp) OperationID() sdk.OperationID { return o.id }

func (o *pacedOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	if d, ok := ctx.Deadline(); ok {
		*o.deadline = d
	}
	if _, err := pacing.ExecuteWaitingForLocalCapacity(ctx, o.gw, &emptypb.Empty{}, &pacing.PacingBudget{}, pacedConnectorID, o.id, p.Config.OrganizationID); err != nil {
		return nil, err
	}
	return &sdk.Cursor{Data: json.RawMessage(`{}`)}, nil
}

type pacedConnector struct {
	gw       *denyOnceGateway
	deadline *time.Time
}

func (pacedConnector) ID() sdk.ConnectorID { return pacedConnectorID }
func (pacedConnector) DisplayName() string { return "Paced probe" }
func (pacedConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryIssueTracking}
}
func (pacedConnector) ValidateConfig(context.Context, *sdk.ConnectorConfig) error {
	return nil
}

func (c pacedConnector) Operations() []sdk.OperationDescriptor {
	op := func(id sdk.OperationID, timeout time.Duration) sdk.OperationDescriptor {
		return sdk.OperationDescriptor{
			ID: id, DisplayName: string(id), IngestionType: sdk.IngestionPull, ExecutionTimeout: timeout,
			Factory: func() sdk.Operation { return &pacedOp{id: id, gw: c.gw, deadline: c.deadline} },
		}
	}
	return []sdk.OperationDescriptor{op(pacedOpDefault, 0), op(pacedOpOwn, pacedOwnTimeout)}
}

// `sync` runs an operation under the deadline `serve` would give it: the
// operation's ExecutionTimeout, or the scheduler's default. Without one, a
// paced operation cannot wait out a rejection from the local rate limiter
// (pacing never sleeps without a deadline), so a manual sync failed on the
// first local denial that a scheduled run absorbs.
func TestSyncRunsUnderTheScheduledDeadline(t *testing.T) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("sync deadline", err)
		t.Skip("no test database")
	}
	db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	for _, tc := range []struct {
		op   sdk.OperationID
		want time.Duration
	}{
		{pacedOpDefault, schedule.DefaultTaskTimeout},
		{pacedOpOwn, pacedOwnTimeout},
	} {
		t.Run(string(tc.op), func(t *testing.T) {
			ctx := context.Background()
			const org = "00000000-0000-4000-8000-0000000d1e0a"
			var deadline time.Time
			gw := &denyOnceGateway{}
			reg := sdk.NewConnectorRegistry()
			reg.Register(pacedConnector{gw: gw, deadline: &deadline})
			configs, states, runs := store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db)
			require.NoError(t, WriteConfig(ctx, store.PostgresTxRunner{}, configs, &sdk.ConnectorConfig{
				OrganizationID: org, ConnectorID: pacedConnectorID, Settings: json.RawMessage(`{}`),
				Credentials: json.RawMessage(`{}`), Enabled: true, HealthState: sdk.HealthStateHealthy,
			}))
			a := &App{
				Registry: reg, Configs: configs, States: states, Runs: runs,
				Orchestrator: runner.NewOrchestrator(reg, configs, states, runs, sink.NewChannelSink(), fakevendor.GatewayFactory(nil)).WithTxRunner(store.PostgresTxRunner{}),
			}

			start := time.Now()
			res, err := a.SyncOnce(ctx, org, pacedConnectorID, tc.op, sdk.TenantKindOrganization)
			require.NoError(t, err, "the run waits out the local denial instead of failing: %+v", res)
			assert.Equal(t, "completed", res.Status)
			assert.Equal(t, int32(2), gw.calls.Load(), "the denied call was retried once after the wait")
			require.False(t, deadline.IsZero(), "the operation ran with a deadline")
			assert.WithinDuration(t, start.Add(tc.want), deadline, 30*time.Second)
		})
	}
}

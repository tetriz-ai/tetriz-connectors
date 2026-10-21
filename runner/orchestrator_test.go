// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	// Aliased: the bare name collides with the connector's own testutil package.
	prommetrics "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	mock_dao "github.com/tetriz-ai/tetriz-connectors/internal/mocks/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

// --- Test helpers ---

// mockFetchOp implements sdk.APIFetchOperation for testing.
type mockFetchOp struct {
	opID      sdk.OperationID
	executeFn func(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error)
}

func (m *mockFetchOp) OperationID() sdk.OperationID { return m.opID }
func (m *mockFetchOp) Execute(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	return m.executeFn(ctx, params)
}

// mockConnector implements sdk.Connector for testing.
type mockConnector struct {
	id  sdk.ConnectorID
	ops []sdk.OperationDescriptor
}

func (c *mockConnector) ID() sdk.ConnectorID { return c.id }
func (c *mockConnector) Categories() []sdk.ConnectorCategory {
	return []sdk.ConnectorCategory{sdk.CategoryVCS}
}
func (c *mockConnector) DisplayName() string                   { return string(c.id) }
func (c *mockConnector) Operations() []sdk.OperationDescriptor { return c.ops }
func (c *mockConnector) ValidateConfig(_ context.Context, _ *sdk.ConnectorConfig) error {
	return nil
}

// MockedDependencies bundles all mock dependencies for orchestrator tests.
type MockedDependencies struct {
	MockConfigDAO *mock_dao.MockIConnectorConfigDAO
	MockStateDAO  *mock_dao.MockISyncStateDAO
	MockRunDAO    *mock_dao.MockISyncRunDAO
}

func NewMockedDependencies(t *testing.T) *MockedDependencies {
	ctrl := gomock.NewController(t)
	runs := mock_dao.NewMockISyncRunDAO(ctrl)
	// Every run first closes its operation's abandoned runs, and renews its
	// lease while it executes; the tests here have none to close.
	runs.EXPECT().CloseAbandoned(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	runs.EXPECT().RenewLease(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return &MockedDependencies{
		MockConfigDAO: mock_dao.NewMockIConnectorConfigDAO(ctrl),
		MockStateDAO:  mock_dao.NewMockISyncStateDAO(ctrl),
		MockRunDAO:    runs,
	}
}

func setupRegistry(connectorID sdk.ConnectorID, opID sdk.OperationID, executeFn func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)) *sdk.ConnectorRegistry {
	registry := sdk.NewConnectorRegistry()
	registry.Register(&mockConnector{
		id: connectorID,
		ops: []sdk.OperationDescriptor{
			{
				ID:            opID,
				DisplayName:   string(opID),
				IngestionType: sdk.IngestionPull,
				Factory: func() sdk.Operation {
					return &mockFetchOp{opID: opID, executeFn: executeFn}
				},
			},
		},
	})
	return registry
}

// setupRegistryRequiringTenantType is setupRegistry with the op tagged
// RequiresTenantType, so the orchestrator's fail-loud intent guard applies.
func setupRegistryRequiringTenantType(connectorID sdk.ConnectorID, opID sdk.OperationID, executeFn func(context.Context, *sdk.ExecutionParams) (*sdk.Cursor, error)) *sdk.ConnectorRegistry {
	registry := sdk.NewConnectorRegistry()
	registry.Register(&mockConnector{
		id: connectorID,
		ops: []sdk.OperationDescriptor{
			{
				ID:                 opID,
				DisplayName:        string(opID),
				IngestionType:      sdk.IngestionPull,
				RequiresTenantType: true,
				Factory: func() sdk.Operation {
					return &mockFetchOp{opID: opID, executeFn: executeFn}
				},
			},
		},
	})
	return registry
}

// noopGatewayFactory returns a nil gateway. Tests use mock operations that don't call the gateway.
var noopGatewayFactory = sdk.VendorGatewayFactory(
	func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		return nil, nil
	},
)

// --- Tests ---

// TestOrchestrator_RunOperationAppliesContextEnricher pins that the host's
// context enricher decorates the context the operation runs with, and that
// the orchestrator's own stamps (org id on outgoing metadata, sync run id) are
// already on the context the enricher receives.
func TestOrchestrator_RunOperationAppliesContextEnricher(t *testing.T) {
	mocks := NewMockedDependencies(t)
	sink := sink.NewChannelSink()

	type enrichKey struct{}
	var gotEnriched any
	var gotOrgMD []string
	var enricherSawRunID string
	registry := setupRegistry("github", "fetch_prs", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		gotEnriched = ctx.Value(enrichKey{})
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			gotOrgMD = md.Get("org_id")
		}
		return sdk.NewTimestampCursor(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)), nil
	})

	mocks.MockConfigDAO.EXPECT().
		Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
	mocks.MockStateDAO.EXPECT().
		GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
		Return(nil, nil)
	mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mocks.MockStateDAO.EXPECT().
		SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory).
		WithContextEnricher(func(ctx context.Context) context.Context {
			enricherSawRunID = sdk.SyncRunIDFromContext(ctx)
			return context.WithValue(ctx, enrichKey{}, "host-value")
		})
	require.NoError(t, orch.RunOperation(context.Background(), "org_1", "github", "fetch_prs", sdk.TenantKindOrganization, "test"))

	assert.Equal(t, "host-value", gotEnriched, "op ctx must carry what the enricher added")
	assert.Equal(t, []string{"org_1"}, gotOrgMD, "op ctx must carry org_id on outgoing gRPC metadata")
	assert.NotEmpty(t, enricherSawRunID, "the enricher runs after the sync run id is stamped")
}

// TestOrchestrator_RunOperationStampsSyncRunID pins that the operation's ctx carries
// the id of the SyncRun the orchestrator created for it, and that the id matches the
// row it persisted. Without this, deleting or reordering the one WithSyncRunID line
// leaves every emitted record unattributed — the record's sync_run_id silently back to
// NULL — with the whole suite still green, because the sink-side test only proves the
// stamp works once something puts it on the ctx.
func TestOrchestrator_RunOperationStampsSyncRunID(t *testing.T) {
	mocks := NewMockedDependencies(t)
	sink := sink.NewChannelSink()

	var gotRunID string
	registry := setupRegistry("github", "fetch_prs", func(ctx context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
		gotRunID = sdk.SyncRunIDFromContext(ctx)
		return sdk.NewTimestampCursor(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)), nil
	})

	mocks.MockConfigDAO.EXPECT().
		Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
		Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
	mocks.MockStateDAO.EXPECT().
		GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
		Return(nil, nil)

	// Capture the id the orchestrator minted, so the assertion is that the ctx and
	// the persisted run agree — not merely that something non-empty was stamped.
	var createdRunID string
	mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
			createdRunID = run.ID
			return nil
		})
	mocks.MockStateDAO.EXPECT().
		SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
	require.NoError(t, orch.RunOperation(context.Background(), "org_1", "github", "fetch_prs", sdk.TenantKindOrganization, "test"))

	require.NotEmpty(t, createdRunID, "the run row must have been created with an id")
	assert.Equal(t, createdRunID, gotRunID, "op ctx must carry the id of the SyncRun created for this run")
}

func TestOrchestrator_RunOperation(t *testing.T) {
	tests := []struct {
		name string
		// tenantType is the intent passed to RunOperation. Zero value
		// (TENANT_TYPE_UNSPECIFIED) is fine for the cases that do not branch on it.
		tenantType sdk.TenantKind
		// opID overrides the operation id RunOperation is invoked with. Empty
		// defaults to "fetch_prs" (the op the registry helper registers).
		opID    sdk.OperationID
		setupFn func(t *testing.T) *Orchestrator
		wantErr bool
		// wantErrCheck, when set, asserts properties of the returned error
		// (e.g. that the guard surfaced a Terminal SyncError). Runs only when
		// wantErr is true.
		wantErrCheck func(t *testing.T, err error)
	}{
		{
			name: "success lifecycle with cursor update",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					_ = params.Sink.Emit(ctx, &sdk.RawRecord{
						ConnectorID:    "github",
						OperationID:    "fetch_prs",
						OrganizationID: "org_1",
						ExternalID:     "pr-42",
						Payload:        json.RawMessage(`{"title":"fix"}`),
						FetchedAt:      time.Now().UTC(),
					})
					return sdk.NewTimestampCursor(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)), nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{
						ConnectorID:    "github",
						OrganizationID: "org_1",
						Enabled:        true,
					}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)

				mocks.MockRunDAO.EXPECT().
					Create(gomock.Any(), gomock.Any()).
					Return(nil)

				// SaveCursor: at least once for the final save, possibly more for checkpoints.
				mocks.MockStateDAO.EXPECT().
					SaveCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
					Return(nil).
					AnyTimes()

				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusCompleted, run.Status)
						assert.NotNil(t, run.CompletedAt)
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			name: "first run with nil cursor",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					assert.Nil(t, params.Cursor, "first run should have nil cursor")
					return sdk.NewTimestampCursor(time.Now().UTC()), nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)

				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				mocks.MockStateDAO.EXPECT().SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			name: "failure does not save cursor",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					return nil, &sdk.SyncError{
						ConnectorID: "github",
						OperationID: "fetch_prs",
						Kind:        sdk.ErrKindVendorError,
						Cause:       fmt.Errorf("500 internal server error"),
					}
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(sdk.NewTimestampCursor(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)), nil)

				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

				// On failure: Update called with failed status, but SaveCursor is NOT called.
				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status)
						assert.Equal(t, "vendor_error", run.ErrorKind)
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
		},
		{
			// A Terminal *sdk.SyncError from
			// Execute (e.g. a vendorapi 401/404 or oauth invalid_grant) must
			// survive failRun's %w wrapping and stay recoverable via errors.As on
			// RunOperation's returned error — the exact seam the scheduler reads
			// to SkipRetry a dead-credential run instead of burning MaxRetry.
			name:       "terminal sync error propagates to caller",
			tenantType: sdk.TenantKindOrganization,
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					return nil, &sdk.SyncError{
						ConnectorID: "github",
						OperationID: "fetch_prs",
						Kind:        sdk.ErrKindAuth,
						Terminal:    true,
						Cause:       fmt.Errorf("HTTP 401"),
					}
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
				// A Terminal auth-kind failure must drive the health-state machine:
				// markAuthRevokedIfTerminal calls MarkAuthRevoked so the orchestrator's
				// own gate stops repeating this doomed call every tick.
				mocks.MockConfigDAO.EXPECT().
					MarkAuthRevoked(gomock.Any(), "org_1", sdk.ConnectorID("github"), gomock.Any()).
					Return(true, nil)

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
			wantErrCheck: func(t *testing.T, err error) {
				var syncErr *sdk.SyncError
				require.ErrorAs(t, err, &syncErr, "Terminal SyncError must survive the orchestrator error chain")
				assert.True(t, syncErr.Terminal, "Terminal must reach the scheduler so it SkipRetries")
				assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind)
			},
		},
		{
			// A gateway-construction failure must close the run like any other
			// failure. Returning bare would leave the row `running` until its
			// lease expired, closed as abandoned, and emit no failure
			// metric, so a connector that could not build a gateway would be
			// invisible in every sync metric.
			name:       "gateway factory failure fails the run",
			tenantType: sdk.TenantKindOrganization,
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					t.Fatal("operation must not execute when the gateway could not be built")
					return nil, nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status, "run must not be left at running")
						assert.Equal(t, sdk.ErrKindConfigInvalid.String(), run.ErrorKind)
						assert.NotNil(t, run.CompletedAt, "a closed run carries CompletedAt")
						return nil
					})

				failingFactory := sdk.VendorGatewayFactory(
					func(_ context.Context, _ *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
						return nil, fmt.Errorf("credentials absent")
					},
				)
				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, failingFactory)
			},
			wantErr: true,
		},
		{
			name: "disabled config skips execution",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					t.Fatal("should not execute when disabled")
					return nil, nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: false}, nil)

				// No other DAO calls expected.

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			// A connector already marked auth_revoked (by an earlier
			// Terminal auth failure) must short-circuit here — no gateway call,
			// no sync_run row — exactly like the disabled-config case, so future
			// scheduled ticks stop repeating a doomed vendor call.
			name: "auth_revoked config skips execution",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					t.Fatal("should not execute when auth_revoked")
					return nil, nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{
						ConnectorID:    "github",
						OrganizationID: "org_1",
						Enabled:        true,
						HealthState:    sdk.HealthStateAuthRevoked,
					}, nil)

				// No other DAO calls expected — in particular no run created.

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			name: "unknown connector returns error",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := sdk.NewConnectorRegistry() // empty registry

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
		},
		{
			name: "checkpoint saves cursor mid-execution and failure preserves it",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				checkpointCursor := sdk.NewTimestampCursor(time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC))

				registry := setupRegistry("github", "fetch_prs", func(ctx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					// Simulate: checkpoint after first page succeeds...
					assert.NotNil(t, params.Checkpoint, "checkpoint function should be provided")
					err := params.Checkpoint(ctx, checkpointCursor)
					require.NoError(t, err)

					// ...then second page fails.
					return nil, &sdk.SyncError{
						ConnectorID: "github",
						OperationID: "fetch_prs",
						Kind:        sdk.ErrKindRateLimit,
						Cause:       fmt.Errorf("429 too many requests"),
					}
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)

				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

				// SaveCursor called once (by checkpoint), NOT by the final success path.
				mocks.MockStateDAO.EXPECT().
					SaveCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
					Return(nil).
					Times(1)

				// Run marked as failed (execution returned error after checkpoint).
				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status)
						assert.Equal(t, "rate_limit", run.ErrorKind)
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
		},
		{
			// The Checkpoint closure uses a detached context.
			// When the connector calls params.Checkpoint AFTER the parent ctx
			// is already canceled (asynq deadline fired mid-iter), the
			// closure's persistence call must still succeed so the checkpoint
			// is durable. Without the WithoutCancel wrap in the orchestrator,
			// the SaveCursor would fail immediately and the connector would
			// lose the just-completed repo's progress.
			name: "Checkpoint closure persists even when parent ctx is canceled",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				canceledCursor := sdk.NewTimestampCursor(time.Date(2026, 5, 20, 14, 0, 0, 0, time.UTC))

				registry := setupRegistry("github", "fetch_prs", func(opCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					// Cancel the parent ctx, THEN call Checkpoint. The
					// detached-ctx wrap in the orchestrator should make the
					// downstream SaveCursor succeed despite the cancellation.
					innerCtx, cancelInner := context.WithCancel(opCtx)
					cancelInner()
					require.NotNil(t, params.Checkpoint, "checkpoint must be provided")
					checkpointErr := params.Checkpoint(innerCtx, canceledCursor)
					assert.NoError(t, checkpointErr,
						"Checkpoint must persist even when caller's ctx is canceled (orchestrator detaches)")
					return canceledCursor, nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

				// SaveCursor must be called TWICE — once from Checkpoint (under
				// the detached ctx), and once from the success-path post-exec
				// block at the end of RunOperation. Both succeed.
				mocks.MockStateDAO.EXPECT().
					SaveCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
					Return(nil).
					Times(2)
				mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			// The orchestrator's post-execution-save failure path. When Execute returns nil error but the post-exec txn fails
			// (SaveCursor returns ctx-cancel or similar), the run row must be
			// marked failed instead of getting stuck at `running` — same
			// defense as the existing failRun path.
			name: "post-execution save failure routes through failRun (no stuck running)",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					return sdk.NewTimestampCursor(time.Date(2026, 5, 20, 14, 0, 0, 0, time.UTC)), nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)

				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

				// SaveCursor fails on the post-exec save (simulating ctx-cancel
				// during the cursor write). Without the orchestrator's detached
				// ctx + failRun fallback, the run row would stay at `running`.
				mocks.MockStateDAO.EXPECT().
					SaveCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs"), gomock.Any()).
					Return(fmt.Errorf("context deadline exceeded"))

				// The orchestrator falls through to failRun, which still
				// updates the run row to status=failed via its own detached ctx.
				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status,
							"post-exec save failure must mark run failed (not stuck running)")
						assert.NotEmpty(t, run.ErrorKind, "errKind must be classified, not empty")
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
		},
		{
			name: "wrapped SyncError extracts error kind via errors.As",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					syncErr := &sdk.SyncError{
						ConnectorID: "github",
						OperationID: "fetch_prs",
						Kind:        sdk.ErrKindRateLimit,
						Cause:       fmt.Errorf("429 too many requests"),
					}
					return nil, fmt.Errorf("operation wrapper: %w", syncErr)
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)

				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)

				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status)
						assert.Equal(t, "rate_limit", run.ErrorKind)
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
		},
		{
			// Threading: the tenant_type passed to RunOperation must reach the
			// operation via ExecutionParams.TenantType, unchanged.
			name:       "tenant_type threads through to ExecutionParams",
			tenantType: sdk.TenantKindIndividual,
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				var gotTenantType sdk.TenantKind
				registry := setupRegistry("github", "fetch_prs", func(_ context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					gotTenantType = params.TenantType
					return sdk.NewTimestampCursor(time.Now().UTC()), nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				mocks.MockStateDAO.EXPECT().SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ *model.SyncRun) error {
					assert.Equal(t, sdk.TenantKindIndividual, gotTenantType,
						"the op must see the tenant_type passed to RunOperation")
					return nil
				})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
		{
			// Fail-loud guard: an op tagged RequiresTenantType handed UNSPECIFIED
			// must be failed (config_invalid) before Execute, never run with an
			// unknown intent.
			name:       "RequiresTenantType op with UNSPECIFIED fails loud before execute",
			tenantType: sdk.TenantKindUnspecified,
			opID:       "fetch_repos",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistryRequiringTenantType("github", "fetch_repos", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
					t.Fatal("Execute must not run when tenant_type is UNSPECIFIED")
					return nil, nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_repos")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				// No SaveCursor: the run never executes.
				mocks.MockRunDAO.EXPECT().
					Update(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, run *model.SyncRun) error {
						assert.Equal(t, model.SyncRunStatusFailed, run.Status)
						assert.Equal(t, sdk.ErrKindConfigInvalid.String(), run.ErrorKind)
						return nil
					})

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: true,
			// The guard must surface a Terminal *sdk.SyncError so the
			// scheduleradapter handler maps it to asynq.SkipRetry — exactly one
			// failed run, recovery via the next FetchSchedules re-stamp.
			wantErrCheck: func(t *testing.T, err error) {
				var syncErr *sdk.SyncError
				require.ErrorAs(t, err, &syncErr, "guard must surface a *sdk.SyncError")
				assert.Equal(t, sdk.ErrKindConfigInvalid, syncErr.Kind)
				assert.True(t, syncErr.Terminal, "UNSPECIFIED guard error must be Terminal")
			},
		},
		{
			// A RequiresTenantType op WITH a resolved intent runs normally — the
			// guard fires only on UNSPECIFIED.
			name:       "RequiresTenantType op with resolved intent runs",
			tenantType: sdk.TenantKindIndividual,
			opID:       "fetch_repos",
			setupFn: func(t *testing.T) *Orchestrator {
				mocks := NewMockedDependencies(t)
				sink := sink.NewChannelSink()

				registry := setupRegistryRequiringTenantType("github", "fetch_repos", func(_ context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
					assert.Equal(t, sdk.TenantKindIndividual, params.TenantType)
					return sdk.NewTimestampCursor(time.Now().UTC()), nil
				})

				mocks.MockConfigDAO.EXPECT().
					Get(gomock.Any(), "org_1", sdk.ConnectorID("github")).
					Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: "org_1", Enabled: true}, nil)
				mocks.MockStateDAO.EXPECT().
					GetCursor(gomock.Any(), "org_1", sdk.ConnectorID("github"), sdk.OperationID("fetch_repos")).
					Return(nil, nil)
				mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				mocks.MockStateDAO.EXPECT().SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

				return NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orch := tt.setupFn(t)

			opID := tt.opID
			if opID == "" {
				opID = "fetch_prs"
			}
			err := orch.RunOperation(context.Background(), "org_1", "github", opID, tt.tenantType, "test")

			if (err != nil) != tt.wantErr {
				t.Errorf("RunOperation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.wantErrCheck != nil {
				tt.wantErrCheck(t, err)
			}
		})
	}
}

func TestMetricStatusForRun(t *testing.T) {
	// A vendor 429 and our own limiter's rejection arrive under the same errKind.
	// Only the wrapped cause tells them apart, and the whole point of the split is
	// that they must not share a label: the first self-heals, the second does not.
	localCause := fmt.Errorf("rate limit exceeded for group cursor-usage: %w", sdk.ErrLocalRateLimit)

	tests := []struct {
		name    string
		errKind string
		cause   error
		want    string
	}{
		{name: "vendor 429 stays deferred", errKind: sdk.ErrKindRateLimit.String(), cause: errors.New("429 from vendor"), want: "deferred"},
		{name: "rate_limit with no cause stays deferred", errKind: sdk.ErrKindRateLimit.String(), cause: nil, want: "deferred"},
		{name: "our own limiter is self_throttled", errKind: sdk.ErrKindRateLimit.String(), cause: localCause, want: "self_throttled"},
		{name: "our own limiter wrapped again is still self_throttled", errKind: sdk.ErrKindRateLimit.String(), cause: fmt.Errorf("executing operation: %w", localCause), want: "self_throttled"},
		{name: "auth stays failed", errKind: sdk.ErrKindAuth.String(), want: "failed"},
		{name: "vendor_error stays failed", errKind: sdk.ErrKindVendorError.String(), want: "failed"},
		{name: "internal stays failed", errKind: sdk.ErrKindInternal.String(), want: "failed"},
		{name: "ctx_exhausted stays failed", errKind: sdk.ErrKindCtxExhausted.String(), want: "failed"},
		{name: "empty defaults to failed", errKind: "", want: "failed"},
		// The local sentinel must not leak past its own errKind: a sink failure that
		// happens to wrap it is still a sink failure, not throttling.
		{name: "local sentinel under another kind stays failed", errKind: sdk.ErrKindSinkFailure.String(), cause: localCause, want: "failed"},
		// The production shape, end to end. The limiter returns a *SyncError whose
		// Cause holds the sentinel, and the operation wraps that with %w — so the
		// walk only reaches the sentinel if SyncError.Unwrap() is in the chain.
		// Building the cause with a bare fmt.Errorf (as the cases above do) would
		// pass even with Unwrap deleted, which is exactly the regression that would
		// silently return every local rejection to the "deferred" blind spot.
		{
			name:    "the real gateway shape resolves through SyncError.Unwrap",
			errKind: sdk.ErrKindRateLimit.String(),
			cause: fmt.Errorf("cursor usage: fetching page %d: %w", 3, &sdk.SyncError{
				Kind:  sdk.ErrKindRateLimit,
				Cause: fmt.Errorf("rate limit exceeded for group %s: %w", "cursor-usage", sdk.ErrLocalRateLimit),
			}),
			want: "self_throttled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, metricStatusForRun(tt.errKind, tt.cause))
		})
	}
}

// TestOrchestrator_RunOperationFetchCacheWiring proves RunOperation threads the
// connector fetch-cache onto ExecutionParams: when
// WithFetchCache was called it builds a fetchCacheAdapter scoped to the run's
// org/connector/cap; when it was NOT called the params keep FetchCache nil (the
// nil-guard that lets the capability be simply absent for an installation).
func TestOrchestrator_RunOperationFetchCacheWiring(t *testing.T) {
	const orgID = "org_1"

	tests := []struct {
		name           string
		withFetchCache bool
		maxBytes       int
		wantCache      bool
	}{
		{
			name:           "with WithFetchCache: adapter built scoped to org/connector/cap",
			withFetchCache: true,
			maxBytes:       262144,
			wantCache:      true,
		},
		{
			name:           "with WithFetchCache: custom cap",
			withFetchCache: true,
			maxBytes:       1024,
			wantCache:      true,
		},
		{
			name:           "without WithFetchCache: cache nil",
			withFetchCache: false,
			wantCache:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mocks := NewMockedDependencies(t)
			sink := sink.NewChannelSink()

			var gotParams *sdk.ExecutionParams
			registry := setupRegistry("github", "fetch_prs", func(_ context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
				gotParams = params
				return sdk.NewTimestampCursor(time.Now().UTC()), nil
			})

			mocks.MockConfigDAO.EXPECT().
				Get(gomock.Any(), orgID, sdk.ConnectorID("github")).
				Return(&sdk.ConnectorConfig{ConnectorID: "github", OrganizationID: orgID, Enabled: true}, nil)
			mocks.MockStateDAO.EXPECT().
				GetCursor(gomock.Any(), orgID, sdk.ConnectorID("github"), sdk.OperationID("fetch_prs")).
				Return(nil, nil)
			mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			mocks.MockStateDAO.EXPECT().SaveCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

			orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			if tt.withFetchCache {
				ctrl := gomock.NewController(t)
				fcDAO := mock_dao.NewMockIFetchCacheDAO(ctrl)
				orch = orch.WithFetchCache(fcDAO, func() int { return tt.maxBytes })
			}

			require.NoError(t, orch.RunOperation(context.Background(), orgID, "github", "fetch_prs", sdk.TenantKindOrganization, "test"))
			require.NotNil(t, gotParams)

			if tt.wantCache {
				require.NotNil(t, gotParams.FetchCache)
				adapter, ok := gotParams.FetchCache.(*fetchCacheAdapter)
				require.True(t, ok, "FetchCache should be a *fetchCacheAdapter")
				assert.Equal(t, orgID, adapter.orgID)
				assert.Equal(t, sdk.ConnectorID("github"), adapter.connectorID)
				assert.Equal(t, tt.maxBytes, adapter.maxValueBytes)
			} else {
				assert.Nil(t, gotParams.FetchCache)
			}
		})
	}
}

// TestOrchestrator_MarkAuthRevokedWriteFailureMetric pins the two revoke-write
// outcomes as distinguishable series: a failed write moves only
// HealthWriteFailuresTotal{direction=revoke}, a successful one moves only
// HealthStateTransitionsTotal. Asserting both deltas per case is the point — a
// test that only checked the failure counter would pass even if the failure path
// also recorded a transition, which is the confusion the counter exists to end.
// Each case uses its own (org, connector) labels because these counters are
// process-global and other cases in this package touch them.
func TestOrchestrator_MarkAuthRevokedWriteFailureMetric(t *testing.T) {
	tests := []struct {
		name        string
		orgID       string
		connectorID sdk.ConnectorID
		// markErr, when set, is what MarkAuthRevoked returns instead of a
		// successful transition.
		markErr             error
		wantWriteFailDelta  float64
		wantTransitionDelta float64
	}{
		{
			name:                "revoke write fails: only the write-failure counter moves",
			orgID:               "org_revoke_write_fail",
			connectorID:         "github",
			markErr:             fmt.Errorf("connector db unreachable"),
			wantWriteFailDelta:  1,
			wantTransitionDelta: 0,
		},
		{
			name:                "revoke write lands: only the transition counter moves",
			orgID:               "org_revoke_write_ok",
			connectorID:         "slack",
			wantWriteFailDelta:  0,
			wantTransitionDelta: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mocks := NewMockedDependencies(t)
			sink := sink.NewChannelSink()

			registry := setupRegistry(tt.connectorID, "fetch_prs", func(_ context.Context, _ *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return nil, &sdk.SyncError{
					ConnectorID: tt.connectorID,
					OperationID: "fetch_prs",
					Kind:        sdk.ErrKindAuth,
					Terminal:    true,
					Cause:       fmt.Errorf("HTTP 401"),
				}
			})

			cfg := &sdk.ConnectorConfig{ConnectorID: tt.connectorID, OrganizationID: tt.orgID, Enabled: true}
			mocks.MockConfigDAO.EXPECT().Get(gomock.Any(), tt.orgID, tt.connectorID).Return(cfg, nil)
			mocks.MockStateDAO.EXPECT().
				GetCursor(gomock.Any(), tt.orgID, tt.connectorID, sdk.OperationID("fetch_prs")).
				Return(nil, nil)
			mocks.MockRunDAO.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			mocks.MockRunDAO.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
			mocks.MockConfigDAO.EXPECT().
				MarkAuthRevoked(gomock.Any(), tt.orgID, tt.connectorID, gomock.Any()).
				Return(tt.markErr == nil, tt.markErr)

			writeFailBefore := prommetrics.ToFloat64(obs.HealthWriteFailuresTotal.WithLabelValues(tt.orgID, string(tt.connectorID), obs.HealthWriteRevoke))
			transitionBefore := prommetrics.ToFloat64(obs.HealthStateTransitionsTotal.WithLabelValues(tt.orgID, string(tt.connectorID), string(sdk.HealthStateAuthRevoked)))

			orch := NewOrchestrator(registry, mocks.MockConfigDAO, mocks.MockStateDAO, mocks.MockRunDAO, sink, noopGatewayFactory)
			require.Error(t, orch.RunOperation(context.Background(), tt.orgID, tt.connectorID, "fetch_prs", sdk.TenantKindOrganization, "test"))

			writeFailAfter := prommetrics.ToFloat64(obs.HealthWriteFailuresTotal.WithLabelValues(tt.orgID, string(tt.connectorID), obs.HealthWriteRevoke))
			transitionAfter := prommetrics.ToFloat64(obs.HealthStateTransitionsTotal.WithLabelValues(tt.orgID, string(tt.connectorID), string(sdk.HealthStateAuthRevoked)))

			assert.Equal(t, tt.wantWriteFailDelta, writeFailAfter-writeFailBefore,
				"connector_health_write_failures_total{direction=revoke} delta")
			assert.Equal(t, tt.wantTransitionDelta, transitionAfter-transitionBefore,
				"connector_health_state_transitions_total{to=auth_revoked} delta")
			// The gate reads the persisted state, so a failed write must leave the
			// loaded config healthy — that is precisely why nothing pauses.
			assert.Equal(t, sdk.HealthState(""), cfg.HealthState,
				"markAuthRevokedIfTerminal must not mutate the in-memory config's health state")
		})
	}
}

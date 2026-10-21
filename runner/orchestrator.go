// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

var (
	syncRunTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_sync_run_total",
			Help: "Total connector sync runs by connector, operation, and outcome.",
		},
		[]string{"org_id", "connector_id", "operation_id", "status"},
	)

	syncRunDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "connector_sync_run_duration_seconds",
			Help:    "Duration of connector sync runs in seconds.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~68min
		},
		[]string{"org_id", "connector_id", "operation_id"},
	)

	syncRunErrorTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "connector_sync_run_error_total",
			Help: "Total connector sync run errors by connector and error kind.",
		},
		[]string{"org_id", "connector_id", "operation_id", "error_kind"},
	)

	// lastSuccessfulSyncTimestampSeconds records the Unix-seconds timestamp of the
	// most recent successful sync per (org, connector, operation). Freshness is
	// read as `time() - connector_last_successful_sync_timestamp_seconds`,
	// which catches tenants that have silently stopped syncing even when fleet-wide
	// error rates look healthy.
	lastSuccessfulSyncTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connector_last_successful_sync_timestamp_seconds",
			Help: "Unix timestamp (seconds) of the most recent successful sync for an (org, connector, operation) triple.",
		},
		[]string{"org_id", "connector_id", "operation_id"},
	)
)

// emptyCursor reports whether c holds no state: no data, or the JSON null an
// empty cursor is stored as.
func emptyCursor(c *sdk.Cursor) bool {
	d := bytes.TrimSpace(c.Data)
	return len(d) == 0 || string(d) == "null"
}

// executeRecovered runs the operation and turns a panic into an internal
// SyncError carrying the panic value, so the run is failed and recorded rather
// than left `running`. The stack goes to the log, not the run row.
func executeRecovered(ctx context.Context, op sdk.APIFetchOperation, params *sdk.ExecutionParams, connectorID sdk.ConnectorID, opID sdk.OperationID, orgID string) (cursor *sdk.Cursor, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error(ctx, "operation panicked", zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			cursor = nil
			err = &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    opID,
				OrganizationID: orgID,
				Kind:           sdk.ErrKindInternal,
				Cause:          fmt.Errorf("operation panicked: %v", r),
			}
		}
	}()
	return op.Execute(ctx, params)
}

// Orchestrator runs one operation for one organization. It takes its sink at
// construction and builds a vendor gateway per run from the organization's
// credentials.
type Orchestrator struct {
	registry       *sdk.ConnectorRegistry
	configDAO      store.IConnectorConfigDAO
	stateDAO       store.ISyncStateDAO
	runDAO         store.ISyncRunDAO
	sink           sdk.DataSink
	gatewayFactory sdk.VendorGatewayFactory

	// Optional collaborators handed to operations on ExecutionParams.
	//
	// budgetLimiters is keyed by connector id. The orchestrator picks the
	// matching limiter per RunOperation call and places it on
	// ExecutionParams.BudgetLimiter, so operations stay unaware of the map.
	blobStore      sdk.BlobStore
	budgetLimiters map[sdk.ConnectorID]sdk.BudgetObserver

	// host is what WithHostServices set. An operation whose RequiresHost
	// names a service missing here fails before it runs.
	host sdk.HostServices

	// logger is what WithLogger set; nil keeps the process logger.
	logger *slog.Logger

	// tx scopes every store call to the run's tenant. Defaults to
	// store.PostgresTxRunner.
	tx store.TxRunner

	// enrichContext, when set, decorates the context each operation runs
	// with (for example to carry host-specific request metadata).
	enrichContext func(ctx context.Context) context.Context

	// Optional fetch-cache collaborators, populated
	// via WithFetchCache. When fetchCacheDAO is nil (WithFetchCache not called),
	// RunOperation leaves ExecutionParams.FetchCache nil — the feature is simply
	// absent for that installation. The max-value-bytes closure resolves the
	// dynamic per-row size cap when the adapter is built.
	fetchCacheDAO             store.IFetchCacheDAO
	fetchCacheMaxValueBytesFn func() int

	// runLease is how long a run's row stays `running` without a heartbeat
	// before a reconciler may close it as abandoned. WithRunLease sets it.
	runLease time.Duration

	// simulated is what WithSimulatedOrgs set: an organization it reports
	// is served by the simulator, so its config is not validated.
	simulated func(orgID string) bool
}

// NewOrchestrator creates a new Orchestrator. memstore.Store satisfies
// configDAO, stateDAO and runDAO, so one store can fill all three.
func NewOrchestrator(
	registry *sdk.ConnectorRegistry,
	configDAO store.IConnectorConfigDAO,
	stateDAO store.ISyncStateDAO,
	runDAO store.ISyncRunDAO,
	sink sdk.DataSink,
	gatewayFactory sdk.VendorGatewayFactory,
) *Orchestrator {
	return &Orchestrator{
		registry:       registry,
		configDAO:      configDAO,
		stateDAO:       stateDAO,
		runDAO:         runDAO,
		sink:           sink,
		gatewayFactory: gatewayFactory,
		tx:             store.PostgresTxRunner{},
		runLease:       DefaultRunLease,
	}
}

// DefaultRunLease is how long a run's row stays `running` after its last
// heartbeat before it counts as abandoned. A run renews its lease every third
// of it while the operation executes.
const DefaultRunLease = 2 * time.Minute

// MinRunLease is the shortest lease WithRunLease sets.
const MinRunLease = time.Second

// WithRunLease sets how long a run's row stays `running` after its last
// heartbeat before RunOperation, for the same operation, or a reconciler
// calling ISyncRunDAO.CloseAbandoned closes it as failed, kind abandoned.
// A non-positive d keeps DefaultRunLease, and a d below MinRunLease is raised
// to it.
func (o *Orchestrator) WithRunLease(d time.Duration) *Orchestrator {
	switch {
	case d <= 0:
	case d < MinRunLease:
		o.runLease = MinRunLease
	default:
		o.runLease = d
	}
	return o
}

// errLeaseLost is the cause the heartbeat cancels a run's context with when
// the run can no longer hold its lease.
var errLeaseLost = errors.New("the run lost its lease: its row was closed as abandoned, or no renewal landed for a whole lease")

// guarded calls f and turns a panic in it into an internal SyncError, so a
// connector's ValidateConfig, an operation factory or a gateway factory that
// panics fails the run instead of the process. The stack goes to the log.
func guarded[T any](ctx context.Context, what string, connectorID sdk.ConnectorID, opID sdk.OperationID, orgID string, f func() (T, error)) (out T, panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error(ctx, what+" panicked", zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			var zero T
			out, panicked = zero, true
			err = &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    opID,
				OrganizationID: orgID,
				Kind:           sdk.ErrKindInternal,
				Cause:          fmt.Errorf("%s panicked: %v", what, r),
			}
		}
	}()
	out, err = f()
	return out, false, err
}

// WithTxRunner replaces the transaction runner. Use store.NoopTxRunner with
// the in-memory store.
func (o *Orchestrator) WithTxRunner(tx store.TxRunner) *Orchestrator {
	if tx != nil {
		o.tx = tx
	}
	return o
}

// WithContextEnricher sets a function applied to the context of every
// operation run, after the orchestrator has stamped the org, connector,
// operation and sync-run ids.
func (o *Orchestrator) WithContextEnricher(f func(ctx context.Context) context.Context) *Orchestrator {
	o.enrichContext = f
	return o
}

// WithBlobStore sets the store the diff, description, repository-tree and
// file-content operations write large bodies to. Without one, those
// operations fail.
func (o *Orchestrator) WithBlobStore(blobStore sdk.BlobStore) *Orchestrator {
	o.blobStore = blobStore
	return o
}

// WithBudgetObservers sets the per-connector vendor rate budgets, keyed by
// connector id; the orchestrator hands each operation the one for its
// connector. The diff and file-content operations fail without theirs, and
// the operations that record observed vendor rate limits skip that step.
func (o *Orchestrator) WithBudgetObservers(budgetLimiters map[sdk.ConnectorID]sdk.BudgetObserver) *Orchestrator {
	o.budgetLimiters = budgetLimiters
	return o
}

// WithLogger sends every entry a run logs with its context, the orchestrator's
// own and the operation's, to l. Without it they go to the process logger,
// slog.Default() unless the command set one.
func (o *Orchestrator) WithLogger(l *slog.Logger) *Orchestrator {
	o.logger = l
	return o
}

// WithHostServices sets the services only an embedding host provides. An
// operation whose OperationDescriptor.RequiresHost names a service that is
// nil in h fails, terminal and config_invalid, before it runs.
func (o *Orchestrator) WithHostServices(h sdk.HostServices) *Orchestrator {
	o.host = h
	return o
}

// WithFetchCache configures the connector fetch-cache collaborators.
// repo is the raw store; the max-value-bytes closure
// resolves the dynamic per-row size cap. These are optional — when WithFetchCache
// is not called, RunOperation leaves ExecutionParams.FetchCache nil, so operations
// that consult the cache simply find it absent.
func (o *Orchestrator) WithFetchCache(
	repo store.IFetchCacheDAO,
	maxValueBytesFn func() int,
) *Orchestrator {
	o.fetchCacheDAO = repo
	o.fetchCacheMaxValueBytesFn = maxValueBytesFn
	return o
}

// WithSimulatedOrgs names the organizations whose traffic goes to the
// simulator. The simulator ignores credentials, so RunOperation does not call
// Connector.ValidateConfig for them; every other organization's config is
// validated before its operation runs.
func (o *Orchestrator) WithSimulatedOrgs(isSimulated func(orgID string) bool) *Orchestrator {
	o.simulated = isSimulated
	return o
}

// RunOperation loads the config, starts a sync-run row, executes the operation
// with the stored cursor, then flushes the sink, saves the returned cursor and
// marks the row completed. A failed step ends the run.
func (o *Orchestrator) RunOperation(
	ctx context.Context,
	orgID string,
	connectorID sdk.ConnectorID,
	opID sdk.OperationID,
	tenantType sdk.TenantKind,
	trigger string,
) error {
	_, err := o.RunOperationWithID(ctx, orgID, connectorID, opID, tenantType, trigger)
	return err
}

// RunOperationWithID is RunOperation that also returns the id of the sync-run
// row this call created, so a caller can read its own run rather than the
// operation's latest. The id is empty when no row was created: the connector
// is paused, or the run failed before its row was written.
func (o *Orchestrator) RunOperationWithID(
	ctx context.Context,
	orgID string,
	connectorID sdk.ConnectorID,
	opID sdk.OperationID,
	tenantType sdk.TenantKind,
	trigger string,
) (runID string, err error) {
	run := &model.SyncRun{}
	err = o.runOperation(ctx, orgID, connectorID, opID, tenantType, trigger, run)
	return run.ID, err
}

func (o *Orchestrator) runOperation(
	ctx context.Context,
	orgID string,
	connectorID sdk.ConnectorID,
	opID sdk.OperationID,
	tenantType sdk.TenantKind,
	trigger string,
	run *model.SyncRun,
) error {
	orgID = orgid.Canonical(orgID)
	ctx = logger.NewContext(ctx, o.logger)
	// Enrich context with operation-scoped fields so that all downstream
	// logger.Info(ctx, ...) calls auto-extract org_id, connector_id, operation_id.
	ctx = reqctx.WithOrgID(ctx, orgID)
	ctx = reqctx.WithConnectorID(ctx, string(connectorID))
	ctx = reqctx.WithOperationID(ctx, string(opID))

	// Pre-create every series the connector health-state machine can touch at 0 for
	// this (org, connector) — both directions, including the two emitted from
	// integrations on reconnect. Prometheus counters only materialize a labeled child
	// on first use; if a real transition's Inc() were the first touch, the series'
	// initial sample would already read 1 with no prior 0 in the window, and
	// an increase() over the series would never see a rise.
	//
	// This sits above the syncPaused gate deliberately: a connector already parked at
	// auth_revoked still reaches here on every scheduled tick, which is what keeps a
	// 0 baseline under the reset-side series for exactly the connectors that can later
	// need a reset.
	metricOrgID := tenantlabel.OrgID(orgID)
	obs.HealthStateTransitionsTotal.WithLabelValues(metricOrgID, string(connectorID), string(sdk.HealthStateAuthRevoked)).Add(0)
	obs.HealthStateTransitionsTotal.WithLabelValues(metricOrgID, string(connectorID), string(sdk.HealthStateHealthy)).Add(0)
	obs.HealthWriteFailuresTotal.WithLabelValues(metricOrgID, string(connectorID), obs.HealthWriteRevoke).Add(0)
	obs.HealthWriteFailuresTotal.WithLabelValues(metricOrgID, string(connectorID), obs.HealthWriteReset).Add(0)

	// Step 1: Look up connector in registry.
	connector, err := o.registry.Get(connectorID)
	if err != nil {
		logger.Error(ctx, "connector not found in registry", zap.String(logger.Trigger, trigger), zap.Error(err))
		return fmt.Errorf("orchestrator: %w", err)
	}

	// Step 2: Resolve operation descriptor.
	opDesc, err := o.registry.GetOperation(connectorID, opID)
	if err != nil {
		logger.Error(ctx, "operation not found in registry", zap.String(logger.Trigger, trigger), zap.Error(err))
		return fmt.Errorf("orchestrator: %w", err)
	}

	// Steps 3-6: Load config, cursor, and create sync run — short-lived transaction
	// so the vendor API call (step 9) doesn't hold a DB connection open.
	var cfg *sdk.ConnectorConfig
	var cursor *sdk.Cursor
	// cursorVersion is the stored version this run read and will save over:
	// SaveCursor refuses a save once another writer has moved it on.
	var cursorVersion atomic.Int64
	var cursorBeforeJSON json.RawMessage
	// leaseStart is taken before the lease is first written, so a lease
	// measured from it never outlives the stored one.
	var leaseStart time.Time
	var gateway sdk.VendorGateway

	if err := o.tx.RunInScope(ctx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		// Step 3: Load connector config.
		var getErr error
		cfg, getErr = o.configDAO.Get(sCtx, orgID, connectorID)
		if getErr != nil {
			return fmt.Errorf("load config: %w", getErr)
		}

		// Step 4: Check enabled flag and the health-state gate.
		if syncPaused(cfg) {
			return nil
		}

		// Step 5: Load last cursor.
		cursor, getErr = o.stateDAO.GetCursor(sCtx, orgID, connectorID, opID)
		if getErr != nil && !store.IsNotFound(getErr) {
			return fmt.Errorf("load cursor: %w", getErr)
		}
		// A not-found cursor means first run — leave cursor nil and proceed.
		// An empty stored cursor is the reset an operation asked for by
		// returning &sdk.Cursor{}: the run starts as a first run too.
		if cursor != nil {
			cursorVersion.Store(cursor.Version)
			if emptyCursor(cursor) {
				cursor = nil
			}
		}

		// Serialize cursor for audit trail.
		if cursor != nil {
			marshaledCursor, marshalErr := json.Marshal(cursor)
			if marshalErr != nil {
				logger.Error(sCtx, "failed to marshal cursor_before for audit trail", zap.String(logger.Trigger, trigger), zap.Error(marshalErr))
				cursorBeforeJSON = json.RawMessage(`{"error":"cursor_before_marshal_failed"}`)
			} else {
				cursorBeforeJSON = marshaledCursor
			}
		}

		// Step 5b: Close this operation's runs whose lease expired: their
		// process stopped (killed, or lost its database) without closing them.
		// Leases are compared on the store's clock, not this process's.
		now := time.Now().UTC()
		if n, closeErr := o.runDAO.CloseAbandoned(sCtx, orgID, connectorID, opID); closeErr != nil {
			return fmt.Errorf("close abandoned runs: %w", closeErr)
		} else if n > 0 {
			logger.WarnWithCtx(sCtx, "closed abandoned runs whose lease expired", zap.Int64("runs", n))
		}

		// Step 6: Create SyncRun record, then start its lease on the store's
		// clock.
		*run = model.SyncRun{
			ID:             uuid.NewString(),
			OrganizationID: orgID,
			ConnectorID:    connectorID,
			OperationID:    opID,
			Status:         model.SyncRunStatusRunning,
			StartedAt:      now,
			CursorBefore:   cursorBeforeJSON,
			Trigger:        trigger,
		}
		if err := o.runDAO.Create(sCtx, run); err != nil {
			return err
		}
		leaseStart = time.Now()
		return o.runDAO.RenewLease(sCtx, orgID, run.ID, o.runLease)
	}); err != nil {
		run.ID = "" // the transaction rolled back: no row was created
		logger.Error(ctx, "pre-execution setup failed", zap.String(logger.Trigger, trigger), zap.Error(err))
		return fmt.Errorf("orchestrator: %w", err)
	}

	// Baselined on the cursor this run loaded, then advanced by each write that lands —
	// a run may persist several times (per-page Checkpoint) before it returns.
	cursorHealth := obs.NewCursorHealthTracker(cursor)

	if syncPaused(cfg) {
		// Clear any stale freshness gauge entries for this connector so an
		// intentionally-disabled or auth_revoked connector does not read as
		// stale.
		// DeletePartialMatch removes every (operation_id) row under this
		// (org_id, connector_id).
		lastSuccessfulSyncTimestampSeconds.DeletePartialMatch(prometheus.Labels{
			"org_id":       metricOrgID,
			"connector_id": string(connectorID),
		})
		// Same reasoning for the cursor-age gauge: a paused connector never reaches a
		// cursor save, so its last value would sit there ageing until the process
		// restarts and report a connector nobody expects to advance as stuck. The
		// regression COUNTER is deliberately left alone — a regression that happened did
		// happen, and a counter read over a trailing window retires it on its own.
		obs.CursorOldestEntryAgeSeconds.DeletePartialMatch(prometheus.Labels{
			"org_id":       metricOrgID,
			"connector_id": string(connectorID),
		})
		logger.Info(ctx, "connector sync skipped",
			zap.String(logger.Trigger, trigger),
			zap.Bool("enabled", cfg.Enabled),
			zap.String("health_state", string(cfg.HealthState)))
		return nil
	}

	// Fail loud on missing host services, before the config is read for
	// anything else: an operation that needs a host's
	// work list or lineage services cannot do anything useful without them,
	// and completing empty would hide that. Terminal because the services are
	// wired at construction; retrying the same task cannot provide them.
	if missing := o.host.Missing(opDesc.RequiresHost); len(missing) > 0 {
		hostErr := sdk.MissingHostServicesError(connectorID, opID, orgID, missing)
		return fmt.Errorf("host services guard: %w", o.failRun(ctx, orgID, run, sdk.ErrKindConfigInvalid.String(), hostErr, 0))
	}

	// Step 4a: Validate the stored config. A config the connector rejects
	// cannot run correctly, and retrying the same task reads the same config,
	// so the failure is terminal config_invalid, not a retryable error.
	if o.simulated == nil || !o.simulated(orgID) {
		_, panicked, vErr := guarded(ctx, "ValidateConfig", connectorID, opID, orgID, func() (struct{}, error) {
			return struct{}{}, connector.ValidateConfig(ctx, cfg)
		})
		if panicked {
			return fmt.Errorf("config guard: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), vErr, 0))
		}
		if vErr != nil {
			cfgErr := &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    opID,
				OrganizationID: orgID,
				Kind:           sdk.ErrKindConfigInvalid,
				Terminal:       true,
				Cause:          fmt.Errorf("validate config: %w", vErr),
			}
			return fmt.Errorf("config guard: %w", o.failRun(ctx, orgID, run, sdk.ErrKindConfigInvalid.String(), cfgErr, 0))
		}
	}

	// Step 4b: Create per-execution VendorGateway from org credentials (no DB needed).
	//
	// Closes the run through failRun like every later failure does. The run row was
	// created back in Step 6, and only its expired lease would close it, so
	// returning bare here would leave it `running` as abandoned and emit no failure
	// metric at all — leaving a connector that cannot build a gateway invisible in
	// every sync metric.
	//
	// config_invalid, not internal: gateway construction does no I/O (vendor config
	// is process-cached at boot; the factory only parses this org's stored
	// credentials), so a failure here means the stored config is bad — absent or
	// wiped credentials, a malformed app id — never a transient blip. That is the
	// definition of a fault no retry fixes, so it belongs in its own kind;
	// under `internal` a failure rate says little about a slow-cadence
	// connector even in total outage. Not auth: this must not drive auth_revoked, because a
	// malformed config is not a dead credential and pausing on it would be wrong.
	gateway, panicked, gwErr := guarded(ctx, "gateway factory", connectorID, opID, orgID, func() (sdk.VendorGateway, error) {
		return o.gatewayFactory(ctx, cfg)
	})
	if panicked {
		return fmt.Errorf("create gateway: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), gwErr, 0))
	}
	if gwErr != nil {
		logger.Error(ctx, "failed to get vendor gateway from factory", zap.String(logger.Trigger, trigger), zap.Error(gwErr))
		return fmt.Errorf("create gateway: %w", o.failRun(ctx, orgID, run, sdk.ErrKindConfigInvalid.String(), gwErr, 0))
	}

	// Step 7: Instantiate operation via factory.
	op, _, factoryErr := guarded(ctx, "operation factory", connectorID, opID, orgID, func() (sdk.Operation, error) {
		return opDesc.Factory(), nil
	})
	if factoryErr != nil {
		return fmt.Errorf("instantiate operation: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), factoryErr, 0))
	}

	// Step 8: Type-assert to APIFetchOperation.
	fetchOp, ok := op.(sdk.APIFetchOperation)
	if !ok {
		return fmt.Errorf("instantiate fetch operation: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), fmt.Errorf(
			"operation %s is %T, expected APIFetchOperation", opID, op), 0))
	}

	// Fail-loud on missing intent: an op that branches on tenant_type cannot run
	// correctly with an unresolved intent. UNSPECIFIED is the scheduler's signal
	// that intent could not be resolved; executing here would silently pick the
	// wrong endpoint, so surface it as a failed run rather than produce wrong data.
	//
	// Terminal:true because the intent is fixed in the payload, not re-derived
	// per attempt — re-running the identical task only re-fails. The scheduler
	// honours Terminal to fail once rather than exhaust MaxRetry.
	if opDesc.RequiresTenantType && tenantType == sdk.TenantKindUnspecified {
		guardErr := &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    opID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindConfigInvalid,
			Terminal:       true,
			Cause: fmt.Errorf(
				"operation %s requires a resolved tenant_type but got TENANT_TYPE_UNSPECIFIED", opID),
		}
		return fmt.Errorf("tenant_type guard: %w", o.failRun(ctx, orgID, run, sdk.ErrKindConfigInvalid.String(), guardErr, 0))
	}

	// Step 9: Execute the operation.
	logger.Info(ctx, "executing operation",
		zap.String(logger.Trigger, trigger),
	)

	// Select the budget limiter matching this connector. A missing entry is
	// tolerated here: the diff and file-content ops fail on a nil limiter, and
	// the others ignore the field or skip what needs it.
	var bl sdk.BudgetObserver
	if o.budgetLimiters != nil {
		if l, ok := o.budgetLimiters[connectorID]; ok && l != nil {
			bl = l
		}
	}

	// Wrap the sink so we can count records emitted during this run and
	// populate connector_sync_runs.records_emitted. Operations remain
	// unaware of the counter — it lives on the wrapper.
	countingSink := newCountingSink(o.sink, orgID)

	// leaseLost is set when the heartbeat stops the run; from then on the
	// sink refuses records and Checkpoint persists nothing. cpPanic holds the
	// first panic a Checkpoint recovered, which fails the run even when the
	// operation swallowed Checkpoint's error.
	var leaseLost atomic.Bool
	var cpPanic atomic.Pointer[string]

	params := &sdk.ExecutionParams{
		Config:        cfg,
		Cursor:        cursor,
		Sink:          countingSink,
		VendorGateway: gateway,
		ConfigUpdater: &configUpdaterAdapter{dao: o.configDAO, tx: o.tx},
		TenantType:    tenantType,
		BlobStore:     o.blobStore,
		BudgetLimiter: bl,
		Host:          o.host,
		Checkpoint: func(cpCtx context.Context, cpCursor *sdk.Cursor) (cpErr error) {
			// A panic while persisting (a bug in the sink or the store) fails this
			// checkpoint instead of escaping the run.
			defer func() {
				if r := recover(); r != nil {
					logger.Error(cpCtx, "checkpoint panicked", zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
					cpErr = fmt.Errorf("checkpoint panicked: %v", r)
					msg := cpErr.Error()
					cpPanic.CompareAndSwap(nil, &msg)
				}
			}()
			if cpCursor == nil {
				return errors.New("checkpoint: nil cursor")
			}
			// Detach from cpCtx so a parent ctx-cancel (asynq deadline fired
			// during a long Execute) does not immediately fail the Flush +
			// SaveCursor pair. The connector is calling us specifically to
			// persist progress; losing that write because the same ctx that
			// triggered the cancel is also wired into the DB call would
			// defeat the purpose. Mirrors failRun's defense.
			persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(cpCtx), 5*time.Second)
			defer cancelPersist()
			if err := countingSink.Flush(persistCtx); err != nil {
				return fmt.Errorf("checkpoint flush: %w", err)
			}
			// A run that lost its lease is stopped by the sink, whose Flush above
			// then refuses with errLeaseLost; this catches a lease lost during it.
			if leaseLost.Load() {
				return errLeaseLost
			}
			cpCursor.UpdatedAt = time.Now().UTC()
			cpCursor.Version = cursorVersion.Load()
			// Wrap: RunInScope returns the callback's error verbatim.
			if err := o.tx.RunInScope(persistCtx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
				return o.stateDAO.SaveCursor(sCtx, orgID, connectorID, opID, cpCursor)
			}); err != nil {
				return fmt.Errorf("checkpoint save cursor: %w", err)
			}
			cursorVersion.Store(cpCursor.Version)
			// A checkpoint persists straight to connector_sync_states, so a cursor that
			// regresses here and then never reaches the post-Execute path (the run
			// crashes or times out mid-pagination) would otherwise go unrecorded —
			// exactly the case the metric exists for. Compared against the last cursor
			// we know landed, not the run's starting one, so a multi-page run does not
			// re-count one regression on every later page.
			cursorHealth.Record(cpCursor, string(connectorID), string(opID), metricOrgID)
			return nil
		},
	}

	// Attach the fetch-cache capability for this run. The nil branch covers a
	// caller/test that never called WithFetchCache. WithFetchCache sets the DAO
	// and the max-value-bytes closure together, so a non-nil DAO here guarantees
	// the closure is non-nil.
	if o.fetchCacheDAO != nil {
		params.FetchCache = &fetchCacheAdapter{
			dao:           o.fetchCacheDAO,
			orgID:         orgID,
			connectorID:   connectorID,
			maxValueBytes: o.fetchCacheMaxValueBytesFn(),
		}
	}

	// Attach org_id to outgoing gRPC metadata, so a host capability that is a
	// gRPC client (a work list, a blame sink) carries the tenant to its server.
	ctx = metadata.AppendToOutgoingContext(ctx, "org_id", orgID)

	// Also place org_id on the in-process context so the vendor-gateway
	// rate-limiter can scope its Redis bucket per org (ADR-0004 §3).
	// Without this the limiter keys collapse to the bare group and one org's
	// burst drains the shared bucket for everyone.
	ctx = reqctx.WithOrgID(ctx, orgID)

	// Attribute every record this run emits to the run — the sink stamps it from
	// ctx, so it must be set before Execute.
	ctx = sdk.WithSyncRunID(ctx, run.ID)
	if o.enrichContext != nil {
		ctx = o.enrichContext(ctx)
	}

	// Step 9: Execute the operation — vendor API calls happen here.
	// No DB transaction is held open during this potentially long call.
	// Record the credential swaps the run makes (an OAuth refresh rotating
	// the refresh token), so a terminal auth failure after one is fenced on
	// the rotated credentials, the ones that failed.
	ctx = store.WithCredentialSwaps(ctx)
	execCtx, cancelExec := context.WithCancelCause(ctx)
	stopRun := func(cause error) {
		leaseLost.Store(true)
		countingSink.stop(cause)
		cancelExec(cause)
	}
	stopHeartbeat := o.heartbeat(ctx, orgID, run.ID, leaseStart, stopRun)
	newCursor, execErr := executeRecovered(execCtx, fetchOp, params, connectorID, opID, orgID)
	stopHeartbeat()
	cancelExec(nil)
	if leaseLost.Load() {
		// The heartbeat stopped the run: its row was closed as abandoned, or
		// no renewal landed for a whole lease. Writing anything further could
		// overwrite another run's work, so the run ends here as abandoned.
		cause := errLeaseLost
		if execErr != nil {
			cause = fmt.Errorf("%w (the operation returned: %w)", errLeaseLost, execErr)
		}
		return fmt.Errorf("execute operation: %w", o.failRun(ctx, orgID, run, sdk.ErrKindAbandoned.String(), cause, countingSink.Count()))
	}

	// Step 10/11: Handle success or failure — short-lived transaction for DB writes.
	completedAt := time.Now().UTC()
	run.CompletedAt = &completedAt

	// A Checkpoint panic is a bug in the sink or the store: the run fails
	// even if the operation ignored Checkpoint's error.
	if msg := cpPanic.Load(); msg != nil {
		panicErr := &sdk.SyncError{ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID, Kind: sdk.ErrKindInternal, Cause: errors.New(*msg)}
		return fmt.Errorf("execute operation: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), panicErr, countingSink.Count()))
	}

	if execErr != nil {
		errKind := sdk.ErrKindInternal.String()
		var syncErr *sdk.SyncError
		if errors.As(execErr, &syncErr) {
			errKind = syncErr.Kind.String()
		}
		// Reclassify when the underlying error is a ctx deadline that already
		// fired before we got here. Distinguishes "the vendor / sink is sad"
		// from "we ran out of task budget downstream of throttling/retries"
		// (ADR-0004 §9). Keeps SQS and sink monitoring quiet during rate-limit
		// storms.
		if classifiedAsCtxExhausted(ctx, execErr) {
			errKind = sdk.ErrKindCtxExhausted.String()
		}
		logger.Error(ctx, "failed to execute operation", zap.String(logger.ErrorKind, errKind), zap.Error(execErr))
		o.markAuthRevokedIfTerminal(ctx, orgID, connectorID, cfg.CredentialsUpdatedAt, syncErr)
		return fmt.Errorf("execute operation: %w", o.failRun(ctx, orgID, run, errKind, execErr, countingSink.Count()))
	}

	_, flushPanicked, flushErr := guarded(ctx, "final sink flush", connectorID, opID, orgID, func() (struct{}, error) {
		return struct{}{}, countingSink.Flush(ctx)
	})
	if flushPanicked {
		return fmt.Errorf("flush sink: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), flushErr, countingSink.Count()))
	}
	if err := flushErr; err != nil {
		errKind := sdk.ErrKindSinkFailure.String()
		if classifiedAsCtxExhausted(ctx, err) {
			errKind = sdk.ErrKindCtxExhausted.String()
		}
		logger.Error(ctx, "failed to flush sink", zap.String(logger.ErrorKind, errKind), zap.Error(err))
		return fmt.Errorf("flush sink: %w", o.failRun(ctx, orgID, run, errKind, err, countingSink.Count()))
	}

	// Post-execution: save cursor + update run in one short transaction.
	//
	// Detached context: when the parent ctx is already canceled (e.g. asynq
	// deadline fired during the long Execute call), the inner closure would
	// fail on its first DB call and return before flipping run.Status to
	// Completed, leaving the row `running` until its lease expires and it is
	// closed as abandoned. Mirrors the same defense in failRun.
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, postPanicked, postErr := guarded(postCtx, "post-execution write", connectorID, opID, orgID, func() (struct{}, error) {
		return struct{}{}, o.tx.RunInScope(postCtx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
			return o.persistFinished(sCtx, orgID, connectorID, opID, run, newCursor, &cursorVersion, completedAt, countingSink.Count(), trigger)
		})
	})
	if postPanicked {
		return fmt.Errorf("persist post-execution state: %w", o.failRun(ctx, orgID, run, sdk.ErrKindInternal.String(), postErr, countingSink.Count()))
	}
	if postErr != nil {
		// Fall through to failRun so the row is marked failed (not stuck at
		// `running`). Classify as ctx_exhausted when the parent ctx was the
		// trigger — matches the existing reclassification for execErr/Flush.
		errKind := sdk.ErrKindInternal.String()
		if classifiedAsCtxExhausted(ctx, postErr) {
			errKind = sdk.ErrKindCtxExhausted.String()
		}
		logger.Error(ctx, "failed to persist post-execution state", zap.String(logger.Trigger, trigger), zap.String(logger.ErrorKind, errKind), zap.Error(postErr))
		return fmt.Errorf("persist post-execution state: %w", o.failRun(ctx, orgID, run, errKind, postErr, countingSink.Count()))
	}

	// After the transaction has COMMITTED, not inside it. SaveCursor succeeding is not
	// the cursor landing: a later failure in the same transaction (the run-row update, or
	// the commit itself) rolls the cursor write back, and recording inside would advance
	// the tracker for a cursor state that never existed — a false regression on the next
	// run's comparison, against a counter whose premise is that any increment is a defect.
	if newCursor != nil {
		cursorHealth.Record(newCursor, string(connectorID), string(opID), metricOrgID)
	}

	duration := completedAt.Sub(run.StartedAt).Seconds()
	syncRunTotal.WithLabelValues(metricOrgID, string(connectorID), string(opID), "completed").Inc()
	syncRunDurationSeconds.WithLabelValues(metricOrgID, string(connectorID), string(opID)).Observe(duration)
	lastSuccessfulSyncTimestampSeconds.WithLabelValues(metricOrgID, string(connectorID), string(opID)).Set(float64(completedAt.Unix()))

	logger.Info(ctx, "operation completed successfully", zap.String(logger.Trigger, trigger))
	return nil
}

// persistFinished saves a finished run's cursor and marks its row completed,
// in the caller's transaction. It first renews the run's lease as a fence: a
// row closed as abandoned answers store.ErrRunClosed before the cursor is
// written, which matters for stores without transactions (memstore, the JSON
// file), where a later failure cannot roll the cursor back.
func (o *Orchestrator) persistFinished(sCtx context.Context, orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID,
	run *model.SyncRun, newCursor *sdk.Cursor, cursorVersion *atomic.Int64, completedAt time.Time, records int64, trigger string,
) error {
	if err := o.runDAO.RenewLease(sCtx, orgID, run.ID, o.runLease); err != nil {
		return fmt.Errorf("fence the run's lease: %w", err)
	}
	if newCursor != nil {
		newCursor.UpdatedAt = completedAt
		// A conflict means another run saved a newer cursor since this one
		// read it: nothing is overwritten, the transaction rolls back, and the
		// run fails as a retryable internal error, so the retry starts from the
		// newer cursor.
		newCursor.Version = cursorVersion.Load()
		if err := o.stateDAO.SaveCursor(sCtx, orgID, connectorID, opID, newCursor); err != nil {
			return fmt.Errorf("save cursor: %w", err)
		}
		cursorAfterJSON, marshalErr := json.Marshal(newCursor)
		if marshalErr != nil {
			logger.Error(sCtx, "failed to marshal cursor_after for audit trail", zap.String(logger.Trigger, trigger), zap.Error(marshalErr))
		} else {
			run.CursorAfter = cursorAfterJSON
		}
	}
	run.Status = model.SyncRunStatusCompleted
	run.RecordsEmitted = records
	return o.runDAO.Update(sCtx, run)
}

// heartbeat renews the run's lease every third of it until the returned stop
// is called, which waits for the renewer to exit. leaseStart is a time taken
// before the current lease was written. A failed renewal is logged and retried
// on the next tick. The run is stopped through stopRun with errLeaseLost when
// its row is no longer running (ErrRunClosed: it was closed as abandoned), or
// at the last tick before its lease could have expired without a renewal
// (each renewal's lease is measured from a time taken before the call), so a
// run never outlives the lease a reconciler sees.
func (o *Orchestrator) heartbeat(ctx context.Context, orgID, runID string, leaseStart time.Time, stopRun func(error)) (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	interval := o.runLease / 3
	go func() {
		defer close(exited)
		t := time.NewTicker(interval)
		defer t.Stop()
		lastStart := leaseStart
		for {
			select {
			case <-done:
				return
			case <-t.C:
				attempt := time.Now()
				hbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				err := o.tx.RunInScope(hbCtx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
					return o.runDAO.RenewLease(sCtx, orgID, runID, o.runLease)
				})
				cancel()
				switch {
				case err == nil:
					lastStart = attempt
				case errors.Is(err, store.ErrRunClosed):
					logger.WarnWithCtx(ctx, "the run's row was closed while it ran; stopping it", zap.String(logger.SyncRunID, runID))
					stopRun(errLeaseLost)
					return
				default:
					logger.WarnWithCtx(ctx, "failed to renew the run's lease", zap.String(logger.SyncRunID, runID), zap.Error(err))
					if !time.Now().Add(interval).Before(lastStart.Add(o.runLease)) {
						stopRun(errLeaseLost)
						return
					}
				}
			}
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

// metricStatusForRun picks the status label for the connector_sync_run_total
// counter. Rate-limit errors are back-pressure: the asynq scheduler honours the
// SyncError.RetryAfterSeconds and re-runs the operation, so labelling them
// "failed" would count expected throttling in the failure ratio (one chatty
// tenant can drag a single connector's failure ratio up). The DB row keeps SyncRunStatusFailed + ErrorKind="rate_limit" so
// the run history surface is unchanged.
//
// A rate-limit rejection from OUR OWN limiter is a different animal and gets its
// own label. A vendor 429 means the vendor is pacing us and the next run proceeds,
// so excluding it from the failure ratios is right. A local rejection means our
// configured budget cannot cover what this operation needs, and it does not
// self-heal: every subsequent run meets the same budget, the position stops
// advancing, and under one shared "deferred" label nothing distinguishes that from
// healthy throttling.
//
// The label is a third value rather than "failed" on purpose. The failure ratios
// match on `failed` and divide by `failed|completed`, so folding it in would count
// every connector that merely paces itself as failing; a separate value leaves
// those ratios untouched and lets a monitor ask the question that actually
// matters — self-throttling with nothing completing.
func metricStatusForRun(errKind string, cause error) string {
	if errKind == sdk.ErrKindRateLimit.String() {
		if errors.Is(cause, sdk.ErrLocalRateLimit) {
			return "self_throttled"
		}
		return "deferred"
	}
	return "failed"
}

// syncPaused reports whether RunOperation should skip this connector's
// scheduled sync entirely — either the connector is disabled, or its
// credential was confirmed dead (see markAuthRevokedIfTerminal) and stays
// gated until a fresh credential write resets it.
func syncPaused(cfg *sdk.ConnectorConfig) bool {
	return !cfg.Enabled || cfg.HealthState == sdk.HealthStateAuthRevoked
}

// markAuthRevokedIfTerminal transitions the connector to auth_revoked when a
// sync run fails with a Terminal auth-kind error — the credential is
// confirmed dead (an in-request refresh was attempted and failed), so future
// scheduled runs are paused via the RunOperation gate until a fresh
// credential write resets the state, instead of repeating the identical
// doomed vendor call every tick. Best-effort: a failure here is logged, not
// propagated — the sync failure itself is still reported via failRun.
//
// credentialsUpdatedAt is the CredentialsUpdatedAt of the ConnectorConfig this
// run loaded at the start of RunOperation, before the (potentially long)
// Execute call. It fences MarkAuthRevoked's write against a credential
// rewrite that landed concurrently — e.g. a user reconnects with a valid
// credential while this run's stale failure is still in flight — so a
// long-tail failure from before the reconnect can never re-revoke a config
// whose credentials have since changed.
func (o *Orchestrator) markAuthRevokedIfTerminal(ctx context.Context, orgID string, connectorID sdk.ConnectorID, credentialsUpdatedAt time.Time, syncErr *sdk.SyncError) {
	if syncErr == nil || syncErr.Kind != sdk.ErrKindAuth || !syncErr.Terminal {
		return
	}
	// The run's own refresh rotated the credentials: the failure is the
	// rotated credentials', so fence on what the refresh wrote. A credential
	// write from outside the run (a reconnect) is not recorded here, and the
	// fence still makes the pause lose to it.
	if fence, ok := store.CredentialSwap(ctx, orgID, connectorID); ok {
		credentialsUpdatedAt = fence
	}

	// Detached context, matching failRun's defense: the parent ctx may already be
	// canceled (e.g. asynq deadline fired during Execute), and this write must
	// land regardless so the connector doesn't keep retrying next tick.
	updateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	var transitioned bool
	err := o.tx.RunInScope(updateCtx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		var markErr error
		transitioned, markErr = o.configDAO.MarkAuthRevoked(sCtx, orgID, connectorID, credentialsUpdatedAt)
		if markErr != nil {
			return fmt.Errorf("mark auth revoked: %w", markErr)
		}
		return nil
	})
	if err != nil {
		// The counter makes this loss visible in the metrics; the log below is its durable record.
		obs.HealthWriteFailuresTotal.WithLabelValues(tenantlabel.OrgID(orgID), string(connectorID), obs.HealthWriteRevoke).Inc()
		logger.Error(ctx, "failed to mark connector auth_revoked", zap.Error(err))
		return
	}
	if transitioned {
		obs.HealthStateTransitionsTotal.WithLabelValues(
			tenantlabel.OrgID(orgID), string(connectorID), string(sdk.HealthStateAuthRevoked),
		).Inc()
		logger.Info(ctx, "connector health state transitioned to auth_revoked")
	}
}

// failRun updates the SyncRun with failure details and returns an error.
// recordsEmitted is the partial emission count up to the point of failure —
// useful for diagnosing "operation ran for a while then died" vs "died at
// startup" without correlating records by sync_run_id.
//
// cause is the original error that triggered the failure. Wrapping it with
// %w preserves any sdk.SyncError underneath (in particular
// RetryAfterSeconds parsed from a vendor 429/503), so asynq's RetryDelayFunc
// can `errors.As` the result and honour the vendor's wait window. Without
// this, the scheduler would always fall back to the default exponential
// backoff and re-hammer the API on the next tick (ADR-0004 §4).
func (o *Orchestrator) failRun(ctx context.Context, orgID string, run *model.SyncRun, errKind string, cause error, recordsEmitted int64) error {
	completedAt := time.Now().UTC()
	run.Status = model.SyncRunStatusFailed
	run.CompletedAt = &completedAt
	run.ErrorKind = errKind
	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}
	run.ErrorMessage = errMsg
	run.RecordsEmitted = recordsEmitted

	metricOrgID := tenantlabel.OrgID(orgID)
	syncRunTotal.WithLabelValues(metricOrgID, string(run.ConnectorID), string(run.OperationID), metricStatusForRun(errKind, cause)).Inc()
	syncRunErrorTotal.WithLabelValues(metricOrgID, string(run.ConnectorID), string(run.OperationID), errKind).Inc()
	if !run.StartedAt.IsZero() {
		syncRunDurationSeconds.WithLabelValues(metricOrgID, string(run.ConnectorID), string(run.OperationID)).Observe(completedAt.Sub(run.StartedAt).Seconds())
	}

	logger.Error(ctx, "sync run failed",
		zap.String(logger.SyncRunID, run.ID),
		zap.String(logger.ErrorKind, errKind),
		zap.String(logger.ErrorMessage, errMsg),
	)

	// Use a detached context so the "mark as failed" DB write succeeds even when
	// the parent context is already expired (e.g. after a deadline exceeded).
	// Without this, sync runs get permanently stuck in "running" status.
	updateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if updateErr := o.tx.RunInScope(updateCtx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		return o.runDAO.Update(sCtx, run)
	}); updateErr != nil {
		logger.Error(ctx, "failed to update sync run to failed",
			zap.String(logger.SyncRunID, run.ID),
			zap.Error(updateErr))
	}

	if cause != nil {
		return fmt.Errorf("orchestrator: operation failed [%s]: %w", errKind, cause)
	}
	return fmt.Errorf("orchestrator: operation failed [%s]", errKind)
}

// configUpdaterAdapter adapts IConnectorConfigDAO to the sdk.ConfigUpdater interface.
type configUpdaterAdapter struct {
	dao store.IConnectorConfigDAO
	tx  store.TxRunner
}

// UpdateSettings writes only the settings column, under PatchSettings' row lock,
// so it keeps credentials, enabled, overrides and a disconnect that land
// around it.
func (a *configUpdaterAdapter) UpdateSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, settings json.RawMessage) error {
	if err := a.tx.RunInScope(ctx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		return a.dao.PatchSettings(sCtx, orgID, connectorID, func(json.RawMessage) (json.RawMessage, error) { return settings, nil })
	}); err != nil {
		return fmt.Errorf("updating connector settings in scope: %w", err)
	}
	return nil
}

// PatchSettings delegates to the row-locked DAO method. Tenant scope is
// established at the adapter boundary so the DAO's `tenantdb.DB(ctx, ...)`
// call inside the transaction sees the right `app.current_tenant` GUC.
func (a *configUpdaterAdapter) PatchSettings(ctx context.Context, orgID string, connectorID sdk.ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error {
	if err := a.tx.RunInScope(ctx, orgID, store.ScopeEnforced, func(sCtx context.Context) error {
		return a.dao.PatchSettings(sCtx, orgID, connectorID, mutator)
	}); err != nil {
		return fmt.Errorf("patching connector settings in scope: %w", err)
	}
	return nil
}

// Collectors returns the package's Prometheus collectors. Nothing registers them
// at import; app.RegisterMetrics registers them with the host's registry.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		syncRunTotal,
		syncRunDurationSeconds,
		syncRunErrorTotal,
		lastSuccessfulSyncTimestampSeconds,
	}
}

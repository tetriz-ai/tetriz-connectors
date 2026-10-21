// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// OperationID uniquely identifies an operation within a connector (e.g., "fetch_prs").
type OperationID string

// IngestionType enumerates supported ingestion methods.
type IngestionType int

const (
	IngestionPull       IngestionType = iota // Polling REST/GraphQL APIs
	IngestionWebhook                         // Inbound HTTP push
	IngestionSSE                             // Server-Sent Events
	IngestionWebSocket                       // WebSocket streams
	IngestionGraphQLSub                      // GraphQL subscriptions
	IngestionBulkImport                      // File/bulk data import
)

var ingestionTypeNames = [...]string{
	IngestionPull:       "pull",
	IngestionWebhook:    "webhook",
	IngestionSSE:        "sse",
	IngestionWebSocket:  "websocket",
	IngestionGraphQLSub: "graphql_sub",
	IngestionBulkImport: "bulk_import",
}

// String returns a human-readable name for the ingestion type.
func (t IngestionType) String() string {
	if t >= 0 && int(t) < len(ingestionTypeNames) {
		return ingestionTypeNames[t]
	}
	return fmt.Sprintf("unknown(%d)", int(t))
}

// OperationDescriptor is the static declaration of an operation.
// It describes WHAT the operation does and HOW it should be scheduled.
type OperationDescriptor struct {
	// ID uniquely identifies this operation within its connector.
	ID OperationID

	// DisplayName is human-readable (e.g., "Fetch Pull Requests").
	DisplayName string

	// Description explains what data this operation fetches.
	Description string

	// DefaultSchedule is the default cron expression for polling operations.
	// Empty for webhook-only or stream-only operations.
	// Can be overridden per-organization via ConnectorConfig.OperationOverrides.
	DefaultSchedule string // cron expression, e.g., "*/15 * * * *"

	// IngestionType declares the primary ingestion method.
	IngestionType IngestionType

	// ExecutionTimeout overrides the scheduler's default per-task deadline for
	// this operation. Zero means use the default.
	//
	// Set it when an operation's work is bounded by a vendor rate limit rather
	// than by its own speed: a bulk reader that must pace itself to stay inside
	// a per-minute budget can need far longer than a default sized for ops that
	// issue a handful of calls. Too short a deadline does not merely slow such
	// an op down — it cancels it mid-run, and an op that skips work on
	// cancellation will not advance past it.
	ExecutionTimeout time.Duration

	// RequiresMultiUser marks operations that fetch whole-account /
	// multi-user data (e.g. GitHub organization(login:) roll-ups), as
	// opposed to repo-scoped data. A scheduler can use it to skip such an
	// op for a single-user (INDIVIDUAL) tenant, for which whole-account
	// data is meaningless; the skip follows the tenant kind, not what
	// the vendor would serve. Default false: a repo-scoped op
	// needs no annotation.
	RequiresMultiUser bool

	// RequiresTenantType marks operations that branch on the tenant intent
	// (tenant_type) at execution — e.g. repo discovery must pick the org vs the
	// individual endpoint. Orthogonal to RequiresMultiUser: that flag decides
	// whether to *skip the whole op* for an individual at schedule-build, this one
	// asserts the op cannot run *correctly* without a resolved intent. When true,
	// the orchestrator fails the run loud if ExecutionParams.TenantType is
	// TENANT_TYPE_UNSPECIFIED instead of executing with an unknown intent. Default
	// false: an op whose behaviour does not vary by intent needs no annotation.
	RequiresTenantType bool

	// StaggerGroup names a set of operations whose DefaultSchedules must hold a fixed
	// spacing relative to each other for every organization, yet still shift together
	// per-org so the fleet does not fire in lockstep. Members share one per-org phase and
	// each sits at its StaggerOffsetMinutes within the repeat period; see
	// cronstagger.GroupEntryFor for the guarantee. All members must share one repeat
	// period (derived from each member's DefaultSchedule), and their offsets must be
	// distinct modulo that period, or two members would collapse onto one minute. Empty
	// (the default) means the op is staggered on its own per-operation phase, independent
	// of any other op.
	StaggerGroup string

	// StaggerOffsetMinutes is this op's fixed offset, in minutes, from its StaggerGroup's
	// shared per-org phase. Ignored when StaggerGroup is empty. Must be non-negative and
	// distinct, modulo the group's repeat period, from every other member's offset.
	StaggerOffsetMinutes int

	// RequiresHost lists the HostServices the operation cannot run without.
	// The orchestrator fails a run, terminal and config_invalid, when one is
	// missing, and the schedule source schedules the operation only for a
	// host that provides all of them. Empty (the default) means the operation
	// needs no host service. Do not list a service the operation can do
	// without, such as TrackedRefs where it falls back to the default branch.
	RequiresHost []HostService

	// Factory creates a new instance of the operation for each execution.
	// Fresh instance per execution prevents state leaks between runs.
	Factory OperationFactory
}

// OperationFactory creates a fresh operation instance.
// Returns the base Operation interface; the orchestrator type-asserts to the
// specific operation type (APIFetchOperation, WebhookOperation, etc.)
// based on OperationDescriptor.IngestionType.
type OperationFactory func() Operation

// Operation is the base interface for all operation types.
// It is intentionally lean — just identification. Specific behavior
// is defined by the typed interfaces below (APIFetchOperation, WebhookOperation, etc.)
type Operation interface {
	// OperationID returns the unique identifier for this operation within its connector.
	OperationID() OperationID
}

// APIFetchOperation is for polling REST/GraphQL APIs (the most common pattern).
// Each invocation of Execute performs one "sync cycle":
//
//	fetch data since last cursor -> emit records to sink -> return new cursor.
type APIFetchOperation interface {
	Operation

	// Execute performs the data fetch.
	//
	// Contract:
	//   - ctx carries cancellation, deadline, and organization-scoped values.
	//   - params contains config, cursor, sink, logger, and vendor gateway.
	//   - Returns the new cursor to persist, and any error. A nil cursor keeps
	//     the stored one. An empty cursor (&Cursor{}) resets it: the next run
	//     receives a nil cursor, as on a first run.
	//
	// The framework handles:
	//   - Passing the stored cursor in params.Cursor (nil on a first run)
	//   - Recording the sync run (start time, end time, records emitted, error)
	//   - Observability (metrics, structured logging)
	//
	// The connector author handles:
	//   - Making API calls (via VendorGateway)
	//   - Pagination within a single sync cycle
	//   - Constructing RawRecord instances and emitting them to the sink
	//   - Interpreting and producing cursors
	//
	// The framework recovers a panic in Execute, and in the code it calls on
	// the operation's goroutine, as a failed run. A panic in a goroutine the
	// operation starts itself still crashes the process: Go cannot recover a
	// panic from another goroutine, so such a goroutine recovers its own.
	Execute(ctx context.Context, params *ExecutionParams) (*Cursor, error)
}

// CheckpointFunc persists intermediate progress during execution. The
// framework implementation flushes buffered sink records, then saves the
// cursor in a separate write, so a later run resumes from the checkpoint
// rather than starting over. It returns an error when either step fails. The
// cursor must be non-nil: a nil cursor is a programming error.
type CheckpointFunc func(ctx context.Context, cursor *Cursor) error

// ConfigUpdater allows operations to persist settings changes back to the
// connector config.
//
// `UpdateSettings` replaces the settings JSON and nothing else: it writes no
// credentials, enabled flag or operation overrides, and it does not revive a
// disconnected connector (it returns a not-found error). Empty settings write
// nothing. It replaces the whole JSON, so two operations that write disjoint
// keys of the same settings (e.g. fetch_prs cache vs fetch_commits cache)
// overwrite each other; use PatchSettings for that.
//
// `PatchSettings` is the race-safe variant: the DAO loads the row under a
// `FOR UPDATE` lock, hands the current bytes to the mutator, and persists
// the result inside the same transaction. Concurrent persist callers
// serialize at the row level and each sees the previous winner's writes.
type ConfigUpdater interface {
	UpdateSettings(ctx context.Context, orgID string, connectorID ConnectorID, settings json.RawMessage) error
	PatchSettings(ctx context.Context, orgID string, connectorID ConnectorID, mutator func(current json.RawMessage) (json.RawMessage, error)) error
}

// FetchCache is best-effort, connector-private memoization across runs. The framework owns the DAO, the (org, connector)
// scoping, the batched conditional upsert, the "updated_at moves only on a real
// change" invariant, and the degrade-on-error + per-row size-guard
// contract — so a connector just picks cacheType namespaces and (de)serializes
// its own value shape into/out of the raw JSON.
//
// Best-effort by contract: the implementation NEVER fails the sync on a store
// error. A GetMany failure degrades to a miss (empty result), a PutMany failure
// is dropped — so a cache-table outage costs a re-fetch, never a broken op.
type FetchCache interface {
	GetMany(ctx context.Context, cacheType string, entryKeys []string) (map[string]json.RawMessage, error)
	PutMany(ctx context.Context, cacheType string, entries map[string]json.RawMessage) error
}

// ExecutionParams bundles everything an operation needs.
type ExecutionParams struct {
	Config        *ConnectorConfig
	Cursor        *Cursor // nil on first run
	Sink          DataSink
	VendorGateway VendorGateway
	Checkpoint    CheckpointFunc // optional; nil when called outside the orchestrator
	ConfigUpdater ConfigUpdater  // optional; nil if config updates not supported
	FetchCache    FetchCache     // optional; nil when caching is unsupported

	// TenantType is the org's resolved tenant intent, threaded from the job
	// payload. A descriptor/runtime value (not persisted), so an op that branches
	// on intent reads it here. TenantKindUnspecified when the scheduler could
	// not resolve intent; an op tagged RequiresTenantType is failed loud before
	// Execute rather than handed an unknown intent.
	TenantType TenantKind

	// BlobStore and BudgetLimiter are optional; nil when the runner does not
	// provide them. The command provides both.
	BlobStore     BlobStore      // large bodies written beside records
	BudgetLimiter BudgetObserver // vendor-scoped header-aware rate budget

	// Host holds the services only an embedding host provides. Every service
	// an operation lists in OperationDescriptor.RequiresHost is set when the
	// orchestrator runs it.
	Host HostServices
}

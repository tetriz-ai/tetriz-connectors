// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package connectorsource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/cronstagger"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// opFetchFileContent is scheduled only for organizations where
// VCSContentEnabledFunc returns true. A nil func schedules it for none.
const opFetchFileContent sdk.OperationID = vendorids.OpFetchFileContent

// VCSContentEnabledFunc reports whether an organization's fetch_file_content
// pull is on. Injected as a closure so the source stays unaware of how a host
// decides.
type VCSContentEnabledFunc func(orgID string) bool

// Connector ids the source needs by name, declared here so this generic source
// does not depend on a concrete connector package. Exported so a host can key its
// RolloutGates without a bare literal.
const (
	ConnectorIDCursor     sdk.ConnectorID = "cursor"
	ConnectorIDClaudeCode sdk.ConnectorID = "claude-code"
	ConnectorIDCodex      sdk.ConnectorID = "codex"
	ConnectorIDKiro       sdk.ConnectorID = "kiro"
)

// RolloutEnabledFunc reports whether a connector's gate is open for an org.
//
// A closed gate stops schedules even for an organisation that already has a config row;
// that is why the gate covers the whole connector.
type RolloutEnabledFunc func(orgID string) bool

// RolloutGates maps a connector to the per-organisation gate on emitting its schedules.
//
// Absence and a nil value mean different things, and the difference is load-bearing. A
// connector ABSENT from the map is not gated and is always emitted. A connector
// PRESENT with a nil func is gated off for every organisation. Collapsing the two would
// turn a forgotten registration into an ungated connector, and that failure direction
// emits rather than withholds — the expensive one.
//
// A nil or empty map therefore gates nothing. runner/app passes nil, so `serve` gates
// no connector.
type RolloutGates map[sdk.ConnectorID]RolloutEnabledFunc

// ConnectorScheduleSource implements schedule.ScheduleSource.
// It reads connector configs and schedule overrides from the DB
// and produces ScheduleDefinition entries for Pull-type operations.
type ConnectorScheduleSource struct {
	configDAO   store.IConnectorConfigDAO
	scheduleDAO store.IScheduleDAO
	registry    *sdk.ConnectorRegistry

	// isVCSContentEnabled gates emitting the fetch_file_content schedule per org.
	// Nil means the feature is off everywhere (the op is never scheduled).
	isVCSContentEnabled VCSContentEnabledFunc

	// rolloutGates gates emitting a connector's schedules per org, for the gated
	// connectors. See RolloutGates for what absence versus a nil value means.
	rolloutGates RolloutGates

	// hostServices are the host services the host provides. An operation
	// whose RequiresHost names one not listed here is never scheduled. See
	// WithHostServices.
	hostServices map[sdk.HostService]struct{}

	// liveIndividualSet returns the current live-individual org-ID set. Read fresh
	// every FetchSchedules (not cached on the config), so a config that flips
	// individual/org takes effect on the next refresh with no backfill. Injected
	// so tests can supply a fixed set; defaults to tenantlabel.LiveIndividualSet.
	liveIndividualSet func() map[string]struct{}

	// tenants is the authoritative tenant_type source for an org absent from
	// the lagging live-individual set (the absence is not proof the org is an
	// organization). Nil is valid — it degrades the miss path to UNSPECIFIED
	// (fail-safe; see resolveOrgTenantType). The why lives on tenantTypeFor.
	tenants sdk.TenantDirectory

	// orgTenantType caches authoritative per-org tenant_type resolutions so the
	// authoritative RPC fires at most once per org per process. An org's
	// tenant_type is immutable (an org never becomes an individual and vice
	// versa), so a process-lifetime cache never serves a stale-in-a-way-that-
	// matters value; this bounds load on the tenant directory even though orgs vastly
	// outnumber individuals and the stamp runs per-due-config per scheduler tick.
	// Values are sdk.TenantKind; only successful resolutions are stored
	// (a failed lookup is never cached, so a transient lookup error is retried).
	orgTenantType sync.Map

	// noDirectoryLogged is reset at the start of each refresh, so a source
	// with no tenant directory logs that once per refresh, not once per org.
	noDirectoryLogged atomic.Bool

	// lastGood holds, per connector and organization, the definitions the
	// last successful read of that connector's configs produced. When a
	// connector's configs cannot be listed, or one organization's row does
	// not decrypt, the affected organizations keep these definitions, so a
	// failed read does not unregister their tasks. Guarded by lastGoodMu.
	lastGoodMu sync.Mutex
	lastGood   map[sdk.ConnectorID]map[string]keptSchedules
}

// NewConnectorScheduleSource creates a new ConnectorScheduleSource. tenants
// is used for the authoritative single-org tenant_type lookup on a live-set
// miss; a nil directory degrades that lookup to UNSPECIFIED (fail-safe — the
// orchestrator's RequiresTenantType guard fails the run loud rather than
// mis-routing).
func NewConnectorScheduleSource(
	configDAO store.IConnectorConfigDAO,
	scheduleDAO store.IScheduleDAO,
	registry *sdk.ConnectorRegistry,
	tenants sdk.TenantDirectory,
	isVCSContentEnabled VCSContentEnabledFunc,
	rolloutGates RolloutGates,
) *ConnectorScheduleSource {
	return &ConnectorScheduleSource{
		configDAO:           configDAO,
		scheduleDAO:         scheduleDAO,
		registry:            registry,
		liveIndividualSet:   tenantlabel.LiveIndividualSet,
		tenants:             tenants,
		isVCSContentEnabled: isVCSContentEnabled,
		rolloutGates:        rolloutGates,
	}
}

// WithHostServices tells the source which host services (sdk.HostServices)
// the host provides; it replaces any earlier call. The source schedules an
// operation only when every service in its RequiresHost is provided, so a
// scheduled run never fires only to fail for a missing service. Without a
// call the host provides none. A `sync` of a withheld operation still runs
// it, and the orchestrator fails it naming what is missing.
func (s *ConnectorScheduleSource) WithHostServices(provided ...sdk.HostService) *ConnectorScheduleSource {
	s.hostServices = make(map[sdk.HostService]struct{}, len(provided))
	for _, svc := range provided {
		s.hostServices[svc] = struct{}{}
	}
	return s
}

// providesHostFor reports whether the host provides every service op
// requires.
func (s *ConnectorScheduleSource) providesHostFor(op sdk.OperationDescriptor) bool {
	for _, svc := range op.RequiresHost {
		if _, ok := s.hostServices[svc]; !ok {
			return false
		}
	}
	return true
}

// keptSchedules is one organization's definitions for one connector from the
// last pass that read them, with the scheduler_cron_entries labels of each,
// so a kept set is counted in the metric like a freshly read one.
type keptSchedules struct {
	defs    []schedule.ScheduleDefinition
	entries []cronEntry
}

// cronEntry is the labels one definition was counted under.
type cronEntry struct {
	staggerMode string
	fromDefault bool
}

// keep appends k's definitions to defs and counts them in rec.
func (k keptSchedules) keep(defs []schedule.ScheduleDefinition, rec *schedule.CronEntriesRecorder, id sdk.ConnectorID) []schedule.ScheduleDefinition {
	for _, e := range k.entries {
		rec.Add(string(id), e.staggerMode, e.fromDefault)
	}
	return append(defs, k.defs...)
}

// Name returns the source name.
func (s *ConnectorScheduleSource) Name() string {
	return "connector"
}

// FetchSchedules loads connector configs and schedule overrides from the DB
// and produces ScheduleDefinition entries for all enabled Pull-type operations.
func (s *ConnectorScheduleSource) FetchSchedules(ctx context.Context) ([]schedule.ScheduleDefinition, error) {
	// Defer Publish so every return, an early error included, republishes this source's
	// series: a failed read counts the last good schedules it keeps, as GetConfigs keeps them.
	cronEntries := schedule.NewCronEntriesRecorder(s.Name())
	defer cronEntries.Publish()

	// Load all schedule overrides (org-level), enabled or not, into a lookup
	// map: a disabled override pauses its operation.
	overrides, err := s.scheduleDAO.ListAll(ctx)
	if err != nil {
		// The provider keeps this source's last good definitions; count them,
		// so the metric shows what stays scheduled.
		s.lastGoodMu.Lock()
		for id, orgs := range s.lastGood {
			for _, k := range orgs {
				_ = k.keep(nil, cronEntries, id)
			}
		}
		s.lastGoodMu.Unlock()
		return nil, fmt.Errorf("load schedule overrides: %w", err)
	}
	overrideMap := make(map[string]*model.ScheduleOverride, len(overrides))
	for _, o := range overrides {
		overrideMap[jobKey(o.OrganizationID, o.ConnectorID, o.OperationID)] = o
	}

	var defs []schedule.ScheduleDefinition

	// Snapshot the live-individual set once per refresh so every config in this
	// build sees a consistent view, and so a single atomic read is reused across
	// all configs rather than re-read per op.
	individualSet := s.liveIndividualSet()
	s.noDirectoryLogged.Store(false)

	s.lastGoodMu.Lock()
	defer s.lastGoodMu.Unlock()
	if s.lastGood == nil {
		s.lastGood = make(map[sdk.ConnectorID]map[string]keptSchedules)
	}

	// Iterate registered connectors x org configs.
	connectors := s.registry.List()
	for _, conn := range connectors {
		previous := s.lastGood[conn.ID()]
		configs, listErr := s.configDAO.ListEnabledByConnector(ctx, conn.ID())
		var undecryptable *store.UndecryptableRowsError
		var kept map[string]keptSchedules
		if errors.As(listErr, &undecryptable) {
			// One organization's row that does not decrypt keeps that
			// organization's last good schedules; the rows that decrypted
			// are scheduled from their current config.
			kept = make(map[string]keptSchedules, len(undecryptable.OrgIDs))
			for _, org := range undecryptable.OrgIDs {
				logger.Error(ctx, "scheduleradapter: a config's credentials do not decrypt; keeping its last good schedules",
					zap.String(logger.ConnectorID, string(conn.ID())), zap.String(logger.OrgID, org),
					zap.Int("kept", len(previous[org].defs)), zap.Error(undecryptable.Err))
				if p, ok := previous[org]; ok {
					kept[org] = p
				}
			}
			listErr = nil
		}
		if listErr != nil {
			// A failed read is not a request to unregister: the connector
			// keeps every organization's last good schedules.
			keptCount := 0
			for _, p := range previous {
				defs = p.keep(defs, cronEntries, conn.ID())
				keptCount += len(p.defs)
			}
			logger.Error(ctx, "scheduleradapter: failed to list configs for connector; keeping its last good schedules",
				zap.String(logger.ConnectorID, string(conn.ID())),
				zap.Int("kept", keptCount),
				zap.Error(listErr),
			)
			continue
		}
		current := make(map[string]keptSchedules, len(configs)+len(kept))
		for org, p := range kept {
			current[org] = p
			defs = p.keep(defs, cronEntries, conn.ID())
		}

		// An operation's DefaultSchedule is the same expression for every org, so resolve how
		// each is staggered ONCE per connector rather than per (org, op). Only the descriptor
		// default is staggered — a connector_schedules row or an OperationOverrides value is an
		// operator's explicit choice and passes through verbatim (resolveSchedule reports which
		// tier the expression came from).
		//
		// A stagger-group member's default is emitted via the group primitive, so its step is
		// resolved here and it is NOT run through cronstagger.Resolve — which would classify the
		// pinned `p-59/15` members as modeNone and log a spurious "cannot be staggered" warning
		// every refresh. Only a group member whose schedule the primitive cannot place falls
		// through to a per-op slot, and that genuine misconfiguration is warned once per fetch.
		opDefaultSlots := make(map[sdk.OperationID]cronstagger.Slot)
		opGroupSteps := make(map[sdk.OperationID]int)
		for _, op := range conn.Operations() {
			if op.IngestionType != sdk.IngestionPull || op.DefaultSchedule == "" {
				continue
			}
			if op.StaggerGroup != "" {
				if step, ok := cronstagger.GroupStaggerStep(op.DefaultSchedule); ok {
					opGroupSteps[op.ID] = step
					continue
				}
				logger.WarnWithCtx(ctx, "scheduleradapter: stagger-group member's default is not group-placeable; falling back to per-op stagger",
					zap.String(logger.ConnectorID, string(conn.ID())),
					zap.String("operation", string(op.ID)),
					zap.String("group", op.StaggerGroup),
					zap.String("schedule", op.DefaultSchedule),
				)
			}
			opDefaultSlots[op.ID] = cronstagger.Resolve(ctx, s.Name(), op.DefaultSchedule)
		}

		for _, cfg := range configs {
			// One resolved kind per config drives the multi-user skip, the queue
			// and the payload stamp, so the three never disagree.
			kind := s.tenantTypeFor(ctx, individualSet, cfg.OrganizationID)
			for _, op := range conn.Operations() {
				if op.IngestionType != sdk.IngestionPull {
					continue
				}
				if !s.providesHostFor(op) {
					continue
				}

				// Skip multi-user ops for INDIVIDUAL tenants by intent (our
				// tenant_type, not what the vendor would serve); see
				// skipForIndividualTenant for the fail-open contract.
				if skipForIndividualTenant(kind, op) {
					continue
				}

				// Gate the VCS content pull (AI coding-agent configuration
				// files): closed, no schedule is emitted for the op.
				if op.ID == opFetchFileContent && !s.vcsContentEnabled(cfg.OrganizationID) {
					recordSuppressedSchedule(individualSet, conn.ID(), cfg.OrganizationID)
					continue
				}

				// Connector-wide, and withholding only — see RolloutEnabledFunc.
				if !s.rolloutAllows(conn.ID(), cfg.OrganizationID) {
					recordSuppressedSchedule(individualSet, conn.ID(), cfg.OrganizationID)
					continue
				}

				key := jobKey(cfg.OrganizationID, conn.ID(), op.ID)
				cronExpr, fromDefault := resolveSchedule(overrideMap, key, cfg, op)
				if cronExpr == "" {
					continue
				}
				// Only the descriptor default is staggered — an override is verbatim. A
				// stagger-group member rotates on the group's shared per-org phase, so the
				// whole group shifts together while its members keep the fixed spacing their
				// offsets encode. Every other op gets its own per-operation phase, keyed on
				// org/connector/operation so an org's several ops on one interval (e.g. many
				// `*/30` ops) do not all collapse onto one minute.
				if fromDefault {
					if step, grouped := opGroupSteps[op.ID]; grouped {
						cronExpr = cronstagger.GroupEntryFor(
							groupStaggerKey(cfg.OrganizationID, conn.ID(), op.StaggerGroup), step, op.StaggerOffsetMinutes)
					} else {
						cronExpr = opDefaultSlots[op.ID].EntryFor(key)
					}
				}

				payload, marshalErr := json.Marshal(ConnectorSyncPayload{
					OrgID:       cfg.OrganizationID,
					ConnectorID: conn.ID(),
					OperationID: op.ID,
					TenantType:  kind,
				})
				if marshalErr != nil {
					logger.Error(ctx, "scheduleradapter: failed to marshal payload",
						zap.String("key", key),
						zap.Error(marshalErr),
					)
					continue
				}

				def := schedule.ScheduleDefinition{
					ID:             key,
					JobType:        schedule.JobTypeConnectorSync,
					CronExpression: cronExpr,
					Payload:        payload,
					Queue:          queueForKind(kind),
					MaxRetry:       3,
					Timeout:        op.ExecutionTimeout,
				}
				defs = append(defs, def)

				// staggerMode is the descriptor default's classification, keyed on opGroupSteps
				// membership (NOT op.StaggerGroup): a group member reports "group", a member that
				// fell back to a per-op slot reports that slot's real mode. It is independent of
				// origin — a grouped op an operator overrode still reports "group" here (the
				// override cron is emitted verbatim, but the label describes the default; see Add).
				staggerMode := opDefaultSlots[op.ID].Mode()
				if _, grouped := opGroupSteps[op.ID]; grouped {
					staggerMode = cronstagger.GroupStaggerMode
				}
				cronEntries.Add(string(conn.ID()), staggerMode, fromDefault)
				k := current[cfg.OrganizationID]
				k.defs = append(k.defs, def)
				k.entries = append(k.entries, cronEntry{staggerMode: staggerMode, fromDefault: fromDefault})
				current[cfg.OrganizationID] = k
			}
		}
		s.lastGood[conn.ID()] = current
	}

	return defs, nil
}

// vcsContentEnabled reports whether the fetch_file_content pull is on for orgID.
// A nil gate means the feature is off everywhere.
func (s *ConnectorScheduleSource) vcsContentEnabled(orgID string) bool {
	return s.isVCSContentEnabled != nil && s.isVCSContentEnabled(orgID)
}

// rolloutAllows reports whether this connector may emit schedules for orgID. A
// connector with no gate registered is not gated, so it always may; a
// registered-but-nil gate withholds every organisation.
func (s *ConnectorScheduleSource) rolloutAllows(id sdk.ConnectorID, orgID string) bool {
	gate, underRollout := s.rolloutGates[id]
	if !underRollout {
		return true
	}
	return gate != nil && gate(orgID)
}

// skipForIndividualTenant reports whether op must be skipped at schedule-build
// because it requires multi-user (whole-account) data and the tenant's
// resolved kind is INDIVIDUAL. Fail-open: an unresolved kind (UNSPECIFIED)
// never skips, so a multi-user op is scheduled rather than silently dropped —
// a wrongly-scheduled op surfaces as a vendor permission error, never as
// missing data for an org.
func skipForIndividualTenant(kind sdk.TenantKind, op sdk.OperationDescriptor) bool {
	return op.RequiresMultiUser && kind == sdk.TenantKindIndividual
}

// queueForKind routes an INDIVIDUAL tenant's tasks to the individual lane and
// every other tenant's, an unresolved one included, to the standard lane.
func queueForKind(kind sdk.TenantKind) string {
	if kind == sdk.TenantKindIndividual {
		return schedule.QueueIndividualConnectorSync
	}
	return schedule.QueueConnectorSync
}

// tenantTypeFor resolves the tenant intent stamped on a job payload. The stamp
// is behavioral — it picks the GitHub fetch_repos listing endpoint downstream —
// so it must not be stale: a cache HIT on the live-individual set is trusted,
// but a MISS is resolved authoritatively rather than assumed to be ORGANIZATION.
//
//   - orgID in the snapshotted live-individual set → INDIVIDUAL (trust the cache,
//     no RPC).
//   - orgID absent (or the set is cold/nil) → authoritative single-org lookup of
//     the tenant directory, cached per-org. The absence
//     of an org from the lagging set is NOT proof it is an organization — a
//     freshly-provisioned individual is absent until the next refresh, so
//     assuming ORGANIZATION there mis-routes its first fetch_repos.
//
// resolveOrgTenantType owns the miss path's caching, normalization, and the
// fail-safe UNSPECIFIED-on-failure contract.
func (s *ConnectorScheduleSource) tenantTypeFor(ctx context.Context, individualSet map[string]struct{}, orgID string) sdk.TenantKind {
	if _, isIndividual := individualSet[orgID]; isIndividual {
		return sdk.TenantKindIndividual
	}
	return s.resolveOrgTenantType(ctx, orgID)
}

// orgTenantTypeRPCTimeout bounds a single tenant directory lookup. FetchSchedules
// resolves every absent org under one shared per-source budget, so an
// unbounded call would let one slow lookup starve the resolution of
// every remaining org in that tick; a tight per-call cap keeps the cost local.
const orgTenantTypeRPCTimeout = 5 * time.Second

// resolveOrgTenantType returns orgID's authoritative tenant_type, serving a
// cached resolution when present and otherwise asking the tenant directory and caching
// the result.
//
// A *successful* RPC is a definitive answer and is cached, including a response
// of UNSPECIFIED: per the TenantDirectory contract an unset tenant_type is
// treated as ORGANIZATION, so it is normalized to ORGANIZATION before caching.
// Caching the raw UNSPECIFIED instead would poison the org: the orchestrator's
// RequiresTenantType guard terminal-fails on UNSPECIFIED, permanently starving
// every unclassified org of its fetch_repos for the process lifetime.
//
// Only a *failure* yields UNSPECIFIED (and caches nothing): a nil client or an RPC
// error. UNSPECIFIED is then the scheduler's "intent unresolved" signal — the
// downstream guard fails the run loud rather than mis-routing, and the next tick
// retries. Safe for concurrent use — the cache is a sync.Map and a benign
// duplicate RPC under a concurrent miss only costs one extra call, never a wrong
// value.
func (s *ConnectorScheduleSource) resolveOrgTenantType(ctx context.Context, orgID string) sdk.TenantKind {
	if cached, ok := s.orgTenantType.Load(orgID); ok {
		// Only resolveOrgTenantType writes this map, always a sdk.TenantKind,
		// so the assertion never fails; on the impossible miss, fall through to a
		// fresh authoritative lookup rather than panic.
		if tt, isTenantType := cached.(sdk.TenantKind); isTenantType {
			return tt
		}
	}
	if s.tenants == nil {
		if s.noDirectoryLogged.CompareAndSwap(false, true) {
			logger.Info(ctx, "scheduleradapter: no tenant directory; organizations outside the live-individual set are stamped UNSPECIFIED this refresh")
		}
		return sdk.TenantKindUnspecified
	}
	rpcCtx, cancel := context.WithTimeout(ctx, orgTenantTypeRPCTimeout)
	defer cancel()
	tenantType, err := lookupTenantType(rpcCtx, s.tenants, orgID)
	if err != nil {
		logger.Error(ctx, "scheduleradapter: authoritative tenant_type lookup failed; stamping UNSPECIFIED",
			zap.String(logger.OrgID, orgID),
			zap.Error(err),
		)
		return sdk.TenantKindUnspecified
	}
	if tenantType == sdk.TenantKindUnspecified {
		// Unset on a successful lookup is an unclassified org (TenantDirectory
		// contract). Treat as ORGANIZATION, never the poison-pill UNSPECIFIED.
		tenantType = sdk.TenantKindOrganization
	}
	s.orgTenantType.Store(orgID, tenantType)
	return tenantType
}

// lookupTenantType asks the directory for orgID's kind and turns a panic in
// the directory into that organization's lookup error, so one organization's
// panic costs only its own schedules.
func lookupTenantType(ctx context.Context, dir sdk.TenantDirectory, orgID string) (kind sdk.TenantKind, err error) {
	defer func() {
		if r := recover(); r != nil {
			kind, err = sdk.TenantKindUnspecified, fmt.Errorf("tenant directory panicked: %v", r)
		}
	}()
	return dir.TenantType(ctx, orgID) //nolint:wrapcheck // the caller logs it with the org id
}

// resolveSchedule determines the effective cron expression for an operation, and reports
// whether that expression is the OperationDescriptor default (the only tier the caller
// per-org staggers — an operator's explicit override is emitted verbatim):
//  1. connector_schedules table override        (fromDefault=false; a disabled row returns "", pausing the operation)
//  2. ConnectorConfig.OperationOverrides         (fromDefault=false)
//  3. OperationDescriptor.DefaultSchedule        (fromDefault=true)
//  4. Empty string (skip)
//
// fromDefault is true only when the returned expression came from op.DefaultSchedule; the
// caller skips an empty expression before staggering, so an empty default never reaches the
// stagger path. An override whose schedule the scheduler's cron parser rejects is logged and
// skipped, so the next tier applies and the operation keeps a schedule.
func resolveSchedule(
	overrides map[string]*model.ScheduleOverride,
	key string,
	cfg *sdk.ConnectorConfig,
	op sdk.OperationDescriptor,
) (cronExpr string, fromDefault bool) {
	// Priority 1: connector_schedules table override. A disabled row pauses
	// the operation.
	if override, ok := overrides[key]; ok {
		if !override.Enabled {
			return "", false
		}
		if validOverrideCron(override.CronExpression, cfg, op) {
			return override.CronExpression, false
		}
	}

	// Priority 2: ConnectorConfig.OperationOverrides
	if raw, ok := cfg.OperationOverrides[op.ID]; ok {
		var opOverride struct {
			Enabled  *bool  `json:"enabled"`
			Schedule string `json:"schedule"`
		}
		if unmarshalErr := json.Unmarshal(raw, &opOverride); unmarshalErr != nil {
			// Malformed per-operation override: skip Priority 2 and fall through to the default schedule.
			logger.Warn("scheduleradapter: failed to unmarshal operation override; using default schedule",
				zap.String("key", key),
				zap.Error(unmarshalErr),
			)
		} else {
			if opOverride.Enabled != nil && !*opOverride.Enabled {
				return "", false
			}
			if opOverride.Schedule != "" && validOverrideCron(opOverride.Schedule, cfg, op) {
				return opOverride.Schedule, false
			}
		}
	}

	// Priority 3: OperationDescriptor default
	return op.DefaultSchedule, true
}

// validOverrideCron reports whether an override's schedule parses with the parser the
// scheduler validates definitions with, logging it when it does not.
func validOverrideCron(expr string, cfg *sdk.ConnectorConfig, op sdk.OperationDescriptor) bool {
	if _, err := cron.ParseStandard(expr); err != nil {
		logger.Warn("scheduleradapter: operation override schedule is not a cron expression; ignoring it",
			zap.String(logger.OrgID, cfg.OrganizationID),
			zap.String(logger.ConnectorID, string(cfg.ConnectorID)),
			zap.String(logger.OperationID, string(op.ID)),
			zap.String("schedule", expr),
			zap.Error(err),
		)
		return false
	}
	return true
}

// jobKey returns a deterministic key for a scheduled operation.
func jobKey(orgID string, connectorID sdk.ConnectorID, opID sdk.OperationID) string {
	return fmt.Sprintf("%s/%s/%s", orgID, connectorID, opID)
}

// groupStaggerKey is the phase key every member of one org's stagger group shares, so the group
// rotates as a unit per org. Deliberately distinct from jobKey (per-operation): sharing the phase
// is what keeps the members spaced by their fixed offsets instead of each hashing to an
// independent minute.
func groupStaggerKey(orgID string, connectorID sdk.ConnectorID, group string) string {
	return fmt.Sprintf("%s/%s/%s", orgID, connectorID, group)
}

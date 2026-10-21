// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package testkit holds what a connector author needs to test a connector
// without a vendor account or a database: an in-memory sink, a scriptable
// fake vendor (package testkit/fakevendor), and RunConformance, the checks
// every connector in this module passes.
//
// The suite checks error handling and checkpoints only against a vendor it
// can make fail: set ConformanceRun.Faults to a FaultInjector. A
// *fakevendor.Server is one, so a connector tested the fakevendor way passes
// its server; for any other test server, wrap its handler with FaultHandler.
// RunConformance documents each check.
package testkit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/internal/cronstagger"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// ConformanceRun asks RunConformance to also run one pull operation through
// the orchestrator, twice, against an in-memory store.
type ConformanceRun struct {
	// Operation is the operation to run.
	Operation sdk.OperationID
	// Config is the organization's connector config; its ConnectorID is
	// set to the connector's id.
	Config sdk.ConnectorConfig
	// Gateway builds the vendor gateway, typically one pointed at a fake
	// vendor.
	Gateway sdk.VendorGatewayFactory
	// TenantKind is passed to the run. Zero means organization.
	TenantKind sdk.TenantKind
	// Resume, when set, checks that the second run resumes from the cursor
	// the first run stored. It receives that cursor and the second run's
	// first vendor call, and returns an error when the call does not carry
	// the cursor's position. Without it the suite cannot tell a connector
	// that ignores its cursor from one that honours it.
	Resume func(stored *sdk.Cursor, first VendorCall) error
	// WantExternalIDs, when set, lists external ids the two runs together
	// must emit at least once, so a connector that drops records fails.
	WantExternalIDs []string
	// AddBetweenRuns, when set, is called after the first run. It adds
	// records to the vendor and returns their external ids, which the second
	// run must emit: a connector whose cursor skips records added since the
	// first run fails.
	AddBetweenRuns func() []string
	// Faults, when set, is the vendor the gateway talks to, made to fail on
	// demand. The suite then runs the error checks: each vendor failure
	// (401, 403, 429, 500, 503, a response slower than the run's deadline)
	// must fail the run with the right error kind. A *fakevendor.Server is
	// one; FaultHandler makes any test server one.
	Faults FaultInjector
	// Checkpoints, with Faults, runs the checkpoint check: the vendor fails
	// every call after the operation's first Checkpoint, and the next run
	// must resume from that checkpoint. The listing must make a vendor call
	// after its first checkpoint (for a paging operation, span at least two
	// pages).
	Checkpoints bool
}

// VendorCall is one call an operation made through its vendor gateway: an
// HTTP request (from Do or DoJSON) or a proto request (from Execute).
type VendorCall struct {
	Request *http.Request
	Message proto.Message
}

// RunConformance checks a connector's declaration, and runs the operations
// in runs. It reports every violation it finds, not only the first.
//
// The declaration checks: an id and a display name; Categories returns a
// fresh slice each call; at least one operation; operation ids unique and
// non-empty; every factory non-nil, returning a fresh operation whose
// OperationID matches its descriptor; every pull operation implements
// sdk.APIFetchOperation; every default schedule parses as a five-field cron
// expression or an @ descriptor; every stagger group has one repeat period
// and distinct offsets; every RequiresHost entry is one of the sdk.Host*
// constants, listed once; and an operation with RequiresHost, run with no
// host services (as `serve` and `sync` run it), is refused with
// sdk.MissingHostServicesError: a failed, terminal config_invalid run that
// names the missing services, before the operation executes.
//
// Each run: the orchestrator completes the run twice, the second from the
// cursor the first stored; every emitted record names this connector, the
// operation and the organization, and carries an external id and a fetch
// time; the stored cursor is valid JSON; with Resume set, the second run's
// first vendor call resumes from that cursor; with WantExternalIDs set, every
// listed id is emitted at least once; with AddBetweenRuns set, the second run
// emits every record added after the first.
//
// With Faults set, each vendor failure fails a fresh run, recorded with the
// error kind the scheduler acts on:
//
//	401, 403            auth, Terminal (the config is paused as auth_revoked)
//	429, Retry-After: 7 rate_limit, RetryAfterSeconds 7
//	500, 503            vendor_error, Retryable and not Terminal
//	no answer before    ctx_exhausted: the run's context error reaches
//	the deadline        the orchestrator (wrapped with %w, not swallowed)
//
// With Checkpoints set as well, the vendor fails every call after the
// operation's first Checkpoint; that run must fail, and the next must
// complete from the checkpointed cursor (checked with Resume, when set). No
// record emitted before the checkpoint may be emitted again, except one at
// the cursor boundary: a record whose DataDate is the latest DataDate emitted
// before the checkpoint, which an inclusive cursor reads again (delivery is
// at least once at the boundary). With WantExternalIDs set, every listed id
// is emitted before the checkpoint or by the resumed run.
//
// # Making the vendor fail
//
// The error and checkpoint checks need a vendor that fails on demand: a
// FaultInjector. A *fakevendor.Server is one, so a connector built on the
// fakevendor pattern passes its server as Faults. For any other
// httptest.Server, wrap its handler with FaultHandler and pass that:
//
//	faults := testkit.FaultHandler(myVendorHandler)
//	srv := httptest.NewServer(faults)
//	testkit.RunConformance(t, conn, testkit.ConformanceRun{..., Faults: faults})
//
// The fault reaches the connector as the vendor's HTTP response, through the
// connector's own gateway, so the check covers the connector's error
// mapping and its gateway's. A gateway that retries a failure retries into
// the same fault, which lasts until the check ends.
func RunConformance(t *testing.T, conn sdk.Connector, runs ...ConformanceRun) {
	t.Helper()
	runConformance(tReporter{t}, conn, runs...)
}

// reporter is the part of *testing.T the checks use, so the suite's own
// tests can record failures instead of failing.
type reporter interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Run(name string, f func(reporter))
}

type tReporter struct{ *testing.T }

func (r tReporter) Run(name string, f func(reporter)) {
	r.T.Run(name, func(t *testing.T) { f(tReporter{t}) })
}

func runConformance(t reporter, conn sdk.Connector, runs ...ConformanceRun) {
	t.Helper()
	checkDeclaration(t, conn)
	checkHostRefusal(t, conn, sdk.HostServices{})
	for _, r := range runs {
		if nilFactory(conn, r.Operation) {
			// Already reported by the declaration check; running it would
			// only panic inside the orchestrator and hide that report.
			t.Errorf("%s/%s: cannot run an operation with a nil factory", conn.ID(), r.Operation)
			continue
		}
		t.Run(fmt.Sprintf("run/%s", r.Operation), func(t reporter) { checkRun(t, conn, r) })
		if r.Faults != nil {
			checkFaults(t, conn, r)
			if r.Checkpoints {
				t.Run(fmt.Sprintf("checkpoint/%s", r.Operation), func(t reporter) { checkCheckpoint(t, conn, r) })
			}
		} else if r.Checkpoints {
			t.Errorf("%s/%s: Checkpoints needs Faults, to make the vendor fail after the checkpoint", conn.ID(), r.Operation)
		}
	}
}

func nilFactory(conn sdk.Connector, op sdk.OperationID) bool {
	for _, d := range conn.Operations() {
		if d.ID == op && d.Factory == nil {
			return true
		}
	}
	return false
}

func checkDeclaration(t reporter, conn sdk.Connector) {
	t.Helper()
	if conn.ID() == "" {
		t.Errorf("connector id is empty")
	}
	if conn.DisplayName() == "" {
		t.Errorf("%s: display name is empty", conn.ID())
	}
	if a := conn.Categories(); len(a) > 0 {
		a[0] = "mutated-by-conformance"
		if b := conn.Categories(); len(b) > 0 && b[0] == "mutated-by-conformance" {
			t.Errorf("%s: Categories returns a shared slice; return a new one each call", conn.ID())
		}
	}
	ops := conn.Operations()
	if len(ops) == 0 {
		t.Errorf("%s: no operations", conn.ID())
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	seen := map[sdk.OperationID]bool{}
	groups := map[string][]sdk.OperationDescriptor{}
	for _, d := range ops {
		if d.ID == "" {
			t.Errorf("%s: an operation has an empty id", conn.ID())
			continue
		}
		if seen[d.ID] {
			t.Errorf("%s: operation id %q is declared twice; the registry would return only the first", conn.ID(), d.ID)
		}
		seen[d.ID] = true
		checkRequiresHost(t, conn.ID(), d)
		if d.Factory == nil {
			t.Errorf("%s/%s: nil factory", conn.ID(), d.ID)
			continue
		}
		a, b := d.Factory(), d.Factory()
		if a == nil {
			t.Errorf("%s/%s: factory returned nil", conn.ID(), d.ID)
			continue
		}
		if got := a.OperationID(); got != d.ID {
			t.Errorf("%s/%s: factory's operation reports id %q", conn.ID(), d.ID, got)
		}
		// Distinct zero-size values may share an address, so only a pointer to
		// a type with state can be told apart.
		if va := reflect.ValueOf(a); va.Kind() == reflect.Pointer && va.Type().Elem().Size() > 0 && va.Pointer() == reflect.ValueOf(b).Pointer() {
			t.Errorf("%s/%s: factory returns the same instance twice; runs would share state", conn.ID(), d.ID)
		}
		if d.IngestionType == sdk.IngestionPull {
			if _, ok := a.(sdk.APIFetchOperation); !ok {
				t.Errorf("%s/%s: pull operation %T does not implement sdk.APIFetchOperation", conn.ID(), d.ID, a)
			}
		}
		if d.DefaultSchedule != "" {
			if _, err := parser.Parse(d.DefaultSchedule); err != nil {
				t.Errorf("%s/%s: default schedule %q: %v", conn.ID(), d.ID, d.DefaultSchedule, err)
			}
		}
		if d.StaggerGroup != "" {
			groups[d.StaggerGroup] = append(groups[d.StaggerGroup], d)
		}
	}
	for g, members := range groups {
		step, offsets := -1, map[int]sdk.OperationID{}
		for _, m := range members {
			s, ok := cronstagger.GroupStaggerStep(m.DefaultSchedule)
			if !ok {
				t.Errorf("%s: stagger group %q member %s has schedule %q with no repeat period", conn.ID(), g, m.ID, m.DefaultSchedule)
				continue
			}
			if step == -1 {
				step = s
			} else if s != step {
				t.Errorf("%s: stagger group %q mixes repeat periods %d and %d", conn.ID(), g, step, s)
			}
			if m.StaggerOffsetMinutes < 0 {
				t.Errorf("%s: stagger group %q member %s has a negative offset", conn.ID(), g, m.ID)
			}
			if step > 0 {
				o := m.StaggerOffsetMinutes % step
				if other, dup := offsets[o]; dup {
					t.Errorf("%s: stagger group %q members %s and %s share offset %d", conn.ID(), g, other, m.ID, o)
				}
				offsets[o] = m.ID
			}
		}
	}
	reg := sdk.NewConnectorRegistry()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%s: registering into an empty registry panicked: %v", conn.ID(), r)
			}
		}()
		reg.Register(conn)
	}()
}

func checkRun(t reporter, conn sdk.Connector, r ConformanceRun) {
	t.Helper()
	ctx := context.Background()
	h := newHarness(t, conn, r.Operation, r.Config, r.Gateway, r.TenantKind)
	var afterFirst *sdk.Cursor
	var added []string
	emittedByFirst := 0
	for i := 1; i <= 2; i++ {
		if i == 2 {
			afterFirst = h.cursor()
			h.calls.reset()
			emittedByFirst = len(h.sink.Records())
			if r.AddBetweenRuns != nil {
				added = r.AddBetweenRuns()
			}
		}
		h.mustComplete(ctx, i)
	}
	for i, rec := range h.sink.Records() {
		if rec == nil {
			t.Errorf("record %d is nil", i)
			continue
		}
		if rec.ConnectorID != string(conn.ID()) || rec.OperationID != string(r.Operation) || rec.OrganizationID != h.cfg.OrganizationID {
			t.Errorf("record %d names %s/%s/%s, want %s/%s/%s", i, rec.ConnectorID, rec.OperationID, rec.OrganizationID, conn.ID(), r.Operation, h.cfg.OrganizationID)
		}
		if rec.ExternalID == "" {
			t.Errorf("record %d has no external id; downstream cannot deduplicate it", i)
		}
		if rec.FetchedAt.IsZero() || rec.FetchedAt.After(time.Now().Add(time.Minute)) {
			t.Errorf("record %d has fetch time %v", i, rec.FetchedAt)
		}
	}
	if c := h.cursor(); c != nil && len(c.Data) > 0 && !json.Valid(c.Data) {
		t.Errorf("stored cursor is not valid JSON: %s", c.Data)
	}
	if r.Resume != nil {
		switch first, ok := h.calls.first(); {
		case afterFirst == nil:
			t.Errorf("the first run stored no cursor, so the second run cannot resume")
		case !ok:
			t.Errorf("the second run made no vendor call")
		default:
			if err := r.Resume(afterFirst, first); err != nil {
				t.Errorf("the second run does not resume from the stored cursor: %v", err)
			}
		}
	}
	if len(added) > 0 {
		second := externalIDs(h.sink.Records()[emittedByFirst:])
		for _, id := range added {
			if !second[id] {
				t.Errorf("external id %q, added between the runs, was not emitted by the second run", id)
			}
		}
	}
	if len(r.WantExternalIDs) > 0 {
		got := externalIDs(h.sink.Records())
		for _, id := range r.WantExternalIDs {
			if !got[id] {
				t.Errorf("external id %q was never emitted", id)
			}
		}
	}
}

// externalIDs is the set of external ids in recs.
func externalIDs(recs []*sdk.RawRecord) map[string]bool {
	out := map[string]bool{}
	for _, rec := range recs {
		if rec != nil {
			out[rec.ExternalID] = true
		}
	}
	return out
}

// conformanceOrg is the organization the suite runs as when the config
// names none.
const conformanceOrg = "00000000-0000-4000-8000-00000000c0f0"

// harness is one connector config in a fresh in-memory store, run by an
// orchestrator with no host services. Each check that changes state the
// next would see (a failed run, an auth_revoked config) gets its own.
type harness struct {
	t     reporter
	conn  sdk.Connector
	op    sdk.OperationID
	kind  sdk.TenantKind
	cfg   sdk.ConnectorConfig
	mem   *memstore.Store
	sink  *ChannelSink
	calls *callLog
	orch  *runner.Orchestrator
}

func newHarness(t reporter, conn sdk.Connector, op sdk.OperationID, cfg sdk.ConnectorConfig, gw sdk.VendorGatewayFactory, kind sdk.TenantKind) *harness {
	t.Helper()
	h := &harness{t: t, conn: conn, op: op, kind: kind, cfg: cfg, mem: memstore.New(), sink: NewChannelSink(), calls: &callLog{}}
	h.cfg.ConnectorID = conn.ID()
	if h.cfg.OrganizationID == "" {
		h.cfg.OrganizationID = conformanceOrg
	}
	h.cfg.Enabled = true
	if h.kind == sdk.TenantKindUnspecified {
		h.kind = sdk.TenantKindOrganization
	}
	if err := h.mem.Save(context.Background(), &h.cfg); err != nil {
		t.Fatalf("store config: %v", err)
	}
	reg := sdk.NewConnectorRegistry()
	reg.Register(conn)
	gateway := func(ctx context.Context, c *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
		g, err := gw(ctx, c)
		if err != nil || g == nil {
			return g, err
		}
		return recordingGateway{inner: g, log: h.calls}, nil
	}
	h.orch = runner.NewOrchestrator(reg, h.mem, h.mem, h.mem, h.sink, gateway).WithTxRunner(store.NoopTxRunner{})
	return h
}

// run runs the operation once and returns its run row and RunOperation's
// error. A missing run row is fatal.
func (h *harness) run(ctx context.Context, i int) (*model.SyncRun, error) {
	h.t.Helper()
	err := h.orch.RunOperation(ctx, h.cfg.OrganizationID, h.conn.ID(), h.op, h.kind, "conformance")
	row, getErr := h.mem.GetLatest(context.Background(), h.cfg.OrganizationID, h.conn.ID(), h.op)
	if getErr != nil {
		h.t.Fatalf("run %d: no run row (%v): %v", i, err, getErr)
	}
	return row, err
}

// mustComplete runs the operation once and fails unless the run completes.
func (h *harness) mustComplete(ctx context.Context, i int) {
	h.t.Helper()
	row, err := h.run(ctx, i)
	if err != nil {
		h.t.Fatalf("run %d: %v", i, err)
	}
	if row.Status != model.SyncRunStatusCompleted {
		h.t.Fatalf("run %d: status %q (%s: %s)", i, row.Status, row.ErrorKind, row.ErrorMessage)
	}
}

// cursor is the stored cursor, or nil.
func (h *harness) cursor() *sdk.Cursor {
	c, err := h.mem.GetCursor(context.Background(), h.cfg.OrganizationID, h.conn.ID(), h.op)
	if err != nil {
		return nil
	}
	return c
}

// callLog records vendor calls in order.
type callLog struct {
	mu    sync.Mutex
	calls []VendorCall
}

func (l *callLog) add(c VendorCall) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, c)
}

func (l *callLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = nil
}

func (l *callLog) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

func (l *callLog) first() (VendorCall, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.calls) == 0 {
		return VendorCall{}, false
	}
	return l.calls[0], true
}

// recordingGateway passes every call to inner and records it.
type recordingGateway struct {
	inner sdk.VendorGateway
	log   *callLog
}

func (g recordingGateway) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	g.log.add(VendorCall{Request: req})
	return g.inner.Do(ctx, req) //nolint:wrapcheck // a transparent wrapper
}

func (g recordingGateway) DoJSON(ctx context.Context, method, url string, reqBody, respBody any) error {
	req, err := http.NewRequestWithContext(ctx, method, url, http.NoBody)
	if err == nil {
		g.log.add(VendorCall{Request: req})
	}
	return g.inner.DoJSON(ctx, method, url, reqBody, respBody) //nolint:wrapcheck // a transparent wrapper
}

func (g recordingGateway) WithRateLimits(c []sdk.RateLimitConfig) sdk.VendorGateway {
	return recordingGateway{inner: g.inner.WithRateLimits(c), log: g.log}
}

func (g recordingGateway) ForGroup(group string) sdk.VendorGateway {
	return recordingGateway{inner: g.inner.ForGroup(group), log: g.log}
}

func (g recordingGateway) Execute(ctx context.Context, req proto.Message) (proto.Message, error) {
	g.log.add(VendorCall{Message: req})
	return g.inner.Execute(ctx, req) //nolint:wrapcheck // a transparent wrapper
}

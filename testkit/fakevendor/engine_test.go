// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakevendor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/store"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// stores is one backing store under test: the in-memory store, and the
// Postgres store when a test database is reachable.
type stores struct {
	configs store.IConnectorConfigDAO
	states  store.ISyncStateDAO
	runs    store.ISyncRunDAO
	tx      store.TxRunner
}

var pgStores func(t *testing.T) stores

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err == nil {
		db, _, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
		if err != nil {
			fmt.Fprintf(os.Stderr, "engine tests: postgres setup: %v\n", err)
			os.Exit(1)
		}
		pgStores = func(*testing.T) stores {
			return stores{store.NewConnectorConfigRepo(db, nil), store.NewSyncStateRepo(db), store.NewSyncRunRepo(db), store.PostgresTxRunner{}}
		}
		code := testguard.Run(m)
		cleanup()
		os.Exit(code)
	}
	testdb.SkipOrFail("engine (postgres half)", errors.New("no test database"))
	os.Exit(testguard.Run(m))
}

// eachStore runs fn once per available store.
func eachStore(t *testing.T, fn func(t *testing.T, s stores)) {
	t.Run("memory", func(t *testing.T) {
		mem := memstore.New()
		fn(t, stores{mem, mem, mem, store.NoopTxRunner{}})
	})
	if pgStores != nil {
		t.Run("postgres", func(t *testing.T) { fn(t, pgStores(t)) })
	}
}

var orgSeq int

// newOrg returns a fresh organization id, so tests sharing a Postgres
// database do not see each other's rows.
func newOrg() string {
	orgSeq++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", orgSeq)
}

func items(n int, start time.Time) []fakevendor.Item {
	out := make([]fakevendor.Item, n)
	for i := range out {
		out[i] = fakevendor.Item{ID: fmt.Sprintf("item-%02d", i+1), Title: "t", UpdatedAt: start.Add(time.Duration(i) * time.Minute)}
	}
	return out
}

type harness struct {
	t    *testing.T
	s    stores
	org  string
	srv  *fakevendor.Server
	sink *testkit.ChannelSink
	orch *runner.Orchestrator
}

func newHarness(t *testing.T, s stores, srv *fakevendor.Server) *harness {
	t.Helper()
	h := &harness{t: t, s: s, org: newOrg(), srv: srv, sink: testkit.NewChannelSink()}
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: srv.URL, PageSize: 2})
	creds, _ := json.Marshal(fakevendor.Credentials{Token: "fixture-token"})
	cfg := &sdk.ConnectorConfig{OrganizationID: h.org, ConnectorID: fakevendor.ConnectorID, Settings: settings, Credentials: creds, Enabled: true}
	require.NoError(t, s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error { return s.configs.Save(ctx, cfg) }))
	reg := sdk.NewConnectorRegistry()
	reg.Register(fakevendor.Connector{})
	h.orch = runner.NewOrchestrator(reg, s.configs, s.states, s.runs, h.sink, fakevendor.GatewayFactory(nil)).WithTxRunner(s.tx)
	return h
}

func (h *harness) run(ctx context.Context) error {
	return h.orch.RunOperation(ctx, h.org, fakevendor.ConnectorID, fakevendor.OpFetchItems, sdk.TenantKindOrganization, "test")
}

func (h *harness) latest() *model.SyncRun {
	var r *model.SyncRun
	require.NoError(h.t, h.s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error {
		var err error
		r, err = h.s.runs.GetLatest(ctx, h.org, fakevendor.ConnectorID, fakevendor.OpFetchItems)
		return err
	}))
	return r
}

type cursorView struct {
	Since     time.Time `json:"since"`
	PageToken string    `json:"page_token"`
	HighWater time.Time `json:"high_water"`
}

func (h *harness) cursor() *cursorView {
	var c *sdk.Cursor
	require.NoError(h.t, h.s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error {
		got, err := h.s.states.GetCursor(ctx, h.org, fakevendor.ConnectorID, fakevendor.OpFetchItems)
		if store.IsNotFound(err) {
			return nil
		}
		c = got
		return err
	}))
	if c == nil {
		return nil
	}
	var v cursorView
	require.NoError(h.t, json.Unmarshal(c.Data, &v))
	return &v
}

func (h *harness) keys() []string {
	var out []string
	for _, r := range h.sink.Records() {
		out = append(out, r.ExternalID)
	}
	return out
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Pagination and checkpointing: a run pages through every item, and a later
// run resumes from the stored high-water mark instead of starting over.
func TestEngine_PaginatesAndResumes(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(5, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		require.NoError(t, h.run(context.Background()))
		assert.Equal(t, []string{"item-01", "item-02", "item-03", "item-04", "item-05"}, h.keys())
		assert.Equal(t, 3, srv.Calls(), "five items at two per page is three pages")
		assert.Equal(t, int64(5), h.latest().RecordsEmitted)
		c := h.cursor()
		require.NotNil(t, c)
		assert.True(t, c.HighWater.Equal(t0.Add(4*time.Minute)))
		assert.Empty(t, c.PageToken)

		srv.SetItems(append(items(5, t0), fakevendor.Item{ID: "item-06", UpdatedAt: t0.Add(10 * time.Minute)})...)
		require.NoError(t, h.run(context.Background()))
		// The resumed listing starts after the last item emitted, so only the
		// new item comes back.
		assert.Equal(t, []string{"item-06"}, h.keys()[5:])
	})
}

// A vendor 429 fails the run as a rate limit whose Retry-After the scheduler
// reads through RetryAfterHinter; the cursor stays at the last checkpoint.
func TestEngine_RateLimitCarriesRetryAfter(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(5, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(2, fakevendor.Fault{Status: 429, RetryAfter: 120, Body: "slow down"})
		err := h.run(context.Background())
		require.Error(t, err)
		var hint schedule.RetryAfterHinter
		require.True(t, errors.As(err, &hint), "the run error must expose the vendor's wait")
		assert.Equal(t, 120*time.Second, hint.RetryAfter())
		r := h.latest()
		assert.Equal(t, model.SyncRunStatusFailed, r.Status)
		assert.Equal(t, "rate_limit", r.ErrorKind)
		c := h.cursor()
		require.NotNil(t, c, "the first page's checkpoint survives the failure")
		assert.Equal(t, t0.Add(time.Minute).Format(time.RFC3339Nano)+"|item-02", c.PageToken, "the checkpoint holds the keyset of the last item served")

		// Retry after the wait: the run resumes at the checkpointed page.
		require.NoError(t, h.run(context.Background()))
		assert.Equal(t, []string{"item-01", "item-02", "item-03", "item-04", "item-05"}, h.keys())
	})
}

// A 5xx is retryable and fails the run as a vendor error.
func TestEngine_ServerErrorIsRetryable(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(3, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(1, fakevendor.Fault{Status: 502, Body: "bad gateway"})
		err := h.run(context.Background())
		require.Error(t, err)
		var se *sdk.SyncError
		require.True(t, errors.As(err, &se))
		assert.False(t, se.Terminal)
		assert.Equal(t, "vendor_error", h.latest().ErrorKind)
		require.NoError(t, h.run(context.Background()))
		assert.Len(t, h.keys(), 3)
	})
}

// A 401 is a terminal auth failure: the connector moves to auth_revoked and
// the next scheduled run does nothing until a credential write resets it.
func TestEngine_DeadCredentialPausesConnector(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(3, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(1, fakevendor.Fault{Status: 401, Body: "bad token"})
		err := h.run(context.Background())
		var se *sdk.SyncError
		require.True(t, errors.As(err, &se))
		assert.True(t, se.Terminal)
		var cfg *sdk.ConnectorConfig
		require.NoError(t, s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error {
			var e error
			cfg, e = s.configs.Get(ctx, h.org, fakevendor.ConnectorID)
			return e
		}))
		assert.Equal(t, sdk.HealthStateAuthRevoked, cfg.HealthState)

		calls := srv.Calls()
		require.NoError(t, h.run(context.Background()), "a paused connector's run returns nil")
		assert.Equal(t, calls, srv.Calls(), "a paused connector makes no vendor call")
	})
}

// A page whose items lost their identity fails the run and does not advance
// the cursor: nothing downstream could deduplicate those records.
func TestEngine_ShapeChangeFailsWithoutAdvancing(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(2, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(1, fakevendor.Fault{DropID: true})
		require.Error(t, h.run(context.Background()))
		assert.Empty(t, h.keys())
		assert.Nil(t, h.cursor())
	})
}

// A page token that does not advance is an error, not an endless loop.
func TestEngine_StuckPageTokenFails(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(5, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(2, fakevendor.Fault{StuckToken: true})
		srv.FailCall(3, fakevendor.Fault{StuckToken: true})
		err := h.run(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "did not advance")
	})
}

// A duplicate inside one page is emitted twice with the same external id;
// the sink does not deduplicate, the downstream key does.
func TestEngine_DuplicatesKeepOneKey(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(2, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(1, fakevendor.Fault{Duplicate: true})
		require.NoError(t, h.run(context.Background()))
		assert.Equal(t, []string{"item-01", "item-02", "item-01"}, h.keys())
	})
}

// A deadline mid-listing fails the run as ctx_exhausted; the checkpoint the
// run reached stays, and the next run picks up from it.
func TestEngine_DeadlineKeepsCheckpoint(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(5, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		srv.FailCall(2, fakevendor.Fault{Delay: 2 * time.Second})
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		require.Error(t, h.run(ctx))
		assert.Equal(t, "ctx_exhausted", h.latest().ErrorKind)
		c := h.cursor()
		require.NotNil(t, c)
		assert.Equal(t, t0.Add(time.Minute).Format(time.RFC3339Nano)+"|item-02", c.PageToken, "the checkpoint holds the keyset of the last item served")
		require.NoError(t, h.run(context.Background()))
		assert.Equal(t, []string{"item-01", "item-02", "item-03", "item-04", "item-05"}, h.keys())
	})
}

// Replaying from a reset cursor re-emits every record under the same
// external ids: delivery is at least once, and the key is what makes it
// idempotent downstream.
func TestEngine_ReplayIsIdempotentByKey(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(4, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		require.NoError(t, h.run(context.Background()))
		first := h.keys()
		require.NoError(t, s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error {
			return s.states.DeleteByConnector(ctx, h.org, fakevendor.ConnectorID)
		}))
		require.NoError(t, h.run(context.Background()))
		assert.Equal(t, first, h.keys()[len(first):])
	})
}

// Crash recovery: a run whose final row update cannot be written (as when
// its process is killed) stays "running" only until its lease expires. The
// next run of the operation then closes it as failed, kind abandoned, and
// resumes from the cursor of its last checkpoint.
func TestEngine_AbandonedRunIsClosedWhenItsLeaseExpires(t *testing.T) {
	const lease = 300 * time.Millisecond
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(3, t0)...)
		defer srv.Close()
		broken := s
		broken.runs = failingUpdates{s.runs}
		h := newHarness(t, broken, srv)
		h.orch.WithRunLease(lease)
		_ = h.run(context.Background())
		orphan := h.latest()
		assert.Equal(t, model.SyncRunStatusRunning, orphan.Status)
		require.NotNil(t, orphan.LeaseExpiresAt)

		h2 := *h
		h2.s = s
		h2.orch = runner.NewOrchestrator(registry(), s.configs, s.states, s.runs, h.sink, fakevendor.GatewayFactory(nil)).WithTxRunner(s.tx).WithRunLease(lease)
		require.NoError(t, h2.run(context.Background()))
		listRuns := func() []*model.SyncRun {
			var runs []*model.SyncRun
			require.NoError(t, s.tx.RunInScope(context.Background(), h.org, store.ScopeEnforced, func(ctx context.Context) error {
				var err error
				runs, err = s.runs.ListByOperation(ctx, h.org, fakevendor.ConnectorID, fakevendor.OpFetchItems, 10)
				return err
			}))
			return runs
		}
		runs := listRuns()
		require.Len(t, runs, 2)
		assert.Equal(t, model.SyncRunStatusRunning, runs[1].Status, "within its lease the orphan still reads running")

		time.Sleep(time.Until(*orphan.LeaseExpiresAt) + 50*time.Millisecond)
		require.NoError(t, h2.run(context.Background()))
		runs = listRuns()
		require.Len(t, runs, 3)
		closed := runs[2]
		assert.Equal(t, orphan.ID, closed.ID)
		assert.Equal(t, model.SyncRunStatusFailed, closed.Status, "the expired orphan is closed")
		assert.Equal(t, "abandoned", closed.ErrorKind)
		assert.Contains(t, closed.ErrorMessage, "lease expired")
		require.NotNil(t, closed.CompletedAt)
		assert.Equal(t, model.SyncRunStatusCompleted, runs[0].Status)
	})
}

type failingUpdates struct{ store.ISyncRunDAO }

func (failingUpdates) Update(context.Context, *model.SyncRun) error {
	return errors.New("store unavailable")
}

func registry() *sdk.ConnectorRegistry {
	r := sdk.NewConnectorRegistry()
	r.Register(fakevendor.Connector{})
	return r
}

// Concurrent claim: nothing stops two runs of one operation overlapping (only
// the scheduler's task id does), but the cursor save is a compare-and-swap. A
// run whose checkpoint or final save finds the cursor moved on fails with a
// retryable cursor conflict instead of overwriting it; at least one run
// completes, together they emit every item, and the cursor ends at the top.
func TestEngine_ConcurrentRunsBothComplete(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(6, t0)...)
		defer srv.Close()
		h := newHarness(t, s, srv)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = h.run(context.Background())
			}(i)
		}
		wg.Wait()
		completed := 0
		for _, err := range errs {
			if err == nil {
				completed++
				continue
			}
			require.ErrorIs(t, err, store.ErrCursorConflict)
		}
		require.GreaterOrEqual(t, completed, 1)
		seen := map[string]bool{}
		for _, k := range h.keys() {
			seen[k] = true
		}
		assert.Len(t, seen, 6, "together the runs emit every item")
		assert.GreaterOrEqual(t, len(h.keys()), 6)
		c := h.cursor()
		require.NotNil(t, c)
		assert.True(t, c.HighWater.Equal(t0.Add(5*time.Minute)))
	})
}

// The fake connector passes the conformance suite it demonstrates.
func TestFakeConnectorConforms(t *testing.T) {
	srv := fakevendor.New(items(3, t0)...)
	defer srv.Close()
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: srv.URL})
	testkit.RunConformance(t, fakevendor.Connector{}, testkit.ConformanceRun{
		Operation:   fakevendor.OpFetchItems,
		Config:      sdk.ConnectorConfig{Settings: settings},
		Gateway:     fakevendor.GatewayFactory(nil),
		Faults:      srv,
		Checkpoints: true,
	})
}

// closingRuns closes the run's own row as abandoned just before the run's
// final lease fence, as a reconciler would after the lease ran out.
type closingRuns struct {
	store.ISyncRunDAO
	tx    store.TxRunner
	calls int
}

func (c *closingRuns) RenewLease(ctx context.Context, orgID, runID string, lease time.Duration) error {
	c.calls++
	if c.calls == 2 {
		// In its own committed transaction, as a reconciler's would be.
		if err := c.tx.RunInScope(context.WithoutCancel(ctx), orgID, store.ScopeEnforced, func(sCtx context.Context) error {
			if err := c.ISyncRunDAO.RenewLease(sCtx, orgID, runID, -time.Hour); err != nil {
				return err
			}
			_, err := c.CloseAbandoned(sCtx, orgID, fakevendor.ConnectorID, fakevendor.OpFetchItems)
			return err
		}); err != nil {
			return err
		}
	}
	return c.ISyncRunDAO.RenewLease(ctx, orgID, runID, lease)
}

// A run whose row was closed before it finished saves no final cursor (the
// cursor stays at its last checkpoint, taken while it held its lease) and
// keeps the closed row, on every store, including Postgres.
func TestEngine_ClosedRunSavesNoCursor(t *testing.T) {
	eachStore(t, func(t *testing.T, s stores) {
		srv := fakevendor.New(items(3, t0)...)
		defer srv.Close()
		closing := s
		closing.runs = &closingRuns{ISyncRunDAO: s.runs, tx: s.tx}
		h := newHarness(t, closing, srv)
		require.ErrorIs(t, h.run(context.Background()), store.ErrRunClosed)
		assert.Equal(t, "abandoned", h.latest().ErrorKind)
		c := h.cursor()
		require.NotNil(t, c)
		assert.NotEmpty(t, c.PageToken, "the cursor is the last checkpoint's, not the finished run's")
	})
}

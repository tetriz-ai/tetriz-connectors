// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FaultInjector is a test vendor the conformance suite can make fail.
// *fakevendor.Server implements it, and FaultHandler wraps any handler in
// one.
type FaultInjector interface {
	// SetFault makes every request the vendor receives from now on wait
	// delay (or until the request is abandoned) and then, when status is
	// non-zero, answer status, with a Retry-After header of retryAfter
	// seconds when retryAfter > 0. SetFault(0, 0, 0) restores normal
	// answers.
	SetFault(status, retryAfter int, delay time.Duration)
}

// FaultHandler wraps next in a FaultInjector: serve it with
// httptest.NewServer and pass it as ConformanceRun.Faults.
func FaultHandler(next http.Handler) *FaultyHandler {
	return &FaultyHandler{next: next}
}

// FaultyHandler is an http.Handler that answers with the fault SetFault set,
// or passes the request to the handler it wraps.
type FaultyHandler struct {
	next http.Handler

	mu                 sync.Mutex
	status, retryAfter int
	delay              time.Duration
}

var _ FaultInjector = (*FaultyHandler)(nil)

// SetFault implements FaultInjector.
func (h *FaultyHandler) SetFault(status, retryAfter int, delay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status, h.retryAfter, h.delay = status, retryAfter, delay
}

// ServeHTTP implements http.Handler.
func (h *FaultyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	status, retryAfter, delay := h.status, h.retryAfter, h.delay
	h.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if status == 0 {
		h.next.ServeHTTP(w, r)
		return
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	http.Error(w, http.StatusText(status), status)
}

// faultCase is one vendor failure and the run failure it must cause.
type faultCase struct {
	name       string
	status     int
	retryAfter int
	// delay holds every answer back; deadline is the run's context deadline.
	delay, deadline time.Duration
	kind            sdk.SyncErrorKind
	// check reports what is wrong with the run's SyncError, or "".
	check func(se *sdk.SyncError) string
}

const faultRetryAfter = 7

func terminalAuth(se *sdk.SyncError) string {
	if se == nil || !se.Terminal {
		return "the error is not a terminal SyncError, so the scheduler retries a dead credential and the config is never paused"
	}
	return ""
}

var faultCases = []faultCase{
	{name: "401", status: http.StatusUnauthorized, kind: sdk.ErrKindAuth, check: terminalAuth},
	{name: "403", status: http.StatusForbidden, kind: sdk.ErrKindAuth, check: terminalAuth},
	{name: "429", status: http.StatusTooManyRequests, retryAfter: faultRetryAfter, kind: sdk.ErrKindRateLimit, check: func(se *sdk.SyncError) string {
		if se == nil || se.RetryAfterSeconds != faultRetryAfter {
			got := 0
			if se != nil {
				got = se.RetryAfterSeconds
			}
			return fmt.Sprintf("the error carries RetryAfterSeconds %d, want the vendor's Retry-After %d", got, faultRetryAfter)
		}
		return ""
	}},
	{name: "500", status: http.StatusInternalServerError, kind: sdk.ErrKindVendorError, check: retryableVendor},
	{name: "503", status: http.StatusServiceUnavailable, kind: sdk.ErrKindVendorError, check: retryableVendor},
	{name: "deadline", delay: 10 * time.Second, deadline: 300 * time.Millisecond, kind: sdk.ErrKindCtxExhausted},
}

func retryableVendor(se *sdk.SyncError) string {
	if se == nil || !se.Retryable || se.Terminal {
		return "the error is not a retryable, non-terminal SyncError, so the scheduler gives up on a passing vendor outage"
	}
	return ""
}

// checkFaults runs each fault case in a fresh store.
func checkFaults(t reporter, conn sdk.Connector, r ConformanceRun) {
	t.Helper()
	for _, fc := range faultCases {
		t.Run(fmt.Sprintf("faults/%s/%s", r.Operation, fc.name), func(t reporter) {
			t.Helper()
			h := newHarness(t, conn, r.Operation, r.Config, r.Gateway, r.TenantKind)
			r.Faults.SetFault(fc.status, fc.retryAfter, fc.delay)
			defer r.Faults.SetFault(0, 0, 0)
			deadline := fc.deadline
			if deadline == 0 {
				deadline = 30 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			row, err := h.run(ctx, 1)
			what := fmt.Sprintf("%s/%s with the vendor answering %s", conn.ID(), r.Operation, fc.name)
			if fc.status == 0 {
				what = fmt.Sprintf("%s/%s with the vendor not answering before the run's deadline", conn.ID(), r.Operation)
			}
			if err == nil || row.Status != model.SyncRunStatusFailed {
				t.Errorf("%s: the run ended %q, want failed with %s", what, row.Status, fc.kind)
				return
			}
			if h.calls.len() == 0 {
				t.Errorf("%s: the run made no vendor call, so the fault never reached it (%s: %s)", what, row.ErrorKind, row.ErrorMessage)
				return
			}
			if row.ErrorKind != fc.kind.String() {
				t.Errorf("%s: the run recorded error kind %q, want %q (%s)", what, row.ErrorKind, fc.kind, row.ErrorMessage)
				return
			}
			if fc.check != nil {
				var se *sdk.SyncError
				if !errors.As(err, &se) {
					se = nil
				}
				if msg := fc.check(se); msg != "" {
					t.Errorf("%s: %s (%v)", what, msg, err)
				}
			}
		})
	}
}

// checkCheckpoint fails the vendor after the operation's first checkpoint,
// then checks the next run resumes from it.
func checkCheckpoint(t reporter, conn sdk.Connector, r ConformanceRun) {
	t.Helper()
	ctx := context.Background()
	var (
		mu         sync.Mutex
		checkpoint *sdk.Cursor
		beforeCP   int
		armed      bool
		h          *harness
	)
	watched := checkpointWatch{Connector: conn, op: r.Operation, saved: func(c *sdk.Cursor) {
		mu.Lock()
		defer mu.Unlock()
		if !armed {
			return
		}
		armed = false
		cp := *c
		cp.Data = append([]byte(nil), c.Data...)
		checkpoint, beforeCP = &cp, len(h.sink.Records())
		r.Faults.SetFault(http.StatusInternalServerError, 0, 0)
	}}
	hh := newHarness(t, watched, r.Operation, r.Config, r.Gateway, r.TenantKind)
	mu.Lock()
	h, armed = hh, true
	mu.Unlock()
	defer r.Faults.SetFault(0, 0, 0)
	row, err := h.run(ctx, 1)
	r.Faults.SetFault(0, 0, 0)
	mu.Lock()
	cp, n := checkpoint, beforeCP
	armed = false // the run after the failure checkpoints freely
	mu.Unlock()
	switch {
	case cp == nil:
		t.Errorf("%s/%s: the run never called Checkpoint (it ended %q); the checkpoint check needs a listing longer than one page", conn.ID(), r.Operation, row.Status)
		return
	case err == nil || row.Status != model.SyncRunStatusFailed:
		t.Errorf("%s/%s: the run completed although the vendor failed every call after its first checkpoint; the checkpoint check needs a vendor call after it", conn.ID(), r.Operation)
		return
	}
	stored := h.cursor()
	if stored == nil || string(stored.Data) != string(cp.Data) {
		t.Errorf("%s/%s: after the failed run the stored cursor is not the checkpoint %s", conn.ID(), r.Operation, cp.Data)
	}
	emittedByFirst := len(h.sink.Records())
	h.calls.reset()
	h.mustComplete(ctx, 2)
	if r.Resume != nil {
		if first, ok := h.calls.first(); !ok {
			t.Errorf("%s/%s: the run after the failure made no vendor call", conn.ID(), r.Operation)
		} else if rerr := r.Resume(cp, first); rerr != nil {
			t.Errorf("%s/%s: the run after the failure does not resume from the checkpoint: %v", conn.ID(), r.Operation, rerr)
		}
	}
	all := h.sink.Records()
	before, resumed := all[:n], all[emittedByFirst:]
	var boundary time.Time
	for _, rec := range before {
		if rec != nil && rec.DataDate.After(boundary) {
			boundary = rec.DataDate
		}
	}
	beforeIDs := map[string]*sdk.RawRecord{}
	for _, rec := range before {
		if rec != nil {
			beforeIDs[rec.ExternalID] = rec
		}
	}
	reported := map[string]bool{}
	for _, rec := range resumed {
		if rec == nil || reported[rec.ExternalID] {
			continue
		}
		prev, ok := beforeIDs[rec.ExternalID]
		if !ok || (!boundary.IsZero() && !prev.DataDate.Before(boundary)) {
			continue
		}
		reported[rec.ExternalID] = true
		t.Errorf("%s/%s: external id %q, emitted before the checkpoint, was emitted again by the run that resumed from it", conn.ID(), r.Operation, rec.ExternalID)
	}
	if len(r.WantExternalIDs) > 0 {
		got := externalIDs(before)
		for id := range externalIDs(resumed) {
			got[id] = true
		}
		for _, id := range r.WantExternalIDs {
			if !got[id] {
				t.Errorf("%s/%s: external id %q was emitted neither before the checkpoint nor by the run that resumed from it", conn.ID(), r.Operation, id)
			}
		}
	}
}

// checkpointWatch is conn with operation op's Checkpoint reporting each
// cursor it saved to saved.
type checkpointWatch struct {
	sdk.Connector
	op    sdk.OperationID
	saved func(c *sdk.Cursor)
}

func (w checkpointWatch) Operations() []sdk.OperationDescriptor {
	ops := w.Connector.Operations()
	for i := range ops {
		inner := ops[i].Factory
		if ops[i].ID != w.op || inner == nil {
			continue
		}
		ops[i].Factory = func() sdk.Operation {
			op := inner()
			if f, ok := op.(sdk.APIFetchOperation); ok {
				return checkpointWatchOp{f, w.saved}
			}
			return op
		}
	}
	return ops
}

type checkpointWatchOp struct {
	sdk.APIFetchOperation
	saved func(c *sdk.Cursor)
}

func (o checkpointWatchOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	if cp := p.Checkpoint; cp != nil {
		p.Checkpoint = func(ctx context.Context, c *sdk.Cursor) error {
			if err := cp(ctx, c); err != nil {
				return err
			}
			o.saved(c)
			return nil
		}
	}
	c, err := o.APIFetchOperation.Execute(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", o.OperationID(), err)
	}
	return c, nil
}

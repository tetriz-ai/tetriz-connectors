// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// countingSink wraps a sdk.DataSink for one run. It tracks the number of
// records successfully emitted, which the orchestrator stores as
// connector_sync_runs.records_emitted, including for runs that fail mid-way.
// It hands a batch to the inner sink one record at a time, so a batch that
// fails part way still counts the records published before the failure.
//
// It also refuses a record whose organization is not the run's: a connector
// that names another tenant fails its run rather than publishing into that
// tenant's data.
type countingSink struct {
	inner sdk.DataSink
	orgID string
	count atomic.Int64
	// stopped, once set, refuses every later Emit and Flush with its error:
	// a run that lost its lease publishes nothing more.
	stopped atomic.Pointer[error]
}

// stop refuses every later Emit and Flush with err.
func (s *countingSink) stop(err error) { s.stopped.Store(&err) }

func (s *countingSink) stoppedErr() error {
	if p := s.stopped.Load(); p != nil {
		return *p
	}
	return nil
}

func newCountingSink(inner sdk.DataSink, orgID string) *countingSink {
	return &countingSink{inner: inner, orgID: orgID}
}

// checkOrg refuses a record for an organization other than the run's, which
// fails the run.
func (s *countingSink) checkOrg(record *sdk.RawRecord) error {
	if record != nil && record.OrganizationID != s.orgID {
		return &sdk.SyncError{
			ConnectorID:    sdk.ConnectorID(record.ConnectorID),
			OperationID:    sdk.OperationID(record.OperationID),
			OrganizationID: s.orgID,
			Kind:           sdk.ErrKindInternal,
			Terminal:       true,
			Cause: fmt.Errorf("record %q names organization %q, not the run's %q",
				record.ExternalID, record.OrganizationID, s.orgID),
		}
	}
	return nil
}

func (s *countingSink) Emit(ctx context.Context, record *sdk.RawRecord) error {
	if err := s.stoppedErr(); err != nil {
		return err
	}
	if err := s.checkOrg(record); err != nil {
		return err
	}
	if err := s.inner.Emit(ctx, record); err != nil {
		return fmt.Errorf("emitting: %w", err)
	}
	s.count.Add(1)
	return nil
}

// EmitBatch emits the records one at a time through Emit, so the count is
// exact when the batch fails part way: the records the inner sink took
// before the failure are counted, and the rest are not sent.
func (s *countingSink) EmitBatch(ctx context.Context, records []*sdk.RawRecord) error {
	for _, record := range records {
		if err := s.Emit(ctx, record); err != nil {
			return fmt.Errorf("emitting batch: %w", err)
		}
	}
	return nil
}

func (s *countingSink) Flush(ctx context.Context) error {
	if err := s.stoppedErr(); err != nil {
		return err
	}
	if err := s.inner.Flush(ctx); err != nil {
		return fmt.Errorf("flushing sink: %w", err)
	}
	return nil
}

func (s *countingSink) Count() int64 {
	return s.count.Load()
}

// classifiedAsCtxExhausted reports whether the supplied error is a downstream
// symptom of an already-expired parent context. Rate-limit storms and stalled
// vendor calls eventually exhaust the asynq task budget; downstream calls
// (sink Emit, SQS SendMessage, follow-on HTTP requests) then return
// context.DeadlineExceeded / Canceled and look like sink or vendor failures.
// Reclassifying them as ErrKindCtxExhausted keeps SQS monitoring and vendor
// error rates honest — the upstream throttling/timeout is the real cause.
func classifiedAsCtxExhausted(parent context.Context, err error) bool {
	if err == nil {
		return false
	}
	// parent.Err() is nil while the parent is alive, and otherwise carries the
	// concrete cancellation cause (Canceled / DeadlineExceeded). Inspecting that
	// cause — rather than discarding it after a bare nil check — keeps the parent
	// state and the downstream symptom on the same footing.
	parentExpired := errors.Is(parent.Err(), context.DeadlineExceeded) || errors.Is(parent.Err(), context.Canceled)
	if !parentExpired {
		// Parent is still alive — a ctx error from the inner call is genuine
		// (e.g. an explicit per-call timeout deeper in the stack).
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

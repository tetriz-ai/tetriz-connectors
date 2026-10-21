// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// errEdit is the fake vendor with the error its operation returns changed by
// edit, as a connector with a broken error mapping would return it.
type errEdit struct {
	fakevendor.Connector
	edit func(ctx context.Context, p *sdk.ExecutionParams, err error) (*sdk.Cursor, error)
}

func (e errEdit) Operations() []sdk.OperationDescriptor {
	return withOp(func(in sdk.APIFetchOperation) sdk.Operation { return errEditOp{in, e.edit} })(e.Connector.Operations())
}

type errEditOp struct {
	sdk.APIFetchOperation
	edit func(ctx context.Context, p *sdk.ExecutionParams, err error) (*sdk.Cursor, error)
}

func (o errEditOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	c, err := o.APIFetchOperation.Execute(ctx, p)
	if err == nil {
		return c, nil
	}
	return o.edit(ctx, p, err)
}

// editSyncError applies f to the SyncError the operation returns.
func editSyncError(f func(se *sdk.SyncError)) errEdit {
	return errEdit{edit: func(_ context.Context, _ *sdk.ExecutionParams, err error) (*sdk.Cursor, error) {
		var se *sdk.SyncError
		if errors.As(err, &se) {
			f(se)
		}
		return nil, err
	}}
}

// Each error check reports a connector whose error mapping is broken.
func TestConformanceCatchesBrokenErrorHandling(t *testing.T) {
	cases := []struct {
		name string
		conn sdk.Connector
		want string
	}{
		{"auth failure not terminal", editSyncError(func(se *sdk.SyncError) {
			if se.Kind == sdk.ErrKindAuth {
				se.Terminal = false
			}
		}), "answering 401: the error is not a terminal SyncError"},
		{"403 read as not found", editSyncError(func(se *sdk.SyncError) {
			if se.HTTPStatus == http.StatusForbidden {
				se.Kind = sdk.ErrKindNotFound
			}
		}), `answering 403: the run recorded error kind "not_found", want "auth"`},
		{"rate limit drops the Retry-After hint", editSyncError(func(se *sdk.SyncError) { se.RetryAfterSeconds = 0 }),
			"answering 429: the error carries RetryAfterSeconds 0, want the vendor's Retry-After 7"},
		{"5xx not retryable", editSyncError(func(se *sdk.SyncError) {
			if se.Kind == sdk.ErrKindVendorError {
				se.Retryable = false
			}
		}), "answering 500: the error is not a retryable, non-terminal SyncError"},
		{"503 marked terminal", editSyncError(func(se *sdk.SyncError) {
			if se.HTTPStatus == http.StatusServiceUnavailable {
				se.Terminal = true
			}
		}), "answering 503: the error is not a retryable"},
		{"every vendor failure a plain error", errEdit{edit: func(_ context.Context, _ *sdk.ExecutionParams, err error) (*sdk.Cursor, error) {
			return nil, errors.New(err.Error())
		}}, `answering 401: the run recorded error kind "internal", want "auth"`},
		{"deadline reported as a vendor error", errEdit{edit: func(_ context.Context, _ *sdk.ExecutionParams, err error) (*sdk.Cursor, error) {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, &sdk.SyncError{Kind: sdk.ErrKindVendorError, Retryable: true, Cause: fmt.Errorf("vendor: %v", err)}
			}
			return nil, err
		}}, `not answering before the run's deadline: the run recorded error kind "vendor_error", want "ctx_exhausted"`},
		{"deadline swallowed", errEdit{edit: func(ctx context.Context, p *sdk.ExecutionParams, err error) (*sdk.Cursor, error) {
			if ctx.Err() != nil {
				return p.Cursor, nil
			}
			return nil, err
		}}, `not answering before the run's deadline: the run ended "completed", want failed with ctx_exhausted`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			runConformance(rec, c.conn, vendorRun(t))
			if !containsError(rec.errs, c.want) {
				t.Fatalf("want a failure containing %q, got %v", c.want, rec.errs)
			}
		})
	}
}

// checkpointEdit is the fake vendor with the cursor it checkpoints changed.
type checkpointEdit struct {
	fakevendor.Connector
	edit func(p *sdk.ExecutionParams, c *sdk.Cursor) *sdk.Cursor // nil: never checkpoint
}

func (e checkpointEdit) Operations() []sdk.OperationDescriptor {
	return withOp(func(in sdk.APIFetchOperation) sdk.Operation {
		return brokenOp{in, func(p *sdk.ExecutionParams) {
			cp := p.Checkpoint
			if e.edit == nil {
				p.Checkpoint = nil
				return
			}
			p.Checkpoint = func(ctx context.Context, c *sdk.Cursor) error { return cp(ctx, e.edit(p, c)) }
		}}
	})(e.Connector.Operations())
}

// Each checkpoint defect is reported.
func TestConformanceCatchesBrokenCheckpoints(t *testing.T) {
	future, _ := json.Marshal(map[string]any{"high_water": t0.Add(24 * time.Hour), "high_water_id": "~"})
	cases := []struct {
		name string
		conn sdk.Connector
		want string
	}{
		{"never checkpoints", checkpointEdit{}, "the run never called Checkpoint"},
		{"checkpoints the starting cursor", checkpointEdit{edit: func(p *sdk.ExecutionParams, _ *sdk.Cursor) *sdk.Cursor {
			return &sdk.Cursor{Data: []byte(`{}`)}
		}}, `external id "item-01", emitted before the checkpoint, was emitted again by the run that resumed from it`},
		{"checkpoints past what it emitted", checkpointEdit{edit: func(*sdk.ExecutionParams, *sdk.Cursor) *sdk.Cursor {
			return &sdk.Cursor{Data: future}
		}}, `external id "item-03" was emitted neither before the checkpoint nor by the run that resumed from it`},
		{"resumed run ignores the checkpoint", broken{wrap: func(p *sdk.ExecutionParams) {
			if p.Cursor != nil && json.Valid(p.Cursor.Data) {
				var st map[string]any
				_ = json.Unmarshal(p.Cursor.Data, &st)
				if st["page_token"] != nil {
					p.Cursor = nil
				}
			}
		}}, "the run after the failure does not resume from the checkpoint"},
		{"Checkpoints without Faults", fakevendor.Connector{}, "Checkpoints needs Faults"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			run := vendorRun(t)
			if c.name == "Checkpoints without Faults" {
				run.Faults = nil
			}
			runConformance(rec, c.conn, run)
			if !containsError(rec.errs, c.want) {
				t.Fatalf("want a failure containing %q, got %v", c.want, rec.errs)
			}
		})
	}
}

// A record at the cursor boundary, which an inclusive cursor reads again, is
// not a re-emission: the fake vendor with an inclusive checkpoint passes.
func TestConformanceAllowsTheCheckpointBoundary(t *testing.T) {
	inclusive := checkpointEdit{edit: func(_ *sdk.ExecutionParams, c *sdk.Cursor) *sdk.Cursor {
		var st map[string]any
		_ = json.Unmarshal(c.Data, &st)
		delete(st, "page_token")
		delete(st, "high_water_id") // resume from updated_since, inclusive
		b, _ := json.Marshal(st)
		return &sdk.Cursor{Data: b}
	}}
	rec := &recorder{}
	run := vendorRun(t)
	run.Resume = nil
	runConformance(rec, inclusive, run)
	if len(rec.errs) > 0 {
		t.Fatalf("an inclusive checkpoint fails conformance: %v", rec.errs)
	}
}

// FaultHandler makes any test server a FaultInjector: the fake vendor's
// handler behind it passes every check.
func TestFaultHandlerPassesTheFakeVendor(t *testing.T) {
	vendor := fakevendor.New(fakevendor.Item{ID: "a", UpdatedAt: t0}, fakevendor.Item{ID: "b", UpdatedAt: t0.Add(time.Minute)}, fakevendor.Item{ID: "c", UpdatedAt: t0.Add(2 * time.Minute)})
	defer vendor.Close()
	faults := FaultHandler(vendor.Config.Handler)
	srv := httptest.NewServer(faults)
	defer srv.Close()
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: srv.URL})
	rec := &recorder{}
	runConformance(rec, fakevendor.Connector{}, ConformanceRun{
		Operation:       fakevendor.OpFetchItems,
		Config:          sdk.ConnectorConfig{Settings: settings},
		Gateway:         fakevendor.GatewayFactory(nil),
		WantExternalIDs: []string{"a", "b", "c"},
		Faults:          faults,
		Checkpoints:     true,
	})
	if len(rec.errs) > 0 {
		t.Fatalf("the fake vendor behind FaultHandler fails conformance: %v", rec.errs)
	}
}

// hostEdit is the fake vendor with RequiresHost set on its operation.
func hostEdit(services ...sdk.HostService) declEdit {
	return declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
		ops[0].RequiresHost = services
		return ops
	}}
}

// A RequiresHost declaration naming no real service, or one twice, is
// reported; a valid one passes, refused by the orchestrator as it should be.
func TestConformanceChecksRequiresHost(t *testing.T) {
	for _, c := range []struct {
		name string
		conn sdk.Connector
		want string
	}{
		{"unknown service", hostEdit("work-queue"), `RequiresHost names unknown host service "work-queue"`},
		{"service listed twice", hostEdit(sdk.HostWorkQueue, sdk.HostWorkQueue), `RequiresHost lists "work_queue" twice`},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			runConformance(rec, c.conn)
			if !containsError(rec.errs, c.want) {
				t.Fatalf("want a failure containing %q, got %v", c.want, rec.errs)
			}
		})
	}
	t.Run("valid declaration", func(t *testing.T) {
		rec := &recorder{}
		runConformance(rec, hostEdit(sdk.HostWorkQueue, sdk.HostBlameSink))
		if len(rec.errs) > 0 {
			t.Fatalf("a valid RequiresHost fails conformance: %v", rec.errs)
		}
	})
}

// stubQueue is a WorkQueue that is set but never called.
type stubQueue struct{ sdk.WorkQueue }

// The refusal check reports an operation that runs although a service it
// requires is missing. The orchestrator always refuses, so the check is
// proved against one handed the service the operation's declaration says
// is missing, which runs the operation as a runner ignoring RequiresHost
// would.
func TestConformanceHostRefusalCatchesARunThatIsNotRefused(t *testing.T) {
	rec := &recorder{}
	checkHostRefusal(rec, hostEdit(sdk.HostWorkQueue), sdk.HostServices{WorkQueue: stubQueue{}})
	want := `want config_invalid: "operation fakevendor/fetch_items needs host services: work_queue"`
	if !containsError(rec.errs, want) {
		t.Fatalf("want a failure containing %q, got %v", want, rec.errs)
	}
}

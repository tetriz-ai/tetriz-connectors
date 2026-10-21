// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

// recorder is a reporter that collects failures instead of failing.
type recorder struct{ errs []string }

type fatal struct{}

func (r *recorder) Helper()                      {}
func (r *recorder) Errorf(f string, args ...any) { r.errs = append(r.errs, fmt.Sprintf(f, args...)) }
func (r *recorder) Fatalf(f string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(f, args...))
	panic(fatal{})
}
func (r *recorder) Run(_ string, f func(reporter)) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(fatal); !ok {
				panic(p)
			}
		}
	}()
	f(r)
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func vendorRun(t *testing.T) ConformanceRun {
	t.Helper()
	var items []fakevendor.Item
	for i := 1; i <= 5; i++ {
		items = append(items, fakevendor.Item{ID: fmt.Sprintf("item-%02d", i), UpdatedAt: t0.Add(time.Duration(i) * time.Minute)})
	}
	vendor := fakevendor.New(items...)
	added := fakevendor.Item{ID: "item-06", UpdatedAt: t0.Add(10 * time.Minute)}
	t.Cleanup(vendor.Close)
	settings, _ := json.Marshal(fakevendor.Settings{BaseURL: vendor.URL})
	return ConformanceRun{
		Operation: fakevendor.OpFetchItems,
		Config:    sdk.ConnectorConfig{Settings: settings},
		Gateway:   fakevendor.GatewayFactory(nil),
		Resume: func(c *sdk.Cursor, first VendorCall) error {
			return fakevendor.ResumesFrom(c, first.Request)
		},
		WantExternalIDs: []string{"item-01", "item-02", "item-03", "item-04", "item-05"},
		AddBetweenRuns: func() []string {
			vendor.SetItems(append(items, added)...)
			return []string{added.ID}
		},
		Faults:      vendor,
		Checkpoints: true,
	}
}

// broken wraps the fake vendor's operation with a defect.
type broken struct {
	fakevendor.Connector
	wrap func(p *sdk.ExecutionParams)
}

func (b broken) Operations() []sdk.OperationDescriptor {
	ops := b.Connector.Operations()
	for i := range ops {
		inner := ops[i].Factory
		ops[i].Factory = func() sdk.Operation {
			return brokenOp{inner().(sdk.APIFetchOperation), b.wrap}
		}
	}
	return ops
}

type brokenOp struct {
	sdk.APIFetchOperation
	wrap func(p *sdk.ExecutionParams)
}

func (o brokenOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	o.wrap(p)
	return o.APIFetchOperation.Execute(ctx, p) //nolint:wrapcheck // test wrapper
}

// editSink changes or drops records on their way to the real sink.
type editSink struct {
	sdk.DataSink
	edit func(r *sdk.RawRecord) bool // false drops the record
}

func (s editSink) Emit(ctx context.Context, r *sdk.RawRecord) error {
	if !s.edit(r) {
		return nil
	}
	return s.DataSink.Emit(ctx, r) //nolint:wrapcheck // test wrapper
}

func (s editSink) EmitBatch(ctx context.Context, rs []*sdk.RawRecord) error {
	for _, r := range rs {
		if err := s.Emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func TestConformancePassesTheFakeVendor(t *testing.T) {
	rec := &recorder{}
	runConformance(rec, fakevendor.Connector{}, vendorRun(t))
	if len(rec.errs) > 0 {
		t.Fatalf("the fake vendor fails conformance: %v", rec.errs)
	}
}

func TestConformanceCatchesABrokenConnector(t *testing.T) {
	cases := []struct {
		name string
		wrap func(p *sdk.ExecutionParams)
		want string
		conn sdk.Connector // replaces broken{wrap} when set
	}{
		{"cursor ignored", func(p *sdk.ExecutionParams) { p.Cursor = nil }, "does not resume from the stored cursor", nil},
		{"external id empty", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.ExternalID = ""; return true }}
		}, "has no external id", nil},
		{"cursor saved as now", nil, `"item-06", added between the runs, was not emitted by the second run`, cursorNow{}},
		{"records lost", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { return r.ExternalID != "item-03" }}
		}, `external id "item-03" was never emitted`, nil},
		{"record names another connector", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.ConnectorID = "other"; return true }}
		}, "names other/", nil},
		{"record names another operation", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.OperationID = "other_op"; return true }}
		}, "/other_op/", nil},
		{"record names another organization", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.OrganizationID = "another-org"; return true }}
		}, `names organization "another-org"`, nil},
		{"record has no fetch time", func(p *sdk.ExecutionParams) {
			p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.FetchedAt = time.Time{}; return true }}
		}, "has fetch time", nil},
		{"operation declared twice", nil, "is declared twice", duplicateOp{}},
		{"categories slice shared", nil, "returns a shared slice", &sharedCategories{cats: []sdk.ConnectorCategory{"vcs"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			var conn sdk.Connector = broken{wrap: c.wrap}
			if c.conn != nil {
				conn = c.conn
			}
			runConformance(rec, conn, vendorRun(t))
			if !containsError(rec.errs, c.want) {
				t.Fatalf("want a failure containing %q, got %v", c.want, rec.errs)
			}
		})
	}
}

func containsError(errs []string, want string) bool {
	for _, e := range errs {
		if strings.Contains(e, want) {
			return true
		}
	}
	return false
}

// cursorNow is the fake vendor with a connector that stores "everything up to
// now" as its cursor, so a record added later with an older timestamp is
// never listed.
type cursorNow struct{ fakevendor.Connector }

func (c cursorNow) Operations() []sdk.OperationDescriptor {
	ops := c.Connector.Operations()
	for i := range ops {
		inner := ops[i].Factory
		ops[i].Factory = func() sdk.Operation { return cursorNowOp{inner().(sdk.APIFetchOperation)} }
	}
	return ops
}

type cursorNowOp struct{ sdk.APIFetchOperation }

func (o cursorNowOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	if _, err := o.APIFetchOperation.Execute(ctx, p); err != nil {
		return nil, err //nolint:wrapcheck // test wrapper
	}
	b, _ := json.Marshal(map[string]any{"high_water": time.Now().UTC(), "high_water_id": "~"})
	return &sdk.Cursor{Data: b}, nil
}

// duplicateOp declares its first operation twice.
type duplicateOp struct{ fakevendor.Connector }

func (d duplicateOp) Operations() []sdk.OperationDescriptor {
	ops := d.Connector.Operations()
	return append(ops, ops[0])
}

// sharedCategories returns the same slice on every call.
type sharedCategories struct {
	fakevendor.Connector
	cats []sdk.ConnectorCategory
}

func (s *sharedCategories) Categories() []sdk.ConnectorCategory { return s.cats }

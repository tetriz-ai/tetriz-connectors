// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit/fakevendor"
)

// declEdit is the fake vendor with its declaration changed by edit.
type declEdit struct {
	fakevendor.Connector
	id, name string
	edit     func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor
}

func (d declEdit) ID() sdk.ConnectorID {
	if d.id == "-" {
		return ""
	}
	return d.Connector.ID()
}

func (d declEdit) DisplayName() string {
	if d.name == "-" {
		return ""
	}
	return d.Connector.DisplayName()
}

func (d declEdit) Operations() []sdk.OperationDescriptor {
	ops := d.Connector.Operations()
	if d.edit != nil {
		return d.edit(ops)
	}
	return ops
}

type statefulOp struct {
	sdk.APIFetchOperation
	_ int // gives the type a size so two instances are distinguishable
}

type notAPull struct{}

func (notAPull) OperationID() sdk.OperationID { return fakevendor.OpFetchItems }

// cursorOp wraps the real operation and replaces the cursor it returns.
type cursorOp struct {
	sdk.APIFetchOperation
	next func(c *sdk.Cursor, p *sdk.ExecutionParams) (*sdk.Cursor, error)
}

func (o cursorOp) Execute(ctx context.Context, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
	c, err := o.APIFetchOperation.Execute(ctx, p)
	if err != nil {
		return nil, err //nolint:wrapcheck // test wrapper
	}
	return o.next(c, p)
}

func withOp(wrap func(sdk.APIFetchOperation) sdk.Operation) func([]sdk.OperationDescriptor) []sdk.OperationDescriptor {
	return func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
		inner := ops[0].Factory
		ops[0].Factory = func() sdk.Operation { return wrap(inner().(sdk.APIFetchOperation)) }
		return ops
	}
}

// Each declaration defect the suite documents, and two run defects the existing
// table does not cover, must be reported.
func TestEdge_ConformanceCatchesDeclarationAndRunDefects(t *testing.T) {
	shared := &statefulOp{APIFetchOperation: &fakevendor.FetchItemsOp{}}
	second := func(ops []sdk.OperationDescriptor, d sdk.OperationDescriptor) []sdk.OperationDescriptor {
		return append(ops, d)
	}
	cases := []struct {
		name string
		conn sdk.Connector
		want string
		run  bool // also run the operation; declaration-only cases do not
	}{
		{"empty id", declEdit{id: "-"}, "connector id is empty", false},
		{"empty display name", declEdit{name: "-"}, "display name is empty", false},
		{"no operations", declEdit{edit: func([]sdk.OperationDescriptor) []sdk.OperationDescriptor { return nil }}, "no operations", false},
		{"empty operation id", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			return second(ops, sdk.OperationDescriptor{})
		}}, "an operation has an empty id", false},
		{"nil factory", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].Factory = nil
			return ops
		}}, "nil factory", false},
		{"factory returns nil", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].Factory = func() sdk.Operation { return nil }
			return ops
		}}, "factory returned nil", false},
		{"factory shares one stateful instance", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].Factory = func() sdk.Operation { return shared }
			return ops
		}}, "returns the same instance twice", false},
		{"operation reports another id", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops = second(ops, ops[0])
			ops[1].ID = "fetch_other"
			return ops
		}}, `factory's operation reports id "fetch_items"`, false},
		{"pull op is not an APIFetchOperation", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].Factory = func() sdk.Operation { return notAPull{} }
			return ops
		}}, "does not implement sdk.APIFetchOperation", false},
		{"six-field cron", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].DefaultSchedule = "0 */15 * * * *"
			return ops
		}}, "default schedule", false},
		{"stagger offsets collide modulo the period", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			b := ops[0]
			b.ID = "fetch_b"
			ops[0].StaggerGroup, b.StaggerGroup = "g", "g"
			ops[0].StaggerOffsetMinutes, b.StaggerOffsetMinutes = 1, 16 // 16 mod 15 == 1
			b.Factory = func() sdk.Operation { return notAPullB{} }
			return second(ops, b)
		}}, "share offset 1", false},
		{"stagger group mixes periods", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			b := ops[0]
			b.ID, b.DefaultSchedule = "fetch_b", "*/30 * * * *"
			ops[0].StaggerGroup, b.StaggerGroup = "g", "g"
			b.StaggerOffsetMinutes = 3
			b.Factory = func() sdk.Operation { return notAPullB{} }
			return second(ops, b)
		}}, "mixes repeat periods", false},
		{"stagger member with a fixed-hour schedule", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].StaggerGroup, ops[0].DefaultSchedule = "g", "0 3 * * *"
			return ops
		}}, "with no repeat period", false},
		{"negative stagger offset", declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
			ops[0].StaggerGroup, ops[0].StaggerOffsetMinutes = "g", -1
			return ops
		}}, "negative offset", false},
		{"cursor is not JSON", declEdit{edit: withOp(func(in sdk.APIFetchOperation) sdk.Operation {
			return cursorOp{in, func(*sdk.Cursor, *sdk.ExecutionParams) (*sdk.Cursor, error) {
				return &sdk.Cursor{Data: []byte("not json")}, nil
			}}
		})}, "run 1", true},
		{"connector cannot read its own cursor back", declEdit{edit: withOp(func(in sdk.APIFetchOperation) sdk.Operation {
			return cursorOp{in, func(c *sdk.Cursor, p *sdk.ExecutionParams) (*sdk.Cursor, error) {
				if p.Cursor != nil {
					return nil, errors.New("unreadable cursor")
				}
				return c, nil
			}}
		})}, "run 2", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			if c.run {
				runConformance(rec, c.conn, vendorRun(t))
			} else {
				runConformance(rec, c.conn)
			}
			if !containsError(rec.errs, c.want) {
				t.Fatalf("want a failure containing %q, got %v", c.want, rec.errs)
			}
		})
	}
}

// The suite "reports every violation it finds". A nil factory is reported by
// the declaration check, but the run phase then calls the nil factory inside
// the orchestrator and panics, so the author gets a nil-pointer stack instead
// of the suite's report.
func TestEdge_ConformanceRunWithANilFactoryReportsInsteadOfPanicking(t *testing.T) {
	conn := declEdit{edit: func(ops []sdk.OperationDescriptor) []sdk.OperationDescriptor {
		ops[0].Factory = nil
		return ops
	}}
	rec := &recorder{}
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		runConformance(rec, conn, vendorRun(t))
	}()
	if panicked != nil {
		t.Fatalf("the run phase panicked on a nil factory (%v); reported so far: %v", panicked, rec.errs)
	}
	if !containsError(rec.errs, "nil factory") {
		t.Fatalf("nil factory not reported: %v", rec.errs)
	}
}

type notAPullB struct{ fakevendor.FetchItemsOp }

func (notAPullB) OperationID() sdk.OperationID { return "fetch_b" }

// A record stamped with a fetch time from the future (a clock or unit bug) is
// reported, as the suite documents.
func TestEdge_ConformanceCatchesAFutureFetchTime(t *testing.T) {
	rec := &recorder{}
	conn := broken{wrap: func(p *sdk.ExecutionParams) {
		p.Sink = editSink{p.Sink, func(r *sdk.RawRecord) bool { r.FetchedAt = time.Now().Add(time.Hour); return true }}
	}}
	runConformance(rec, conn, vendorRun(t))
	if !containsError(rec.errs, "has fetch time") {
		t.Fatalf("a future fetch time was not reported: %v", rec.errs)
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

// The completeness test reads two declarations, made from one
// ops_<connector>_test.go file per connector:
//
//   - declareNoRecords: the operation emits no record, with the reason.
//   - declareEmits: the record operationIds the operation emits, when they
//     are not just its own id (a webhook that emits under another id).
//
// An operation with neither declaration must have the schema
// <connector>/<operation>.schema.json.
var (
	noRecords = map[string]map[string]string{}
	emits     = map[string]map[string][]string{}
)

// declareNoRecords records that connector's operation op emits no record.
func declareNoRecords(connector, op, reason string) {
	if reason == "" {
		panic(fmt.Sprintf("declareNoRecords %s/%s: empty reason", connector, op))
	}
	if noRecords[connector] == nil {
		noRecords[connector] = map[string]string{}
	}
	noRecords[connector][op] = reason
}

// declareEmits records the operationIds of the records op emits. List op
// itself when it also emits under its own id.
func declareEmits(connector, op string, recordOps ...string) {
	if len(recordOps) == 0 {
		panic(fmt.Sprintf("declareEmits %s/%s: no operationIds", connector, op))
	}
	if emits[connector] == nil {
		emits[connector] = map[string][]string{}
	}
	emits[connector][op] = recordOps
}

// TestEveryRecordOperationHasASchema checks that every operation of every
// registered connector that emits records has a schema for each record
// operationId it emits, and that no schema is orphaned.
func TestEveryRecordOperationHasASchema(t *testing.T) {
	if err := schemas.Load(); err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	reg := app.NewRegistry(&blobstore.Memory{})

	want := map[schemas.Ref]string{} // schema -> the operation that needs it
	known := map[string]map[string]bool{}
	for _, c := range reg.List() {
		cid := string(c.ID())
		known[cid] = map[string]bool{}
		for _, op := range c.Operations() {
			oid := string(op.ID)
			known[cid][oid] = true
			if _, skip := noRecords[cid][oid]; skip {
				if _, both := emits[cid][oid]; both {
					t.Errorf("%s/%s: declared both as emitting no records and as emitting %v", cid, oid, emits[cid][oid])
				}
				continue
			}
			recordOps := emits[cid][oid]
			if recordOps == nil {
				recordOps = []string{oid}
			}
			for _, r := range recordOps {
				want[schemas.Ref{Connector: cid, Operation: r}] = oid
			}
		}
	}

	var missing []string
	for ref, op := range want {
		if _, ok := schemas.Schema(ref.Connector, ref.Operation); !ok {
			missing = append(missing, fmt.Sprintf("%s (emitted by %s)", ref.Path(), op))
		}
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Errorf("missing schema %s: add it, or declare the operation in ops_<connector>_test.go", m)
	}

	for _, ref := range schemas.List() {
		if _, ok := want[ref]; !ok {
			t.Errorf("orphaned schema %s: no registered operation emits records with this operationId", ref.Path())
		}
	}

	for _, decl := range []map[string]map[string]string{noRecords, stringsOf(emits)} {
		for cid, ops := range decl {
			for oid := range ops {
				if !known[cid][oid] {
					t.Errorf("stale declaration %s/%s: no such registered operation", cid, oid)
				}
			}
		}
	}
}

func stringsOf(m map[string]map[string][]string) map[string]map[string]string {
	out := map[string]map[string]string{}
	for cid, ops := range m {
		out[cid] = map[string]string{}
		for oid := range ops {
			out[cid][oid] = ""
		}
	}
	return out
}

// TestSchemasCompile checks every schema compiles and declares draft 2020-12
// and its own $id.
func TestSchemasCompile(t *testing.T) {
	if err := schemas.Load(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range schemas.List() {
		b, _ := schemas.Schema(ref.Connector, ref.Operation)
		doc := mustObject(t, ref, b)
		if doc["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
			t.Errorf("%s: $schema = %v, want draft 2020-12", ref.Path(), doc["$schema"])
		}
		wantID := "https://github.com/tetriz-ai/tetriz-connectors/schemas/" + ref.Path()
		if doc["$id"] != wantID {
			t.Errorf("%s: $id = %v, want %s", ref.Path(), doc["$id"], wantID)
		}
		if _, ok := doc["title"]; !ok {
			t.Errorf("%s: no title", ref.Path())
		}
	}
}

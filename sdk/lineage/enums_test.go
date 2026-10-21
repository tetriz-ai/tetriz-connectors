// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package lineage

import (
	"encoding/json"
	"strconv"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
)

// The host's walker returns every evidence type and state, so each must have a name: a value
// without one reads as a bare number in String() and in protojson.
func TestWalkerEnumValuesHaveNames(t *testing.T) {
	for v := range int32(5) {
		evidence := defectlineagev1.DefectLineageEvidenceType(v)
		if got := evidence.String(); got == strconv.Itoa(int(v)) {
			t.Errorf("DefectLineageEvidenceType(%d).String() = %q, want a name", v, got)
		}
		b, err := protojson.Marshal(&defectlineagev1.LineageBlameResult{
			CulpritSha:   "abc",
			EvidenceType: evidence,
		})
		if err != nil {
			t.Fatalf("protojson.Marshal: %v", err)
		}
		var out map[string]any
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if v == 0 {
			if _, ok := out["evidenceType"]; ok {
				t.Errorf("evidence type 0: evidenceType emitted for the zero value: %s", b)
			}
			continue
		}
		if got, ok := out["evidenceType"].(string); !ok || got != evidence.String() {
			t.Errorf("evidence type %d: evidenceType = %#v, want the string %q (%s)", v, out["evidenceType"], evidence.String(), b)
		}
	}

	for v := range int32(9) {
		state := defectlineagev1.DefectLineageState(v)
		if got := state.String(); got == strconv.Itoa(int(v)) {
			t.Errorf("DefectLineageState(%d).String() = %q, want a name", v, got)
		}
		b, err := protojson.Marshal(&defectlineagev1.SubmitLineageBlameRequest{
			BugFixPrId: "fix",
			Status:     state,
		})
		if err != nil {
			t.Fatalf("protojson.Marshal: %v", err)
		}
		var out map[string]any
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if v == 0 {
			continue
		}
		if got, ok := out["status"].(string); !ok || got != state.String() {
			t.Errorf("state %d: status = %#v, want the string %q (%s)", v, out["status"], state.String(), b)
		}
	}
}

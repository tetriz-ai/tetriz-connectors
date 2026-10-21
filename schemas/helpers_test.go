// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"encoding/json"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

func mustObject(t *testing.T, ref schemas.Ref, b []byte) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("%s: %v", ref.Path(), err)
	}
	return doc
}

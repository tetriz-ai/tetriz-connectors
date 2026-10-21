// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A host's cursor-rewind tool (a job that rewinds persisted cursors) decodes and
// re-encodes this package's persisted cursor entries, and it cannot import this
// unexported type — it hand-declares the same JSON tags. If a tag here is renamed,
// that job silently stops recognising the entry, rewrites it in the wrong encoding,
// and the next run drops the cursor and re-walks the repo from the lookback floor.
//
// This assertion is the contract that makes such a rename fail here instead. On a
// deliberate change, update the job's codec in the same commit.
func TestCursorEntryJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(repoCursorState{Timestamp: time.Date(2026, 8, 6, 7, 0, 0, 0, time.UTC)})
	require.NoError(t, err)

	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &keyed))
	assert.Contains(t, keyed, "ts", "the rewind job keys this connector's shape off \"ts\"")
}

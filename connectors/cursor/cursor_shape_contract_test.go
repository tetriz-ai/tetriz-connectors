// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// TestUsagePositionJSONContract pins the field names a stored position is written and
// read under.
//
// The reason is not a compile break — there is none. The position lives as JSON in
// connector_sync_states, and parseUsagePosition treats a missing floor as "no sync has
// ever run". So renaming a tag does not fail anywhere: every stored position for every
// organisation quietly becomes unreadable-as-absent, and the next scheduled run starts
// the ninety-day lookback again for each of them, against an endpoint with a per-minute
// budget. Both sides of a rename move together, so neither the type system nor any other
// test in this package notices.
//
// What this does NOT guard, stated so the next reader does not over-trust it: the
// host's cursor-rewind tool hand-declares the VCS connectors' cursor encodings,
// and for those packages that second declaration is the reason their shape is pinned.
// It is not the reason here — its supported set is vendor.VCSConnectorIDs and it refuses
// this connector by name, failing closed rather than mis-decoding. If Cursor is ever
// added there, that job's codec becomes a second declaration of these same tags and has
// to change with them.
func TestUsagePositionJSONContract(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(usagePosition{
		FloorDate:     "2026-08-19",
		BackfillUntil: "2026-08-19",
	})
	require.NoError(t, err)

	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &keyed))

	for _, key := range []string{"floor_date", "backfill_until"} {
		assert.Contains(t, keyed, key,
			"a stored position is keyed off %q; renaming it orphans every organisation's floor", key)
	}
	assert.Len(t, keyed, 2,
		"a new field is a new thing every ALREADY-STORED position lacks, so decide what its absence "+
			"must mean before adding it here — the safe direction is whatever keeps the window narrow")

	// The tags are only half the contract: the reader has to agree with them, and it has to
	// agree with what is ALREADY ON DISK. So the reader is checked against a frozen literal
	// rather than against the bytes just marshalled above — those came from the current
	// type, so a rename that updated the struct and the key list together would read them
	// back happily while every stored row stopped parsing. The literal is the one thing
	// here a rename cannot quietly bring with it.
	//
	// Updating it is not forbidden, it is just not something anyone does by accident: it
	// means deciding what happens to the positions already written in the old shape.
	const storedPosition = `{"floor_date":"2026-08-19","backfill_until":"2026-08-19"}`

	pos, present, err := parseUsagePosition(&sdk.Cursor{Data: []byte(storedPosition)})
	require.NoError(t, err)
	require.True(t, present, "a stored position must not read as though no sync had ever run")
	assert.Equal(t, "2026-08-19", pos.Floor.Format(dateLayout))
	assert.Equal(t, "2026-08-19", pos.BackfillUntil.Format(dateLayout))
}

// TestUsagePosition_AbsentTargetOmitsTheKey pins the omitempty: an unset backfill target
// is absent on the wire, and a position without one parses to the zero time, which reads
// as a satisfied target rather than an outstanding one.
func TestUsagePosition_AbsentTargetOmitsTheKey(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &keyed))
	assert.NotContains(t, keyed, "backfill_until",
		"an unset target must be absent")

	const noTargetPosition = `{"floor_date":"2026-08-19"}`

	pos, present, err := parseUsagePosition(&sdk.Cursor{Data: []byte(noTargetPosition)})
	require.NoError(t, err)
	require.True(t, present)
	assert.True(t, pos.BackfillUntil.IsZero(),
		"and it must parse to the zero time, which is what makes it read as satisfied")
}

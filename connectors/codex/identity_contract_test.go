// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A record's identity must survive every field the vendor may rename. These tests prove it stays
// put when the vendor restates everything it is free to restate. They run the walk and compare
// what came out, which a static check of the source could not do: it would still pass if a future
// edit derived the key from a renameable field through a helper it could not follow.

// renameableFields are the values OpenAI may restate without telling us — a model identifier
// that arrives as a slug one week and a display name the next, a client identifier that changes
// when a tool is rebranded, and an address that changes when a person does.
//
// The catalogue records a sibling vendor returning the same model as both a slug and a display
// name, which is what makes this a live concern rather than a hypothetical one.
//
// The field NAMES here are the ones the pinned contract uses — `models` and `client_id`, not
// `model` and `client`. That distinction is the whole value of these fixtures: a row carrying a
// field the operation will never parse cannot detect the fork this test exists to prevent, so a
// near-miss name would leave the guard permanently green while doing nothing.
var renameableFields = []struct {
	name   string
	before string
	after  string
}{
	{
		name:   "the models breakdown",
		before: `"models":[{"model":"gpt-5-codex","tokens":10}]`,
		after:  `"models":[{"model":"GPT-5 Codex","tokens":10}]`,
	},
	{name: "the client identifier", before: `"client_id":"CODEX_CLI"`, after: `"client_id":"CODEX_WEB"`},
	{name: "the developer's address", before: `"actor":{"email":"a@example.com"}`, after: `"actor":{"email":"renamed@example.com"}`},
	{name: "the code attribution block", before: `"code_attribution":null`, after: `"code_attribution":{"lines":4}`},
	{name: "every counter", before: `"credits":1,"tokens":null`, after: `"credits":999,"tokens":42`},
}

// TestRecordIdentitySurvivesEveryRenameableField asserts that a record's identity survives
// every renameable field.
//
// A renameable component does not error when it moves. It changes the uniqueness tuple, so the
// record forks into a second row instead of updating the first — and every metric built on
// those rows double-counts, silently and permanently. The failure has no error path, which is
// why it needs a test that would catch it rather than a convention that says not to.
func TestRecordIdentitySurvivesEveryRenameableField(t *testing.T) {
	t.Parallel()

	day := yesterday()
	idsFor := func(t *testing.T, row string) []string {
		t.Helper()
		gw := newScriptedGateway().on(day, page(t, false, "", row))
		sink := &collectingSink{}
		_, err := (&FetchUsageOp{}).Execute(context.Background(),
			testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
				Settings{WorkspaceID: testWorkspace}))
		require.NoError(t, err)
		return sink.externalIDs()
	}

	for _, f := range renameableFields {
		t.Run(f.name+" changing must not move the record", func(t *testing.T) {
			t.Parallel()
			before := idsFor(t, `{"user_id":"u_1",`+f.before+`}`)
			after := idsFor(t, `{"user_id":"u_1",`+f.after+`}`)
			require.Len(t, before, 1)
			assert.Equal(t, before, after,
				"a vendor restating %s must update the stored row, not fork a second one", f.name)
		})
	}
}

// TestRecordIdentityMovesOnlyWithItsThreeComponents is the other half, and without it the test
// above is satisfied by an identity that never changes at all — a constant key would pass every
// assertion up there while collapsing an entire workspace into one row.
func TestRecordIdentityMovesOnlyWithItsThreeComponents(t *testing.T) {
	t.Parallel()

	day := yesterday()
	idFor := func(t *testing.T, workspace, developer string, d time.Time) string {
		t.Helper()
		gw := newScriptedGateway().on(d, page(t, false, "", fmt.Sprintf(`{"user_id":%q}`, developer)))
		sink := &collectingSink{}
		_, err := (&FetchUsageOp{}).Execute(context.Background(),
			testParams(t, gw, sink, cursorAt(t, d.AddDate(0, 0, -1).Format(dateLayout)),
				Settings{WorkspaceID: workspace}))
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		return sink.records[0].ExternalID
	}

	base := idFor(t, testWorkspace, "u_1", day)
	assert.NotEqual(t, base, idFor(t, "4d5e6f7a-8b9c-4d0e-9f1a-2b3c4d5e6f70", "u_1", day), "a different workspace is a different record")
	assert.NotEqual(t, base, idFor(t, testWorkspace, "u_2", day), "a different developer is a different record")
	assert.NotEqual(t, base, idFor(t, testWorkspace, "u_1", day.AddDate(0, 0, -1)), "a different day is a different record")
}

// TestOneDeveloperInTwoWorkspacesStaysTwoRecords is the reason the workspace is in the key at
// all, and neither sibling design faced it.
//
// A developer's identifier is an OpenAI ACCOUNT identifier rather than a workspace-local one,
// so the same person carries the same identifier in both workspaces. Without the workspace in
// the key, an organisation reading two of them — or re-pointed from one to the other — would
// have the second's rows overwrite the first's for the same developer and day, inside one
// tenant, with nothing to notice it.
func TestOneDeveloperInTwoWorkspacesStaysTwoRecords(t *testing.T) {
	t.Parallel()

	day := yesterday()
	collect := func(t *testing.T, workspace string) *sdk.RawRecord {
		t.Helper()
		gw := newScriptedGateway().on(day, page(t, false, "", `{"user_id":"same_person"}`))
		sink := &collectingSink{}
		_, err := (&FetchUsageOp{}).Execute(context.Background(),
			testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
				Settings{WorkspaceID: workspace}))
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		return sink.records[0]
	}

	a, b := collect(t, "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"), collect(t, "2b3c4d5e-6f7a-4b8c-9d0e-1f2a3b4c5d6e")
	assert.NotEqual(t, a.ExternalID, b.ExternalID,
		"one person in two workspaces must not collapse into one row on the same day")
	// Same person, same day: everything except the workspace agrees, which is exactly the
	// collision the workspace component prevents.
	assert.Equal(t, a.DataDate, b.DataDate)
	assert.Equal(t, a.Metadata["developer_id"], b.Metadata["developer_id"])
	assert.NotEqual(t, a.Metadata["workspace_id"], b.Metadata["workspace_id"])
}

// TestTheKeysDayAndTheBusinessDateNeverDisagree. The day sits in the uniqueness tuple twice —
// the key's trailing component and the DataDate column — so two derivations that disagreed
// would split one developer-day into two rows: the precise failure the identity exists to
// prevent. Asserted across a whole window rather than one day, because a single-day test
// cannot distinguish "computed once" from "computed twice, identically, today".
func TestTheKeysDayAndTheBusinessDateNeverDisagree(t *testing.T) {
	t.Parallel()

	last := yesterday()
	gw := newScriptedGateway()
	for i := 0; i < 6; i++ {
		gw.on(last.AddDate(0, 0, -i), page(t, false, "", `{"user_id":"u_1"}`))
	}
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, last.AddDate(0, 0, -6).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)
	require.NotEmpty(t, sink.records)

	for _, r := range sink.records {
		parts := strings.Split(r.ExternalID, RecordKeyDelimiter)
		require.Len(t, parts, 3, "the key is workspace, developer and day")
		assert.Equal(t, r.DataDate.Format(dateLayout), parts[2],
			"the key's day and the business date must be one value, not two derivations that happen to agree")
		assert.Equal(t, parts[2], r.Metadata["data_date"],
			"and the metadata copy must be that same value")
	}
}

// TestPayloadIsStoredWithoutInterpretation. Everything not named as identity stays exactly as
// the vendor sent it — a null counter stays null rather than becoming zero, and a field nobody
// has seen survives. A parser flattening either would understate every later aggregate, and no
// consumer could tell the difference afterwards because the upsert leaves no history.
func TestPayloadIsStoredWithoutInterpretation(t *testing.T) {
	t.Parallel()

	day := yesterday()
	row := `{"user_id":"u_1","credits":12,"tokens":null,"model_breakdown":null,` +
		`"nested":{"deep":[1,null,{"x":true}]},"unseen_field":"kept"}`
	gw := newScriptedGateway().on(day, page(t, false, "", row))
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)
	require.Len(t, sink.records, 1)

	// Byte equality, not JSONEq. JSONEq parses and normalises both sides, so a connector that
	// decoded the row and re-encoded it would pass — while having changed number
	// representations and collapsed duplicate keys before any consumer saw them. The contract
	// is the bytes as received, so the bytes are what is compared.
	assert.Equal(t, row, string(sink.records[0].Payload),
		"the payload must be the vendor's bytes verbatim, not a re-encoding that happens to be equivalent JSON")
	var got map[string]any
	require.NoError(t, json.Unmarshal(sink.records[0].Payload, &got))
	for _, k := range []string{"tokens", "model_breakdown"} {
		require.Contains(t, got, k)
		assert.Nilf(t, got[k], "%s is null in the response and must stay null — absent is not zero", k)
	}
	assert.Equal(t, "kept", got["unseen_field"])
}

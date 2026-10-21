// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

// TestSchema_FetchCodexUsage runs the operation over the row shapes it accepts and
// validates every emitted payload against schemas/codex/fetch_codex_usage.schema.json.
func TestSchema_FetchCodexUsage(t *testing.T) {
	t.Parallel()

	day := yesterday()
	rows := []string{
		fmt.Sprintf(`{"user_id":"u_1","bucket_start_time":%d,"actor":{"email":"a@example.com"},"credits":12,"tokens":null,"a_field_we_have_never_seen":{"x":1}}`, day.Unix()),
		`{"user_id":"u_2","actor":{"email":null},"messages":null}`,
		`{"user_id":"u_3","actor":null}`,
		`{"user_id":"u_4"}`,
	}
	gw := newScriptedGateway().on(day, page(t, false, "", rows...))
	sink := &collectingSink{}
	_, err := (&FetchUsageOp{}).Execute(context.Background(),
		testParams(t, gw, sink, cursorAt(t, day.AddDate(0, 0, -1).Format(dateLayout)),
			Settings{WorkspaceID: testWorkspace}))
	require.NoError(t, err)

	require.Len(t, sink.records, len(rows))
	for _, r := range sink.records {
		assert.NoError(t, schemas.Validate(r.ConnectorID, r.OperationID, r.Payload), r.ExternalID)
	}

	// The schema bites: a row the operation refuses does not validate.
	assert.Error(t, schemas.Validate("codex", "fetch_codex_usage", []byte(`{"user_id":""}`)))
}

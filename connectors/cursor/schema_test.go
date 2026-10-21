// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestSchema_FetchUsageEvents runs the operation over the event shapes the vendor is
// known to send and validates every emitted payload against
// schemas/cursor/fetch_usage_events.schema.json. A change to what the operation stores
// fails here before it reaches a consumer.
func TestSchema_FetchUsageEvents(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 18, 9, 15, 0, 0, time.UTC)
	ms := func(d time.Duration) int64 { return at.Add(d).UnixMilli() }
	// The live capture from internal/gateway/cursor/requests_test.go, the quirky event without a
	// token breakdown, and a minimal event without an email.
	full := json.RawMessage(fmt.Sprintf(`{"timestamp":"%d","userEmail":"svc@example.com",`+
		`"model":"gpt-5","kind":"Example plan","maxMode":false,"requestsCosts":1,`+
		`"isTokenBasedCall":true,"isChargeable":true,"isHeadless":false,`+
		`"tokenUsage":{"inputTokens":1000,"outputTokens":100,"cacheWriteTokens":0,"cacheReadTokens":200,"totalCents":2.5},`+
		`"chargedCents":2.5,"cursorTokenFee":0,"serviceAccountId":"null",`+
		`"conversationId":"00000000-0000-4000-8000-000000000000"}`, ms(0)))
	quirky := json.RawMessage(fmt.Sprintf(`{"timestamp":"%d","conversationId":"conv-q","userEmail":"svc@example.com",`+
		`"serviceAccountId":"null","kind":"Example plan","chargedCents":2.5}`, ms(time.Minute)))
	minimal := json.RawMessage(fmt.Sprintf(`{"timestamp":"%d","conversationId":"conv-m"}`, ms(2*time.Minute)))

	gw := &fakeGateway{pages: [][]json.RawMessage{{full, quirky, minimal, event("conv-a", "dev@example.com", at, "")}}}
	sink := testkit.NewChannelSink()
	cur, err := newUsagePositionCursor(usagePosition{FloorDate: "2026-08-19"})
	require.NoError(t, err)

	_, err = (&FetchUsageEventsOp{}).Execute(context.Background(), newParams(t, gw, sink, cur))
	require.NoError(t, err)

	records := sink.Records()
	require.Len(t, records, 4)
	for _, r := range records {
		assert.NoError(t, schemas.Validate(r.ConnectorID, r.OperationID, r.Payload), r.ExternalID)
	}

	// The schema bites: an event the operation would skip does not validate.
	assert.Error(t, schemas.Validate("cursor", "fetch_usage_events", []byte(`{"timestamp":1787184000000,"conversationId":"c"}`)))
}

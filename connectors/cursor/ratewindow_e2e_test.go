// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter/store"
	cursorgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/cursor"
	"github.com/tetriz-ai/tetriz-connectors/internal/reqctx"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// redirectDoer sends every request to the stub instead of api.cursor.com, leaving
// the gateway's own rate-limited doer wrapped around it untouched.
type redirectDoer struct {
	target string
	calls  atomic.Int64
}

func (d *redirectDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	// The request already carries its Authorization header, so a target that is not
	// loopback would send a real credential wherever it points. Refusing here means a
	// future edit fails loudly instead of exfiltrating one silently.
	if !strings.HasPrefix(d.target, "http://127.0.0.1:") && !strings.HasPrefix(d.target, "http://[::1]:") {
		return nil, fmt.Errorf("redirectDoer target %q is not loopback", d.target)
	}
	d.calls.Add(1)
	// Redirect in place rather than rebuilding: a fresh request would drop the
	// body, and the body is what carries the chunk's date range.
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(d.target, "http://")
	req.Host = ""
	return http.DefaultClient.Do(req)
}

// TestRateWindowEndToEnd drives the real operation through the real vendor gateway
// and the real Redis sliding-window limiter, with only the vendor stubbed. It is
// the deploy scenario reproduced locally: a first sync, a 90-day lookback sliced
// into chunkDays-wide chunks, requests issued back-to-back.
//
// Run with EVENTS_PER_CHUNK to model a busier org: above usagePageSize a chunk
// needs a second page, which is a second request inside the same window.
func TestRateWindowEndToEnd(t *testing.T) {
	perChunk := 3
	if v := os.Getenv("EVENTS_PER_CHUNK"); v != "" {
		_, err := fmt.Sscanf(v, "%d", &perChunk)
		require.NoError(t, err)
		require.Positivef(t, perChunk, "EVENTS_PER_CHUNK must be positive, got %d", perChunk)
	}

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			StartDate int64 `json:"startDate"`
			Page      int32 `json:"page"`
			PageSize  int32 `json:"pageSize"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		remaining := perChunk - int(body.Page-1)*usagePageSize
		if remaining < 0 {
			remaining = 0
		}
		if remaining > usagePageSize {
			remaining = usagePageSize
		}
		events := make([]json.RawMessage, 0, remaining)
		for i := 0; i < remaining; i++ {
			ms := body.StartDate + int64(i)*1000
			events = append(events, json.RawMessage(fmt.Sprintf(
				`{"timestamp":"%d","conversationId":"conv-%d-%d","userEmail":"dev@example.com"}`,
				ms, body.StartDate, int(body.Page)*usagePageSize+i)))
		}
		out, _ := json.Marshal(map[string]any{"usageEvents": events, "totalUsageEventsCount": perChunk})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
	defer stub.Close()

	addr := os.Getenv("TETRIZ_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if pingErr := rdb.Ping(context.Background()).Err(); pingErr != nil {
		// Naming the cause: "redis unreachable" and "redis rejected the connection"
		// need different fixes, and a bare skip hides which one this is.
		t.Skipf("local redis not usable, so the real limiter cannot be exercised: %v", pingErr)
	}
	defer func() { _ = rdb.Close() }()

	// The limiter keys on the org, so a per-run org id isolates this run's window
	// from any other without clearing shared state.
	orgID := fmt.Sprintf("org-e2e-%d", time.Now().UnixNano())

	doer := &redirectDoer{target: stub.URL}
	gw, err := cursorgw.NewCursorVendorGateway(doer, "key_test", store.NewSlidingWindowLogWithRedis(rdb))
	require.NoError(t, err)

	sink := testkit.NewChannelSink()
	params := &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{
			ConnectorID:    connectorID,
			OrganizationID: orgID,
			Credentials:    json.RawMessage(`{"api_key":"key_test"}`),
			Settings:       json.RawMessage(`{}`),
		},
		Sink:          sink,
		VendorGateway: gw,
	}

	// Only the orchestrator stamps the org onto the context, and this drives the
	// operation directly — without it the limiter keys on the shared group bucket
	// and a rerun inside the window inherits the previous run's exhaustion.
	ctx := reqctx.WithOrgID(context.Background(), orgID)
	// A deadline, because the operation now paces rather than failing on the first
	// rejection from our own limiter — and it only paces against a KNOWN budget, so a
	// deadline-less run refuses to wait. Without one this test's high-volume mode
	// (EVENTS_PER_CHUNK above a window's worth of pages) would stop exercising the
	// very behaviour it exists to reproduce. The scheduled path carries 30 minutes;
	// this mirrors it so the walk can spend real windows.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	start := time.Now()
	cur, execErr := (&FetchUsageEventsOp{}).Execute(ctx, params)

	floor := "(none)"
	if cur != nil {
		var pos struct {
			FloorDate string `json:"floor_date"`
		}
		if unmarshalErr := json.Unmarshal(cur.Data, &pos); unmarshalErr != nil {
			floor = fmt.Sprintf("(unreadable: %v)", unmarshalErr)
		} else {
			floor = pos.FloorDate
		}
	}
	outcome := "COMPLETED"
	if execErr != nil {
		outcome = "FAILED: " + execErr.Error()
	}
	t.Logf("RESULT events/chunk=%d vendor_calls=%d emitted=%d floor_after=%s elapsed=%s\n  %s",
		perChunk, doer.calls.Load(), int64(len(sink.Records())), floor, time.Since(start).Round(time.Millisecond), outcome)

	// Asserting, not just reporting: a log-only run passes while proving nothing,
	// which is the shape of the defect this whole reproduction exists to catch.
	require.NoError(t, execErr, "a first sync must finish in one run")
	require.Greaterf(t, doer.calls.Load(), int64(1),
		"only %d request reached the vendor — the window admitted one request and rejected the rest, "+
			"which is the failure this reproduces", doer.calls.Load())
	// Derived rather than assumed one-call-per-chunk: above usagePageSize a chunk
	// takes several pages while still emitting exactly perChunk, so the naive
	// calls*perChunk identity fails on correct behaviour.
	// The walk ends on a SHORT page, not a page count, so a chunk holding an exact
	// multiple of usagePageSize costs one extra empty page to observe the end.
	pagesPerChunk := int64(perChunk/usagePageSize + 1)
	emitted := int64(len(sink.Records()))
	require.Zero(t, emitted%int64(perChunk), "a covered chunk emits exactly its events")
	require.Equal(t, (emitted/int64(perChunk))*pagesPerChunk, doer.calls.Load(),
		"each covered chunk should account for exactly its own pages")
}

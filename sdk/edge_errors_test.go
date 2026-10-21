// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A kind or ingestion type outside the declared range must name itself as
// unknown, never panic: these String methods run inside log lines and metric
// labels, where a value decoded from a stored row or a proto is not range-checked.
func TestEdge_EnumStringOutOfRangeDoesNotPanic(t *testing.T) {
	for _, k := range []SyncErrorKind{-1, 99} {
		assert.NotPanics(t, func() { _ = k.String() }, "SyncErrorKind(%d).String()", int(k))
	}
	for _, it := range []IngestionType{-1, 99} {
		assert.NotPanics(t, func() { _ = it.String() }, "IngestionType(%d).String()", int(it))
	}
	assert.Equal(t, "unknown(99)", SyncErrorKind(99).String())
}

// The scheduler and repotally classify with errors.As / errors.Is through
// several layers of %w and errors.Join; the SyncError and the sentinel it
// wraps must both stay reachable through all of them.
func TestEdge_SyncErrorSurvivesDeepWrapping(t *testing.T) {
	inner := &SyncError{Kind: ErrKindRateLimit, Cause: fmt.Errorf("gateway: %w", ErrLocalRateLimit), RetryAfterSeconds: 7}
	wrapped := errors.Join(errors.New("other"), fmt.Errorf("op: %w", fmt.Errorf("page 3: %w", inner)))

	var se *SyncError
	require.ErrorAs(t, wrapped, &se)
	assert.Same(t, inner, se)
	assert.Equal(t, 7*time.Second, se.RetryAfter())
	assert.ErrorIs(t, wrapped, ErrLocalRateLimit)
	assert.Contains(t, wrapped.Error(), "(retry after 7s)")

	// A SyncError with no cause unwraps to nil, so errors.Is stops cleanly.
	bare := &SyncError{Kind: ErrKindAuth}
	assert.NoError(t, bare.Unwrap())
	assert.False(t, errors.Is(bare, context.Canceled))
}

// An empty cursor is stored as the JSON value null. The runner hands the next
// run a nil cursor instead (runner TestEmptyCursorResets), but an extractor
// given Data == "null" directly must still reject it like a nil or empty
// cursor rather than decode it as a zero timestamp / zero page / nil map.
func TestEdge_NullCursorDataIsRejectedLikeAnEmptyCursor(t *testing.T) {
	c := &Cursor{Data: json.RawMessage("null")}

	_, err := TimestampFromCursor(c)
	assert.Error(t, err, "TimestampFromCursor(null) returned a zero time and no error")

	_, _, err = PageFromCursor(c)
	assert.Error(t, err, "PageFromCursor(null) returned page 0 and no error")

	_, err = CompositeFromCursor(c)
	assert.Error(t, err, "CompositeFromCursor(null) returned a nil map and no error")
}

// A timestamp cursor built from a non-UTC time keeps the instant and is stored
// in UTC, so two runs in different process zones agree on the position.
func TestEdge_TimestampCursorNormalisesZoneButKeepsTheInstant(t *testing.T) {
	zone := time.FixedZone("UTC+2", 2*3600)
	local := time.Date(2026, 1, 1, 1, 0, 0, 123456789, zone) // 2025-12-31T23:00Z
	got, err := TimestampFromCursor(NewTimestampCursor(local))
	require.NoError(t, err)
	assert.True(t, got.Equal(local))
	assert.Equal(t, time.UTC, got.Location())
	assert.Equal(t, 2025, got.Year(), "the instant crosses the year boundary in UTC")
}

// Registration and lookup take locks; concurrent Register/Get/List/GetOperation
// must be race-free (run under -race) and each id lands exactly once.
func TestEdge_RegistryIsSafeForConcurrentUse(t *testing.T) {
	r := NewConnectorRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			r.Register(&edgeConnector{id: ConnectorID(fmt.Sprintf("c%02d", i))})
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = r.Get(ConnectorID(fmt.Sprintf("c%02d", i)))
			_ = r.List()
			_, _ = r.GetOperation(ConnectorID(fmt.Sprintf("c%02d", i)), "op")
		}(i)
	}
	wg.Wait()
	assert.Len(t, r.List(), 32)
	_, err := r.GetOperation("c05", "missing")
	assert.ErrorContains(t, err, "operation missing not found in connector c05")
	_, err = r.GetOperation("nope", "op")
	assert.ErrorContains(t, err, "connector not found: nope")
}

type edgeConnector struct{ id ConnectorID }

func (c *edgeConnector) ID() ConnectorID                 { return c.id }
func (c *edgeConnector) Categories() []ConnectorCategory { return []ConnectorCategory{CategoryVCS} }
func (c *edgeConnector) DisplayName() string             { return string(c.id) }
func (c *edgeConnector) Operations() []OperationDescriptor {
	return []OperationDescriptor{{ID: "op"}}
}
func (c *edgeConnector) ValidateConfig(context.Context, *ConnectorConfig) error { return nil }

// A Retry-After hint too large for a time.Duration is clamped, not
// overflowed into a negative wait.
func TestRetryAfterDoesNotOverflow(t *testing.T) {
	e := &SyncError{RetryAfterSeconds: math.MaxInt}
	assert.Equal(t, time.Duration(math.MaxInt64), e.RetryAfter())
	assert.Equal(t, 90*time.Second, (&SyncError{RetryAfterSeconds: 90}).RetryAfter())
	assert.Zero(t, (&SyncError{RetryAfterSeconds: -5}).RetryAfter())
}

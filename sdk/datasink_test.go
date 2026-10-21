// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/async/goroutine"
	"github.com/tetriz-ai/tetriz-connectors/internal/async/waitgroup"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

// Compile-time check that ChannelSink satisfies the DataSink interface.
var _ sdk.DataSink = (*testkit.ChannelSink)(nil)

func newTestRecord(externalID string) *sdk.RawRecord {
	return &sdk.RawRecord{
		ConnectorID:    "github",
		OperationID:    "fetch_prs",
		OrganizationID: "org_1",
		ExternalID:     externalID,
		Payload:        json.RawMessage(`{"key":"value"}`),
		FetchedAt:      time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
	}
}

func TestChannelSink_Emit(t *testing.T) {
	type args struct {
		records []*sdk.RawRecord
	}
	tests := []struct {
		name      string
		args      args
		wantCount int
		wantErr   bool
	}{
		{
			name:      "emits a single record",
			args:      args{records: []*sdk.RawRecord{newTestRecord("r1")}},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name: "emits multiple records sequentially",
			args: args{records: []*sdk.RawRecord{
				newTestRecord("r1"),
				newTestRecord("r2"),
				newTestRecord("r3"),
			}},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name:      "emits nil record without erroring",
			args:      args{records: []*sdk.RawRecord{nil}},
			wantCount: 1,
			wantErr:   false,
		},
		{
			name:      "no emissions yields empty buffer",
			args:      args{records: nil},
			wantCount: 0,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			var sink2 sdk.DataSink = sink

			var err error
			for _, r := range tt.args.records {
				err = sink2.Emit(context.Background(), r)
				if err != nil {
					break
				}
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Emit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Len(t, sink.Records(), tt.wantCount)
		})
	}
}

func TestChannelSink_EmitBatch(t *testing.T) {
	type args struct {
		batches [][]*sdk.RawRecord
	}
	tests := []struct {
		name      string
		args      args
		wantCount int
		wantErr   bool
	}{
		{
			name: "emits a single batch",
			args: args{batches: [][]*sdk.RawRecord{
				{newTestRecord("r1"), newTestRecord("r2")},
			}},
			wantCount: 2,
			wantErr:   false,
		},
		{
			name: "emits multiple batches cumulatively",
			args: args{batches: [][]*sdk.RawRecord{
				{newTestRecord("r1")},
				{newTestRecord("r2"), newTestRecord("r3")},
			}},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name:      "emits empty batch as no-op",
			args:      args{batches: [][]*sdk.RawRecord{{}}},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name:      "emits nil batch as no-op",
			args:      args{batches: [][]*sdk.RawRecord{nil}},
			wantCount: 0,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := testkit.NewChannelSink()
			var sink2 sdk.DataSink = sink

			var err error
			for _, b := range tt.args.batches {
				err = sink2.EmitBatch(context.Background(), b)
				if err != nil {
					break
				}
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("EmitBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Len(t, sink.Records(), tt.wantCount)
		})
	}
}

func TestChannelSink_Flush(t *testing.T) {
	sink := testkit.NewChannelSink()
	var sink2 sdk.DataSink = sink

	require.NoError(t, sink2.Emit(context.Background(), newTestRecord("r1")))
	require.NoError(t, sink2.Flush(context.Background()))

	// Flush is a no-op — records must remain after flush.
	assert.Len(t, sink.Records(), 1)
}

func TestChannelSink_Drain(t *testing.T) {
	sink := testkit.NewChannelSink()
	ctx := context.Background()

	require.NoError(t, sink.EmitBatch(ctx, []*sdk.RawRecord{
		newTestRecord("r1"),
		newTestRecord("r2"),
	}))

	drained := sink.Drain()
	assert.Len(t, drained, 2)
	assert.Empty(t, sink.Records(), "Drain should clear the buffer")

	require.NoError(t, sink.Emit(ctx, newTestRecord("r3")))
	assert.Len(t, sink.Records(), 1, "sink must be usable after Drain")
}

func TestChannelSink_Records_ReturnsCopy(t *testing.T) {
	sink := testkit.NewChannelSink()
	require.NoError(t, sink.Emit(context.Background(), newTestRecord("r1")))

	snapshot := sink.Records()
	require.NoError(t, sink.Emit(context.Background(), newTestRecord("r2")))

	// Mutating later should not affect the earlier snapshot.
	assert.Len(t, snapshot, 1)
	assert.Len(t, sink.Records(), 2)
}

func TestChannelSink_Concurrent_Emit(t *testing.T) {
	sink := testkit.NewChannelSink()
	ctx := context.Background()

	const (
		writers         = 16
		emitsPerWriter  = 50
		batchesPerMixer = 10
		batchSize       = 5
	)

	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		goroutine.RunWithDefaultTimeout(ctx, func(_ context.Context) {
			defer wg.Done()
			for range emitsPerWriter {
				_ = sink.Emit(ctx, newTestRecord("r"))
			}
			for range batchesPerMixer {
				batch := make([]*sdk.RawRecord, batchSize)
				for k := range batchSize {
					batch[k] = newTestRecord("b")
				}
				_ = sink.EmitBatch(ctx, batch)
			}
		})
	}
	waitgroup.SafeWaitWithDefaultTimeout(&wg)

	want := writers * (emitsPerWriter + batchesPerMixer*batchSize)
	assert.Len(t, sink.Records(), want)
}

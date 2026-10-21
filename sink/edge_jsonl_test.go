// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Many emitters sharing one JSONL file publisher must never interleave bytes
// inside a line: every line is one whole envelope, and none is lost.
func TestEdge_JSONLConcurrentWritersProduceWholeLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	s := NewConnectorSink(NewJSONLPublisher(f))

	const writers, perWriter = 16, 40
	// A payload large enough that an unserialised write could tear.
	big := json.RawMessage(`{"body":"` + strings.Repeat("x", 8000) + `"}`)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				r := &sdk.RawRecord{ConnectorID: "c", OperationID: "op", OrganizationID: "org", ExternalID: fmt.Sprintf("%d-%d", w, i), Payload: big, FetchedAt: time.Unix(0, 0).UTC()}
				assert.NoError(t, s.Emit(context.Background(), r))
			}
		}()
	}
	wg.Wait()
	require.NoError(t, f.Close())

	rf, err := os.Open(path)
	require.NoError(t, err)
	defer rf.Close() //nolint:errcheck // test read
	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	seen := map[string]bool{}
	for sc.Scan() {
		var env connectorpb.ConnectorDataEnvelope
		require.NoError(t, protojson.Unmarshal(sc.Bytes(), &env), "every line is one whole envelope")
		require.Len(t, env.Events, 1)
		seen[env.Events[0].ExternalId] = true
	}
	require.NoError(t, sc.Err())
	assert.Len(t, seen, writers*perWriter, "no record is lost or duplicated")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }

// A writer that fails (a closed file, a full disk) surfaces as an Emit error
// that still names the cause, so the run fails as a sink failure instead of
// reporting records it never wrote.
func TestEdge_JSONLWriteFailureIsReturned(t *testing.T) {
	s := NewConnectorSink(NewJSONLPublisher(failingWriter{}))
	err := s.Emit(context.Background(), &sdk.RawRecord{ConnectorID: "c", OperationID: "op", OrganizationID: "org", ExternalID: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrClosed)
}

// A nil record in the middle of a batch stops the batch there: the records
// before it are published, nothing after it is.
func TestEdge_EmitBatchStopsAtANilRecord(t *testing.T) {
	pub := &mockQueuePublisher{}
	s := NewConnectorSink(pub)
	err := s.EmitBatch(context.Background(), []*sdk.RawRecord{
		newTestRecord("c", "op", "org", "a"), nil, newTestRecord("c", "op", "org", "b"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be nil")
	assert.Equal(t, 1, pub.callCount())
}

// Unicode identities and metadata survive the JSONL round trip byte for byte,
// a zero DataDate is omitted rather than sent as year 1, and a set one is kept.
func TestEdge_JSONLRoundTripsUnicodeAndDates(t *testing.T) {
	var buf bytes.Buffer
	s := NewConnectorSink(NewJSONLPublisher(&buf))
	day := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
	recs := []*sdk.RawRecord{
		{ConnectorID: "c", OperationID: "op", OrganizationID: "org", ExternalID: "ветка/日本語/😀", Metadata: map[string]string{"名前": "値 "}, Payload: json.RawMessage(`{"t":"é"}`)},
		{ConnectorID: "c", OperationID: "op", OrganizationID: "org", ExternalID: "dated", DataDate: day},
	}
	require.NoError(t, s.EmitBatch(context.Background(), recs))

	sc := bufio.NewScanner(&buf)
	var envs []*connectorpb.ConnectorDataEnvelope
	for sc.Scan() {
		var env connectorpb.ConnectorDataEnvelope
		require.NoError(t, protojson.Unmarshal(sc.Bytes(), &env))
		envs = append(envs, &env)
	}
	require.Len(t, envs, 2, "U+2028 inside a value must not split a line")
	e := envs[0].Events[0]
	assert.Equal(t, "ветка/日本語/😀", e.ExternalId)
	assert.Equal(t, "値 ", e.Metadata["名前"])
	assert.JSONEq(t, `{"t":"é"}`, string(e.Payload))
	assert.Nil(t, e.DataDate, "a zero DataDate is not sent")
	require.NotNil(t, envs[1].Events[0].DataDate)
	assert.True(t, day.Equal(envs[1].Events[0].DataDate.AsTime()))
}

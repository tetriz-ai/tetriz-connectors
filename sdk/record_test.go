// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawRecord_JSONRoundTrip(t *testing.T) {
	type args struct {
		record RawRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "round-trips with full payload",
			args: args{
				record: RawRecord{
					ConnectorID:    "github",
					OperationID:    "fetch_prs",
					OrganizationID: "org_123",
					ExternalID:     "pr-42",
					Payload:        json.RawMessage(`{"title":"fix bug","number":42}`),
					FetchedAt:      time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
					Metadata:       map[string]string{"repo": "acme/widgets"},
				},
			},
			wantErr: false,
		},
		{
			name: "round-trips with nil metadata",
			args: args{
				record: RawRecord{
					ConnectorID:    "slack",
					OperationID:    "fetch_messages",
					OrganizationID: "org_456",
					ExternalID:     "msg-1",
					Payload:        json.RawMessage(`{"text":"hello"}`),
					FetchedAt:      time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			wantErr: false,
		},
		{
			name: "round-trips with nil payload as JSON null",
			args: args{
				record: RawRecord{
					ConnectorID:    "test",
					OperationID:    "test_op",
					OrganizationID: "org_1",
					ExternalID:     "id-1",
					Payload:        nil,
					FetchedAt:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.args.record)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			var decoded RawRecord
			err = json.Unmarshal(data, &decoded)
			require.NoError(t, err)
			assert.Equal(t, tt.args.record.ConnectorID, decoded.ConnectorID)
			assert.Equal(t, tt.args.record.OperationID, decoded.OperationID)
			assert.Equal(t, tt.args.record.OrganizationID, decoded.OrganizationID)
			assert.Equal(t, tt.args.record.ExternalID, decoded.ExternalID)
			assert.True(t, tt.args.record.FetchedAt.Equal(decoded.FetchedAt))

			if tt.args.record.Payload != nil {
				assert.JSONEq(t, string(tt.args.record.Payload), string(decoded.Payload))
			}
		})
	}
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubConnector is a minimal Connector implementation for testing.
type stubConnector struct {
	id   ConnectorID
	name string
	ops  []OperationDescriptor
}

func (s *stubConnector) ID() ConnectorID { return s.id }
func (s *stubConnector) Categories() []ConnectorCategory {
	return []ConnectorCategory{CategoryVCS}
}
func (s *stubConnector) DisplayName() string                                        { return s.name }
func (s *stubConnector) Operations() []OperationDescriptor                          { return s.ops }
func (s *stubConnector) ValidateConfig(_ context.Context, _ *ConnectorConfig) error { return nil }

func TestConnectorRegistry_Register_And_Get(t *testing.T) {
	type args struct {
		connectors []*stubConnector
		getID      ConnectorID
	}
	tests := []struct {
		name    string
		args    args
		want    ConnectorID
		wantErr bool
	}{
		{
			name: "registers and retrieves a connector",
			args: args{
				connectors: []*stubConnector{
					{id: "github", name: "GitHub"},
				},
				getID: "github",
			},
			want:    "github",
			wantErr: false,
		},
		{
			name: "returns error for unknown connector",
			args: args{
				connectors: []*stubConnector{
					{id: "github", name: "GitHub"},
				},
				getID: "nonexistent",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := NewConnectorRegistry()
			for _, c := range tt.args.connectors {
				reg.Register(c)
			}

			got, err := reg.Get(tt.args.getID)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.ID())
		})
	}
}

func TestConnectorRegistry_Register_DuplicatePanics(t *testing.T) {
	reg := NewConnectorRegistry()
	c := &stubConnector{id: "github", name: "GitHub"}
	reg.Register(c)

	assert.Panics(t, func() {
		reg.Register(c)
	})
}

func TestConnectorRegistry_List(t *testing.T) {
	reg := NewConnectorRegistry()
	reg.Register(&stubConnector{id: "github", name: "GitHub"})
	reg.Register(&stubConnector{id: "slack", name: "Slack"})

	got := reg.List()
	assert.Len(t, got, 2)
}

func TestConnectorRegistry_GetOperation(t *testing.T) {
	type args struct {
		connectorID ConnectorID
		opID        OperationID
	}
	tests := []struct {
		name    string
		args    args
		want    OperationID
		wantErr bool
	}{
		{
			name:    "returns operation descriptor",
			args:    args{connectorID: "github", opID: "fetch_prs"},
			want:    "fetch_prs",
			wantErr: false,
		},
		{
			name:    "returns error for unknown operation",
			args:    args{connectorID: "github", opID: "nonexistent"},
			wantErr: true,
		},
		{
			name:    "returns error for unknown connector",
			args:    args{connectorID: "nonexistent", opID: "fetch_prs"},
			wantErr: true,
		},
	}

	reg := NewConnectorRegistry()
	reg.Register(&stubConnector{
		id:   "github",
		name: "GitHub",
		ops: []OperationDescriptor{
			{ID: "fetch_prs", DisplayName: "Fetch Pull Requests"},
		},
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reg.GetOperation(tt.args.connectorID, tt.args.opID)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.ID)
		})
	}
}

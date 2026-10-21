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

func TestNewTimestampCursor(t *testing.T) {
	type args struct {
		t time.Time
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "creates cursor from timestamp",
			args: args{t: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)},
		},
		{
			name: "creates cursor from zero time",
			args: args{t: time.Time{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := NewTimestampCursor(tt.args.t)
			assert.NotNil(t, cursor)
			assert.NotEmpty(t, cursor.Data)
			assert.False(t, cursor.UpdatedAt.IsZero())
		})
	}
}

func TestTimestampFromCursor(t *testing.T) {
	refTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	type args struct {
		cursor *Cursor
	}
	tests := []struct {
		name    string
		args    args
		want    time.Time
		wantErr bool
	}{
		{
			name:    "round-trips timestamp correctly",
			args:    args{cursor: NewTimestampCursor(refTime)},
			want:    refTime,
			wantErr: false,
		},
		{
			name:    "returns error for nil cursor",
			args:    args{cursor: nil},
			wantErr: true,
		},
		{
			name:    "returns error for empty data",
			args:    args{cursor: &Cursor{Data: nil}},
			wantErr: true,
		},
		{
			name:    "returns error for malformed JSON",
			args:    args{cursor: &Cursor{Data: json.RawMessage(`{bad`)}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TimestampFromCursor(tt.args.cursor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got))
		})
	}
}

func TestPageCursor_RoundTrip(t *testing.T) {
	type args struct {
		page      int
		pageToken string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "round-trips page and token",
			args: args{page: 5, pageToken: "abc123"},
		},
		{
			name: "round-trips with empty token",
			args: args{page: 0, pageToken: ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := NewPageCursor(tt.args.page, tt.args.pageToken)
			assert.NotNil(t, cursor)

			gotPage, gotToken, err := PageFromCursor(cursor)
			require.NoError(t, err)
			assert.Equal(t, tt.args.page, gotPage)
			assert.Equal(t, tt.args.pageToken, gotToken)
		})
	}
}

func TestPageFromCursor_Errors(t *testing.T) {
	type args struct {
		cursor *Cursor
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "returns error for nil cursor",
			args:    args{cursor: nil},
			wantErr: true,
		},
		{
			name:    "returns error for empty data",
			args:    args{cursor: &Cursor{Data: nil}},
			wantErr: true,
		},
		{
			name:    "returns error for malformed JSON",
			args:    args{cursor: &Cursor{Data: json.RawMessage(`{bad`)}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := PageFromCursor(tt.args.cursor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestCompositeCursor_RoundTrip(t *testing.T) {
	parts := map[string]json.RawMessage{
		"prs":    json.RawMessage(`{"page":3}`),
		"issues": json.RawMessage(`{"since":"2024-01-01"}`),
	}

	cursor := NewCompositeCursor(parts)
	assert.NotNil(t, cursor)

	gotParts, err := CompositeFromCursor(cursor)
	require.NoError(t, err)
	assert.Len(t, gotParts, 2)
	assert.JSONEq(t, `{"page":3}`, string(gotParts["prs"]))
	assert.JSONEq(t, `{"since":"2024-01-01"}`, string(gotParts["issues"]))
}

func TestCompositeFromCursor_Errors(t *testing.T) {
	type args struct {
		cursor *Cursor
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "returns error for nil cursor",
			args:    args{cursor: nil},
			wantErr: true,
		},
		{
			name:    "returns error for empty data",
			args:    args{cursor: &Cursor{Data: nil}},
			wantErr: true,
		},
		{
			name:    "returns error for malformed JSON",
			args:    args{cursor: &Cursor{Data: json.RawMessage(`not-json`)}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompositeFromCursor(tt.args.cursor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

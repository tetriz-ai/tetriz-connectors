// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasCategory(t *testing.T) {
	type args struct {
		connector Connector
		category  ConnectorCategory
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "matches category when connector has it",
			args: args{
				connector: &stubConnector{id: "github", name: "GitHub"},
				category:  CategoryVCS,
			},
			want: true,
		},
		{
			name: "does not match when connector lacks category",
			args: args{
				connector: &stubConnector{id: "github", name: "GitHub"},
				category:  CategoryMessaging,
			},
			want: false,
		},
		{
			name: "returns false for nil connector",
			args: args{connector: nil, category: CategoryVCS},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasCategory(tt.args.connector, tt.args.category)
			assert.Equal(t, tt.want, got)
		})
	}
}

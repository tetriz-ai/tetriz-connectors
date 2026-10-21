// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gitlab

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitProjectID(t *testing.T) {
	type args struct {
		projectID int64
	}
	tests := []struct {
		name          string
		args          args
		wantGroupID   int64
		wantRepoIndex int
	}{
		{
			name:          "9100101 maps to group 91001 repo index 1",
			args:          args{projectID: 9100101},
			wantGroupID:   91001,
			wantRepoIndex: 1,
		},
		{
			name:          "9100203 maps to group 91002 repo index 3",
			args:          args{projectID: 9100203},
			wantGroupID:   91002,
			wantRepoIndex: 3,
		},
		{
			name:          "9100301 maps to group 91003 repo index 1",
			args:          args{projectID: 9100301},
			wantGroupID:   91003,
			wantRepoIndex: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGroup, gotIndex := splitProjectID(tt.args.projectID)
			assert.Equal(t, tt.wantGroupID, gotGroup)
			assert.Equal(t, tt.wantRepoIndex, gotIndex)
		})
	}
}

func TestOrgNameForProject(t *testing.T) {
	type args struct {
		projectID int64
	}
	tests := []struct {
		name      string
		args      args
		wantOrg   string
		wantFound bool
	}{
		{name: "known project 9100101 is demo-gl-regulated", args: args{projectID: 9100101}, wantOrg: "demo-gl-regulated", wantFound: true},
		{name: "zero repo index is not a valid encoding", args: args{projectID: 9100100}, wantFound: false},
		{name: "unknown group 404s", args: args{projectID: 1234501}, wantFound: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := orgNameForProject(tt.args.projectID)
			if found != tt.wantFound {
				t.Fatalf("found = %v, want %v", found, tt.wantFound)
			}
			if found {
				assert.Equal(t, tt.wantOrg, got)
			}
		})
	}
}

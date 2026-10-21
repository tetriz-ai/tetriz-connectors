// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"testing"
)

func TestIsSimulatorOrg(t *testing.T) {
	tests := []struct {
		name  string
		ids   []string
		orgID string
		want  bool
	}{
		{
			name:  "org ID in list",
			ids:   []string{"demo-org-1", "demo-org-2"},
			orgID: "demo-org-1",
			want:  true,
		},
		{
			name:  "second entry matches",
			ids:   []string{"demo-org-1", "demo-org-2"},
			orgID: "demo-org-2",
			want:  true,
		},
		{
			name:  "org ID not in list",
			ids:   []string{"demo-org-1"},
			orgID: "real-org-3",
			want:  false,
		},
		{
			name:  "empty list",
			ids:   []string{},
			orgID: "any-org",
			want:  false,
		},
		{
			name:  "nil list",
			ids:   nil,
			orgID: "any-org",
			want:  false,
		},
		{
			name:  "partial prefix does not match",
			ids:   []string{"demo-org"},
			orgID: "demo-org-extra",
			want:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{SimulatorOrgIDs: tt.ids}
			if got := cfg.IsSimulatorOrg(tt.orgID); got != tt.want {
				t.Errorf("IsSimulatorOrg(%q) = %v, want %v", tt.orgID, got, tt.want)
			}
		})
	}
}

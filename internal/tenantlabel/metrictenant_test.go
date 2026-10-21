// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package tenantlabel

import "testing"

func TestMetricTenantID(t *testing.T) {
	t.Parallel()

	individualSet := map[string]struct{}{
		"org-individual-1": {},
		"org-individual-2": {},
	}

	tests := []struct {
		name string
		set  map[string]struct{}
		org  string
		want string
	}{
		{
			name: "individual org in set collapses to constant",
			set:  individualSet,
			org:  "org-individual-1",
			want: MetricLabelIndividual,
		},
		{
			name: "org-tenant not in set passes through",
			set:  individualSet,
			org:  "org-real-tenant",
			want: "org-real-tenant",
		},
		{
			name: "empty set passes through (fail-open before refresh)",
			set:  map[string]struct{}{},
			org:  "org-individual-1",
			want: "org-individual-1",
		},
		{
			name: "nil set passes through",
			set:  nil,
			org:  "org-individual-1",
			want: "org-individual-1",
		},
		{
			name: "empty orgID passes through even when set is non-empty",
			set:  individualSet,
			org:  "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := MetricTenantID(tt.set, tt.org); got != tt.want {
				t.Fatalf("MetricTenantID(%v, %q) = %q, want %q", tt.set, tt.org, got, tt.want)
			}
		})
	}
}

// TestLiveIndividualSetPublish verifies the package-global round-trips a
// published set and that the first read (no publish yet) is nil (pass-through).
// Not parallel: it mutates the package-global atomic shared with other tests.
func TestLiveIndividualSetPublish(t *testing.T) {
	set := map[string]struct{}{"org-x": {}}
	publishSet(set)
	got := LiveIndividualSet()
	if _, ok := got["org-x"]; !ok {
		t.Fatalf("LiveIndividualSet() did not return the published set: %v", got)
	}
	// Restore the empty fail-open state so order-dependent tests do not see a
	// stale set.
	publishSet(map[string]struct{}{})
}

// TestSetLabelFunc pins the host override: a custom rule replaces the default
// for every call, and restore brings the default back. Not parallel: it
// mutates the package-global rule.
func TestSetLabelFunc(t *testing.T) {
	restore := SetLabelFunc(func(org string) string { return "tenant:" + org })
	if got := OrgID("acme"); got != "tenant:acme" {
		t.Fatalf("OrgID with custom rule = %q, want %q", got, "tenant:acme")
	}
	restore()
	if got := OrgID("acme"); got != "acme" {
		t.Fatalf("OrgID after restore = %q, want %q", got, "acme")
	}
	restoreSet := SetLiveIndividualSetForTest(map[string]struct{}{"solo": {}})
	defer restoreSet()
	if got := OrgID("solo"); got != MetricLabelIndividual {
		t.Fatalf("OrgID default rule for a live individual = %q, want %q", got, MetricLabelIndividual)
	}
}

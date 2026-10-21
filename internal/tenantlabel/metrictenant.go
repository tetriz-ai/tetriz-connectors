// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package tenantlabel turns an organization id into the value of the org_id
// label on the repository's Prometheus metrics.
//
// By default the label is the organization id itself, except that ids in the
// live-individual set (published by the host with SetLiveIndividualSet)
// collapse to MetricLabelIndividual, so many single-user tenants do not fan a
// per-org metric out to one series each. A host with its own labelling
// scheme replaces the whole rule with SetLabelFunc.
package tenantlabel

import "sync/atomic"

// MetricLabelIndividual is the collapsed label value substituted for any org ID
// that belongs to a live individual tenant. The double-underscore avoids any
// collision with a real org UUID.
const MetricLabelIndividual = "__individual__"

// LabelFunc maps an organization id to its metric label value.
type LabelFunc func(orgID string) string

// liveIndividualSet holds the current live-individual org-ID set. The stored
// value is always a read-only map[string]struct{} (or nil before the first
// publish); callers must never mutate it.
var liveIndividualSet atomic.Value

// labelFunc holds the active LabelFunc; nil means the default rule.
var labelFunc atomic.Value

// LiveIndividualSet returns the current live-individual org-ID set for passing to
// MetricTenantID. The returned map is read-only and must not be mutated. Returns
// nil before any set has been published (nil means pass-through).
func LiveIndividualSet() map[string]struct{} {
	s, _ := liveIndividualSet.Load().(map[string]struct{})
	return s
}

// SetLiveIndividualSet publishes the set of organization ids whose label
// collapses to MetricLabelIndividual under the default rule. The map must not
// be mutated after the call.
func SetLiveIndividualSet(set map[string]struct{}) {
	publishSet(set)
}

func publishSet(set map[string]struct{}) {
	liveIndividualSet.Store(set)
}

// SetLabelFunc replaces the labelling rule for every metric in the
// repository and returns a function that restores the previous rule. A nil f
// restores the default rule.
func SetLabelFunc(f LabelFunc) (restore func()) {
	prev, _ := labelFunc.Load().(LabelFunc)
	labelFunc.Store(f)
	return func() { labelFunc.Store(prev) }
}

// MetricTenantID returns the collapsed metric label for orgID: the constant
// MetricLabelIndividual when orgID is in the live-individual set, otherwise orgID
// unchanged. An empty set (nil) or an empty orgID always passes through.
func MetricTenantID(individualSet map[string]struct{}, orgID string) string {
	if orgID == "" || len(individualSet) == 0 {
		return orgID
	}
	if _, ok := individualSet[orgID]; ok {
		return MetricLabelIndividual
	}
	return orgID
}

// OrgID returns the metric label for orgID under the active rule. Every
// org_id-labelled emission in the repository wraps its org id through it.
func OrgID(orgID string) string {
	if f, _ := labelFunc.Load().(LabelFunc); f != nil {
		return f(orgID)
	}
	return MetricTenantID(LiveIndividualSet(), orgID)
}

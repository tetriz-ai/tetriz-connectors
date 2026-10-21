// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"fmt"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"
)

// HostService names one of the services in HostServices. An operation lists
// the ones it cannot run without in OperationDescriptor.RequiresHost.
type HostService string

// The host services. Each value is the name used in error messages and in
// the `connectors` listing.
const (
	// HostWorkQueue is HostServices.WorkQueue.
	HostWorkQueue HostService = "work_queue"
	// HostTrackedRefs is HostServices.TrackedRefs.
	HostTrackedRefs HostService = "tracked_refs"
	// HostBlameSink is HostServices.BlameSink.
	HostBlameSink HostService = "blame_sink"
	// HostLineageWalker is HostServices.LineageWalker.
	HostLineageWalker HostService = "lineage_walker"
	// HostDefectScorer is HostServices.DefectScorer.
	HostDefectScorer HostService = "defect_scorer"
)

// HostServices are the optional services only an embedding host can
// provide: work lists the host owns and the defect-lineage logic. The
// command (`serve` and `sync`) provides none of them. Each field may be nil.
//
// The orchestrator refuses to run an operation whose
// OperationDescriptor.RequiresHost names a service that is nil here, and the
// schedule source does not schedule it.
type HostServices struct {
	// WorkQueue lists the pull requests whose diffs and the files whose
	// contents need fetching, and takes the results.
	WorkQueue WorkQueue
	// TrackedRefs names the branches of each repository that count. Optional
	// for every built-in operation: without it they read the default branch.
	TrackedRefs TrackedRefsResolver
	// BlameSink lists the bug fixes to trace back to the change that
	// introduced the bug (defect lineage), and takes the results.
	BlameSink lineage.BlameSink
	// LineageWalker runs the blame walk for one fix.
	LineageWalker lineage.Walker
	// DefectScorer scores each candidate change against the fix.
	DefectScorer lineage.Scorer
}

// Has reports whether the service s is set.
func (h HostServices) Has(s HostService) bool {
	switch s {
	case HostWorkQueue:
		return h.WorkQueue != nil
	case HostTrackedRefs:
		return h.TrackedRefs != nil
	case HostBlameSink:
		return h.BlameSink != nil
	case HostLineageWalker:
		return h.LineageWalker != nil
	case HostDefectScorer:
		return h.DefectScorer != nil
	default:
		return false
	}
}

// Provided returns the services that are set, in declaration order.
func (h HostServices) Provided() []HostService {
	var out []HostService
	for _, s := range []HostService{HostWorkQueue, HostTrackedRefs, HostBlameSink, HostLineageWalker, HostDefectScorer} {
		if h.Has(s) {
			out = append(out, s)
		}
	}
	return out
}

// Missing returns the services in required that are not set, in the order
// required lists them. An unknown name is always missing.
func (h HostServices) Missing(required []HostService) []HostService {
	var out []HostService
	for _, s := range required {
		if !h.Has(s) {
			out = append(out, s)
		}
	}
	return out
}

// MissingHostServicesError is the error for an operation run without host
// services it requires: kind config_invalid and terminal, because retrying
// cannot provide them. Its message names the operation and the services, as
// in "operation github/fetch_pr_diffs needs host services: work_queue".
func MissingHostServicesError(connectorID ConnectorID, opID OperationID, orgID string, missing []HostService) *SyncError {
	names := make([]string, len(missing))
	for i, s := range missing {
		names[i] = string(s)
	}
	return &SyncError{
		ConnectorID:    connectorID,
		OperationID:    opID,
		OrganizationID: orgID,
		Kind:           ErrKindConfigInvalid,
		Terminal:       true,
		Cause: fmt.Errorf("operation %s/%s needs host services: %s",
			connectorID, opID, strings.Join(names, ", ")),
	}
}

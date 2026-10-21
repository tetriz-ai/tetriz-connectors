// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schedule

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSchedulerGaugesRegistered verifies the early-warning gauges register
// on the default registry without panic and expose their expected names. Package
// init (promauto) does the registration; this test fails loudly if a name
// collides or a metric is dropped.
func TestSchedulerGaugesRegistered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		metricName string
		emit       func()
	}{
		{
			name:       "refresh duration histogram",
			metricName: "scheduler_refresh_duration_seconds",
			emit:       func() { schedulerRefreshDurationSeconds.Observe(0.1) },
		},
		{
			name:       "periodic tasks gauge",
			metricName: "scheduler_periodic_tasks",
			emit:       func() { schedulerPeriodicTasks.Set(7) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.emit()
			got, err := testutil.GatherAndCount(prometheus.DefaultGatherer, tt.metricName)
			if err != nil {
				t.Fatalf("gather metric %q: %v", tt.metricName, err)
			}
			if got == 0 {
				t.Fatalf("metric %q not registered / not collectible", tt.metricName)
			}
		})
	}
}

// TestSchedulerMetricNames guards the naming convention (scheduler_
// prefix, _seconds histogram suffix) so a rename does not silently break the
// queries that key on these names. It reads the names off the live
// metric descriptors, not shadow constants, so a rename in metrics.go is caught.
func TestSchedulerMetricNames(t *testing.T) {
	t.Parallel()

	durationName := metricName(t, schedulerRefreshDurationSeconds.Desc().String())
	tasksName := metricName(t, schedulerPeriodicTasks.Desc().String())

	if !strings.HasPrefix(durationName, "scheduler_") || !strings.HasSuffix(durationName, "_seconds") {
		t.Fatalf("refresh duration metric name %q breaks the histogram naming convention", durationName)
	}
	if !strings.HasPrefix(tasksName, "scheduler_") {
		t.Fatalf("periodic tasks metric name %q breaks the scheduler prefix convention", tasksName)
	}
}

// TestCronEntriesRecorder guards the two invariants schedulerCronEntries depends on: a
// source's Publish resets only its OWN series (so each source's writers,
// which share the vec in one process, never wipe each other), and a bucket absent from a
// later pass is dropped rather than left stale.
//
// Not parallel: it mutates the package-global schedulerCronEntries vec, and cleans up its
// own source label at the end so it does not leak into the other metric tests.
func TestCronEntriesRecorder(t *testing.T) {
	const srcA, srcB = "test-src-a", "test-src-b"
	reset := func() {
		schedulerCronEntries.DeletePartialMatch(prometheus.Labels{labelSource: srcA})
		schedulerCronEntries.DeletePartialMatch(prometheus.Labels{labelSource: srcB})
	}
	reset()
	defer reset()

	want := func(msg string, exp float64, source, connectorID, mode, origin string) {
		t.Helper()
		if got := testutil.ToFloat64(schedulerCronEntries.WithLabelValues(source, connectorID, mode, origin)); got != exp {
			t.Errorf("%s: got %v, want %v", msg, got, exp)
		}
	}

	// Source A: two entries in one bucket, one in another.
	recA := NewCronEntriesRecorder(srcA)
	recA.Add("bitbucket", "verbatim", true)
	recA.Add("bitbucket", "verbatim", true)
	recA.Add("github", "minute_phase", false)
	recA.Publish()
	want("bucket counts entries", 2, srcA, "bitbucket", "verbatim", originStaggeredDefault)
	want("override maps to operator_override origin", 1, srcA, "github", "minute_phase", originOperatorOverride)

	// Source B publishing must leave source A's series untouched (scoped reset).
	recB := NewCronEntriesRecorder(srcB)
	recB.Add("identity_enrichment", "minute_phase", true)
	recB.Publish()
	want("A survives B's publish", 2, srcA, "bitbucket", "verbatim", originStaggeredDefault)
	want("B's own bucket set", 1, srcB, "identity_enrichment", "minute_phase", originStaggeredDefault)

	// A re-publish of A that no longer emits the github bucket drops it, keeps B intact.
	recA2 := NewCronEntriesRecorder(srcA)
	recA2.Add("bitbucket", "group", true)
	recA2.Publish()
	want("new bucket set on re-publish", 1, srcA, "bitbucket", "group", originStaggeredDefault)
	want("vanished bucket is dropped, not stale", 0, srcA, "github", "minute_phase", originOperatorOverride)
	want("B still intact after A re-publish", 1, srcB, "identity_enrichment", "minute_phase", originStaggeredDefault)
}

// metricName extracts the fqName from a prometheus Desc string, which has the
// shape `Desc{fqName: "name", help: ...}`.
func metricName(t *testing.T, desc string) string {
	t.Helper()
	const marker = `fqName: "`
	i := strings.Index(desc, marker)
	if i < 0 {
		t.Fatalf("could not parse fqName from desc %q", desc)
	}
	rest := desc[i+len(marker):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatalf("could not parse fqName terminator from desc %q", desc)
	}
	return rest[:j]
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"testing"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/runner/app"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// datedRecordOps lists, per simulated connector, the record operationIds that
// may carry a DataDate under the rule in sdk.RawRecord: event records dated by
// the event, and day-partitioned usage. Every other record operationId is a
// current-state snapshot and must carry none.
var datedRecordOps = map[string]map[string]bool{
	"github": {
		"fetch_prs": true, "fetch_issues": true, "fetch_commits": true,
		"fetch_reviews": true, "fetch_pr_comments": true, "fetch_copilot_usage": true,
	},
	"gitlab": {
		"fetch_prs": true, "fetch_issues": true, "fetch_commits": true,
		"fetch_reviews": true, "fetch_pr_comments": true,
		// Lifecycle and commit records from hooks, dated by the event.
		"webhook_events": true,
	},
	"bitbucket": {
		"fetch_prs": true, "fetch_issues": true, "fetch_commits": true,
		"fetch_reviews": true, "fetch_pr_comments": true,
	},
	"jira": {
		"fetch_issues": true, "fetch_comments": true, "fetch_worklogs": true,
	},
}

// TestSimulatedRecordsFollowTheDataDateRule runs every operation of the
// simulated connectors and checks the DataDate rule in sdk.RawRecord, so a
// date-keyed sink (sink/postgres) keeps one row per item:
//
//   - a current-state record (any operationId not in datedRecordOps, and any
//     lifecycle record with event_type "snapshot") carries no DataDate;
//   - no record is dated by the fetch: a DataDate inside the sync's own
//     wall-clock window is the fetch time, not the day the data describes.
func TestSimulatedRecordsFollowTheDataDateRule(t *testing.T) {
	h := newSimHarness(t)
	for _, v := range simVendors {
		t.Run(v.connector, func(t *testing.T) {
			h.put(v.connector, v.settings)
			for _, op := range simOrder(t, v) {
				start := time.Now()
				run := h.sync(v.connector, op)
				end := time.Now()
				if run.err != nil {
					// A gap or a failure is reported by TestSimulatedPayloadsMatchSchemas.
					continue
				}
				for _, e := range run.events {
					if e.GetDataDate() == nil {
						continue
					}
					dd := e.GetDataDate().AsTime()
					recOp := e.GetOperationId()
					switch {
					case !datedRecordOps[v.connector][recOp]:
						t.Errorf("%s/%s: record %s (operationId %s) is current state but has DataDate %s",
							v.connector, op, e.GetExternalId(), recOp, dd.Format(time.RFC3339))
					case e.GetMetadata()["event_type"] == "snapshot":
						t.Errorf("%s/%s: snapshot record %s has DataDate %s; a snapshot carries none",
							v.connector, op, e.GetExternalId(), dd.Format(time.RFC3339))
					case !dd.Before(start.Add(-time.Second)) && !dd.After(end.Add(time.Second)):
						t.Errorf("%s/%s: record %s is dated by the fetch (%s)",
							v.connector, op, e.GetExternalId(), dd.Format(time.RFC3339Nano))
					}
				}
			}
		})
	}
}

// simOrder is the order TestSimulatedPayloadsMatchSchemas runs a vendor's
// operations in: discovery first, then the rest.
func simOrder(t *testing.T, v simVendor) []string {
	t.Helper()
	c, err := app.NewRegistry(nil).Get(sdk.ConnectorID(v.connector))
	if err != nil {
		t.Fatal(err)
	}
	order := append([]string(nil), v.first...)
	for _, op := range c.Operations() {
		if !contains(v.first, string(op.ID)) {
			order = append(order, string(op.ID))
		}
	}
	return order
}

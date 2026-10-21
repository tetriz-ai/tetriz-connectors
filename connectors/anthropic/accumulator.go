// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// recordAccumulator groups vendor rows by record identity and emits once per identity.
//
// A consumer updates a record in place by ExternalID, so an identity must be emitted once
// with all its rows, even when they span pages.
//
// So the accumulator's lifetime is the unit the walk checkpoints: a whole calendar day for
// the activity walk, a whole chunk for the report walk. Nothing is emitted until every page
// of that unit has been read.
//
// Holding a whole unit is what costs memory, and the unit is only as small as the walk's
// checkpoint grain — a report chunk is up to a month of every actor's rows. maxRowsPerUnit
// bounds it explicitly rather than relying on the page cap, which bounds requests and not
// rows, and on a request budget that lives in another package's config.
type recordAccumulator struct {
	opID  sdk.OperationID
	orgID string
	// order preserves first-seen order so the emitted sequence is stable across runs,
	// which keeps a diff of two runs readable.
	order []string
	rows  map[string][]json.RawMessage
	dates map[string]time.Time
	// total counts rows across every identity, which is what actually grows.
	total int
}

// maxRowsPerUnit caps the rows one unit may accumulate before the walk gives up on it.
//
// A whole unit is held before anything is emitted (see the type doc), so this is the bound on
// resident memory. Two hundred thousand vendor rows is on the order of a hundred megabytes of
// raw JSON in one worker, which is the budget it is sized against — rows rather than bytes
// because the count is what the walk can cheaply track per row.
//
// It fails rather than emitting what it has: a partial unit would emit each identity with only
// the rows read so far, overwriting a complete record.
const maxRowsPerUnit = 200_000

// overCap reports the unit having grown past what may be held, naming the lever that applies
// to THIS operation's shape. A report unit is a date range an operator can narrow; an activity
// unit is one calendar day, where chunk_days does nothing — sending an operator to a knob that
// cannot move is worse than saying the shape is simply too large.
func (a *recordAccumulator) overCap() error {
	if a.total <= maxRowsPerUnit {
		return nil
	}
	lever := "one calendar day of this organisation's rows exceeds what a unit may hold"
	if reportOperations[a.opID] {
		lever = "narrow chunk_days so a unit fits in memory"
	}
	return &sdk.SyncError{
		ConnectorID:    connectorID,
		OperationID:    a.opID,
		OrganizationID: a.orgID,
		Kind:           sdk.ErrKindVendorError,
		Cause: fmt.Errorf("%s accumulated %d rows in one unit, over the %d it may hold; %s",
			a.opID, a.total, maxRowsPerUnit, lever),
	}
}

func newRecordAccumulator(opID sdk.OperationID, orgID string) *recordAccumulator {
	return &recordAccumulator{
		opID:  opID,
		orgID: orgID,
		rows:  map[string][]json.RawMessage{},
		dates: map[string]time.Time{},
	}
}

// add files one vendor row under an identity. Row order within an identity is preserved:
// the payload is the vendor's rows verbatim, so re-ordering them would be a parse.
//
// It reports when the unit has grown past what may be held, so the caller fails the unit
// instead of emitting a partial one.
func (a *recordAccumulator) add(externalID string, dataDate time.Time, row json.RawMessage) error {
	if _, seen := a.rows[externalID]; !seen {
		a.order = append(a.order, externalID)
		a.dates[externalID] = dataDate
	}
	a.rows[externalID] = append(a.rows[externalID], row)
	a.total++
	if err := a.overCap(); err != nil {
		return fmt.Errorf("filing a row under %q: %w", externalID, err)
	}
	return nil
}

// emitAll hands one record per identity to the sink.
//
// single controls the payload shape, and the two differ by design rather than convenience.
// An endpoint returning exactly one row per identity stores that object as-is, because
// wrapping it in a single-element array would make every consumer unwrap it for nothing.
// An endpoint whose grain is finer than the identity stores the array, which is what keeps
// the identity free of renameable values.
func (a *recordAccumulator) emitAll(ctx context.Context, sink sdk.DataSink, single bool) error {
	// Re-checked at the point of no return, so a caller that ever drops add's error still
	// cannot emit a partial unit.
	if err := a.overCap(); err != nil {
		return fmt.Errorf("emitting a unit of %s: %w", a.opID, err)
	}

	for _, externalID := range a.order {
		rows := a.rows[externalID]

		var payload json.RawMessage
		switch {
		case single && len(rows) == 1:
			payload = rows[0]
		case single:
			// More rows than the endpoint's contract allows for this identity. Storing
			// either one would silently drop the others, so this is a vendor-shape change
			// and has to stop the run rather than pick a winner.
			return &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    a.opID,
				OrganizationID: a.orgID,
				Kind:           sdk.ErrKindVendorError,
				Cause: fmt.Errorf("identity %q carries %d rows on an endpoint that returns one per identity",
					externalID, len(rows)),
			}
		default:
			marshalled, err := json.Marshal(rows)
			if err != nil {
				return &sdk.SyncError{
					ConnectorID:    connectorID,
					OperationID:    a.opID,
					OrganizationID: a.orgID,
					Kind:           sdk.ErrKindInternal,
					Cause:          fmt.Errorf("marshalling grouped rows for %q: %w", externalID, err),
				}
			}
			payload = marshalled
		}

		record := &sdk.RawRecord{
			ConnectorID:    string(connectorID),
			OperationID:    string(a.opID),
			OrganizationID: a.orgID,
			ExternalID:     externalID,
			Payload:        payload,
			FetchedAt:      time.Now().UTC(),
			DataDate:       a.dates[externalID],
		}
		if err := sink.Emit(ctx, record); err != nil {
			// A sink failure, not a connector bug: a queue outage would otherwise read as
			// "bug in connector code" and be triaged at the wrong layer.
			return &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    a.opID,
				OrganizationID: a.orgID,
				Kind:           sdk.ErrKindSinkFailure,
				Cause:          fmt.Errorf("emitting record %q: %w", externalID, err),
			}
		}
		// Counted AFTER the sink accepts it, so the series means "reached the queue" rather
		// than "was constructed". Counting before would report healthy volume straight through
		// a queue outage, and the unit's retry would re-count the same record on every attempt.
		recordsEmitted.WithLabelValues(tenantlabel.OrgID(a.orgID), string(a.opID)).Inc()
	}
	return nil
}

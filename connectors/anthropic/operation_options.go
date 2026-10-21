// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// defaultReportRereadDays is how far back a steady-state report run re-reads, so an amount
// the vendor re-priced after the fact is picked up — the stored record is verbatim as of
// its last read, so a re-read is the only way a correction is ever collected.
//
// Fifteen is not a vendor-published bound: Anthropic revises for up to thirty days, so this
// trades a missed older revision against not paying a doubled warehouse cost every day. It is
// a per-organisation setting rather than a constant because the right value depends on how
// often that organisation's watermark moves and how far back its restatements reach — both
// measurable only from real traffic, and revisable without a deploy.
const defaultReportRereadDays = 15

// defaultActivityOverlapDays is how far back an activity run re-enters covered days,
// against late arrival alone — these endpoints carry no watermark and are not revised.
const defaultActivityOverlapDays = 2

// defaultReportChunkDays is how wide one report request's range is, and it is the vendor
// maximum: a wider range is refused with a 400 before anything is ingested.
//
// It is tunable downward because the width interacts with a budget the vendor does not
// publish per request. A chunk is read and emitted as ONE unit — the identity spans the whole
// range, so a partially-read chunk emits nothing — while a run may only issue a bounded
// number of vendor requests before the rate limiter refuses. An organisation whose rows do
// not fit a chunk inside one run's request budget therefore makes NO progress: every attempt
// restarts the same chunk at page one and is refused at the same page. Narrowing the width
// makes each unit affordable, and because a completed chunk is checkpointed, the walk then
// advances a chunk per run instead of stalling.
//
// Splitting on a date boundary is what makes this safe: the record identity carries the
// bucket day, so no narrowing can ever split one identity across two units.
const defaultReportChunkDays = maxReportRangeDays

// reportOperations names the two operations whose unit is a date range, and so the two whose
// unit an operator can narrow with chunk_days. The activity operations' unit is one calendar
// day, which no setting changes.
var reportOperations = map[sdk.OperationID]bool{
	opFetchAnalyticsUserCost:  true,
	opFetchAnalyticsUserUsage: true,
}

// operationOptions are the operation-level defaults an operator can tune per organisation
// through OperationOverrides, without a deploy.
//
// Operation-level rather than connector-level, which is the split the design draws: the
// first-run lookback lives on connector settings because an administrator may want a
// shallower history for the whole connector, while these are properties of one endpoint's
// behaviour.
type operationOptions struct {
	// RereadDays overrides the report re-read window. Zero means the default.
	RereadDays int `json:"reread_days,omitempty"`
	// OverlapDays overrides the activity late-arrival overlap. Zero means the default.
	OverlapDays int `json:"overlap_days,omitempty"`
	// ChunkDays narrows one report request's range. Zero means the vendor maximum.
	ChunkDays int `json:"chunk_days,omitempty"`
}

// parseOperationOptions reads one operation's overrides. An absent entry is not an error:
// every field has a default, and most organisations will never set one.
//
// An unreadable entry IS an error rather than a silent fall back to defaults. An operator
// who set a narrower re-read window to control warehouse cost, and whose value is not being
// read, needs to know — silently ignoring it would present as the connector disregarding a
// deliberate instruction.
func parseOperationOptions(cfg *sdk.ConnectorConfig, opID sdk.OperationID) (operationOptions, error) {
	if cfg == nil || len(cfg.OperationOverrides) == 0 {
		return operationOptions{}, nil
	}
	raw, present := cfg.OperationOverrides[opID]
	if !present || len(raw) == 0 {
		return operationOptions{}, nil
	}
	var opts operationOptions
	if err := json.Unmarshal(raw, &opts); err != nil {
		return operationOptions{}, fmt.Errorf("unmarshalling overrides for %s: %w", opID, err)
	}
	return opts, nil
}

// maxReportRereadDays caps the re-read window, which is the only bound on the trailing pass's
// cost — it is uncapped in chunks by design (see maxForwardChunksPerRun). Without it one
// per-organisation override makes every run re-read a year of report data against a request
// pool the whole operation family shares. Thirty-one days because Anthropic revises for up to
// thirty: a wider window re-reads days that cannot have changed.
const maxReportRereadDays = 31

// rereadDays returns the effective report re-read window, clamped. A value above the cap is
// clamped rather than honoured or rejected: the operator's intent ("re-read more") is served
// as far as it can buy anything, and the excess only costs requests.
func (o operationOptions) rereadDays() int {
	if o.RereadDays <= 0 {
		return defaultReportRereadDays
	}
	return min(o.RereadDays, maxReportRereadDays)
}

// chunkDays returns the effective report chunk width, clamped to the vendor maximum: a value
// above it would be refused by the vendor rather than honoured, so accepting it would turn a
// tuning mistake into a dead operation.
func (o operationOptions) chunkDays() int {
	if o.ChunkDays <= 0 || o.ChunkDays > maxReportRangeDays {
		return defaultReportChunkDays
	}
	return o.ChunkDays
}

// overlapDays returns the effective activity overlap. A non-positive override falls back to
// the default rather than meaning "no overlap", so a typo or a zero-valued field cannot
// silently disable late-arrival catching. Turning the overlap off is deliberately not
// expressible through this knob.
func (o operationOptions) overlapDays() int {
	if o.OverlapDays <= 0 {
		return defaultActivityOverlapDays
	}
	return o.OverlapDays
}

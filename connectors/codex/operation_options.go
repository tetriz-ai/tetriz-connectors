// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package codex

import (
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The two operation-level values an operator owns, and their defaults.
//
// Operation-level rather than connector-level, which is the split the design draws: the
// first-run lookback lives on connector settings because an administrator may want a
// shallower history for the whole connector, while these are properties of how this one
// endpoint is read.
const (
	// defaultOverlapDays is how far back a run re-enters days it has already covered, against
	// late arrival alone. Three because OpenAI publishes no aggregation-lag statement, so the
	// window has to be wider than a bound we could cite.
	//
	// Owned by the operator per organisation: the right value depends on how late that
	// workspace's rows actually settle, which is measurable only from real traffic and should
	// be revisable without a deploy. Re-reading is safe — the write is an update in place —
	// so the cost of the choice is requests, not correctness.
	defaultOverlapDays = 3
	// maxOverlapDays caps it. Without a cap, one per-organisation override makes every run
	// re-read a year of days against a request pool measured in tens per minute, which would
	// stall the walk rather than deepen it. Thirty-one is a month of late arrivals, past any
	// plausible aggregation lag.
	maxOverlapDays = 31

	// defaultPageSize is the page this walk asks for. Owned by the operator per connector: a
	// workspace whose pages time out needs a smaller one, and that is a property of the
	// workspace rather than a fact about the vendor.
	defaultPageSize = 100
	// maxPageSize bounds an override. The value is a hint the vendor may ignore, so this
	// bounds what we ASK for; the row cap in the walk is what bounds what we accept.
	maxPageSize = 1000
)

// operationOptions are the operation-level defaults an operator can tune per organisation
// through OperationOverrides, without a deploy.
type operationOptions struct {
	// OverlapDays overrides the late-arrival overlap. Zero means the default.
	OverlapDays int `json:"overlap_days,omitempty"`
	// PageSize overrides the requested page size. Zero means the default.
	PageSize int `json:"page_size,omitempty"`
}

// parseOperationOptions reads one operation's overrides. An absent entry is not an error:
// every field has a default, and most organisations will never set one.
//
// An unreadable entry IS an error rather than a silent fall back to defaults. An operator who
// narrowed the overlap to control cost, and whose value is not being read, needs to know —
// silently ignoring it presents as the connector disregarding a deliberate instruction.
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

// overlapDays returns the effective overlap, clamped. A value above the cap is clamped rather
// than rejected: the operator's intent ("re-read further back") is served as far as it can buy
// anything, and refusing it would turn a tuning mistake into a dead operation.
func (o operationOptions) overlapDays() int {
	if o.OverlapDays <= 0 {
		return defaultOverlapDays
	}
	return min(o.OverlapDays, maxOverlapDays)
}

// pageSize returns the effective page size, clamped for the same reason.
func (o operationOptions) pageSize() int32 {
	if o.PageSize <= 0 {
		return defaultPageSize
	}
	return int32(min(o.PageSize, maxPageSize)) //nolint:gosec // G115: clamped to maxPageSize above
}

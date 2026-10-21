// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package obs

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// CursorRegressionsTotal counts per-entry cursor watermarks that moved BACKWARD across
// one operation run.
//
// A cursor is monotonic by contract, so this needs no threshold to interpret: any
// increment is a defect. Without it a regressed cursor is invisible until someone runs
// ad-hoc SQL against connector_sync_states, and the damage it causes — re-scanning a
// consumed window every tick — looks like ordinary sync traffic.
//
// Deliberately emitted at the orchestrator's single save point rather than inside a
// connector, so every connector writing a per-entry composite cursor is covered by one
// implementation instead of each vendor needing its own. Labels bounded: connector_id
// ~ 10, operation_id ~ low tens per connector, org_id ~ connected organizations (collapsed for
// individual tenants via metrictenant.OrgID).
var CursorRegressionsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "connector_cursor_regressions_total",
		Help: "Count of per-entry cursor watermarks that moved backward across a run. A cursor is monotonic by contract, so any increment is a defect: the affected entry re-scans an already-consumed window on every subsequent tick, spending vendor budget forever, and its sync state reads as though the entry never synced.",
	},
	[]string{"connector_id", "operation_id", "org_id"},
)

// CursorOldestEntryAgeSeconds is the age of the oldest per-entry watermark an operation
// holds, refreshed on each cursor save.
//
// It separates the two states that look identical in connector_sync_states and whose
// difference is the whole diagnosis: an entry legitimately idle because the vendor has no
// new data, versus one pinned at a synthetic floor. A pin sits at the connector's
// first-sync lookback and never moves, so it parks this gauge near that lookback while a
// healthy entry tracks recent activity — visible without knowing anything about vendor
// state.
//
// It is the oldest entry, not a per-entry series, because per-repo cardinality is
// unbounded in the repo dimension; the oldest entry is what an operator investigates
// first, and the regression counter above carries the precise signal.
var CursorOldestEntryAgeSeconds = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "connector_cursor_oldest_entry_age_seconds",
		Help: "Age of the oldest per-entry watermark an operation's cursor holds, refreshed on each cursor save. Distinguishes an entry idle because there is no new data from one pinned at a synthetic first-sync floor — a pin parks this near the connector's lookback and never moves.",
	},
	[]string{"connector_id", "operation_id", "org_id"},
)

// CursorHealthTracker records cursor health across every cursor write a single run makes.
//
// One run can persist several times — an operation that checkpoints per page writes
// straight to connector_sync_states before it returns — and each write is measured against
// the last cursor known to have LANDED, not against the run's starting cursor. Comparing
// every write against the start would re-count one early regression on each later page.
//
// Safe for concurrent use: nothing guarantees an operation checkpoints from a single
// goroutine.
type CursorHealthTracker struct {
	mu   sync.Mutex
	last *sdk.Cursor
}

// NewCursorHealthTracker starts a tracker from the cursor the run loaded.
func NewCursorHealthTracker(start *sdk.Cursor) *CursorHealthTracker {
	return &CursorHealthTracker{last: start}
}

// Record measures next against the last cursor that landed, then adopts next as the new
// baseline. Call it only AFTER the write has committed: a failed save is retried with the
// same inputs, and recording before the write would count one regression once per attempt
// against a counter whose whole premise is that any increment is a defect.
func (t *CursorHealthTracker) Record(next *sdk.Cursor, connectorID, operationID, orgID string) {
	if t == nil || next == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	previous := t.last
	t.last = next
	// Emit under the lock: the gauge is Set (or its series deleted) per call, so two
	// interleaved recordings emitting out of order would leave the series describing
	// the older cursor. The emit is pure in-memory work, so serializing it is free.
	RecordCursorHealth(previous, next, connectorID, operationID, orgID)
}

// JSONValueKind returns the first significant byte of a JSON value, or 0 when there is
// none — '"' for a string, '{' for an object, and so on.
//
// Encodings are discriminated on this rather than by attempting a decode and discarding the
// error. "Not this shape" is an expected, per-entry-every-run condition, so it must not
// produce an error at all; what remains, a value whose shape was recognised and still would
// not parse, then means what it should — genuinely malformed data worth reporting.
//
// Exported because a connector decoding its own multi-encoding cache values needs the same
// discrimination (bitbucket's prProbeEntry), and two copies of a shape rule drift.
func JSONValueKind(raw []byte) byte {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return 0
	}
	return trimmed[0]
}

// compositeEntries returns a cursor's per-entry map, reporting whether it has one. A cursor
// that is nil, empty, or not a JSON object has no entries to measure and the caller records
// nothing; only a malformed object is worth reporting.
func compositeEntries(c *sdk.Cursor, connectorID, operationID, orgID string) (map[string]json.RawMessage, bool) {
	if c == nil || JSONValueKind(c.Data) != '{' {
		return nil, false
	}
	parts, err := sdk.CompositeFromCursor(c)
	if err != nil {
		// Identified: these are the *NoCtx logger variants, so there is no ambient
		// context to recover the owner from, and a corrupt-cursor line naming nothing is
		// unactionable.
		logger.ErrorNoCtx("connector cursor health: cursor data is a JSON object but did not decode; skipping cursor measurement for this run",
			zap.String(logger.ConnectorID, connectorID), zap.String(logger.OperationID, operationID),
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, false
	}
	return parts, true
}

// isPerEntryCursorKey reports whether a decoded cursor key identifies a per-entry
// watermark rather than a struct field name.
//
// The test is load-bearing, not defensive. sdk.CompositeFromCursor unmarshals ANY top-level
// JSON object into a map, so it does NOT fail on a flat cursor: sdk.NewTimestampCursor's
// {"last_updated_at": "<RFC3339>"} decodes as a one-entry "composite" whose single value
// is a perfectly readable timestamp. Most connectors write a flat cursor, and this is
// recorded for every operation, so without this test they would all publish a one-entry
// cursor age measured against thresholds derived from the VCS lookback windows.
//
// Three shapes are in use: "workspace/repo" (bitbucket, github), a numeric project id
// (gitlab), and an upper-case project key such as "PROJ" (jira). Jira matters as much as
// the VCS trio here — its ops carry the same 12-month first-sync window, so a pinned jira
// project fails identically — and it is excluded from the cursor-rewind job for an unrelated
// reason (that job re-ENCODES an entry and would write jira's shape wrong), which does not
// apply to a read-only measurement.
//
// A composite keyed some other way is not measured rather than mismeasured, which is the
// safe direction for a metric. The flat-cursor field names this must keep excluding are all
// lower-case snake_case ("last_updated_at", "page", "page_token"), and that is exactly why
// the un-slashed alphabetic form must be upper-case: a flat cursor decodes here as a
// one-entry composite whose value IS a timestamp, so nothing but the key can tell the two
// apart. Widening to lower-case keys to pick up another connector would re-admit them and
// reopen the defect this predicate exists to close.
//
// KNOWN UNCOVERED, as a consequence: linear keys its per-team composite by team id
// (lower-case, hyphenated) and stores the watermark under "since", so linear cursors yield
// no samples. Covering it needs a per-connector key/field contract rather than a shared
// shape heuristic — until then linear cursor pins are outside what these series can see,
// and the absence of samples is the honest reading, not health. fetch_repo_tree is
// uncovered for a different reason: see cursorEntryWatermark.
func isPerEntryCursorKey(key string) bool {
	if key == "" {
		return false
	}
	if strings.Contains(key, "/") {
		return true
	}
	// A numeric project id, or an upper-case project key (letters, digits, underscore,
	// starting with a letter). A lower-case rune anywhere rejects the key, which is what
	// separates "PROJ" from "page_token".
	allDigits := true
	for i, r := range key {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'A' && r <= 'Z':
			allDigits = false
		case r == '_' && i > 0:
			allDigits = false
		default:
			return false
		}
	}
	// Reject a leading digit on the alphanumeric form only; a wholly numeric key is
	// gitlab's project id and is valid.
	return allDigits || (key[0] >= 'A' && key[0] <= 'Z')
}

// cursorEntryWatermark extracts an entry's watermark from a per-entry composite cursor
// value, accepting every encoding the connectors write. Returns the zero time when the
// value carries no readable watermark, which callers treat as "no comparable watermark"
// rather than as a regression to the zero time.
//
// The encodings are the connectors' own and are read here only to compare two timestamps.
// Nothing is re-encoded, so this deliberately does not identify WHICH shape it read — a
// tool that writes an entry back (the cursor-rewind job) must, because writing the wrong
// shape makes the owning connector drop the entry, and that tool owns that concern.
// Encodings are selected by shape via [JSONValueKind] — see it for why a decode is not used
// as the probe.
//
// A value that matched a shape and still would not decode yields the zero time and logs at
// Debug, never Error: a composite's values need not be watermarks at all. `fetch_repo_tree`
// (registered on all three VCS connectors) keys per repo, so its keys DO pass the per-entry
// test, and stores a head SHA — an Error there would fire once per repo on every healthy
// sync. Such an operation shows up as an absence of samples, which is the signal this
// metric trades in.
func cursorEntryWatermark(raw json.RawMessage) time.Time {
	switch JSONValueKind(raw) {
	case '"':
		var bare time.Time
		if err := json.Unmarshal(raw, &bare); err != nil {
			logger.DebugNoCtx("connector cursor health: cursor entry is a string but not a timestamp; entry not measured",
				zap.ByteString("entry", raw), zap.Error(err))
			return time.Time{}
		}
		return bare
	case '{':
		// The two object encodings tag their watermark differently ("timestamp" for
		// bitbucket's fetch_prs state, "ts" for github/gitlab/jira); an entry carrying
		// neither decodes fine and yields the zero time, which is not an error.
		var object struct {
			Timestamp time.Time `json:"timestamp"`
			TS        time.Time `json:"ts"`
		}
		if err := json.Unmarshal(raw, &object); err != nil {
			logger.DebugNoCtx("connector cursor health: cursor entry is an object but did not decode; entry not measured",
				zap.ByteString("entry", raw), zap.Error(err))
			return time.Time{}
		}
		if !object.Timestamp.IsZero() {
			return object.Timestamp
		}
		return object.TS
	default:
		return time.Time{}
	}
}

// RecordCursorHealth compares the cursor an operation started from against the one it is
// about to persist, and records regressions plus the oldest watermark it now holds.
//
// Only entries present in BOTH cursors are compared: a newly-tracked entry has nothing to
// regress from, and an entry that disappeared cannot be observed here at all. Callers pass
// the already-collapsed org label (metrictenant.OrgID).
//
// Best-effort and side-effect free with respect to the run. A cursor with no per-entry
// watermark to compare records nothing rather than guessing — note that a flat cursor
// still DECODES here (see isPerEntryCursorKey), so the key test, not the decode, is what
// excludes it.
func RecordCursorHealth(before, after *sdk.Cursor, connectorID, operationID, orgID string) {
	afterParts, ok := compositeEntries(after, connectorID, operationID, orgID)
	if !ok {
		return
	}

	var oldest time.Time
	regressions := 0
	beforeParts, hasBefore := compositeEntries(before, connectorID, operationID, orgID)
	for key, raw := range afterParts {
		if !isPerEntryCursorKey(key) {
			continue
		}
		next := cursorEntryWatermark(raw)
		if next.IsZero() {
			continue
		}
		if oldest.IsZero() || next.Before(oldest) {
			oldest = next
		}
		if !hasBefore {
			continue
		}
		prevRaw, ok := beforeParts[key]
		if !ok {
			continue
		}
		if prev := cursorEntryWatermark(prevRaw); !prev.IsZero() && next.Before(prev) {
			regressions++
		}
	}

	if regressions > 0 {
		CursorRegressionsTotal.WithLabelValues(connectorID, operationID, orgID).Add(float64(regressions))
	}
	if !oldest.IsZero() {
		CursorOldestEntryAgeSeconds.WithLabelValues(connectorID, operationID, orgID).Set(time.Since(oldest).Seconds())
		return
	}
	// A composite save with no measurable watermark clears the series. The gauge is
	// process-local and holds its last Set until the pod dies, so once a cursor stops
	// carrying measurable entries (aged entries dropped, entries pruned) a stale value
	// would keep asserting an age that no longer exists, on a cursor that healed. Delete is a no-op for an operation that never published.
	CursorOldestEntryAgeSeconds.DeleteLabelValues(connectorID, operationID, orgID)
}

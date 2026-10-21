// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const dayLayout = "2006-01-02"

// syncState is this operation's cursor schema. The framework treats a cursor's contents
// as opaque per-operation data, which makes it the right home for all three pieces.
type syncState struct {
	// CoveredThrough is a HIGH-WATER MARK: the latest day collection has reached. A
	// checkpoint may only RAISE it, and that is not a refinement — without it the
	// mechanism is actively harmful. A steady-state run begins with the trailing re-read
	// window, whose days lie behind this mark by construction, so checkpointing each
	// completed day would write an EARLIER mark. A run cancelled during that re-read
	// would then leave the organisation further back than it started, with the next
	// window beginning earlier still, ratcheting it backwards indefinitely.
	CoveredThrough string `json:"covered_through,omitempty"`

	// Path is the bucket and prefix this cursor was built against, and it is what makes
	// the mark above safe to trust.
	//
	// A reconnect that corrects the bucket commits the new settings and clears the cursor
	// as two separate writes. A scheduled run landing between them would read the NEW
	// settings beside the OLD mark, walk only the trailing window against a bucket whose
	// history it never saw, raise the mark past all of it — and, because the mark may only
	// rise and no run looks further back than the window, lose that history permanently and
	// silently. It would also write a cursor row back, undoing the clear.
	//
	// Carrying the path turns that into a check at the point of use rather than a race
	// between two writes: a mark built for another path is not a mark, so the run treats
	// itself as a first run. Self-healing, and it needs no lock.
	Path string `json:"path,omitempty"`

	// FailedTerminal is the set of OBJECTS settled as permanently unreadable, keyed by
	// day and object. The trailing window covers days immediately behind the mark, so a
	// settled object sits inside that window on every later run; without this the walk
	// would re-fetch the same broken object forever, which is exactly what settling it
	// was meant to end.
	//
	// Keyed per object, never per day, and that is the difference between abandoning one
	// file and losing a day. A day holds one file per client type, so settling the day
	// would strand its healthy siblings — and because a settled entry is skipped on every
	// later run, those siblings' reports would never arrive at all.
	//
	// The object key carries the vendor's publication timestamp, so a republished file
	// presents a new key and is attempted afresh rather than inheriting the verdict
	// reached about the file it replaces.
	FailedTerminal map[string]string `json:"failed_terminal,omitempty"`

	// FailureCounts tracks failures per object, so an object can reach the terminal
	// threshold across runs. Only a failure of the object ITSELF counts — never a
	// transient one, or a multi-run outage would settle objects as permanently unreadable
	// and their reports would never arrive.
	//
	// The key carries the object, not just the day: a different object failing is a
	// different problem, and letting them accumulate toward one threshold would settle on
	// the strength of unrelated failures.
	//
	// It has to be checkpointed on the FAILURE path: the framework saves a returned cursor
	// only when a run succeeds, so a count returned with an error is discarded, resets
	// every run, never reaches the threshold, and the object is retried forever — the
	// pinned organisation the terminal state exists to prevent.
	FailureCounts map[string]int `json:"failure_counts,omitempty"`
}

// maxConsecutiveFailures is how many runs one object may fail before it is settled and
// the walk stops attempting it. Bounded rather than infinite because the mark cannot pass
// a day that still owes an object: retrying forever would pin the organisation at that day
// and stop collecting everything after it, which is worse than a known, counted gap.
const maxConsecutiveFailures = 5

func loadState(c *sdk.Cursor) (syncState, error) {
	var s syncState
	if c == nil || len(c.Data) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(c.Data, &s); err != nil {
		return s, fmt.Errorf("parsing kiro cursor: %w", err)
	}
	return s, nil
}

func (s syncState) toCursor() (*sdk.Cursor, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("serialising kiro cursor: %w", err)
	}
	return &sdk.Cursor{Data: b}, nil
}

// coveredThroughDay returns the high-water mark as a day, and whether one is set.
func (s syncState) coveredThroughDay() (time.Time, bool) {
	if s.CoveredThrough == "" {
		return time.Time{}, false
	}
	d, err := time.Parse(dayLayout, s.CoveredThrough)
	if err != nil {
		// An unparseable mark is treated as unset rather than fatal: the walk then
		// re-derives its start from the bucket, which is always safe because both stores
		// converge on a re-read. Failing here would strand the organisation instead.
		//
		// It is still logged, because only this code writes the field: a value it cannot
		// read back means the persisted state was corrupted, and the silent cost is a
		// collection window that widens back to the first-run cap without anyone asking.
		logger.ErrorNoCtx(fmt.Sprintf("kiro cursor: discarding unreadable covered-through mark %q: %v",
			s.CoveredThrough, err))
		return time.Time{}, false
	}
	return d.UTC(), true
}

// raiseCoveredThrough moves the mark forward, and never backward.
//
// This is the single place the invariant lives, so a caller cannot lower the mark by
// reaching for the field directly.
func (s *syncState) raiseCoveredThrough(day time.Time) {
	d := day.UTC()
	if cur, ok := s.coveredThroughDay(); ok && !d.After(cur) {
		return
	}
	s.CoveredThrough = d.Format(dayLayout)
}

// failureKey identifies one object on one day. Day-prefixed so a day's entries can be
// found — and cleared — as a group.
func failureKey(day time.Time, objectKey string) string {
	return day.UTC().Format(dayLayout) + "|" + objectKey
}

// isObjectTerminal reports whether one object has been settled as permanently unreadable.
func (s syncState) isObjectTerminal(day time.Time, objectKey string) bool {
	_, ok := s.FailedTerminal[failureKey(day, objectKey)]
	return ok
}

// recordFailure increments the failure count for one object and reports whether that
// object has now reached the terminal threshold.
//
// The caller must only reach here for a failure of the object itself. A transient failure
// counted here would let an outage settle objects as permanently unreadable, and a settled
// object is skipped forever — so its reports never arrive, silently, long after the
// outage ended.
func (s *syncState) recordFailure(day time.Time, objectKey, reason string) bool {
	k := failureKey(day, objectKey)
	if s.FailureCounts == nil {
		s.FailureCounts = map[string]int{}
	}
	s.FailureCounts[k]++
	if s.FailureCounts[k] < maxConsecutiveFailures {
		return false
	}
	if s.FailedTerminal == nil {
		s.FailedTerminal = map[string]string{}
	}
	s.FailedTerminal[k] = reason
	delete(s.FailureCounts, k)
	return true
}

// clearObjectFailure forgets one object's failure history the moment that object is
// collected, and it must not wait for the rest of the day.
//
// The threshold counts CONSECUTIVE failures, and only a per-object clear makes that true.
// A day is incomplete for as long as any of its files is failing, so a day-scoped clear
// never fires while one file is persistently broken — and an intermittent file beside it
// would then keep a stale count across its own successes, accruing to terminal without
// ever failing the threshold number of times in a row. It would be abandoned on the
// strength of its neighbour's failure.
func (s *syncState) clearObjectFailure(day time.Time, objectKey string) {
	delete(s.FailureCounts, failureKey(day, objectKey))
}

// clearTerminal forgets the settled verdicts for one day and reports how many it dropped.
//
// Only the repair path calls this. A settled object is skipped on every ordinary run by
// design, so a repair that did not clear the verdict first would report success having
// attempted nothing.
func (s *syncState) clearTerminal(day time.Time) int {
	prefix := day.UTC().Format(dayLayout) + "|"
	cleared := 0
	for k := range s.FailedTerminal {
		if strings.HasPrefix(k, prefix) {
			delete(s.FailedTerminal, k)
			cleared++
		}
	}
	return cleared
}

// clearFailures forgets a whole day's failure history once the day has been collected.
//
// The per-object clear above covers every object the day still holds; this also discards
// counts for keys the day no longer has at all — an object that failed and was then
// replaced by a corrected file under a new key leaves one behind, and nothing else
// collects it.
func (s *syncState) clearFailures(day time.Time) {
	prefix := day.UTC().Format(dayLayout) + "|"
	for k := range s.FailureCounts {
		if strings.HasPrefix(k, prefix) {
			delete(s.FailureCounts, k)
		}
	}
}

// PreserveHighWaterMark returns fresh with the stored cursor's high-water mark, so a writer
// that does not own the mark cannot move it.
//
// The repair path is that writer. It mutates the settled verdicts and the failure counts and
// never the mark, but it persists a whole cursor — so if a scheduled run advanced the mark
// after the repair read it, writing the repair's snapshot back would ratchet the mark
// BACKWARDS, which is the one thing raiseCoveredThrough exists to prevent. Re-reading the
// stored mark at write time and carrying it across closes that: the repair writes what it
// owns and leaves the rest as it found it, whoever changed it in between.
//
// A nil or unreadable stored cursor means there is no mark to preserve, which is the first
// repair against an organisation that has never run.
func PreserveHighWaterMark(
	ctx context.Context, stored, fresh *sdk.Cursor,
) (*sdk.Cursor, error) {
	freshState, err := loadState(fresh)
	if err != nil {
		return nil, fmt.Errorf("reading the cursor being written: %w", err)
	}
	storedState, err := loadState(stored)
	if err != nil {
		// Not fatal, but not silent either. Treating an unreadable stored cursor as an
		// unset mark is what every other reader does, and failing the write here would
		// strand the repair's own bookkeeping over a value it does not own — so the write
		// proceeds. But a cursor that will not parse is a real fault somewhere upstream,
		// and swallowing it outright would leave the only trace of it in a mark that
		// quietly went missing.
		logger.WarnWithCtx(ctx, "kiro: the stored cursor could not be read, so its mark is treated as unset",
			zap.Error(err))
		return fresh, nil
	}
	freshState.CoveredThrough = storedState.CoveredThrough
	cursor, err := freshState.toCursor()
	if err != nil {
		return nil, fmt.Errorf("serialising the merged cursor: %w", err)
	}
	return cursor, nil
}

// settingsPath is the fingerprint stored alongside the mark. Bucket and prefix are the two
// values every derived key depends on, so a change in either invalidates the mark.
func settingsPath(s Settings) string { return s.Bucket + "/" + s.Prefix }

// adoptPath reports whether the mark must be discarded because it was built for a different
// location, and records the current one either way.
//
// Every write stores the path, so a stored path that is empty is a different location too.
func (s *syncState) adoptPath(path string) (reset bool) {
	if s.Path == path {
		return false
	}
	*s = syncState{Path: path}
	return true
}

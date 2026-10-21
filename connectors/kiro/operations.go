// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// FetchUserActivityReportOp copies the vendor's daily reports into our own object storage
// and emits one pointer record per file.
//
// It parses nothing. The bytes are stored as the vendor wrote them, and the record that
// points at them carries a location and provenance rather than any interpretation — so a
// surprise in the report's columns costs nothing to correct later.
type FetchUserActivityReportOp struct{}

// OperationID identifies this operation within the connector.
func (op *FetchUserActivityReportOp) OperationID() sdk.OperationID {
	return opFetchUserActivityReport
}

// PointerPayload is what a record carries: where the bytes are, and enough provenance to
// trace them. Never the bytes themselves.
//
// Exported because it is a contract rather than an internal shape: this operation writes
// it and a consumer reads it back. A second declaration on the reading side
// would let the two drift silently — the writer renaming a field and the reader finding
// nothing, which reads as a record with no location rather than as a mismatch.
type PointerPayload struct {
	// Bucket and Key locate the stored copy. Together they are the whole point of the
	// record: without both, nothing downstream can find the bytes. Bucket is omitted when
	// the store cannot name itself, so a reader sees an absent key rather than a bucket
	// called "".
	Bucket string `json:"s3_bucket,omitempty"`
	Key    string `json:"s3_key"`
	// SourceBucket and SourceKey are where the vendor wrote it, kept for provenance —
	// the customer's own bucket, which we do not control and cannot re-read forever.
	SourceBucket string `json:"source_bucket"`
	SourceKey    string `json:"source_key"`
	// ContentSHA256 is over the bytes as stored, so a later reader can tell a corrupted
	// copy from a corrected one.
	ContentSHA256 string `json:"content_sha256"`
	// SourceLastModified is the object store's own timestamp, in RFC 3339. It is what
	// distinguishes a republished day from the original.
	SourceLastModified string `json:"source_last_modified"`
}

// Execute walks whole days from the high-water mark to yesterday and collects each.
//
// The granularity is a day because the vendor's own publication unit is a day: it writes
// one file per client type per day, and there is nothing finer to react to.
func (op *FetchUserActivityReportOp) Execute(
	ctx context.Context, params *sdk.ExecutionParams,
) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	_, settings, err := ParseConfig(params.Config)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(),
			OrganizationID: orgID, Kind: sdk.ErrKindConfigInvalid, Cause: err,
		}
	}
	if err := validateSettings(settings); err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(),
			OrganizationID: orgID, Kind: sdk.ErrKindConfigInvalid, Cause: err,
		}
	}

	// The gateway satisfies the framework's interface and carries the narrow one this
	// operation needs. Its absence is a configuration fault rather than a transient
	// failure — the same class the orchestrator uses for a gateway it cannot build, for
	// a connector no retry can fix.
	src, ok := params.VendorGateway.(ObjectSource)
	if !ok {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
			Kind:  sdk.ErrKindConfigInvalid,
			Cause: fmt.Errorf("gateway %T does not provide the kiro object source", params.VendorGateway),
		}
	}
	if params.BlobStore == nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
			Kind:  sdk.ErrKindConfigInvalid,
			Cause: fmt.Errorf("no destination object store was provided"),
		}
	}

	state, err := loadState(params.Cursor)
	if err != nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(),
			OrganizationID: orgID, Kind: sdk.ErrKindConfigInvalid, Cause: err,
		}
	}

	// A mark built for a different bucket is not a mark. The reconnect that changed the
	// path clears the cursor, but that clear and the settings commit are two writes, and a
	// run landing between them would otherwise trust a mark that had never seen this
	// bucket — walking only the trailing window and raising the mark past history nobody
	// collected. Checked here, where the mark is about to be used, rather than raced for.
	if state.adoptPath(settingsPath(settings)) {
		logger.Info(ctx, "kiro: the bucket path changed, so the position starts over",
			zap.String(logger.OrgID, orgID), zap.String("path", settingsPath(settings)))
	}

	// A failure is remembered rather than returned at once, so one unreadable object
	// cannot stop the days after it from being collected. Days are independent units of
	// publication and the mark is what protects the incomplete one from being passed.
	var (
		blocked      bool // an earlier day in this window still owes an object
		firstFailure error
		failures     int
	)

	for _, d := range op.window(state, settings, time.Now().UTC()) {
		outcome, dayErr := op.collectDay(ctx, params, src, settings, state, d)
		if dayErr != nil {
			// A day whose listing failed says nothing about its contents, so the run
			// stops rather than recording an unknown day as an empty one. The mark has
			// not passed it, so the next run reads it again.
			if errors.Is(dayErr, ErrBucketUnreachable) {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
					Kind:  sdk.ErrKindConfigInvalid,
					Cause: fmt.Errorf("collecting %s: %w", d.Format(dayLayout), dayErr),
				}
			}
			return nil, fmt.Errorf("collecting %s for %s: %w", d.Format(dayLayout), orgID, dayErr)
		}

		// Each collected object forgets its own failures, whether or not the day as a
		// whole completed. Waiting for the day would let a persistently broken file hold
		// a recovering neighbour's count open across its successes.
		for _, key := range outcome.Collected {
			state.clearObjectFailure(d, key)
		}

		complete := true
		for _, f := range outcome.Failed {
			if errors.Is(f.Err, ErrBucketUnreachable) {
				return nil, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
					Kind:  sdk.ErrKindConfigInvalid,
					Cause: fmt.Errorf("collecting %s: %w", d.Format(dayLayout), f.Err),
				}
			}
			failures++
			if firstFailure == nil {
				firstFailure = f.Err
			}
			switch {
			// Only a failure of the OBJECT accrues toward settling. Anything else — a
			// throttle, the blob store write, the sink — counts nothing, because
			// counting those would let a multi-run outage settle objects as permanently
			// unreadable and a settled object is never attempted again, so its report
			// would never arrive: worse than retrying forever, which at least stays
			// visible.
			case !errors.Is(f.Err, ErrObjectUnreadable):
				complete = false
			case state.recordFailure(d, f.Key, f.Err.Error()):
				// Settled. It no longer holds the day back, which is the point: the day's
				// other client types are already collected and must not wait on this one.
				ReportsAbandonedTotal.WithLabelValues(tenantlabel.OrgID(orgID)).Inc()
				logger.Error(ctx, "kiro: object settled as permanently unreadable; it will not be attempted again",
					zap.String(logger.OrgID, orgID), zap.String("day", d.Format(dayLayout)),
					zap.String("object", f.Key), zap.Error(f.Err))
			default:
				complete = false
				// Logged on every attempt, not only when it settles: the accrual is the
				// operator's only warning that a report is on its way to being abandoned.
				logger.WarnWithCtx(ctx, "kiro: object unreadable; it will be attempted again",
					zap.String(logger.OrgID, orgID), zap.String("day", d.Format(dayLayout)),
					zap.String("object", f.Key), zap.Error(f.Err))
			}
		}

		if complete {
			state.clearFailures(d)
			// The mark may only move while every earlier day is done. Advancing past a
			// day that still owes an object would carry it out of the trailing re-read
			// window, and the object would then never be collected and never settled —
			// a silent gap, which is the one outcome worse than a slow one.
			//
			// Raising is a no-op for a trailing re-read, which is the point: re-reading
			// updates both stores and records no progress.
			if !blocked {
				state.raiseCoveredThrough(d)
			}
		} else {
			blocked = true
		}

		// Checkpointed whether or not the day completed, because the failure counts live
		// in the same cursor. The framework saves a returned cursor only when a run
		// succeeds, so a count carried out alongside an error would be discarded, reset
		// every run, never reach the threshold, and leave the object retried forever.
		op.checkpoint(ctx, params, state, orgID)

		if len(outcome.Collected) == 0 && outcome.Skipped == 0 &&
			len(outcome.Failed) == 0 && outcome.Unplaceable == 0 {
			// Not an error. Before enablement, and on any day the vendor did not publish,
			// the prefix is legitimately empty; treating that as a failure would make
			// every organisation's pre-enablement history look broken.
			EmptyDaysTotal.WithLabelValues(tenantlabel.OrgID(orgID)).Inc()
			logger.Info(ctx, "kiro: no reports published for day",
				zap.String(logger.OrgID, orgID), zap.String("day", d.Format(dayLayout)))
		}
	}

	if firstFailure != nil {
		// Reported in the generic class, not as a configuration fault.
		//
		// An object we cannot read is not the configuration's fault, and calling it one
		// would tell an operator the stored settings no longer match the vendor when the
		// settings are fine. The bucket-level faults that ARE the configuration's fault
		// return above, before this.
		//
		// A failure rate says little about a connector running once a day, so an
		// object that settles increments reports_abandoned_total instead, and one still
		// accruing logs a warning on every attempt until it settles.
		return nil, fmt.Errorf("collecting for %s: %d object(s) failed, first: %w",
			orgID, failures, firstFailure)
	}

	c, err := state.toCursor()
	if err != nil {
		return nil, fmt.Errorf("returning the kiro cursor: %w", err)
	}
	return c, nil
}

// window returns the days to walk, oldest first.
func (op *FetchUserActivityReportOp) window(
	state syncState, settings Settings, now time.Time,
) []time.Time {
	// Yesterday is the newest complete day: the vendor publishes early in the morning for
	// the day before, so including today would only re-read a day that is not yet written.
	end := now.UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)

	var start time.Time
	if mark, ok := state.coveredThroughDay(); ok {
		// A steady-state run re-reads a short trailing window as well as anything new,
		// because that is the only way a corrected day is ever collected.
		start = mark.AddDate(0, 0, -settings.GetTrailingOverlapDays())
	} else {
		// N days means N days walked, and the loop below includes both ends — so the
		// start is N-1 before the end, not N. Subtracting N walks N+1 days, which reads
		// one day further back than the cap an operator set.
		start = end.AddDate(0, 0, -(settings.GetFirstRunWindowDays() - 1))
	}

	var days []time.Time
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	return days
}

// objectFailure is one object that could not be collected, and why.
type objectFailure struct {
	Key string
	Err error
}

// dayOutcome is what one day's collection produced.
type dayOutcome struct {
	// Collected names the objects stored, not merely how many, because each one's own
	// failure history is cleared by its own success rather than by the day's.
	Collected []string
	Skipped   int // settled as permanently unreadable, so deliberately not attempted
	Failed    []objectFailure
	// Unplaceable counts objects under the day's prefix whose key does not match the
	// documented layout, so the day they belong to cannot be derived. They are not
	// collected and not settled, and the day still completes — see collectDay for why
	// that is the lesser of the two wrongs, and why the count exists.
	Unplaceable int
}

// collectDay attempts every report under one day's prefix.
//
// Every object is attempted, and a failing one does not end the day. A day holds one file
// per client type, so stopping at the first failure would leave the rest unattempted —
// and since map iteration order is randomised, which ones those were would differ run to
// run. A healthy file could then be abandoned along with the broken one it shared a day
// with, having never been tried.
func (op *FetchUserActivityReportOp) collectDay(
	ctx context.Context, params *sdk.ExecutionParams,
	src ObjectSource, settings Settings, state syncState, d time.Time,
) (dayOutcome, error) {
	var out dayOutcome
	prefix := dayPrefix(settings.Prefix, settings.AccountID, settings.Region, d)

	objects, err := src.ListDay(ctx, settings.Bucket, prefix)
	if err != nil {
		return out, fmt.Errorf("listing %s: %w", prefix, err)
	}

	// One object per client type. When a day holds more than one — which happens if the
	// vendor adds a corrected file alongside the original rather than replacing it — the
	// later last-modified wins. Whether the vendor replaces or appends is undocumented,
	// so the design converges either way rather than depending on the answer.
	winners := map[string]SourceObject{}
	for _, o := range objects {
		parsed, err := parseKey(o.Key)
		if err != nil {
			out.Unplaceable++
			ReportsUnplaceableTotal.WithLabelValues(
				tenantlabel.OrgID(params.Config.OrganizationID)).Inc()
			// An object we cannot place is skipped rather than guessed at: filing a report
			// under the wrong day is one of the two ways this design can produce data that
			// looks right and is wrong.
			//
			// And skipped WITHOUT holding the day open, which is the harder half of the
			// choice. Holding it open would pin the organisation at that day the moment
			// the vendor drops anything beside its reports — a manifest, a marker — and
			// nothing here can tell that from a real report under an unexpected name. So
			// the day completes and the object is counted instead, because a loss that is
			// visible in a metric is recoverable and one visible only in a log line is
			// what "silently" means.
			logger.WarnWithCtx(ctx, "kiro: skipping an object that does not match the report layout",
				zap.String("key", o.Key), zap.Error(err))
			continue
		}
		// Newest wins, and a tie is broken explicitly rather than left to listing order.
		//
		// S3 reports LastModified to the second, so two objects republished within the
		// same second compare equal — After is false, the incumbent stays, and which one
		// that is comes from the listing, which S3 returns in lexicographic key order. So
		// the tie silently fell back to picking by filename: exactly the ordering this
		// design rejects, reached through the back door. The vendor's timestamp suffix is
		// what the key sorts on, so taking the greater key at a tie is the same intent as
		// taking the later object, stated instead of inherited.
		cur, seen := winners[parsed.ClientType]
		switch {
		case !seen,
			o.LastModified.After(cur.LastModified),
			o.LastModified.Equal(cur.LastModified) && o.Key > cur.Key:
			winners[parsed.ClientType] = o
		}
	}

	for clientType, o := range winners {
		switch {
		case state.isObjectTerminal(d, o.Key):
			out.Skipped++
		default:
			// Refused on the listing's own figure, before the object is fetched. The cap
			// in the gateway is the backstop for a body that turns out larger than the
			// listing said; this is the cheap check that never spends the read at all,
			// and it is what Size was collected for.
			if o.Size > MaxReportBytes {
				out.Failed = append(out.Failed, objectFailure{
					Key: o.Key,
					Err: fmt.Errorf("report is %d bytes, past the %d-byte ceiling: %w",
						o.Size, MaxReportBytes, ErrObjectUnreadable),
				})
				continue
			}
			if err := op.copyOne(ctx, params, settings, d, clientType, o); err != nil {
				out.Failed = append(out.Failed, objectFailure{Key: o.Key, Err: fmt.Errorf("copying %s: %w", o.Key, err)})
			} else {
				out.Collected = append(out.Collected, o.Key)
				ReportsCopiedTotal.WithLabelValues(
					tenantlabel.OrgID(params.Config.OrganizationID), metricClientType(clientType)).Inc()
			}
		}
	}
	return out, nil
}

// copyOne stores one object's bytes and emits the record that points at them.
func (op *FetchUserActivityReportOp) copyOne(
	ctx context.Context, params *sdk.ExecutionParams,
	settings Settings, d time.Time, clientType string, o SourceObject,
) error {
	src, _ := params.VendorGateway.(ObjectSource)
	body, err := src.Get(ctx, settings.Bucket, o.Key)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", o.Key, err)
	}

	orgID := params.Config.OrganizationID
	dst := destinationKey(orgID, d, clientType)

	// The bytes are written BEFORE the pointer, and the order is the reverse of the
	// intuitive one. A failure between them leaves an object nobody points at, which is
	// inert and overwritten by the next run. The opposite order would leave a record
	// pointing at nothing, which a later reader would treat as real data and fail on.
	if err := params.BlobStore.Write(ctx, dst, body, ""); err != nil {
		return fmt.Errorf("storing %s: %w", dst, err)
	}

	sum := sha256.Sum256(body)
	payload, err := json.Marshal(PointerPayload{
		Bucket:             destinationBucket(params.BlobStore),
		Key:                dst,
		SourceBucket:       settings.Bucket,
		SourceKey:          o.Key,
		ContentSHA256:      hex.EncodeToString(sum[:]),
		SourceLastModified: o.LastModified.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("building the pointer for %s: %w", dst, err)
	}

	// The identity is the client type; the day supplies uniqueness alongside it, because
	// the landing table is unique on the organisation, connector, operation, record key
	// and business date. The vendor's own key is deliberately not the identity: it carries
	// a publication timestamp, so a republished day would present a different key and
	// fork into a second record instead of updating the first.
	//
	// The business date is set explicitly, in co-ordinated universal time. Leaving it
	// unset does not fail — it silently falls back to the day of the fetch, which on a
	// first run would file an entire history under today.
	if err := params.Sink.Emit(ctx, &sdk.RawRecord{
		ConnectorID:    string(connectorID),
		OperationID:    string(op.OperationID()),
		OrganizationID: orgID,
		ExternalID:     clientType,
		Payload:        payload,
		FetchedAt:      time.Now().UTC(),
		DataDate:       d.UTC(),
		Metadata: map[string]string{
			"client_type": clientType,
			"day":         d.UTC().Format(dayLayout),
		},
	}); err != nil {
		return fmt.Errorf("emitting the pointer for %s: %w", dst, err)
	}
	return nil
}

// destinationKey is deterministic in the organisation, the day and the client type, so a
// re-fetch overwrites the same object rather than accumulating copies. The vendor's own
// publication timestamp is kept in the pointer for provenance rather than in the key.
func destinationKey(orgID string, d time.Time, clientType string) string {
	u := d.UTC()
	return fmt.Sprintf("kiro/%s/%04d/%02d/%02d/%s_user_report.csv",
		orgID, u.Year(), int(u.Month()), u.Day(), clientType)
}

// destinationBucket names the store the bytes were written to.
//
// The shared object-store interface exposes no accessor for it — only the concrete client
// does — so this asserts for the narrower capability rather than widening an interface
// every caller in the repository implements. An unnamed store (blobstore.Memory, a
// directory) returns "" rather than a guess, and the payload then omits s3_bucket: the key
// alone still locates the object within whichever store the deployment configured, and a
// wrong name in the record would be worse than an absent one.
func destinationBucket(c any) string {
	type bucketNamer interface{ Bucket() string }
	if bn, ok := c.(bucketNamer); ok {
		return bn.Bucket()
	}
	return ""
}

// checkpoint banks progress mid-run. Best-effort by design: the day's records are already
// published and both stores converge on a re-read, so a failed checkpoint costs a repeat
// rather than correctness, and failing the run over one would be worse than the repeat.
func (op *FetchUserActivityReportOp) checkpoint(
	ctx context.Context, params *sdk.ExecutionParams, state syncState, orgID string,
) {
	if params.Checkpoint == nil {
		return
	}
	c, err := state.toCursor()
	if err != nil {
		logger.WarnWithCtx(ctx, "kiro: could not serialise the cursor to checkpoint",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return
	}
	if err := params.Checkpoint(ctx, c); err != nil {
		logger.WarnWithCtx(ctx, "kiro: checkpoint failed; progress will be repeated next run",
			zap.String(logger.OrgID, orgID), zap.Error(err))
	}
}

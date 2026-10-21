// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/tenantlabel"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// rosterPageSize is the vendor maximum, so most organisations resolve in one request.
const rosterPageSize = 1000

// maxRosterPages bounds the walk. At the page size above this is a million members — far
// past any real organisation — so reaching it means the vendor is not advancing after_id
// rather than that the roster is genuinely that large.
const maxRosterPages = 1000

// rosterMember is the one field lifted out, and only one: the member's identifier, which
// is half the record's identity. The role, the address and everything else stay in the
// payload.
type rosterMember struct {
	ID string `json:"id"`
}

// FetchOrganizationMembersOp stores one record per member per snapshot date.
//
// This is the operation that gives an activity count a denominator: "twelve developers
// used Claude Code" cannot be read as adoption without knowing whether twelve of fifteen
// are licensed or twelve of two hundred. It also names who is licensed and never active,
// which the activity endpoints structurally cannot answer.
//
// It is the one operation outside the analytics family, so it is the one whose reachability
// depends on the credential carrying membership read access. A failure here must not be
// read as a failure of the connector: the other four operations have their own schedules
// and their own cursors, and a roster that cannot be read leaves them running.
type FetchOrganizationMembersOp struct{}

func (o *FetchOrganizationMembersOp) OperationID() sdk.OperationID {
	return opFetchOrganizationMembers
}

// Execute walks the roster on after_id, which is a third pagination shape — neither an
// opaque page cursor nor a page number.
//
// A snapshot rather than a delta: the whole roster is re-read and re-emitted each run,
// keyed by the snapshot date, because there is no incremental endpoint and a member who
// left simply leaves the next snapshot rather than needing a tombstone. The position is not
// carried between runs for the same reason — every run starts from the first page.
func (o *FetchOrganizationMembersOp) Execute(
	ctx context.Context, params *sdk.ExecutionParams,
) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID
	opID := o.OperationID()

	if _, err := ParseSettings(params.Config); err != nil {
		return nil, &sdk.SyncError{
			ConnectorID:    connectorID,
			OperationID:    opID,
			OrganizationID: orgID,
			Kind:           sdk.ErrKindConfigInvalid,
			Cause:          fmt.Errorf("parsing settings: %w", err),
		}
	}

	gw := params.VendorGateway.ForGroup(constants.GroupAnthropicAnalytics)
	snapshot := time.Now().UTC().Truncate(24 * time.Hour)

	var (
		afterID string
		total   int
		// Every member of the completed snapshot, emitted only once pagination finishes.
		pending []*sdk.RawRecord
	)
	for page := 0; page < maxRosterPages; page++ {
		pagesRead.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
		resp, err := gw.Execute(ctx, &vgpb.AnthropicOrganizationMembersRequest{
			AfterId: afterID,
			Limit:   rosterPageSize,
		})
		if err != nil {
			// Unlike the daily walks, a 400 here is unambiguous. The roster request carries no
			// date — it is an after_id snapshot — so the vendor has no "not aggregated yet" to
			// report and every 400 is a defect in our own query. Counting it keeps the two-kind
			// counter covering every operation rather than only the dated ones.
			if isBadRequest(err) {
				vendorBadRequest.WithLabelValues(tenantlabel.OrgID(orgID), string(opID), badRequestQueryDefect).Inc()
			}
			// Not re-classified: the gateway's SyncError carries the kind, the terminality
			// and the vendor's Retry-After, and a fresh wrapper would lose all three.
			return nil, fmt.Errorf("reading the member roster: %w", err)
		}
		members, ok := resp.(*vgpb.AnthropicOrganizationMembersResponse)
		if !ok {
			return nil, &sdk.SyncError{
				ConnectorID:    connectorID,
				OperationID:    opID,
				OrganizationID: orgID,
				Kind:           sdk.ErrKindInternal,
				Cause:          fmt.Errorf("unexpected response type %T for %s", resp, opID),
			}
		}

		rows := members.GetData()
		total += len(rows)
		for _, row := range rows {
			// A member that cannot be keyed holds the snapshot rather than being skipped.
			// This roster is the DENOMINATOR an adoption number divides by, so an
			// incomplete one does not read as missing data — it reads as a smaller
			// organisation, and every rate computed from it is wrong in a plausible-looking
			// direction. The endpoint is also marked beta, so a shape change is the likely
			// cause and wants fixing rather than absorbing.
			var member rosterMember
			if err := json.Unmarshal(row, &member); err != nil {
				rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
				return nil, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
					Kind:  sdk.ErrKindVendorError,
					Cause: fmt.Errorf("unreadable roster member, refusing to store a partial denominator: %w", err),
				}
			}
			if member.ID == "" {
				rowsUnreadable.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
				return nil, &sdk.SyncError{
					ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
					Kind:  sdk.ErrKindVendorError,
					Cause: fmt.Errorf("roster member carries no id, so it has no identity to store under"),
				}
			}
			// Buffered, not emitted. This walk's unit is the WHOLE snapshot, and a record
			// already on the queue cannot be recalled: a later page failing, a malformed
			// member, a stalled cursor or the page cap would leave a partial roster stored
			// under identities that look complete. The next successful attempt on another UTC
			// date keys on that date, so it never overwrites them — the partial day stays as a
			// permanently valid-looking snapshot of an organisation that was never that size.
			// And this roster is the DENOMINATOR an adoption rate divides by, so "smaller
			// organisation" is exactly how the damage reads.
			pending = append(pending, &sdk.RawRecord{
				ConnectorID:    string(connectorID),
				OperationID:    string(opID),
				OrganizationID: orgID,
				// Identity: the member and the snapshot date, both un-renameable. The
				// role lives in the payload, because a role change is a fact about the
				// snapshot rather than a different member.
				ExternalID: member.ID + "|" + snapshot.Format(dateLayout),
				Payload:    row,
				FetchedAt:  time.Now().UTC(),
				DataDate:   snapshot,
			})
		}

		// A page claiming more but returning no cursor cannot be followed, and looping on
		// the same after_id would spin. Stop and report what was read rather than
		// pretending it is the whole roster.
		if members.GetHasMore() && members.GetLastId() == "" {
			// The vendor asserts there is more and hands back nothing to resume from.
			return nil, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind:  sdk.ErrKindVendorError,
				Cause: fmt.Errorf("roster reported more pages but returned no cursor after %d members", total),
			}
		}
		if !members.GetHasMore() {
			logger.Info(ctx, "claude-code roster: snapshot complete",
				zap.Int("members", total), zap.Time("snapshot", snapshot))

			// Emitted here, once the snapshot is whole. A sink failure part-way still leaves a
			// partial roster on the queue — that is the one case buffering cannot remove,
			// because the queue is the boundary — but it fails the run, and the next attempt on
			// the same UTC date re-emits the same identities and upserts over them. The case
			// buffering does remove is the one that could not self-heal: a page or a member
			// failing, where the retry lands on a different date and never overwrites.
			for _, record := range pending {
				if err := params.Sink.Emit(ctx, record); err != nil {
					return nil, &sdk.SyncError{
						ConnectorID:    connectorID,
						OperationID:    opID,
						OrganizationID: orgID,
						Kind:           sdk.ErrKindSinkFailure,
						Cause:          fmt.Errorf("emitting roster record: %w", err),
					}
				}
				// After the sink, per recordsEmitted's contract. The roster is the one
				// operation that emits without the accumulator, so it repeats this itself.
				recordsEmitted.WithLabelValues(tenantlabel.OrgID(orgID), string(opID)).Inc()
			}

			cur, encErr := encodeCursor(rosterPosition{SnapshotDate: snapshot.Format(dateLayout), MemberCount: total})
			if encErr != nil {
				return nil, fmt.Errorf("recording the roster snapshot: %w", encErr)
			}

			// The roster gets the same coverage series as the walks, so a stalled roster
			// reads the same way. A whole snapshot is this operation's unit and it either
			// completes or the run fails, which is why it never yields — and, since every run
			// starts from the first page rather than resuming, why it is never far behind.
			lbl := []string{tenantlabel.OrgID(orgID), string(opID)}
			periodsCovered.WithLabelValues(lbl...).Inc()
			runCovered.WithLabelValues(lbl...).Inc()

			return cur, nil
		}
		// A cursor that does not move means the vendor is serving the same page. Left to the
		// page cap, that spends more wall-clock than this operation's timeout allows, so the
		// run dies on a deadline with the roster re-emitted repeatedly rather than reporting
		// the cursor fault.
		if members.GetLastId() == afterID {
			return nil, &sdk.SyncError{
				ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
				Kind: sdk.ErrKindVendorError,
				Cause: fmt.Errorf("roster cursor %q did not advance after %d members; refusing to store a partial denominator",
					afterID, total),
			}
		}
		afterID = members.GetLastId()
	}

	// The cap was exhausted with pages pending. Unlike a truncated activity day, the
	// records already emitted are not the problem — they are correct as far as they go —
	// but the SNAPSHOT is, because a count that is short reads as a smaller organisation
	// rather than as an incomplete read.
	return nil, &sdk.SyncError{
		ConnectorID: connectorID, OperationID: opID, OrganizationID: orgID,
		Kind: sdk.ErrKindVendorError,
		Cause: fmt.Errorf("roster did not finish within %d pages after %d members; refusing to store a partial denominator",
			maxRosterPages, total),
	}
}

// rosterPosition records the last snapshot rather than a resume point: the walk always
// starts from the first page, so there is nothing to resume. It exists so an operator can
// see when the roster was last read and how large it was without querying the records.
type rosterPosition struct {
	SnapshotDate string `json:"snapshot_date"`
	MemberCount  int    `json:"member_count"`
}

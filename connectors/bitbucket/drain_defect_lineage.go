// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"

	"go.uber.org/zap"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	opDrainDefectLineage = "drain_defect_lineage"
	// defectLineageBatchSize caps how many fixes one tick claims. It is smaller than the GitHub
	// drain's because each Bitbucket fix is a reconstruction — a file-history call plus several
	// per-commit diffs per anchor — against Bitbucket's shared hourly budget, so a tick claims
	// fewer fixes to keep its provider spend bounded.
	defectLineageBatchSize = 10
	// perFixLineageTimeout bounds one fix's full reconstruction. It must comfortably exceed the
	// pacing safety margin plus a rate-limit wait, or the pacing can never sleep and every
	// rate-limit hit fails the fix instantly; the tick-wide pacing budget and the probe cap bound
	// the actual cost, so this only needs to be generous, not tight.
	perFixLineageTimeout = 3 * time.Minute
	// drainExecutionTimeout bounds the whole tick. The op's work is bounded by Bitbucket's rate
	// budget rather than its own speed, so it needs far longer than the scheduler default — too
	// short a deadline cancels the run mid-reconstruction. The pacing budget still caps total sleep.
	// It stays below the host's claim lease (15m by default) so the tick is cut before
	// a fix claimed at its start can outlive its lease — a later tick never re-claims a fix this tick
	// is still reconstructing.
	drainExecutionTimeout = 10 * time.Minute
	// perFixSubmitTimeout bounds the terminal submission on a context derived from the tick, not from
	// the reconstruction's own deadline: a walk that exhausts perFixLineageTimeout leaves an expired
	// context, so reusing it for the submit would fail every timed-out fix's terminal write and leave
	// it claimed to re-reconstruct forever.
	perFixSubmitTimeout = 15 * time.Second
)

// drainDefectLineageRequiresHost lists the host services the operation cannot run without.
var drainDefectLineageRequiresHost = []sdk.HostService{sdk.HostBlameSink}

// DrainDefectLineageOp is the pull side of the Bitbucket defect-lineage drain: it claims a batch
// of PENDING bug fixes from the host, reconstructs each fix's culprit from file history + diffs
// (Bitbucket has no blame endpoint), and submits the culprits back. The host owns the queue and
// the state machine; this op owns only the provider reconstruction.
type DrainDefectLineageOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *DrainDefectLineageOp) OperationID() sdk.OperationID { return opDrainDefectLineage }

// Execute claims one batch of pending fixes and reconstructs each. It fails, terminal,
// without the host's blame sink.
func (op *DrainDefectLineageOp) Execute(parentCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	if missing := params.Host.Missing(drainDefectLineageRequiresHost); len(missing) > 0 {
		return nil, sdk.MissingHostServicesError(connectorID, op.OperationID(), orgID, missing)
	}

	resp, err := params.Host.BlameSink.ListPendingLineageBlame(parentCtx, &defectlineagev1.ListPendingLineageBlameRequest{
		OrganizationId: orgID,
		ConnectorId:    string(connectorID),
		Limit:          defectLineageBatchSize,
	})
	if err != nil {
		logger.Error(parentCtx, "drain_defect_lineage: claiming pending blame failed",
			zap.String(logger.OrgID, orgID), zap.String(logger.ConnectorID, string(connectorID)), zap.Error(err))
		return nil, fmt.Errorf("claiming pending lineage blame: %w", err)
	}

	pending := resp.GetPending()
	if len(pending) == 0 {
		return params.Cursor, nil
	}

	gw := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	// One pacing budget for the whole tick, so total sleep waiting out Bitbucket's shared rate
	// limit is bounded across all fixes, not reset per fix.
	budget := &pacing.PacingBudget{}
	for _, fix := range pending {
		op.walkOne(parentCtx, gw, params.FetchCache, budget, params.Host.BlameSink, orgID, fix)
	}
	return params.Cursor, nil
}

// walkOne reconstructs one claimed fix and submits its outcome. The pacing budget is shared with
// the tick's sibling fixes (constructed in Execute), so total sleep is bounded across the batch.
// Per-fix errors never propagate — the batch continues, and a submit failure (or a transient walk
// error) leaves the fix claimed for its lease to re-offer. The reconstruction and the submission use
// SEPARATE deadlines derived from the tick: a reconstruction that times out must still be able to
// record its terminal state, so the submit cannot ride the walk's already-expired context.
func (op *DrainDefectLineageOp) walkOne(
	parentCtx context.Context,
	gw sdk.VendorGateway,
	fc sdk.FetchCache,
	budget *pacing.PacingBudget,
	client lineage.BlameSink,
	orgID string,
	fix *defectlineagev1.PendingLineageBlame,
) {
	walkCtx, cancelWalk := context.WithTimeout(parentCtx, perFixLineageTimeout)
	state, results, err := WalkFix(walkCtx, gw, fc, budget, orgID, op.OperationID(), fix)
	cancelWalk()

	submitCtx, cancelSubmit := context.WithTimeout(parentCtx, perFixSubmitTimeout)
	op.submitOutcome(submitCtx, client, orgID, fix.GetBugFixPrId(), state, results, err)
	cancelSubmit()
}

// transientWalkErr reports whether a walk error is a "try again next tick" condition — a local
// rate-limit the pacing could not wait out this tick, or a context deadline/cancel — rather than a
// genuine reconstruction failure. A transient error must NOT be recorded terminal: BLAME_UNAVAILABLE
// is never re-offered by the claim, so a passing rate-limit would permanently forfeit a
// reconstructable fix.
func transientWalkErr(err error) bool {
	return errors.Is(err, sdk.ErrLocalRateLimit) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled)
}

// submitOutcome maps one fix's outcome to a terminal submission. A provider failure (walkErr != nil)
// is terminal for this fix — the host records it BLAME_UNAVAILABLE rather than retrying a broken
// walk indefinitely; the results are dropped. A submit failure leaves the fix claimed for its lease.
func (op *DrainDefectLineageOp) submitOutcome(
	ctx context.Context,
	client lineage.BlameSink,
	orgID, fixID string,
	state defectlineagev1.DefectLineageState,
	results []*defectlineagev1.LineageBlameResult,
	walkErr error,
) {
	if walkErr != nil {
		if transientWalkErr(walkErr) {
			// Paced out or cancelled this tick — not a verdict. Leave the fix claimed so its lease
			// re-offers it next tick rather than terminally recording a partial or unavailable result.
			// This holds even when some anchors already resolved: the reconstruction is read-only and
			// idempotent, so the next tick re-resolves them and retries the paced-out anchor, rather
			// than committing a partial culprit set that drops the paced-out anchor forever.
			logger.WarnWithCtx(ctx, "drain_defect_lineage: reconstruction paced out this tick; leaving fix claimed for re-offer",
				zap.String(logger.OrgID, orgID), zap.String("bug_fix_pr_id", fixID), zap.Error(walkErr))
			return
		}
		if len(results) > 0 {
			// A permanent error on one anchor, but others named culprits (state is RESOLVED): keep the
			// culprits found rather than discarding them; the failed anchor is left unattributed.
			logger.WarnWithCtx(ctx, "drain_defect_lineage: partial reconstruction; submitting resolved culprits, some anchors failed",
				zap.String(logger.OrgID, orgID), zap.String("bug_fix_pr_id", fixID), zap.Error(walkErr))
		} else {
			logger.Error(ctx, "drain_defect_lineage: reconstruction failed; recording blame unavailable",
				zap.String(logger.OrgID, orgID), zap.String("bug_fix_pr_id", fixID), zap.Error(walkErr))
			state = defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_BLAME_UNAVAILABLE
			results = nil
		}
	}

	if _, err := client.SubmitLineageBlame(ctx, &defectlineagev1.SubmitLineageBlameRequest{
		OrganizationId: orgID,
		BugFixPrId:     fixID,
		Status:         state,
		Results:        results,
	}); err != nil {
		logger.Error(ctx, "drain_defect_lineage: submitting blame failed; fix stays claimed",
			zap.String(logger.OrgID, orgID), zap.String("bug_fix_pr_id", fixID), zap.Error(err))
	}
}

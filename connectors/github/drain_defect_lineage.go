// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"

	"go.uber.org/zap"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const (
	opDrainDefectLineage = "drain_defect_lineage"
	// defectLineageBatchSize caps how many fixes one tick claims. Each fix is a
	// GraphQL blame walk (a few requests), so the batch is smaller than the
	// diff-fetch batch to keep one tick's provider spend bounded. The tick's own
	// ExecutionTimeout (the scheduler default, 10m) is below the host's claim
	// lease (15m by default), so the tick is cut before a fix claimed
	// at its start can outlive its lease — a later tick therefore never re-claims a
	// fix this tick is still walking, and the two never race their submissions.
	defectLineageBatchSize = 20
	// perFixLineageTimeout bounds one fix's full walk (anchor resolve + per-anchor
	// blame). A fix that overruns is abandoned this tick; its claim lease re-offers it.
	perFixLineageTimeout = 60 * time.Second
	// perFixSubmitTimeout bounds the terminal submission, on a context DERIVED FROM
	// THE TICK, not from the walk's own deadline: a walk that exhausts
	// perFixLineageTimeout leaves an already-expired context, so reusing it for the
	// submit would fail every timed-out fix's BLAME_UNAVAILABLE write and leave the
	// fix claimed to re-walk and re-time-out forever.
	perFixSubmitTimeout = 15 * time.Second
)

// drainDefectLineageRequiresHost lists the host services the operation cannot run without.
var drainDefectLineageRequiresHost = []sdk.HostService{sdk.HostBlameSink, sdk.HostLineageWalker}

// DrainDefectLineageOp is the pull side of the defect-lineage drain: it claims a
// batch of PENDING bug fixes from the host, runs the GitHub blame walk for each,
// and submits the culprits back. The host owns the queue and the state machine;
// this op owns only the provider walk.
type DrainDefectLineageOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *DrainDefectLineageOp) OperationID() sdk.OperationID { return opDrainDefectLineage }

// Execute claims one batch of pending fixes and walks each. It fails, terminal,
// without the host's blame sink and lineage walker.
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

	// The blame walk is GraphQL; the culprit→PR resolution is a REST call, so it rides GitHub's separate
	// REST budget via its own gateway group rather than the GraphQL limiter — the same split the scoring
	// drain makes.
	gw := params.VendorGateway.ForGroup(constants.GroupGitHubGraphQL)
	restGW := params.VendorGateway.ForGroup(constants.GroupGitHubREST)
	for _, fix := range pending {
		op.walkOne(parentCtx, params.Host.LineageWalker, gw, restGW, params.Host.BlameSink, orgID, fix)
	}
	return params.Cursor, nil
}

// walkOne walks one claimed fix and submits its outcome. Per-fix errors never
// propagate — the batch continues, and a submit failure leaves the fix claimed for
// its lease to re-offer. The walk and the submission use SEPARATE deadlines derived
// from the tick: a walk that times out must still be able to record its terminal
// state, so the submit cannot ride the walk's already-expired context.
func (op *DrainDefectLineageOp) walkOne(
	parentCtx context.Context,
	walker lineage.Walker,
	gw, restGW sdk.VendorGateway,
	client lineage.BlameSink,
	orgID string,
	fix *defectlineagev1.PendingLineageBlame,
) {
	walkCtx, cancelWalk := context.WithTimeout(parentCtx, perFixLineageTimeout)
	state, results, err := WalkFix(walkCtx, walker, gw, restGW, orgID, fix)
	cancelWalk()

	submitCtx, cancelSubmit := context.WithTimeout(parentCtx, perFixSubmitTimeout)
	op.submitOutcome(submitCtx, client, orgID, fix.GetBugFixPrId(), state, results, err)
	cancelSubmit()
}

// submitOutcome maps one fix's walk outcome to a terminal submission. A provider failure with no
// culprits (walkErr != nil, no results) is terminal — the host records it BLAME_UNAVAILABLE
// rather than retrying a broken walk indefinitely. When the walk resolved some anchors but a
// sibling anchor failed, the resolved culprits are kept (state RESOLVED) and the failed anchor is
// left unattributed, rather than discarding a real result. A submit failure leaves the fix claimed
// for its lease to re-offer (no inline retry).
func (op *DrainDefectLineageOp) submitOutcome(
	ctx context.Context,
	client lineage.BlameSink,
	orgID, fixID string,
	state defectlineagev1.DefectLineageState,
	results []*defectlineagev1.LineageBlameResult,
	walkErr error,
) {
	if walkErr != nil {
		if len(results) > 0 {
			// Some anchors named culprits but a sibling anchor failed (state is RESOLVED): keep the
			// culprits found rather than discarding them; the failed anchor is left unattributed.
			logger.WarnWithCtx(ctx, "drain_defect_lineage: partial blame walk; submitting resolved culprits, some anchors failed",
				zap.String(logger.OrgID, orgID), zap.String("bug_fix_pr_id", fixID), zap.Error(walkErr))
		} else {
			logger.Error(ctx, "drain_defect_lineage: blame walk failed; recording blame unavailable",
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

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"errors"
	"fmt"
	"testing"

	lineagemocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk/lineage"

	"github.com/tetriz-ai/tetriz-connectors/sdk/lineage"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const drainTestOrg = "11111111-1111-1111-1111-111111111111"

func drainParams(client lineage.BlameSink) *sdk.ExecutionParams {
	return &sdk.ExecutionParams{
		Config: &sdk.ConnectorConfig{OrganizationID: drainTestOrg},
		Cursor: &sdk.Cursor{},
		Host:   sdk.HostServices{BlameSink: client},
	}
}

// A missing blame sink fails, terminal, naming it.
func TestDrainExecute_NilClientFails(t *testing.T) {
	_, err := (&DrainDefectLineageOp{}).Execute(context.Background(), drainParams(nil))
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	require.True(t, se.Terminal)
	require.ErrorContains(t, err, "needs host services: blame_sink")
}

func TestDrainExecute_ClaimErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().ListPendingLineageBlame(gomock.Any(), gomock.Any()).Return(nil, errors.New("host down"))
	_, err := (&DrainDefectLineageOp{}).Execute(context.Background(), drainParams(client))
	require.Error(t, err)
}

// A genuine reconstruction failure is submitted as terminal BLAME_UNAVAILABLE with no results.
func TestSubmitOutcome_GenuineErrorMapsToBlameUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			require.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_BLAME_UNAVAILABLE, req.GetStatus())
			require.Empty(t, req.GetResults())
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})
	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNSPECIFIED, nil, errors.New("provider 500"))
}

// The critical guard: a transient error (rate-limit paced out, or a context deadline) must NOT be
// recorded terminal — SubmitLineageBlame is never called, so the fix stays claimed and its lease
// re-offers it next tick rather than being permanently marked BLAME_UNAVAILABLE. gomock fails the
// test if SubmitLineageBlame is called (no EXPECT registered).
func TestSubmitOutcome_TransientErrorLeavesClaimed(t *testing.T) {
	for _, walkErr := range []error{
		fmt.Errorf("bitbucket reconstruction: %w", sdk.ErrLocalRateLimit),
		fmt.Errorf("diff: %w", context.DeadlineExceeded),
		context.Canceled,
	} {
		ctrl := gomock.NewController(t)
		client := lineagemocks.NewMockBlameSink(ctrl)
		// No SubmitLineageBlame EXPECT — a call would be an unexpected-call failure.
		(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
			defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNSPECIFIED, nil, walkErr)
		ctrl.Finish()
	}
}

// The partial-resolve guard: a transient error that arrives WITH some already-resolved culprits
// (state RESOLVED) must still leave the fix claimed — never commit the partial result terminally.
// Committing it would drop the paced-out anchor's culprit forever; leaving it claimed re-walks the
// idempotent reconstruction next tick and recovers it. SubmitLineageBlame must not be called.
func TestSubmitOutcome_TransientErrorWithPartialResultsLeavesClaimed(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl)
	// No SubmitLineageBlame EXPECT — a call would be an unexpected-call failure.
	results := []*defectlineagev1.LineageBlameResult{{CulpritSha: "resolved-anchor"}}
	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results,
		fmt.Errorf("diff: %w", sdk.ErrLocalRateLimit))
	ctrl.Finish()
}

// A PERMANENT error alongside already-resolved culprits keeps those culprits (state RESOLVED) rather
// than discarding them for BLAME_UNAVAILABLE — the failed anchor is simply left unattributed.
func TestSubmitOutcome_PermanentErrorWithPartialResultsKeepsCulprits(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl)
	results := []*defectlineagev1.LineageBlameResult{{CulpritSha: "resolved-anchor"}}
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			require.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, req.GetStatus())
			require.Len(t, req.GetResults(), 1)
			require.Equal(t, "resolved-anchor", req.GetResults()[0].GetCulpritSha())
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})
	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results, errors.New("provider 500"))
}

// A successful walk forwards its state and results verbatim.
func TestSubmitOutcome_SuccessForwards(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl)
	results := []*defectlineagev1.LineageBlameResult{{CulpritSha: "abc"}}
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			require.Equal(t, defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, req.GetStatus())
			require.Len(t, req.GetResults(), 1)
			require.Equal(t, "abc", req.GetResults()[0].GetCulpritSha())
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})
	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results, nil)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
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
		Host:   sdk.HostServices{BlameSink: client, LineageWalker: stubWalker{}},
	}
}

// stubWalker stands in for the host's blame walk. The tests that use it stop
// before a walk runs, or assert only what the op does with a walk's outcome.
type stubWalker struct{}

func (stubWalker) Walk(context.Context, lineage.Source, *defectlineagev1.PendingLineageBlame) (defectlineagev1.DefectLineageState, []*defectlineagev1.LineageBlameResult, error) {
	return defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, nil, nil
}

func (stubWalker) ResolveCandidatePRs(context.Context, []*defectlineagev1.LineageBlameResult, lineage.CandidatePRResolver) {
}

// A blame sink without a walker fails, terminal, without claiming: claiming
// fixes it cannot walk would hold them until their lease expires.
func TestDrainExecute_NilWalkerFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := lineagemocks.NewMockBlameSink(ctrl) // no expectations: any call fails the test
	params := drainParams(client)
	params.Host.LineageWalker = nil
	_, err := (&DrainDefectLineageOp{}).Execute(context.Background(), params)
	var se *sdk.SyncError
	require.ErrorAs(t, err, &se)
	require.True(t, se.Terminal)
	require.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
	require.ErrorContains(t, err, "needs host services: lineage_walker")
}

// A missing blame sink fails, terminal, naming it.
func TestDrainExecute_NilClientFails(t *testing.T) {
	_, err := (&DrainDefectLineageOp{}).Execute(context.Background(), drainParams(nil))
	require.ErrorContains(t, err, "needs host services: blame_sink")
}

func TestDrainExecute_ClaimErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().ListPendingLineageBlame(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("host down"))

	_, err := (&DrainDefectLineageOp{}).Execute(context.Background(), drainParams(client))
	require.Error(t, err, "want error when claim fails")
}

// empty claim → no walk, cursor returned. VendorGateway is never touched.
func TestDrainExecute_EmptyPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().ListPendingLineageBlame(gomock.Any(), gomock.Any()).
		Return(&defectlineagev1.ListPendingLineageBlameResponse{}, nil)

	op := &DrainDefectLineageOp{}
	cur, err := op.Execute(context.Background(), drainParams(client))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cur == nil {
		t.Fatal("want cursor back on empty batch")
	}
}

// A walk failure is submitted as the terminal BLAME_UNAVAILABLE with no results.
func TestSubmitOutcome_WalkErrorMapsToBlameUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			if req.GetStatus() != defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_BLAME_UNAVAILABLE {
				t.Fatalf("want BLAME_UNAVAILABLE, got %v", req.GetStatus())
			}
			if len(req.GetResults()) != 0 {
				t.Fatalf("walk-failure submission must drop results, got %d", len(req.GetResults()))
			}
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})

	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNSPECIFIED, nil, errors.New("provider 500"))
}

// A walk error that arrives WITH some already-resolved culprits keeps them (state RESOLVED) rather
// than discarding them for BLAME_UNAVAILABLE — a sibling anchor failed, but the culprits found are
// real and the failed anchor is left unattributed.
func TestSubmitOutcome_ErrorWithPartialResultsKeepsCulprits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	results := []*defectlineagev1.LineageBlameResult{{CulpritSha: "resolved-anchor"}}
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			if req.GetStatus() != defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED {
				t.Fatalf("want RESOLVED kept, got %v", req.GetStatus())
			}
			if len(req.GetResults()) != 1 || req.GetResults()[0].GetCulpritSha() != "resolved-anchor" {
				t.Fatalf("resolved culprits must be kept, got %+v", req.GetResults())
			}
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})

	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results, errors.New("sibling anchor 500"))
}

// A successful walk forwards its state and results verbatim.
func TestSubmitOutcome_SuccessForwards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	results := []*defectlineagev1.LineageBlameResult{{CulpritSha: "abc"}}
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *defectlineagev1.SubmitLineageBlameRequest) (*defectlineagev1.SubmitLineageBlameResponse, error) {
			if req.GetStatus() != defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED {
				t.Fatalf("want RESOLVED forwarded, got %v", req.GetStatus())
			}
			if len(req.GetResults()) != 1 || req.GetResults()[0].GetCulpritSha() != "abc" {
				t.Fatalf("results not forwarded: %+v", req.GetResults())
			}
			return &defectlineagev1.SubmitLineageBlameResponse{}, nil
		})

	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_RESOLVED, results, nil)
}

// A submit failure must not panic — the fix stays claimed for lease re-offer.
func TestSubmitOutcome_SubmitErrorNoPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := lineagemocks.NewMockBlameSink(ctrl)
	client.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).Return(nil, errors.New("submit down"))

	(&DrainDefectLineageOp{}).submitOutcome(context.Background(), client, drainTestOrg, "fix-1",
		defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, nil, nil)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	defectlineagev1 "github.com/tetriz-ai/tetriz-connectors/gen/lineage/v1"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	sdkmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	lineagemocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk/lineage"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// requiresHostWant is what each operation's Execute reads from
// ExecutionParams.Host, by the file that holds it, as read from the code.
// TrackedRefs is left out everywhere: the operations that read it fall back
// to the default branch without it.
var requiresHostWant = map[sdk.OperationID]struct {
	file     string
	services []sdk.HostService
}{
	opFetchPRDiffs:       {"fetch_pr_diffs.go", []sdk.HostService{sdk.HostWorkQueue}},
	opFetchFileContent:   {"fetch_file_content.go", []sdk.HostService{sdk.HostWorkQueue}},
	opDrainDefectLineage: {"drain_defect_lineage.go", []sdk.HostService{sdk.HostBlameSink, sdk.HostLineageWalker}},
	opDrainDefectScoring: {"drain_defect_scoring.go", []sdk.HostService{sdk.HostBlameSink, sdk.HostDefectScorer}},
}

// TestRequiresHost_MatchesWhatExecuteReads pins each descriptor's
// RequiresHost to the table above, and checks the package's source: every
// required host service read (params.Host.X, X not TrackedRefs) sits in the
// file of an operation that declares it, and each declared service is read
// there.
func TestRequiresHost_MatchesWhatExecuteReads(t *testing.T) {
	for _, d := range NewConnector(nil, 0).Operations() {
		w, ok := requiresHostWant[d.ID]
		if !ok {
			assert.Empty(t, d.RequiresHost, "%s needs no host service", d.ID)
			continue
		}
		assert.Equal(t, w.services, d.RequiresHost, "%s RequiresHost", d.ID)
	}
	assertHostReadsMatch(t, requiresHostWant)
}

// assertHostReadsMatch scans the package's non-test files for host-service
// reads and compares them with want.
func assertHostReadsMatch(t *testing.T, want map[sdk.OperationID]struct {
	file     string
	services []sdk.HostService
},
) {
	t.Helper()
	fields := map[string]sdk.HostService{
		"WorkQueue": sdk.HostWorkQueue, "BlameSink": sdk.HostBlameSink,
		"LineageWalker": sdk.HostLineageWalker, "DefectScorer": sdk.HostDefectScorer,
	}
	declared := map[string][]sdk.HostService{}
	for _, w := range want {
		declared[w.file] = w.services
	}
	read := regexp.MustCompile(`\.Host\.(\w+)`)
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		var got []sdk.HostService
		for _, m := range read.FindAllStringSubmatch(string(src), -1) {
			if svc, ok := fields[m[1]]; ok && !slices.Contains(got, svc) {
				got = append(got, svc)
			}
		}
		slices.Sort(got)
		wantSvcs := slices.Clone(declared[name])
		slices.Sort(wantSvcs)
		assert.Equal(t, wantSvcs, got, "%s: host services read versus declared", name)
	}
}

// TestRequiresHost_ExecuteFailsWithoutEachAndRunsWithOnlyThose drops each
// declared service in turn and wants the run to fail naming it; then it
// provides only the declared services, with one item of work and a vendor
// that fails every call, and wants no missing-service failure and no panic.
func TestRequiresHost_ExecuteFailsWithoutEachAndRunsWithOnlyThose(t *testing.T) {
	descs := map[sdk.OperationID]sdk.OperationDescriptor{}
	for _, d := range NewConnector(nil, 0).Operations() {
		descs[d.ID] = d
	}
	for id, w := range requiresHostWant {
		t.Run(string(id), func(t *testing.T) {
			for _, drop := range w.services {
				params := requiresHostParams(t, without(w.services, drop))
				_, err := descs[id].Factory().(sdk.APIFetchOperation).Execute(context.Background(), params)
				var se *sdk.SyncError
				require.ErrorAs(t, err, &se, "without %s", drop)
				assert.True(t, se.Terminal)
				assert.Equal(t, sdk.ErrKindConfigInvalid, se.Kind)
				assert.ErrorContains(t, err, "needs host services: "+string(drop))
			}
			params := requiresHostParams(t, w.services)
			_, err := descs[id].Factory().(sdk.APIFetchOperation).Execute(context.Background(), params)
			if err != nil {
				assert.NotContains(t, err.Error(), "needs host services")
			}
		})
	}
}

func without(svcs []sdk.HostService, drop sdk.HostService) []sdk.HostService {
	return slices.DeleteFunc(slices.Clone(svcs), func(s sdk.HostService) bool { return s == drop })
}

// requiresHostParams builds params whose Host sets only svcs, each serving one
// item of work.
func requiresHostParams(t *testing.T, svcs []sdk.HostService) *sdk.ExecutionParams {
	t.Helper()
	ctrl := gomock.NewController(t)
	var host sdk.HostServices
	for _, s := range svcs {
		switch s {
		case sdk.HostWorkQueue:
			wq := sdkmocks.NewMockWorkQueue(ctrl)
			wq.EXPECT().ListPRsPendingDiffFetch(gomock.Any(), gomock.Any()).Return(&workqueuev1.ListPRsPendingDiffFetchResponse{
				Prs: []*workqueuev1.PRDiffFetchCandidate{{PullRequestId: "pr-1", PlatformRepoId: "1", RepoFullName: "acme/widgets", PrNumber: 1}},
			}, nil).AnyTimes()
			wq.EXPECT().UpdatePRDiffStatus(gomock.Any(), gomock.Any()).Return(&workqueuev1.UpdatePRDiffStatusResponse{}, nil).AnyTimes()
			wq.EXPECT().ListPendingFileFetches(gomock.Any(), gomock.Any()).Return(&workqueuev1.ListPendingFileFetchesResponse{
				Records: []*workqueuev1.FileContentRecord{{
					RepositoryId: "r-1", PlatformRepoId: "1", RepoFullName: "acme/widgets", SourcePath: "CLAUDE.md",
					Name: "CLAUDE.md", Branch: "main", Ref: "sha-1", Generation: 1,
					Files: []*workqueuev1.FileContentRecord_FetchedFile{{RelativePath: "CLAUDE.md", BlobSha: "b-1", IsPrimary: true}},
				}},
			}, nil).AnyTimes()
			host.WorkQueue = wq
		case sdk.HostBlameSink:
			bs := lineagemocks.NewMockBlameSink(ctrl)
			bs.EXPECT().ListPendingLineageBlame(gomock.Any(), gomock.Any()).Return(&defectlineagev1.ListPendingLineageBlameResponse{
				Pending: []*defectlineagev1.PendingLineageBlame{{
					BugFixPrId: "fix-1", PlatformRepoId: "1", RepoFullName: "acme/widgets", FixCommitSha: "sha-fix",
					LocationAnchors: []*defectlineagev1.LineageBlameAnchor{{Path: "a.go", StartLine: 1, EndLine: 2}},
				}},
			}, nil).AnyTimes()
			bs.EXPECT().SubmitLineageBlame(gomock.Any(), gomock.Any()).Return(&defectlineagev1.SubmitLineageBlameResponse{}, nil).AnyTimes()
			bs.EXPECT().ListPendingStructuralScoring(gomock.Any(), gomock.Any()).Return(&defectlineagev1.ListPendingStructuralScoringResponse{
				Pending: []*defectlineagev1.PendingStructuralScoring{{
					PlatformRepoId: "1", RepoFullName: "acme/widgets", FixCommitSha: "sha-fix",
					Candidates: []*defectlineagev1.PendingStructuralCandidate{{CandidateLineageId: "c-1", CandidateCommitSha: "sha-c", CandidatePath: "a.go", CandidateLine: 1}},
				}},
			}, nil).AnyTimes()
			bs.EXPECT().SubmitStructuralVerdict(gomock.Any(), gomock.Any()).Return(&defectlineagev1.SubmitStructuralVerdictResponse{}, nil).AnyTimes()
			host.BlameSink = bs
		case sdk.HostLineageWalker:
			w := lineagemocks.NewMockWalker(ctrl)
			w.EXPECT().Walk(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(defectlineagev1.DefectLineageState_DEFECT_LINEAGE_STATE_UNATTRIBUTED, nil, nil).AnyTimes()
			w.EXPECT().ResolveCandidatePRs(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			host.LineageWalker = w
		case sdk.HostDefectScorer:
			sc := lineagemocks.NewMockScorer(ctrl)
			sc.EXPECT().ScorePendingFix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			sc.EXPECT().AddedLinesFromSingleFileDiff(gomock.Any()).Return(nil).AnyTimes()
			sc.EXPECT().AddedLinesForPath(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			host.DefectScorer = sc
		}
	}
	settings, err := json.Marshal(map[string]any{"organization": "acme"})
	require.NoError(t, err)
	return &sdk.ExecutionParams{
		Config:        &sdk.ConnectorConfig{ConnectorID: connectorID, OrganizationID: testOrgID, Settings: settings},
		Cursor:        &sdk.Cursor{},
		Sink:          sdkmocks.NewMockDataSink(ctrl),
		VendorGateway: failingGateway{},
		BlobStore:     sdkmocks.NewMockBlobStore(ctrl),
		BudgetLimiter: allowingBudget{},
		Host:          host,
	}
}

// failingGateway fails every vendor call.
type failingGateway struct{}

var errVendorDown = errors.New("vendor down")

func (failingGateway) Do(context.Context, *http.Request) (*http.Response, error) {
	return nil, errVendorDown
}

func (failingGateway) DoJSON(context.Context, string, string, any, any) error { return errVendorDown }

func (g failingGateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }

func (g failingGateway) ForGroup(string) sdk.VendorGateway { return g }

func (failingGateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, errVendorDown
}

// allowingBudget always has budget and has observed no limit.
type allowingBudget struct{}

func (allowingBudget) Allow(context.Context, string, sdk.RateBucket, int) (bool, time.Duration, error) {
	return true, 0, nil
}

func (allowingBudget) UpdateFromResponse(context.Context, string, http.Header) error { return nil }

func (allowingBudget) GetLimit(context.Context, string, sdk.RateBucket) (int, time.Time, bool, error) {
	return 0, time.Time{}, false, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/model"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// knownHostServices are the sdk.Host* constants.
var knownHostServices = map[sdk.HostService]bool{
	sdk.HostWorkQueue:     true,
	sdk.HostTrackedRefs:   true,
	sdk.HostBlameSink:     true,
	sdk.HostLineageWalker: true,
	sdk.HostDefectScorer:  true,
}

// checkRequiresHost reports a RequiresHost entry that is not an sdk.Host*
// constant, which no host can ever provide, or one listed twice.
func checkRequiresHost(t reporter, id sdk.ConnectorID, d sdk.OperationDescriptor) {
	t.Helper()
	seen := map[sdk.HostService]bool{}
	for _, s := range d.RequiresHost {
		if !knownHostServices[s] {
			t.Errorf("%s/%s: RequiresHost names unknown host service %q; no host provides it, so the operation can never run", id, d.ID, s)
		}
		if seen[s] {
			t.Errorf("%s/%s: RequiresHost lists %q twice", id, d.ID, s)
		}
		seen[s] = true
	}
}

// checkHostRefusal runs every operation that declares RequiresHost with no
// host services, as `serve` and `sync` run it, and reports one the
// orchestrator does not refuse with sdk.MissingHostServicesError before it
// executes. The orchestrator is given runHost, which is empty except in the
// suite's own test of this check. The gateway answers nothing, so an
// operation that does run fails on its first vendor call instead of
// reaching a vendor.
func checkHostRefusal(t reporter, conn sdk.Connector, runHost sdk.HostServices) {
	t.Helper()
	for _, d := range conn.Operations() {
		missing := sdk.HostServices{}.Missing(d.RequiresHost)
		if d.ID == "" || d.Factory == nil || len(missing) == 0 {
			continue
		}
		t.Run(fmt.Sprintf("requires_host/%s", d.ID), func(t reporter) {
			t.Helper()
			h := newHarness(t, conn, d.ID, sdk.ConnectorConfig{}, func(context.Context, *sdk.ConnectorConfig) (sdk.VendorGateway, error) {
				return noVendor{}, nil
			}, sdk.TenantKindUnspecified)
			h.orch.WithHostServices(runHost)
			row, err := h.run(context.Background(), 1)
			want := sdk.MissingHostServicesError(conn.ID(), d.ID, h.cfg.OrganizationID, missing)
			var se *sdk.SyncError
			switch {
			case err == nil || row.Status != model.SyncRunStatusFailed:
				t.Errorf("%s/%s: run without host services %v ended %q; want it refused with %q", conn.ID(), d.ID, missing, row.Status, want.Cause)
			case row.ErrorKind != want.Kind.String() || !strings.Contains(row.ErrorMessage, want.Cause.Error()):
				t.Errorf("%s/%s: run without host services recorded %s: %q; want %s: %q", conn.ID(), d.ID, row.ErrorKind, row.ErrorMessage, want.Kind, want.Cause)
			case !errors.As(err, &se) || !se.Terminal:
				t.Errorf("%s/%s: refusal %v is not a terminal SyncError; the scheduler would retry it", conn.ID(), d.ID, err)
			}
			if n := h.calls.len(); n > 0 {
				t.Errorf("%s/%s: run without host services made %d vendor calls; it must be refused before it executes", conn.ID(), d.ID, n)
			}
		})
	}
}

// errNoVendor is what noVendor answers.
var errNoVendor = errors.New("testkit: the host-services check has no vendor")

// noVendor is a gateway that answers every call with errNoVendor.
type noVendor struct{}

func (noVendor) Do(context.Context, *http.Request) (*http.Response, error) { return nil, errNoVendor }
func (noVendor) DoJSON(context.Context, string, string, any, any) error    { return errNoVendor }
func (n noVendor) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway  { return n }
func (n noVendor) ForGroup(string) sdk.VendorGateway                       { return n }
func (noVendor) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, errNoVendor
}

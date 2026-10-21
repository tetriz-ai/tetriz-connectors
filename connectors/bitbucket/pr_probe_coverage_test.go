// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	tu "github.com/tetriz-ai/tetriz-connectors/testkit"
)

// TestPRProbeEntryDecodes pins the on-disk encoding contract: the object form decodes,
// and any other shape, a bare timestamp string included, is an error the cache reader
// skips as unparseable.
func TestPRProbeEntryDecodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		raw             string
		wantWatermark   string
		wantCoveredFrom string
		wantErr         bool
	}{
		{
			name:    "a bare timestamp string is not an entry",
			raw:     `"2026-08-04T06:55:30.772128Z"`,
			wantErr: true,
		},
		{
			name:            "current object form carries both bounds",
			raw:             `{"w":"2026-08-04T06:55:30.772128Z","cf":"2026-07-05T00:00:00Z"}`,
			wantWatermark:   "2026-08-04T06:55:30.772128Z",
			wantCoveredFrom: "2026-07-05T00:00:00Z",
		},
		{
			name:          "object without cf decodes with no coverage bound",
			raw:           `{"w":"2026-08-04T06:55:30.772128Z"}`,
			wantWatermark: "2026-08-04T06:55:30.772128Z",
		},
		{
			name:    "neither form",
			raw:     `[1,2,3]`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got prProbeEntry
			err := json.Unmarshal([]byte(tt.raw), &got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantWatermark, got.Watermark)
			assert.Equal(t, tt.wantCoveredFrom, got.CoveredFrom)
		})
	}
}

// TestProbeCoversWindow pins the gate decision itself, which is where the truncation
// defect lived: a probe was reusable purely because the PR had not changed, with no
// regard for how much of that PR's history the probe had actually read.
func TestProbeCoversWindow(t *testing.T) {
	t.Parallel()
	prUpdated := time.Date(2026, 8, 4, 6, 55, 30, 0, time.UTC)

	tests := []struct {
		name  string
		entry prProbeEntry
		since time.Time
		want  bool
	}{
		{
			name:  "probe covering the window is reusable",
			entry: newPRProbeEntry(prUpdated, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)),
			since: time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC),
			want:  true,
		},
		{
			name:  "probe starting exactly at the window is reusable",
			entry: newPRProbeEntry(prUpdated, time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)),
			since: time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC),
			want:  true,
		},
		{
			// The defect: the window widened (a rewind, a longer lookback), so the
			// segment before the probe's lower bound was never read. Reusing the probe
			// seals that segment permanently.
			name:  "probe that started after the window must re-probe",
			entry: newPRProbeEntry(prUpdated, time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)),
			since: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "stale watermark re-probes regardless of coverage",
			entry: newPRProbeEntry(prUpdated.Add(-time.Hour), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			since: time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "an entry with no coverage bound re-probes",
			entry: prProbeEntry{Watermark: encodeFetchWatermark(prUpdated)},
			since: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
		{
			name:  "unparseable coverage bound re-probes rather than trusting itself",
			entry: prProbeEntry{Watermark: encodeFetchWatermark(prUpdated), CoveredFrom: "not-a-time"},
			since: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, probeCoversWindow(context.Background(), tt.entry, prUpdated, tt.since))
		})
	}
}

// TestPRProbeReReadsAfterWindowWidens is the end-to-end guard: a PR
// probed under a narrow cursor, then met again after the window widens, must be
// re-probed so the skipped comments are ingested. Without coverage tracking the
// cache would assert completeness it never had, the second run would skip the PR, and
// the earlier comments would be lost rather than deferred.
func TestPRProbeReReadsAfterWindowWidens(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	prUpdated := now.Add(-24 * time.Hour)
	const repo = "myworkspace/myrepo"
	const prKey = repo + "#42"

	// An old comment, inside the widened window but outside the narrow one.
	oldComment := now.Add(-200 * 24 * time.Hour)

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			var subResourceCalls int
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						return bbPRsResponse([]bbPullRequest{{
							ID: 42, State: "OPEN",
							CreatedOn: now.Add(-300 * 24 * time.Hour),
							UpdatedOn: prUpdated,
						}}, false), nil
					case *connectorpb.BitbucketListPRActivityRequest:
						subResourceCalls++
						return bbActivityResponse(nil, false), nil
					case *connectorpb.BitbucketListPRCommentsRequest:
						subResourceCalls++
						return bbCommentsResponse([]bbComment{{ID: 7, CreatedOn: oldComment}}, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}

			// The narrow probe: taken at the PR's current UpdatedOn, but it only read
			// back as far as 30 days ago.
			fake := newFakeFetchCache()
			fake.seed(op.cacheType, prKey, newPRProbeEntry(prUpdated, now.Add(-30*24*time.Hour)))

			// This run's window reaches 250 days back — wider than the probe covered.
			params := testParams(tu.NewChannelSink(), gw, bareCursor(t, map[string]time.Time{
				repo: now.Add(-250 * 24 * time.Hour),
			}))
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{repo},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, fake)

			_, err := op.execute(context.Background(), params)
			require.NoError(t, err)

			assert.Equal(t, 1, subResourceCalls,
				"a probe that never read this far back must be re-probed, not served from cache — serving it seals the unread segment permanently")

			// And the rewritten entry must record the WIDER coverage, so the repair is
			// not repeated on every subsequent run.
			put := fake.putEntries(op.cacheType)
			require.Contains(t, put, prKey)
			rewritten := probeEntryFrom(t, put[prKey])
			coveredFrom, parseErr := time.Parse(time.RFC3339, rewritten.CoveredFrom)
			require.NoError(t, parseErr)
			assert.False(t, coveredFrom.After(now.Add(-250*24*time.Hour).Truncate(watermarkPrecision)),
				"re-probe must record the window it actually covered (%s), else every run re-probes forever", rewritten.CoveredFrom)
		})
	}
}

// TestBareTimestampProbeEntryIsAMiss pins that a cached entry that is not the object
// form, such as a bare timestamp string, is skipped as unparseable like any malformed
// entry, so the PR is re-probed.
func TestBareTimestampProbeEntryIsAMiss(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	prUpdated := now.Add(-24 * time.Hour)
	const repo = "myworkspace/myrepo"
	const prKey = repo + "#42"

	for _, op := range prActivityOps() {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			var subResourceCalls int
			gw := &mockVendorGateway{
				executeFunc: func(_ context.Context, req proto.Message) (proto.Message, error) {
					switch req.(type) {
					case *connectorpb.BitbucketListPullRequestsRequest:
						return bbPRsResponse([]bbPullRequest{{
							ID: 42, State: "OPEN",
							CreatedOn: now.Add(-100 * 24 * time.Hour),
							UpdatedOn: prUpdated,
						}}, false), nil
					case *connectorpb.BitbucketListPRActivityRequest:
						subResourceCalls++
						return bbActivityResponse(nil, false), nil
					case *connectorpb.BitbucketListPRCommentsRequest:
						subResourceCalls++
						return bbCommentsResponse(nil, false), nil
					default:
						return nil, fmt.Errorf("unexpected request type: %T", req)
					}
				},
			}
			// A bare RFC3339 string, which no run writes.
			fake := newFakeFetchCache()
			fake.seed(op.cacheType, prKey, encodeFetchWatermark(prUpdated))

			params := testParams(tu.NewChannelSink(), gw, bareCursor(t, map[string]time.Time{
				repo: now.Add(-90 * 24 * time.Hour),
			}))
			params.Config.Settings = mustJSON(BitbucketSettings{
				Workspace:    "myworkspace",
				Repositories: []string{repo},
			})
			params.ConfigUpdater = &stubConfigUpdater{current: params.Config.Settings}
			withFetchCache(params, fake)

			_, err := op.execute(context.Background(), params)
			require.NoError(t, err)
			assert.NotZero(t, subResourceCalls,
				"an entry that does not decode is a miss, so the PR is re-probed")
		})
	}
}

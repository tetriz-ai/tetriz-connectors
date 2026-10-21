// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package anthropic

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	vgpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	fwmocks "github.com/tetriz-ai/tetriz-connectors/internal/mocks/sdk"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Contract fixtures, one per endpoint.
//
// They assert the STORED RECORD's shape — the identity a row is keyed on, the period it lands
// under, and that the vendor's bytes reach the warehouse untouched.
//
// What they deliberately do NOT do is enumerate documented fields and check each reaches the
// payload. With verbatim storage that is true by construction, so such a test compares a
// fixture against itself and reports a contract it never verified. The assertion that earns its
// keep needs no field list: an UNKNOWN field surviving is what makes the storage contract real,
// and it is what makes a vendor addition safe with no code change.
//
// The identity composition is the other half, and it is genuine production logic — which values
// a record is keyed on, and that no renameable value is among them.
//
// No captured response exists for any of these five endpoints, so the rows below are written
// from the vendor's documented field list. They stand in for the vendor's
// shape; they cannot establish it. The first live run against a real key does that.

// documentedRows is one representative row per endpoint, written from the documentation named in
// each source. It supplies fixture input; the assertions live in the tests.
var documentedRows = map[string]struct {
	source string
	row    string
}{
	// Research catalog, Anthropic section — per-user activity row.
	"activity": {
		source: "Anthropic API documentation (per-user activity)",
		row: `{
			"user": {"id": "user_A", "email_address": "a@example.com", "type": "user"},
			"last_activity_date": "2026-08-25",
			"rbac_group_id": "grp_1",
			"rbac_group_name": "Engineering",
			"claude_code_metrics": {
				"core_metrics": {
					"commit_count": 3,
					"pull_request_count": 1,
					"distinct_session_count": 4,
					"lines_of_code": {"added_count": 120, "removed_count": 30}
				},
				"tool_actions": {
					"edit_tool": {"accepted_count": 9, "rejected_count": 1},
					"multi_edit_tool": {"accepted_count": 2, "rejected_count": 0},
					"write_tool": {"accepted_count": 5, "rejected_count": 0},
					"notebook_edit_tool": {"accepted_count": 0, "rejected_count": 0}
				}
			},
			"chat_metrics": {
				"distinct_conversation_count": 7,
				"message_count": 42,
				"thinking_message_count": 5,
				"distinct_connectors_used_count": 2,
				"distinct_projects_used_count": 3
			},
			"office_metrics": {"excel": {}, "outlook": {}, "powerpoint": {}, "word": {}},
			"web_search_count": 11
		}`,
	},
	// Research catalog — per-user skills row. NOTE: the catalog does not list a per-row `user`
	// object for /skills; its presence under group_by[]=user_id is our inference, and the
	// activity walk the skills op shares hard-fails without it
	// (TestActivityWalk_AnUnreadableRowFailsRatherThanSkipping).
	"skills": {
		source: "Anthropic API documentation (per-user skills) + group_by[]=user_id",
		row: `{
			"user": {"id": "user_A"},
			"skill_name": "code-review",
			"distinct_user_count": 1,
			"attributed_list_price": "1200.000000",
			"claude_code_metrics": {"distinct_session_skill_used_count": 3},
			"chat_metrics": {"distinct_session_skill_used_count": 1},
			"office_metrics": {"excel": {"distinct_session_skill_used_count": 0}}
		}`,
	},
	// Research catalog — per-user cost report row.
	"cost": {
		source: "Anthropic API documentation (user_cost_report)",
		row: `{
			"actor": {"type": "user_actor", "user_id": "user_A", "email": "a@example.com", "name": "A", "deleted": false},
			"amount": "41280.000000",
			"list_amount": "50000.000000",
			"currency": "USD",
			"cost_type": "tokens",
			"token_type": "output_tokens",
			"model": "claude-opus-4",
			"product": "claude_code",
			"context_window": "200k",
			"inference_geo": "us",
			"speed": "standard",
			"rbac_group_id": "grp_1",
			"slack_channel_id": null,
			"starting_at": "2026-08-25T00:00:00Z",
			"ending_at": "2026-08-26T00:00:00Z"
		}`,
	},
	// Research catalog — per-user usage report row.
	"usage": {
		source: "Anthropic API documentation (user_usage_report)",
		row: `{
			"actor": {"type": "user_actor", "user_id": "user_A", "email": "a@example.com"},
			"total_tokens": 1000,
			"uncached_input_tokens": 400,
			"output_tokens": 500,
			"cache_read_input_tokens": 80,
			"cache_creation": {"ephemeral_5m_input_tokens": 10, "ephemeral_1h_input_tokens": 10},
			"server_tool_use": {"web_search_requests": 2},
			"requests": 17,
			"model": "claude-opus-4",
			"product": "claude_code",
			"starting_at": "2026-08-25T00:00:00Z",
			"ending_at": "2026-08-26T00:00:00Z"
		}`,
	},
	// Research catalog — organisation member. `role` is the licence signal and exists on no
	// analytics endpoint, so losing it would remove the only place it appears.
	"roster": {
		source: "Anthropic API documentation (organizations/users)",
		row: `{
			"id": "user_A",
			"added_at": "2026-03-01T10:00:00Z",
			"email": "a@example.com",
			"name": "A",
			"role": "claude_code_user",
			"type": "user"
		}`,
	},
}

// TestTheStoredRecordShape is the contract on what a record IS, per endpoint: the identity it
// is keyed on, the period it lands under, and whether its payload is the vendor's object or an
// array of them. Every expected value comes from production logic, not from the fixture.
func TestTheStoredRecordShape(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)

	for _, tt := range []struct {
		endpoint     string
		emit         func(*testing.T, string) []*sdk.RawRecord
		wantID       string
		wantDataDate time.Time
		wantArray    bool
		why          string
	}{
		{
			endpoint: "activity", emit: emitActivityRow,
			wantID: "user_A|" + yesterday.Format(dateLayout), wantDataDate: yesterday,
			why: "actor and the requested day; /users is already one row per actor per day",
		},
		{
			endpoint: "skills", emit: emitSkillsRow,
			wantID: "user_A|" + yesterday.Format(dateLayout), wantDataDate: yesterday,
			wantArray: true,
			why:       "skill_name is renameable, so it stays out of the key and the rows roll up",
		},
		{
			endpoint: "cost", emit: emitCostRow,
			wantID: "user_A|" + yesterday.Format(dateLayout) + "|claude_code", wantDataDate: yesterday,
			wantArray: true,
			why:       "model is renameable, so it stays out of the key and the per-model rows roll up",
		},
		{
			endpoint: "usage", emit: emitUsageRow,
			wantID: "user_A|" + yesterday.Format(dateLayout) + "|claude_code", wantDataDate: yesterday,
			wantArray: true,
			why:       "the usage endpoint shares the cost endpoint's grain",
		},
		{
			endpoint: "roster", emit: emitRosterRow,
			wantID: "user_A|" + today.Format(dateLayout), wantDataDate: today,
			why: "a snapshot is keyed by the day it was taken, which is today",
		},
	} {
		t.Run(tt.endpoint, func(t *testing.T) {
			t.Parallel()

			spec := documentedRows[tt.endpoint]
			require.NotEmptyf(t, spec.row, "no row for %s (source: %s)", tt.endpoint, spec.source)

			records := tt.emit(t, rowDatedFor(tt.endpoint, spec.row, yesterday))
			require.Len(t, records, 1, "one row must produce exactly one record")

			assert.Equalf(t, tt.wantID, records[0].ExternalID, "identity: %s", tt.why)
			assert.Equal(t, tt.wantDataDate, records[0].DataDate,
				"the period must be set from the walk, not left to FetchedAt")

			isArray := len(records[0].Payload) > 0 && records[0].Payload[0] == '['
			assert.Equalf(t, tt.wantArray, isArray, "payload shape: %s", tt.why)
		})
	}
}

// rowDatedFor pins a report row's bucket to the day the fixture drives, so the identity the walk
// composes is predictable. The activity and roster walks take their period from the walk itself.
func rowDatedFor(endpoint, row string, day time.Time) string {
	if endpoint != "cost" && endpoint != "usage" {
		return row
	}
	return strings.ReplaceAll(row, "2026-08-25T00:00:00Z", day.Format(time.RFC3339))
}

// TestAnUnknownFieldSurvivesIntoThePayload matters more than any named field: it is what makes
// the connector robust to the vendor adding one. The parse is narrow by design, so a field
// nobody has heard of has to land untouched.
func TestAnUnknownFieldSurvivesIntoThePayload(t *testing.T) {
	t.Parallel()

	const unknown = `"a_field_added_later":{"nested":[1,2,3]}`

	for _, tt := range []struct {
		endpoint string
		row      string
		emit     func(*testing.T, string) []*sdk.RawRecord
	}{
		{"activity", `{"user":{"id":"user_A"},` + unknown + `}`, emitActivityRow},
		{"skills", `{"user":{"id":"user_A"},"skill_name":"s",` + unknown + `}`, emitSkillsRow},
		{"cost", `{"actor":{"user_id":"user_A"},"starting_at":"2026-08-25T00:00:00Z","product":"claude_code",` + unknown + `}`, emitCostRow},
		{"usage", `{"actor":{"user_id":"user_A"},"starting_at":"2026-08-25T00:00:00Z","product":"claude_code",` + unknown + `}`, emitUsageRow},
		{"roster", `{"id":"user_A",` + unknown + `}`, emitRosterRow},
	} {
		t.Run(tt.endpoint, func(t *testing.T) {
			t.Parallel()
			records := tt.emit(t, tt.row)
			require.NotEmpty(t, records)
			// Asserted on the raw bytes, which is the level the contract lives at: the claim
			// is that the vendor's row is stored untouched, not that some re-serialisation of
			// it happens to contain an equivalent field.
			assert.Contains(t, string(records[0].Payload), unknown,
				"an unparsed field must reach the warehouse verbatim; this is what makes a vendor addition safe")
		})
	}
}

// TestADecimalAmountSurvivesVerbatim — the vendor sends cost as a decimal STRING in fractional
// cents. Any round-trip through a float loses digits silently, and the loss is invisible in a
// behavioural test because nothing in the walk reads the amount.
func TestADecimalAmountSurvivesVerbatim(t *testing.T) {
	t.Parallel()

	records := emitCostRow(t, `{"actor":{"user_id":"user_A"},"starting_at":"2026-08-25T00:00:00Z",`+
		`"product":"claude_code","amount":"41280.000000","cost_type":null}`)
	require.NotEmpty(t, records)

	assert.Contains(t, string(records[0].Payload), `"41280.000000"`,
		"the decimal string must keep every digit it arrived with")
	assert.Contains(t, string(records[0].Payload), `"cost_type":null`,
		"an explicit null must stay a null rather than becoming a zero value")
}

// --- fixture drivers: each runs the real operation over one row ---

func emitActivityRow(t *testing.T, row string) []*sdk.RawRecord {
	t.Helper()
	return runActivityFixture(t, &FetchAnalyticsUsersOp{}, row)
}

func emitSkillsRow(t *testing.T, row string) []*sdk.RawRecord {
	t.Helper()
	return runActivityFixture(t, &FetchAnalyticsSkillsOp{}, row)
}

func runActivityFixture(t *testing.T, op sdk.APIFetchOperation, row string) []*sdk.RawRecord {
	t.Helper()

	yesterday := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	cur, err := encodeCursor(activityPosition{CoveredThrough: yesterday.AddDate(0, 0, -1).Format(dateLayout)})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req proto.Message) (proto.Message, error) {
			r, isUsers := req.(*vgpb.AnthropicAnalyticsUsersRequest)
			if isUsers && r.GetDate() != yesterday.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsUsersResponse{}, nil
			}
			if s, isSkills := req.(*vgpb.AnthropicAnalyticsSkillsRequest); isSkills &&
				s.GetDate() != yesterday.Format(dateLayout) {
				return &vgpb.AnthropicAnalyticsSkillsResponse{}, nil
			}
			if isUsers {
				return &vgpb.AnthropicAnalyticsUsersResponse{Data: [][]byte{[]byte(row)}}, nil
			}
			return &vgpb.AnthropicAnalyticsSkillsResponse{Data: [][]byte{[]byte(row)}}, nil
		}).AnyTimes()

	sink := &collectingSink{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)
	return sink.records
}

func emitCostRow(t *testing.T, row string) []*sdk.RawRecord {
	t.Helper()
	return runReportFixture(t, &FetchAnalyticsUserCostOp{}, row)
}

func emitUsageRow(t *testing.T, row string) []*sdk.RawRecord {
	t.Helper()
	return runReportFixture(t, &FetchAnalyticsUserUsageOp{}, row)
}

func runReportFixture(t *testing.T, op sdk.APIFetchOperation, row string) []*sdk.RawRecord {
	t.Helper()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -1).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	// Keyed on the request type rather than a flag, so the fixture answers whichever report
	// endpoint the operation actually calls.
	gw.EXPECT().Execute(gomock.Any(),
		gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsUserCostReportRequest{})).
		Return(&vgpb.AnthropicAnalyticsUserCostReportResponse{
			Data: [][]byte{[]byte(row)}, HasMore: false, DataRefreshedAt: timestamppb.New(today),
		}, nil).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(),
		gomock.AssignableToTypeOf(&vgpb.AnthropicAnalyticsUserUsageReportRequest{})).
		Return(&vgpb.AnthropicAnalyticsUserUsageReportResponse{
			Data: [][]byte{[]byte(row)}, HasMore: false, DataRefreshedAt: timestamppb.New(today),
		}, nil).AnyTimes()

	sink := &collectingSink{}
	_, err = op.Execute(context.Background(), testParams(t, gw, sink, cur, Settings{}))
	require.NoError(t, err)
	return sink.records
}

func emitRosterRow(t *testing.T, row string) []*sdk.RawRecord {
	t.Helper()

	ctrl := gomock.NewController(t)
	gw := fwmocks.NewMockVendorGateway(ctrl)
	gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
	gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
		Return(&vgpb.AnthropicOrganizationMembersResponse{
			Data: [][]byte{[]byte(row)}, HasMore: false,
		}, nil).AnyTimes()

	sink := &collectingSink{}
	_, err := (&FetchOrganizationMembersOp{}).Execute(context.Background(),
		testParams(t, gw, sink, nil, Settings{}))
	require.NoError(t, err)
	return sink.records
}

// TestTheWireIdentifierIsPinned. The id keys the stored config, the cursor rows, the schedule
// gate and the catalog entry, so changing it orphans all four at once — silently, for
// organisations already connected. The catalog side is asserted where the catalog lives.
func TestTheWireIdentifierIsPinned(t *testing.T) {
	t.Parallel()
	assert.Equal(t, sdk.ConnectorID("claude-code"), (&Connector{}).ID())
}

// TestARotatedKeyAnsweringForAnotherOrganisationIsRefused is a cross-tenant guard, and the
// report endpoints are the only place the answer appears.
//
// The report endpoints are the only ones that say which organisation answered, so they are the
// only place a drifted key can be detected — and the ERROR KIND is what turns detection into
// protection. A Terminal auth error reaches the orchestrator's auth-revoked transition, which
// pauses every operation on the connector; anything else leaves the three operations whose
// endpoints carry no organisation id running, and they would keep emitting.
func TestARotatedKeyAnsweringForAnotherOrganisationIsRefused(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	cur, err := encodeCursor(reportPosition{
		CoveredThrough:  today.AddDate(0, 0, -1).Format(dateLayout),
		DataRefreshedAt: today.Format(time.RFC3339),
	})
	require.NoError(t, err)

	run := func(t *testing.T, saved, answering string) error {
		t.Helper()
		ctrl := gomock.NewController(t)
		gw := fwmocks.NewMockVendorGateway(ctrl)
		gw.EXPECT().ForGroup(gomock.Any()).Return(gw).AnyTimes()
		gw.EXPECT().Execute(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, proto.Message) (proto.Message, error) {
				return &vgpb.AnthropicAnalyticsUserCostReportResponse{
					Data: [][]byte{[]byte(`{"actor":{"user_id":"user_A"},` +
						`"starting_at":"` + today.AddDate(0, 0, -1).Format(time.RFC3339) + `",` +
						`"product":"claude_code","amount":"100.000000"}`)},
					HasMore: false, DataRefreshedAt: timestamppb.New(today),
					OrganizationId: answering,
				}, nil
			}).AnyTimes()

		sink := &collectingSink{}
		_, execErr := (&FetchAnalyticsUserCostOp{}).Execute(context.Background(),
			testParams(t, gw, sink, cur, Settings{AnthropicOrganizationID: saved}))
		if execErr == nil {
			assert.NotEmpty(t, sink.records, "a run that proceeds should have emitted something")
		} else {
			assert.Empty(t, sink.records, "nothing may be stored once the organisation is wrong")
		}
		return execErr
	}

	t.Run("a different organisation is refused, terminally", func(t *testing.T) {
		t.Parallel()
		err := run(t, "org_saved", "org_someone_else")
		require.Error(t, err)

		var syncErr *sdk.SyncError
		require.ErrorAs(t, err, &syncErr)
		assert.True(t, syncErr.Terminal, "no retry resolves a credential pointing elsewhere")
		assert.Equal(t, sdk.ErrKindAuth, syncErr.Kind,
			"auth-kind is what pauses the WHOLE connector; config-invalid only skips this task's retries, "+
				"leaving the three operations whose endpoints carry no organisation id still running")
		assert.Contains(t, err.Error(), "org_someone_else")
		assert.Contains(t, err.Error(), "reconnect",
			"the operator needs the recovery path, not only the diagnosis")
	})

	t.Run("the same organisation proceeds", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, run(t, "org_saved", "org_saved"))
	})

	t.Run("a configuration saved before the field existed is not a mismatch", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, run(t, "", "org_anything"))
	})

	// Not a mismatch, and still refused — the two are different claims. A mismatch PROVES the
	// key answers for someone else; an omission means the page cannot be checked at all, and
	// the setup probe already refuses that same shape. Letting the scheduled walk store it left
	// the two paths disagreeing, and the thing stored is data this connector cannot attribute.
	t.Run("a vendor that omits the field is refused, but not terminally", func(t *testing.T) {
		t.Parallel()
		err := run(t, "org_saved", "")
		require.Error(t, err)

		var syncErr *sdk.SyncError
		require.ErrorAs(t, err, &syncErr)
		assert.False(t, syncErr.Terminal,
			"a missing field is as likely a transient vendor regression as a wrong key, and pausing "+
				"every operation on the connector is not proportionate to \"cannot verify\"")
		assert.NotEqual(t, sdk.ErrKindAuth, syncErr.Kind,
			"reserve the connector-wide pause for a key that demonstrably answers for another organisation")
		assert.Contains(t, err.Error(), "cannot be checked")
	})

	t.Run("with no saved organisation there is nothing to check against", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, run(t, "", ""))
	})
}

// TestAGuardedOperationRunsFirst is why the schedule order is load-bearing rather than cosmetic.
//
// Only the two report endpoints report which Anthropic organisation answered, so only they can
// detect a key pointing at a different one — and that detection pauses the whole connector. If
// an unguarded operation ran first, it would emit the other organisation's rows before anything
// noticed, once per day, until someone looked.
func TestAGuardedOperationRunsFirst(t *testing.T) {
	t.Parallel()

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	from := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	var firstAt time.Time
	var firstOp sdk.OperationID
	for _, op := range (&Connector{}).Operations() {
		sched, err := parser.Parse(op.DefaultSchedule)
		require.NoErrorf(t, err, "unparseable schedule on %s", op.ID)

		at := sched.Next(from)
		if firstAt.IsZero() || at.Before(firstAt) {
			firstAt, firstOp = at, op.ID
		}
	}

	require.NotEmpty(t, firstOp)
	assert.Truef(t, reportOperations[firstOp],
		"%s runs first but its endpoint reports no organisation id, so a drifted key would emit "+
			"before anything could detect it; a report operation must lead", firstOp)
}

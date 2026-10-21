// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cursorgateway

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
)

func testCursorBaseURLs() map[string]string {
	return map[string]string{
		constants.GroupCursorAdmin: "https://api.cursor.com",
		constants.GroupCursorUsage: "https://api.cursor.com",
	}
}

// capturedMembersResponse is the verbatim shape the vendor returned when the
// endpoint was called live, identifiers and addresses redacted. Kept as the raw
// body rather than a hand-built struct so the parser is exercised against what
// the vendor actually sends — including the empty `name` and the undocumented
// `free-owner` role, both of which were observed.
const capturedMembersResponse = `{"teamMembers":[
  {"name":"Dev One","email":"dev@example.com","id":"user_AAAAAAAAAAAAAAAAAAAAAAAAAA","role":"owner","isRemoved":false},
  {"name":"","email":"svc@example.com","id":"user_BBBBBBBBBBBBBBBBBBBBBBBBBB","role":"member","isRemoved":false},
  {"name":"Dev Two","email":"dev2@example.com","id":"user_CCCCCCCCCCCCCCCCCCCCCCCCCC","role":"free-owner","isRemoved":false}
]}`

func TestListTeamMembersVendorRequest_MethodAndURL(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())

	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, vr.HTTPMethod())
	assert.Equal(t, "https://api.cursor.com/teams/members", vr.URL())

	body, err := vr.Marshal()
	require.NoError(t, err)
	assert.Nil(t, body, "a GET carries no body")
}

// TestListTeamMembersVendorRequest_ResolvesAgainstAdminGroup pins which group's
// base URL this endpoint reads. Both Cursor groups point at the same host today,
// so a mix-up would not surface as a broken URL — it would surface later as the
// wrong rate-limit bucket, which is much harder to notice.
func TestListTeamMembersVendorRequest_ResolvesAgainstAdminGroup(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", map[string]string{
		constants.GroupCursorAdmin: "https://admin.example.com",
		constants.GroupCursorUsage: "https://usage.example.com",
	})

	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	assert.Equal(t, "https://admin.example.com/teams/members", vr.URL(),
		"members must resolve against the admin group, not the usage group")
}

// TestAuthSetter_BasicAuthKeyAsUsernameEmptyPassword is the load-bearing auth
// assertion. Cursor wants the API key as the Basic username with an EMPTY
// password, which is unusual enough that the header is decoded and checked
// rather than compared to a golden string — a wrong separator or a missing colon
// would otherwise pass a naive equality check.
func TestAuthSetter_BasicAuthKeyAsUsernameEmptyPassword(t *testing.T) {
	factory := NewCursorVendorRequest("crsr_secret", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	withAuth, ok := vr.(interface {
		SetAuth(*http.Request) *http.Request
	})
	require.True(t, ok, "the request must set its own auth")

	req, err := http.NewRequest(http.MethodGet, vr.URL(), nil)
	require.NoError(t, err)
	req = withAuth.SetAuth(req)

	user, pass, hasBasic := req.BasicAuth()
	require.True(t, hasBasic, "Authorization must parse as Basic")
	assert.Equal(t, "crsr_secret", user, "the API key is the username")
	assert.Empty(t, pass, "the password must be empty")

	// And the wire form: `Basic base64("key:")` — the trailing colon with nothing
	// after it is the part a hand-rolled header gets wrong.
	raw := req.Header.Get("Authorization")
	encoded := strings.TrimPrefix(raw, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	assert.Equal(t, "crsr_secret:", string(decoded))
}

// TestAuthSetter_EmptyKeySetsNoHeader guards the degenerate case: an absent key
// must not produce a header authenticating as the empty user, which a vendor
// could answer with a confusing 200 on some other principal.
func TestAuthSetter_EmptyKeySetsNoHeader(t *testing.T) {
	a := &authSetter{apiKey: ""}
	req, err := http.NewRequest(http.MethodGet, "https://api.cursor.com/teams/members", nil)
	require.NoError(t, err)

	req = a.SetAuth(req)
	assert.Empty(t, req.Header.Get("Authorization"))
}

func TestListTeamMembersVendorResponse_Unmarshal(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	msg, err := vr.GetResponse().Unmarshal([]byte(capturedMembersResponse))
	require.NoError(t, err)

	resp, ok := msg.(*connectorpb.CursorListTeamMembersResponse)
	require.True(t, ok)
	require.Len(t, resp.GetTeamMembers(), 3)

	// Members are carried through as raw vendor JSON, so the undocumented role
	// value survives rather than being normalised away at the gateway.
	assert.Contains(t, string(resp.GetTeamMembers()[2]), `"role":"free-owner"`)
}

// TestListTeamMembersVendorResponse_EmptyTeam asserts an empty roster is not an
// error. A team with no members is a legitimate response, and treating it as a
// parse failure would make the credential look invalid.
func TestListTeamMembersVendorResponse_EmptyTeam(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	msg, err := vr.GetResponse().Unmarshal([]byte(`{"teamMembers":[]}`))
	require.NoError(t, err)

	resp, ok := msg.(*connectorpb.CursorListTeamMembersResponse)
	require.True(t, ok)
	assert.Empty(t, resp.GetTeamMembers())
}

func TestListTeamMembersVendorResponse_MalformedBody(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	_, err = vr.GetResponse().Unmarshal([]byte(`{"teamMembers":`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshalling cursor list team members response")
}

func TestNewCursorVendorRequest_UnsupportedType(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())

	_, err := factory(&connectorpb.CursorListTeamMembersResponse{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported request type")
}

// TestListTeamMembers_EndToEndAgainstResponder is the slice's demoable proof: the
// request travels over real HTTP to a stand-in for Cursor, carrying the auth the
// vendor requires, and the captured response parses. No live credential, no
// network beyond loopback.
func TestListTeamMembers_EndToEndAgainstResponder(t *testing.T) {
	var gotAuthUser, gotAuthPass string
	var gotAuthOK bool
	var gotPath, gotMethod string

	responder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthUser, gotAuthPass, gotAuthOK = r.BasicAuth()
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(capturedMembersResponse))
	}))
	defer responder.Close()

	factory := NewCursorVendorRequest("crsr_secret", map[string]string{
		constants.GroupCursorAdmin: responder.URL,
	})
	vr, err := factory(&connectorpb.CursorListTeamMembersRequest{})
	require.NoError(t, err)

	req, err := http.NewRequest(vr.HTTPMethod(), vr.URL(), nil)
	require.NoError(t, err)
	req = vr.(interface {
		SetAuth(*http.Request) *http.Request
	}).SetAuth(req)

	resp, err := responder.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/teams/members", gotPath)
	require.True(t, gotAuthOK, "the vendor saw Basic auth")
	assert.Equal(t, "crsr_secret", gotAuthUser)
	assert.Empty(t, gotAuthPass)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	msg, err := vr.GetResponse().Unmarshal(body)
	require.NoError(t, err)
	parsed, ok := msg.(*connectorpb.CursorListTeamMembersResponse)
	require.True(t, ok)
	assert.Len(t, parsed.GetTeamMembers(), 3)
}

// capturedUsageEventsResponse has the shape the vendor returns, with synthetic values.
// It carries the field's three traps — `timestamp` is epoch ms as a *string*,
// `serviceAccountId` is the literal string "null" rather than JSON null, and `kind`
// is a human-readable label rather than an enum — plus the `pagination` and `period`
// envelopes the parser deliberately ignores. Kept raw so the parser is exercised
// against what the vendor actually sends.
const capturedUsageEventsResponse = `{"totalUsageEventsCount":4,
 "pagination":{"numPages":2,"currentPage":1,"pageSize":2,"hasNextPage":true,"hasPreviousPage":false},
 "usageEvents":[{
   "timestamp":"1787184000000","userEmail":"svc@example.com",
   "model":"gpt-5","kind":"Example plan",
   "maxMode":false,"requestsCosts":1,
   "isTokenBasedCall":true,"isChargeable":true,"isHeadless":false,
   "tokenUsage":{"inputTokens":1000,"outputTokens":100,"cacheWriteTokens":0,
                 "cacheReadTokens":200,"totalCents":2.5},
   "chargedCents":2.5,"cursorTokenFee":0,
   "serviceAccountId":"null",
   "conversationId":"00000000-0000-4000-8000-000000000000"
 }],
 "period":{"startDate":1786579200000,"endDate":1787184000000}}`

func TestFilteredUsageEventsVendorRequest_MethodAndURL(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())

	vr, err := factory(&connectorpb.CursorFilteredUsageEventsRequest{StartDate: 1, EndDate: 2})
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, vr.HTTPMethod())
	assert.Equal(t, "https://api.cursor.com/teams/filtered-usage-events", vr.URL())
}

// TestFilteredUsageEventsVendorRequest_ResolvesAgainstUsageGroup pins which group's
// base URL this endpoint reads. Both Cursor groups point at the same host today, so a
// mix-up would not surface as a broken URL — it would surface later as the wrong
// rate-limit bucket (20/min on the Admin API against 60/min here), which is much
// harder to notice.
func TestFilteredUsageEventsVendorRequest_ResolvesAgainstUsageGroup(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", map[string]string{
		constants.GroupCursorAdmin: "https://admin.example.com",
		constants.GroupCursorUsage: "https://usage.example.com",
	})

	vr, err := factory(&connectorpb.CursorFilteredUsageEventsRequest{StartDate: 1, EndDate: 2})
	require.NoError(t, err)

	assert.Equal(t, "https://usage.example.com/teams/filtered-usage-events", vr.URL(),
		"usage events must resolve against the usage group, not the admin group")
}

// TestFilteredUsageEventsVendorRequest_Marshal covers the one piece of judgment in
// the body: a zero page or pageSize is omitted rather than sent. Sending 0 asks the
// vendor for an empty page, where omitting it takes the vendor's own default — so the
// distinction decides whether a caller gets rows at all.
func TestFilteredUsageEventsVendorRequest_Marshal(t *testing.T) {
	tests := []struct {
		name   string
		req    *connectorpb.CursorFilteredUsageEventsRequest
		want   string
		absent []string
	}{
		{
			name:   "zero page and page size are omitted, not sent as 0",
			req:    &connectorpb.CursorFilteredUsageEventsRequest{StartDate: 100, EndDate: 200},
			want:   `{"endDate":200,"startDate":100}`,
			absent: []string{"page", "pageSize"},
		},
		{
			name: "explicit page and page size are sent",
			req: &connectorpb.CursorFilteredUsageEventsRequest{
				StartDate: 100, EndDate: 200, Page: 1, PageSize: 1,
			},
			want: `{"endDate":200,"page":1,"pageSize":1,"startDate":100}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
			vr, err := factory(tt.req)
			require.NoError(t, err)

			body, err := vr.Marshal()
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(body))
			for _, k := range tt.absent {
				assert.NotContains(t, string(body), `"`+k+`"`)
			}
		})
	}
}

// TestFilteredUsageEventsVendorResponse_Unmarshal asserts the parser survives the
// live shape: the count comes from the vendor's own total rather than the page
// length, each event is carried through as opaque bytes with its string timestamp
// and literal "null" intact, and the pagination/period envelopes are ignored without
// error.
func TestFilteredUsageEventsVendorResponse_Unmarshal(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorFilteredUsageEventsRequest{StartDate: 1, EndDate: 2})
	require.NoError(t, err)

	msg, err := vr.GetResponse().Unmarshal([]byte(capturedUsageEventsResponse))
	require.NoError(t, err)
	parsed, ok := msg.(*connectorpb.CursorFilteredUsageEventsResponse)
	require.True(t, ok)

	assert.Equal(t, int64(4), parsed.GetTotalUsageEventsCount(),
		"the count is the vendor's window total, not the length of this page")
	require.Len(t, parsed.GetUsageEvents(), 1)

	event := string(parsed.GetUsageEvents()[0])
	assert.Contains(t, event, `"timestamp":"1787184000000"`, "the string timestamp is preserved verbatim")
	assert.Contains(t, event, `"serviceAccountId":"null"`, `the literal "null" is not coerced`)
	assert.Contains(t, event, `"conversationId":"00000000-0000-4000-8000-000000000000"`,
		"the conversation id is what later attribution work joins on")
}

func TestFilteredUsageEventsVendorResponse_EmptyAndMalformed(t *testing.T) {
	factory := NewCursorVendorRequest("test-key", testCursorBaseURLs())
	vr, err := factory(&connectorpb.CursorFilteredUsageEventsRequest{StartDate: 1, EndDate: 2})
	require.NoError(t, err)

	// A window with no billed calls is a valid answer, not an error — the usage probe
	// depends on this, since it asks for one row over one day.
	msg, err := vr.GetResponse().Unmarshal([]byte(`{"totalUsageEventsCount":0,"usageEvents":[]}`))
	require.NoError(t, err)
	parsed, ok := msg.(*connectorpb.CursorFilteredUsageEventsResponse)
	require.True(t, ok)
	assert.Empty(t, parsed.GetUsageEvents())
	assert.Zero(t, parsed.GetTotalUsageEventsCount())

	_, err = vr.GetResponse().Unmarshal([]byte(`{"usageEvents":`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshalling cursor filtered usage events response")
}

// httpDoer adapts a plain HTTP client to vendorapi.HTTPDoer so a test can drive
// vendorapi.Handle itself rather than re-implementing what it does. Driving Handle is
// the point: it is what applies the request's headers and auth in real use, so a
// hand-built request would pass while the real path sent no Content-Type at all.
type httpDoer struct{ client *http.Client }

func (d *httpDoer) Do(_ context.Context, req *http.Request) (*http.Response, error) {
	return d.client.Do(req) //nolint:wrapcheck // a test adapter; the caller asserts on the error
}

// TestFilteredUsageEvents_EndToEndThroughHandle sends the request through
// vendorapi.Handle to a stand-in for Cursor and parses the captured response, so the
// method, path, media type, body and auth are asserted as one whole on the real path.
func TestFilteredUsageEvents_EndToEndThroughHandle(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotAuthUser, gotContentType string
	var gotAuthOK bool

	responder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotAuthUser, _, gotAuthOK = r.BasicAuth()
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(capturedUsageEventsResponse))
	}))
	defer responder.Close()

	factory := NewCursorVendorRequest("crsr_secret", map[string]string{
		constants.GroupCursorUsage: responder.URL,
	})
	vr, err := factory(&connectorpb.CursorFilteredUsageEventsRequest{
		StartDate: 100, EndDate: 200, Page: 1, PageSize: 1,
	})
	require.NoError(t, err)

	msg, err := vendorapi.Handle(context.Background(), &httpDoer{client: responder.Client()}, vr)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/teams/filtered-usage-events", gotPath)
	// The media type is load-bearing: a server that binds JSON only for it rejects the
	// probe before looking at the key, so every valid connection would read as
	// temporarily unavailable.
	assert.Equal(t, "application/json", gotContentType)
	require.True(t, gotAuthOK, "the vendor saw Basic auth")
	assert.Equal(t, "crsr_secret", gotAuthUser)
	assert.JSONEq(t, `{"startDate":100,"endDate":200,"page":1,"pageSize":1}`, gotBody)

	parsed, ok := msg.(*connectorpb.CursorFilteredUsageEventsResponse)
	require.True(t, ok)
	assert.Len(t, parsed.GetUsageEvents(), 1)
	assert.Equal(t, int64(4), parsed.GetTotalUsageEventsCount())
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/internal/gen/gateway/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// qualifiedNameLiteral captures the GraphQL string literal passed as qualifiedName,
// honouring backslash escapes so an embedded \" does not end the match.
var qualifiedNameLiteral = regexp.MustCompile(`qualifiedName: ("(?:[^"\\]|\\.)*")\)`)

// TestEdge_BranchQueryQuotesTheRefName: git allows '"' in a branch name (check-ref-format
// forbids space, ~ ^ : ? * [ \ and control characters, not quotes), and tracked refs are
// administrator-configured. The branch must be a well-formed GraphQL string literal whose
// value is exactly refs/heads/<branch>, never text that ends the literal early.
func TestEdge_BranchQueryQuotesTheRefName(t *testing.T) {
	for _, branch := range []string{
		"main",
		"feature/ünïcødé-🚀",
		`release/"v2"`,
		`fix") { oid } } } } #`,
	} {
		t.Run(branch, func(t *testing.T) {
			q := buildCommitsQueryForBranch("acme", "repo", branch, time.Time{}, "", 10)
			m := qualifiedNameLiteral.FindStringSubmatch(q)
			require.NotNil(t, m, "query must pass qualifiedName as one string literal argument:\n%s", q)
			got, err := strconv.Unquote(m[1])
			require.NoError(t, err)
			assert.Equal(t, "refs/heads/"+branch, got)
		})
	}
}

// TestEdge_CursorEntriesFromOtherVersions: a stored per-repo entry from a newer release
// (a struct carrying fields this release does not know) decodes to its watermark rather
// than reset the repo to a full lookback. An entry that is not an object, such as a bare
// RFC 3339 string or a number, is dropped like any corrupt entry.
func TestEdge_CursorEntriesFromOtherVersions(t *testing.T) {
	want := time.Date(2026, 9, 1, 10, 0, 0, 123_000_000, time.UTC)
	cursor := sdk.NewCompositeCursor(map[string]json.RawMessage{
		"acme/string": json.RawMessage(`"2026-09-01T12:00:00.123+02:00"`),
		"acme/newer":  json.RawMessage(`{"ts":"2026-09-01T10:00:00.123Z","pc":"abc","sf":"2026-08-01T00:00:00Z","future_field":{"x":1}}`),
		"acme/broken": json.RawMessage(`12345`),
	})

	assert.True(t, getRepoCursorState(cursor, "acme/string").Timestamp.IsZero(), "a bare string is no state")
	newer := getRepoCursorState(cursor, "acme/newer")
	assert.True(t, newer.Timestamp.Equal(want))
	assert.Equal(t, "abc", newer.PageCursor)

	states := existingCursorStates(cursor)
	assert.NotContains(t, states, "acme/string", "an entry that is not an object is skipped like a corrupt one")
	assert.Contains(t, states, "acme/newer")
	assert.NotContains(t, states, "acme/broken", "a corrupt entry is skipped, not carried as a zero state")
}

// TestEdge_PartialDataWithFormattedEmptyRepository: the tolerant path refuses a truncated
// `{"repository":{}}` alongside permission errors. The same empty object with whitespace
// inside it is the same truncated payload and must be refused too.
func TestEdge_PartialDataWithFormattedEmptyRepository(t *testing.T) {
	for _, data := range []string{`{"repository":{}}`, `{"repository": { }}`, "{\"repository\":{\n}}", `{"repository":[ ]}`} {
		t.Run(data, func(t *testing.T) {
			gw := &mockVendorGateway{executeFunc: func(_ context.Context, _ proto.Message) (proto.Message, error) {
				return &connectorpb.GitHubGraphQLResponse{
					Data:   json.RawMessage(data),
					Errors: []*connectorpb.GitHubGraphQLError{{Type: "FORBIDDEN", Message: "Resource not accessible"}},
				}, nil
			}}
			got, err := doGraphQL(context.Background(), gw, "{ q }", true)
			assert.Error(t, err, "an empty repository object is a truncated response, not partial data")
			assert.Nil(t, got)
		})
	}
}

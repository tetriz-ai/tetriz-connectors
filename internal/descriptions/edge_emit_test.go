// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package descriptions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
	"github.com/tetriz-ai/tetriz-connectors/testkit"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

type fakeS3 struct {
	writes map[string][]byte
	err    error
}

func (f *fakeS3) Write(_ context.Context, dst string, content []byte, _ string) error {
	if f.err != nil {
		return f.err
	}
	if f.writes == nil {
		f.writes = map[string][]byte{}
	}
	f.writes[dst] = content
	return nil
}

func validInput() EmitIssueDescriptionInput {
	return EmitIssueDescriptionInput{
		ConnectorID: "jira", OperationID: "fetch_issues_data", OrgID: "org-1",
		IssueKey: "PROJ-1", ExternalID: "PROJ-1", PlatformProjectID: "10000",
		Raw: []byte("# body"), S3Key: "desc/PROJ-1", Meta: map[string]string{},
	}
}

// EmitIssueDescriptionInput.Meta is optional-looking (a plain map on an input
// struct), so a caller that leaves it nil must get a record, not a panic from
// writing description_sha into a nil map.
func TestEdge_NilMetaDoesNotPanic(t *testing.T) {
	in := validInput()
	in.Meta = nil
	sink := testkit.NewChannelSink()
	var err error
	require.NotPanics(t, func() { err = EmitIssueDescription(context.Background(), &fakeS3{}, sink, in) })
	require.NoError(t, err)
	require.Len(t, sink.Records(), 1)
	assert.Contains(t, sink.Records()[0].Metadata["description_sha"], "sha256:")
}

// Whitespace-only and empty bodies, and an unresolved project, write nothing
// and emit nothing; an S3 failure is returned wrapped and nothing is emitted.
func TestEdge_SkipsAndFailures(t *testing.T) {
	for name, edit := range map[string]func(*EmitIssueDescriptionInput){
		"empty":              func(in *EmitIssueDescriptionInput) { in.Raw = nil },
		"whitespace":         func(in *EmitIssueDescriptionInput) { in.Raw = []byte(" \n\t ") },
		"unresolved project": func(in *EmitIssueDescriptionInput) { in.PlatformProjectID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			in := validInput()
			edit(&in)
			s3 := &fakeS3{}
			sink := testkit.NewChannelSink()
			require.NoError(t, EmitIssueDescription(context.Background(), s3, sink, in))
			assert.Empty(t, s3.writes)
			assert.Empty(t, sink.Records())
		})
	}

	boom := errors.New("s3 down")
	sink := testkit.NewChannelSink()
	err := EmitIssueDescription(context.Background(), &fakeS3{err: boom}, sink, validInput())
	require.ErrorIs(t, err, boom)
	assert.Empty(t, sink.Records())
}

// The payload shape is the producer/consumer contract and the sha is over the raw bytes.
func TestEdge_PayloadAndDigest(t *testing.T) {
	sink := testkit.NewChannelSink()
	in := validInput()
	require.NoError(t, EmitIssueDescription(context.Background(), &fakeS3{}, sink, in))
	rec := sink.Records()[0]
	var p IssueDescriptionPayload
	require.NoError(t, json.Unmarshal(rec.Payload, &p))
	assert.Equal(t, IssueDescriptionPayload{S3Key: "desc/PROJ-1", IssueKey: "PROJ-1", PlatformProjectID: "10000"}, p)
	sum := sha256.Sum256(in.Raw)
	assert.Equal(t, "sha256:"+hex.EncodeToString(sum[:]), rec.Metadata["description_sha"])
	assert.Equal(t, string(sdk.ConnectorID("jira")), rec.ConnectorID)
}

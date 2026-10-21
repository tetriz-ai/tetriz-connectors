// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sqs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The inline limit is in bytes, as SQS counts it, not in characters: a body
// with fewer runes than the threshold but more bytes is offloaded.
func TestEdge_ThresholdCountsBytesNotRunes(t *testing.T) {
	q := &fakeSQS{}
	blobs := &blobstore.Memory{}
	p := New(q, "https://queue.example", blobs, "offload")
	// "é" is two bytes; OffloadThreshold/2+1 of them is just over the limit in
	// bytes and well under it in runes.
	org := strings.Repeat("é", OffloadThreshold/2+1)
	require.Less(t, utf8.RuneCountInString(org), OffloadThreshold)
	_, err := p.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: org})
	require.NoError(t, err)
	assert.Equal(t, "true", aws.ToString(q.in[0].MessageAttributes[OffloadAttribute].StringValue))
	body, ok := blobs.Get(aws.ToString(q.in[0].MessageBody))
	require.True(t, ok)
	assert.Contains(t, string(body), org, "the blob holds the unescaped UTF-8 body")
}

type failingBlobs struct{ blobstore.Memory }

func (*failingBlobs) Write(context.Context, string, []byte, string) error {
	return errors.New("bucket unavailable")
}

var _ sdk.BlobStore = (*failingBlobs)(nil)

// When the offload write fails, nothing is sent: a message pointing at a blob
// that does not exist would be unreadable for the consumer.
func TestEdge_FailedOffloadSendsNoMessage(t *testing.T) {
	q := &fakeSQS{}
	p := New(q, "https://queue.example", &failingBlobs{}, "offload")
	_, err := p.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: strings.Repeat("x", OffloadThreshold+1)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket unavailable")
	assert.Empty(t, q.in, "no message is sent for a body that was not stored")
}

type erroringSQS struct{ err error }

func (e erroringSQS) SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	return nil, e.err
}

// A send failure is returned wrapped, so a caller can still match the
// underlying cause (for example a context deadline).
func TestEdge_SendFailureKeepsItsCause(t *testing.T) {
	p := New(erroringSQS{err: context.DeadlineExceeded}, "https://queue.example", nil, "offload")
	id, err := p.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: "org"})
	require.Error(t, err)
	assert.Empty(t, id)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// Two oversized bodies never share a blob key, so one cannot overwrite the
// other before its consumer reads it.
func TestEdge_OffloadKeysAreDistinct(t *testing.T) {
	q := &fakeSQS{}
	blobs := &blobstore.Memory{}
	p := New(q, "https://queue.example", blobs, "offload")
	big := &connectorpb.ConnectorDataEnvelope{OrganizationId: strings.Repeat("x", OffloadThreshold+1)}
	for range 3 {
		_, err := p.Publish(context.Background(), big)
		require.NoError(t, err)
	}
	assert.Len(t, blobs.Keys(), 3)
}

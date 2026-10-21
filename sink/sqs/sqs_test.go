// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sqs

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
)

type fakeSQS struct{ in []*awssqs.SendMessageInput }

func (f *fakeSQS) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.in = append(f.in, in)
	return &awssqs.SendMessageOutput{MessageId: aws.String("m-1")}, nil
}

// A small envelope goes inline; one over the threshold goes to the blob store
// and the message carries its key, flagged by the offload attribute.
func TestPublishOffloadsLargeBodies(t *testing.T) {
	ctx := context.Background()
	q := &fakeSQS{}
	blobs := &blobstore.Memory{}
	p := New(q, "https://queue.example", blobs, "offload")

	id, err := p.Publish(ctx, &connectorpb.ConnectorDataEnvelope{OrganizationId: "org"})
	require.NoError(t, err)
	assert.Equal(t, "m-1", id)
	assert.Contains(t, aws.ToString(q.in[0].MessageBody), `"organizationId":"org"`)
	assert.Equal(t, "false", aws.ToString(q.in[0].MessageAttributes[OffloadAttribute].StringValue))

	big := &connectorpb.ConnectorDataEnvelope{OrganizationId: strings.Repeat("x", OffloadThreshold)}
	_, err = p.Publish(ctx, big)
	require.NoError(t, err)
	key := aws.ToString(q.in[1].MessageBody)
	assert.True(t, strings.HasPrefix(key, "offload/"))
	assert.Equal(t, "true", aws.ToString(q.in[1].MessageAttributes[OffloadAttribute].StringValue))
	body, ok := blobs.Get(key)
	require.True(t, ok)
	assert.Greater(t, len(body), OffloadThreshold)

	_, err = New(q, "https://queue.example", nil, "offload").Publish(ctx, big)
	assert.Error(t, err, "an oversized body with no blob store is an error")
}

// A body of exactly OffloadThreshold bytes goes inline; one byte more goes to
// the blob store.
func TestOffloadThresholdBoundary(t *testing.T) {
	ctx := context.Background()
	q := &fakeSQS{}
	p := New(q, "https://queue.example", &blobstore.Memory{}, "offload")
	const wrapper = len(`{"organizationId":""}`)
	for _, tc := range []struct {
		size    int
		offload string
	}{{OffloadThreshold, "false"}, {OffloadThreshold + 1, "true"}} {
		_, err := p.Publish(ctx, &connectorpb.ConnectorDataEnvelope{OrganizationId: strings.Repeat("x", tc.size-wrapper)})
		require.NoError(t, err)
		last := q.in[len(q.in)-1]
		assert.Equal(t, tc.offload, aws.ToString(last.MessageAttributes[OffloadAttribute].StringValue), "body of %d bytes", tc.size)
		if tc.offload == "false" {
			assert.Len(t, aws.ToString(last.MessageBody), tc.size)
		}
	}
}

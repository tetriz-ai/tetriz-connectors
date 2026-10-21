// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package sqs publishes connector envelopes to an Amazon SQS queue.
//
// The message body is the envelope's protojson. A body larger than
// OffloadThreshold is written to a BlobStore instead, and the message body
// becomes the blob key. Every message carries a string attribute USE_S3,
// "true" or "false", so a consumer knows which of the two it received.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// OffloadThreshold is the largest body, in bytes, sent inline. SQS accepts
// 256 KiB including attributes; the margin leaves room for them.
const OffloadThreshold = 225 * 1024

// OffloadAttribute is the message attribute that says whether the body is a
// blob key.
const OffloadAttribute = "USE_S3"

// SendMessageAPI is the part of the SQS client the publisher calls.
type SendMessageAPI interface {
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// Publisher sends envelopes to one queue.
type Publisher struct {
	client   SendMessageAPI
	queueURL string
	blobs    sdk.BlobStore
	prefix   string
}

// New returns a Publisher for queueURL. blobs receives bodies over
// OffloadThreshold under keys "<keyPrefix>/<uuid>"; with a nil blobs such a
// body is an error.
func New(client SendMessageAPI, queueURL string, blobs sdk.BlobStore, keyPrefix string) *Publisher {
	return &Publisher{client: client, queueURL: queueURL, blobs: blobs, prefix: keyPrefix}
}

// Publish implements sink.Publisher and returns the SQS message id.
func (p *Publisher) Publish(ctx context.Context, envelope *connectorpb.ConnectorDataEnvelope) (string, error) {
	body, err := protojson.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("sqs publisher: marshal envelope: %w", err)
	}
	useBlob := len(body) > OffloadThreshold
	msg := string(body)
	if useBlob {
		if p.blobs == nil {
			return "", errors.New("sqs publisher: message over the inline limit and no blob store configured")
		}
		key := p.prefix + "/" + uuid.NewString()
		if err := p.blobs.Write(ctx, key, body, "private"); err != nil {
			return "", fmt.Errorf("sqs publisher: offload body: %w", err)
		}
		msg = key
	}
	out, err := p.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(msg),
		MessageAttributes: map[string]types.MessageAttributeValue{
			OffloadAttribute: {DataType: aws.String("String"), StringValue: aws.String(strconv.FormatBool(useBlob))},
		},
	})
	if err != nil {
		return "", fmt.Errorf("sqs publisher: send: %w", err)
	}
	return aws.ToString(out.MessageId), nil
}

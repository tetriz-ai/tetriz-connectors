// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sqs_test

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/tetriz-ai/tetriz-connectors/blobstore"
	connectorpb "github.com/tetriz-ai/tetriz-connectors/gen/connector/v1"
	"github.com/tetriz-ai/tetriz-connectors/sink/sqs"
)

// printingQueue stands in for *sqs.Client: it prints each message.
type printingQueue struct{}

func (printingQueue) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	fmt.Println(aws.ToString(in.MessageBody))
	return &awssqs.SendMessageOutput{MessageId: aws.String("1")}, nil
}

// Publish envelopes to a queue. With a real queue, pass an *sqs.Client from
// the AWS SDK.
func ExampleNew() {
	publisher := sqs.New(printingQueue{}, "https://sqs.example.com/queue", &blobstore.Memory{}, "offload")
	_, _ = publisher.Publish(context.Background(), &connectorpb.ConnectorDataEnvelope{OrganizationId: "org-1"})
	// Output: {"organizationId":"org-1"}
}

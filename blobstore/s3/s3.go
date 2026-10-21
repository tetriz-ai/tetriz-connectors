// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package s3 is an sdk.BlobStore that writes to one Amazon S3 bucket.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// PutObjectAPI is the part of the S3 client the store calls.
type PutObjectAPI interface {
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *awss3.DeleteObjectInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error)
}

// Store writes blobs to Bucket.
type Store struct {
	client PutObjectAPI
	bucket string
}

var _ sdk.BlobStore = (*Store)(nil)

// New returns a Store for bucket.
func New(client PutObjectAPI, bucket string) *Store {
	return &Store{client: client, bucket: bucket}
}

// Bucket names the bucket, for records that point at a stored blob.
func (s *Store) Bucket() string { return s.bucket }

// Write puts content under key with the canned ACL acl (empty for none).
func (s *Store) Write(ctx context.Context, key string, content []byte, acl string) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		ACL:           types.ObjectCannedACL(acl),
		Body:          bytes.NewReader(content),
		ContentLength: aws.Int64(int64(len(content))),
	})
	if err != nil {
		return fmt.Errorf("putting object to S3: %w", err)
	}
	return nil
}

// WriteWithOpts puts content under key with the given attributes. Nil opts
// is an error.
func (s *Store) WriteWithOpts(ctx context.Context, key string, content []byte, opts *sdk.BlobWriteOpts) error {
	if opts == nil {
		return errors.New("empty options provided")
	}
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		ACL:           types.ObjectCannedACL(opts.ACL),
		Body:          bytes.NewReader(content),
		ContentLength: aws.Int64(int64(len(content))),
	}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	if opts.ChecksumSHA256 != "" {
		in.ChecksumSHA256 = aws.String(opts.ChecksumSHA256)
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("putting object to S3: %w", err)
	}
	return nil
}

// Delete removes the object under key.
func (s *Store) Delete(ctx context.Context, key string) error {
	if _, err := s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("deleting object from S3: %w", err)
	}
	return nil
}

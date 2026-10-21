// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package s3

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// The package had no tests, so it had no guard; this file adds one.
func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

type fakeS3 struct {
	puts    []*awss3.PutObjectInput
	bodies  [][]byte
	deletes []*awss3.DeleteObjectInput
	err     error
}

func (f *fakeS3) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.puts = append(f.puts, in)
	b, _ := io.ReadAll(in.Body)
	f.bodies = append(f.bodies, b)
	return &awss3.PutObjectOutput{}, f.err
}

func (f *fakeS3) DeleteObject(_ context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	f.deletes = append(f.deletes, in)
	return &awss3.DeleteObjectOutput{}, f.err
}

// An empty body is a valid zero-length object: ContentLength is 0, not unset,
// and the optional attributes stay unset when the options leave them empty.
func TestEdge_EmptyBodyAndUnsetOptions(t *testing.T) {
	f := &fakeS3{}
	s := New(f, "bucket")
	require.NoError(t, s.WriteWithOpts(context.Background(), "k", nil, &sdk.BlobWriteOpts{}))
	require.Len(t, f.puts, 1)
	in := f.puts[0]
	assert.Equal(t, "bucket", aws.ToString(in.Bucket))
	require.NotNil(t, in.ContentLength)
	assert.Zero(t, *in.ContentLength)
	assert.Nil(t, in.ContentType, "no content type is sent when none is given")
	assert.Nil(t, in.ChecksumSHA256, "no checksum is sent when none is given")
	assert.Empty(t, string(in.ACL))
	assert.Empty(t, f.bodies[0])
}

// The body length sent matches the bytes, including for multibyte content.
func TestEdge_ContentLengthIsBytes(t *testing.T) {
	f := &fakeS3{}
	s := New(f, "bucket")
	body := []byte("日本語😀")
	require.NoError(t, s.WriteWithOpts(context.Background(), "k", body, &sdk.BlobWriteOpts{ContentType: "text/plain", ChecksumSHA256: "abc", ACL: "private"}))
	in := f.puts[0]
	assert.Equal(t, int64(len(body)), aws.ToInt64(in.ContentLength))
	assert.Equal(t, "text/plain", aws.ToString(in.ContentType))
	assert.Equal(t, "abc", aws.ToString(in.ChecksumSHA256))
	assert.Equal(t, "private", string(in.ACL))
	assert.Equal(t, body, f.bodies[0])
}

// Nil options make no request, and client errors keep their cause.
func TestEdge_NilOptionsAndErrors(t *testing.T) {
	f := &fakeS3{}
	s := New(f, "bucket")
	assert.Error(t, s.WriteWithOpts(context.Background(), "k", []byte("x"), nil))
	assert.Empty(t, f.puts, "nil options send nothing")

	cause := errors.New("throttled")
	f.err = cause
	assert.ErrorIs(t, s.Write(context.Background(), "k", []byte("x"), ""), cause)
	assert.ErrorIs(t, s.Delete(context.Background(), "k"), cause)
	require.Len(t, f.deletes, 1)
	assert.Equal(t, "k", aws.ToString(f.deletes[0].Key))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package blobstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

func TestDirWritesUnderRoot(t *testing.T) {
	ctx := context.Background()
	d := Dir{Root: t.TempDir()}
	require.NoError(t, d.WriteWithOpts(ctx, "a/b.json", []byte(`{}`), &sdk.BlobWriteOpts{ContentType: "application/json"}))
	b, err := os.ReadFile(filepath.Join(d.Root, "a", "b.json"))
	require.NoError(t, err)
	assert.Equal(t, `{}`, string(b))
	require.NoError(t, d.Delete(ctx, "a/b.json"))
	require.NoError(t, d.Delete(ctx, "a/b.json"), "deleting a missing key is not an error")

	for _, k := range []string{"", "../x", "/etc/x", ".."} {
		assert.Error(t, d.Write(ctx, k, nil, ""), k)
	}
	assert.Error(t, d.WriteWithOpts(ctx, "k", nil, nil), "nil options are rejected, as by the S3 store")
}

func TestMemoryKeepsOptions(t *testing.T) {
	ctx := context.Background()
	m := &Memory{}
	require.NoError(t, m.WriteWithOpts(ctx, "k", []byte("v"), &sdk.BlobWriteOpts{ContentType: "text/plain"}))
	v, ok := m.Get("k")
	require.True(t, ok)
	assert.Equal(t, "v", string(v))
	o, _ := m.Options("k")
	assert.Equal(t, "text/plain", o.ContentType)
	require.NoError(t, m.Delete(ctx, "k"))
	assert.Empty(t, m.Keys())
}

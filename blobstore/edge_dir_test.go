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
)

// A key that names the root itself ("." or one that cleans to it) is not a
// blob. Delete must reject it rather than remove the blob directory, and the
// root must still be there afterwards.
func TestEdge_DirKeyResolvingToRootIsRejected(t *testing.T) {
	ctx := context.Background()
	for _, k := range []string{".", "./", "a/..", "a/b/../..", "./."} {
		t.Run(k, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "blobs")
			require.NoError(t, os.Mkdir(root, 0o750))
			d := Dir{Root: root}
			assert.Error(t, d.Delete(ctx, k), "deleting key %q must be rejected", k)
			assert.Error(t, d.Write(ctx, k, []byte("x"), ""), "writing key %q must be rejected", k)
			_, err := os.Stat(root)
			assert.NoError(t, err, "the blob root survives key %q", k)
		})
	}
}

// Traversal spellings that clean to a path outside the root are rejected;
// ones that stay inside after cleaning land inside.
func TestEdge_DirTraversalSpellings(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	root := filepath.Join(parent, "blobs")
	d := Dir{Root: root}
	for _, k := range []string{"a/../../x", "a/./../../x", "..//x", "../blobs-sibling/x", "a/../../blobs/../x"} {
		assert.Error(t, d.Write(ctx, k, []byte("x"), ""), k)
	}
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	for _, e := range entries {
		assert.Equal(t, "blobs", e.Name(), "nothing is written beside the root")
	}

	require.NoError(t, d.Write(ctx, "a/../b//c", []byte("in"), ""))
	b, err := os.ReadFile(filepath.Join(root, "b", "c"))
	require.NoError(t, err)
	assert.Equal(t, "in", string(b))
}

// A key whose parent path is an existing file cannot be created; the write
// fails with an error instead of panicking or replacing the file.
func TestEdge_DirParentIsAFile(t *testing.T) {
	ctx := context.Background()
	d := Dir{Root: t.TempDir()}
	require.NoError(t, d.Write(ctx, "f", []byte("file"), ""))
	assert.Error(t, d.Write(ctx, "f/child", []byte("x"), ""))
	b, err := os.ReadFile(filepath.Join(d.Root, "f"))
	require.NoError(t, err)
	assert.Equal(t, "file", string(b), "the existing blob is untouched")
}

// The memory store keeps its own copy: changing the caller's slice after the
// write, or the slice Get returned, does not change the stored blob.
func TestEdge_MemoryCopiesContent(t *testing.T) {
	m := &Memory{}
	in := []byte("abc")
	require.NoError(t, m.Write(context.Background(), "k", in, ""))
	in[0] = 'X'
	got, _ := m.Get("k")
	got[1] = 'Y'
	again, ok := m.Get("k")
	require.True(t, ok)
	assert.Equal(t, "abc", string(again))

	_, ok = m.Get("missing")
	assert.False(t, ok)
	require.NoError(t, m.Delete(context.Background(), "missing"), "deleting a missing key is not an error")
}

// A symlink under the root that points outside it does not carry a write or
// a delete out of the root, whether it is an intermediate directory or the
// blob itself. The files outside are untouched and the store reports the
// refusal.
func TestEdge_DirSymlinksDoNotLeaveTheRoot(t *testing.T) {
	ctx := context.Background()
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "victim.json")
	require.NoError(t, os.WriteFile(sentinel, []byte("outside"), 0o600))

	root := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	require.NoError(t, os.Symlink(sentinel, filepath.Join(root, "final")))
	d := Dir{Root: root}

	assert.Error(t, d.Write(ctx, "link/victim.json", []byte("overwritten"), ""), "write through an outward directory link")
	assert.Error(t, d.Write(ctx, "link/new.json", []byte("x"), ""), "create through an outward directory link")
	assert.Error(t, d.Write(ctx, "final", []byte("overwritten"), ""), "write through an outward file link")
	assert.Error(t, d.Delete(ctx, "link/victim.json"), "delete through an outward directory link")

	b, err := os.ReadFile(sentinel)
	require.NoError(t, err, "the outside file survives")
	assert.Equal(t, "outside", string(b))
	_, err = os.Stat(filepath.Join(outside, "new.json"))
	assert.ErrorIs(t, err, os.ErrNotExist, "nothing is created outside the root")

	require.NoError(t, d.Delete(ctx, "final"), "deleting a blob that is a link removes the link")
	_, err = os.Lstat(filepath.Join(root, "final"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(sentinel)
	assert.NoError(t, err, "not the file it points at")
}

// Nested writes, deletes and deleting a missing blob still work, including
// when the root does not exist yet.
func TestEdge_DirNestedWritesAndMissingDeletes(t *testing.T) {
	ctx := context.Background()
	d := Dir{Root: filepath.Join(t.TempDir(), "not", "yet")}
	require.NoError(t, d.Delete(ctx, "a/b/c.json"), "a missing root has nothing to delete")
	require.NoError(t, d.Write(ctx, "a/b/c.json", []byte("c"), ""))
	b, err := os.ReadFile(filepath.Join(d.Root, "a", "b", "c.json"))
	require.NoError(t, err)
	assert.Equal(t, "c", string(b))
	require.NoError(t, d.Delete(ctx, "a/b/c.json"))
	require.NoError(t, d.Delete(ctx, "a/b/c.json"), "deleting a missing blob is not an error")
	require.NoError(t, d.Delete(ctx, "a/x/y.json"), "nor is one under a missing directory")
}

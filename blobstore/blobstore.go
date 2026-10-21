// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package blobstore holds the BlobStore implementations the repository
// ships: an in-memory store for tests and a directory store for a local
// runner. Package blobstore/s3 writes to an S3 bucket.
package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Memory is an in-memory sdk.BlobStore. The zero value is ready to use.
type Memory struct {
	mu    sync.Mutex
	blobs map[string][]byte
	opts  map[string]sdk.BlobWriteOpts
}

var _ sdk.BlobStore = (*Memory)(nil)

// Write stores a copy of content under key.
func (m *Memory) Write(_ context.Context, key string, content []byte, acl string) error {
	return m.put(key, content, sdk.BlobWriteOpts{ACL: acl})
}

// WriteWithOpts stores a copy of content under key. Nil opts is an error,
// as with the S3 store.
func (m *Memory) WriteWithOpts(_ context.Context, key string, content []byte, opts *sdk.BlobWriteOpts) error {
	if opts == nil {
		return errors.New("blobstore: empty options provided")
	}
	return m.put(key, content, *opts)
}

func (m *Memory) put(key string, content []byte, o sdk.BlobWriteOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blobs == nil {
		m.blobs = map[string][]byte{}
		m.opts = map[string]sdk.BlobWriteOpts{}
	}
	m.blobs[key] = append([]byte(nil), content...)
	m.opts[key] = o
	return nil
}

// Delete removes the blob under key.
func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, key)
	delete(m.opts, key)
	return nil
}

// Get returns the blob under key and whether it exists.
func (m *Memory) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.blobs[key]
	return append([]byte(nil), b...), ok
}

// Options returns the write options recorded for key.
func (m *Memory) Options(key string) (sdk.BlobWriteOpts, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.opts[key]
	return o, ok
}

// Keys returns every stored key in sorted order.
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.blobs))
	for k := range m.blobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Dir is an sdk.BlobStore that writes each blob to a file under Root. Keys
// are slash-separated relative paths; a key that would leave Root is
// rejected, including through a symlink under Root that points outside it.
// ACL and content type are not recorded.
type Dir struct {
	Root string
}

var _ sdk.BlobStore = Dir{}

// Write writes content to Root/key, creating Root and parent directories.
func (d Dir) Write(_ context.Context, key string, content []byte, _ string) error {
	rel, err := d.rel(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d.Root, 0o750); err != nil {
		return fmt.Errorf("blobstore: create directory: %w", err)
	}
	root, err := os.OpenRoot(d.Root)
	if err != nil {
		return fmt.Errorf("blobstore: open root: %w", err)
	}
	defer func() { _ = root.Close() }()
	if dir := filepath.Dir(rel); dir != "." {
		if err := root.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("blobstore: create directory: %w", err)
		}
	}
	if err := root.WriteFile(rel, content, 0o600); err != nil {
		return fmt.Errorf("blobstore: write %s: %w", key, err)
	}
	return nil
}

// WriteWithOpts is Write; nil opts is an error, as with the S3 store.
func (d Dir) WriteWithOpts(ctx context.Context, key string, content []byte, opts *sdk.BlobWriteOpts) error {
	if opts == nil {
		return errors.New("blobstore: empty options provided")
	}
	return d.Write(ctx, key, content, opts.ACL)
}

// Delete removes Root/key; a missing file is not an error. A key that is a
// symlink removes the link, not what it points at.
func (d Dir) Delete(_ context.Context, key string) error {
	rel, err := d.rel(key)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(d.Root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("blobstore: open root: %w", err)
	}
	defer func() { _ = root.Close() }()
	if err := root.Remove(rel); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("blobstore: delete %s: %w", key, err)
	}
	return nil
}

// rel returns key as a cleaned path relative to Root. The lexical checks
// reject keys that name Root or leave it; os.Root rejects the rest,
// symlinks included, when the path is opened.
func (d Dir) rel(key string) (string, error) {
	if d.Root == "" {
		return "", errors.New("blobstore: Dir.Root is empty")
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if key == "" || filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("blobstore: key %q leaves the root", key)
	}
	return clean, nil
}

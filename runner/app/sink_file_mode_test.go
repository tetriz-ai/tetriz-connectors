// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The records file is created readable by its owner only: records can hold
// vendor data such as emails.
func TestRecordsFileIsCreatedOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	a := &App{cfg: Config{SinkPath: path}}
	_, err := a.newSink(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

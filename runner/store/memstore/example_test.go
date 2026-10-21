// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package memstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tetriz-ai/tetriz-connectors/runner/store/memstore"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// A file store keeps configs, cursors and runs across restarts in one file.
func ExampleOpenFile() {
	dir, _ := os.MkdirTemp("", "store")
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "store.json")
	ctx := context.Background()

	s, _ := memstore.OpenFile(path, nil)
	_ = s.SaveCursor(ctx, "org-1", "github", "fetch_prs", &sdk.Cursor{Data: json.RawMessage(`{"page":2}`)})

	reopened, _ := memstore.OpenFile(path, nil)
	c, _ := reopened.GetCursor(ctx, "org-1", "github", "fetch_prs")
	fmt.Println(string(c.Data))
	// Output: {"page":2}
}

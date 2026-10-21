// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package store

import (
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// This package had no tests, so it carries its own guard.
func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

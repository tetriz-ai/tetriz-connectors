// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slack

import (
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

func TestMain(m *testing.M) {
	logger.Init("test", nil)
	os.Exit(testguard.Run(m))
}

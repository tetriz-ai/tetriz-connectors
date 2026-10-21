// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package logger

import (
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

func TestMain(m *testing.M) { os.Exit(testguard.Run(m)) }

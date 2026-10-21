// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package migrations embeds the simulator store's schema migrations.
package migrations

import "embed"

// FS holds the migration files.
//
//go:embed *.sql
var FS embed.FS

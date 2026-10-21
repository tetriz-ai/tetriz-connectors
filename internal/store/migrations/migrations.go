// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package migrations embeds the connector store's schema migrations: numbered
// NNNNNN_.up.sql and NNNNNN_.down.sql files applied in order.
package migrations

import "embed"

// FS holds the migration files.
//
//go:embed *.sql
var FS embed.FS

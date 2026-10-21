// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tetriz-ai/tetriz-connectors/internal/testdb"
	"github.com/tetriz-ai/tetriz-connectors/internal/testguard"
)

// testDSN is a database holding the runner store's schema, so every test also
// shows the sink's table sharing a database with it.
var testDSN string

func TestMain(m *testing.M) {
	if err := testdb.Ping(); err != nil {
		testdb.SkipOrFail("postgres sink", err)
		os.Exit(0)
	}
	_, dsn, cleanup, err := testdb.Setup(context.Background(), testdb.Connector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres sink test database setup failed: %v\n", err)
		os.Exit(1)
	}
	testDSN = dsn
	code := testguard.Run(m)
	cleanup()
	os.Exit(code)
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command tetriz-connectors runs the connector runner and the vendor
// simulator. Run it with no arguments for the command list. It is cli.Main
// with no options; a program that adds connectors of its own builds the same
// command from cli.Main.
package main

import (
	"context"
	"io"
	"os"

	"github.com/tetriz-ai/tetriz-connectors/cli"
)

// version is set at release build time with -ldflags "-X main.version=...".
var version = ""

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if version != "" {
		cli.Version = version
	}
	return cli.Main(ctx, args, stdout, stderr)
}

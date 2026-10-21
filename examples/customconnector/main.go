// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command customconnector is tetriz-connectors with one connector of its
// own, "todo", added without forking: the same commands, flags and
// configuration, built from cli.Main with app.WithConnector. `sync` runs the
// todo connector's fetch_todos, and `serve` schedules it every 15 minutes
// beside the built-in connectors.
//
//	go run ./examples/customconnector config put --org "$ORG" --connector todo \
//	  --settings '{"base_url":"https://todo.example"}' --credentials '{"token":"..."}'
//	go run ./examples/customconnector sync --org "$ORG" --connector todo --op fetch_todos
package main

import (
	"context"
	"os"

	"github.com/tetriz-ai/tetriz-connectors/cli"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
)

func main() {
	os.Exit(cli.Main(context.Background(), os.Args[1:], os.Stdout, os.Stderr, Options()...))
}

// Options adds the todo connector, with its gateway, to the runner.
func Options() []app.Option {
	return []app.Option{app.WithConnector(Connector{}, NewGateway)}
}

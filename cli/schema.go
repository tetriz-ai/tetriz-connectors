// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

const schemaUsage = "usage: tetriz-connectors schema [<connector> <operation>]"

// cmdSchema lists every connector and operation that has a record schema,
// or prints one schema.
func cmdSchema(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintf(stdout, "%s\n\n%s\n", schemaUsage,
				"With no arguments, list every connector and operation that has a record schema. With a connector and an operation, print that operation's record schema (JSON Schema).")
			return errHelp
		}
		return usageError{err.Error()}
	}
	switch fs.NArg() {
	case 0:
		last := ""
		for _, ref := range schemas.List() {
			if ref.Connector != last {
				_, _ = fmt.Fprintln(stdout, ref.Connector)
				last = ref.Connector
			}
			_, _ = fmt.Fprintf(stdout, "  %s\n", ref.Operation)
		}
		return nil
	case 2:
		conn, op := fs.Arg(0), fs.Arg(1)
		b, ok := schemas.Schema(conn, op)
		if !ok {
			return usageError{fmt.Sprintf("no record schema for %s %s; tetriz-connectors schema lists them", conn, op)}
		}
		if _, err := stdout.Write(b); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
		if len(b) > 0 && b[len(b)-1] != '\n' {
			_, _ = fmt.Fprintln(stdout)
		}
		return nil
	default:
		return usageError{schemaUsage}
	}
}

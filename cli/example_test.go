// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/cli"
)

// Main is the whole tetriz-connectors command. Here it lists the connectors
// and their operations; the first line is the first connector's id.
func ExampleMain() {
	var out bytes.Buffer
	code := cli.Main(context.Background(), []string{"connectors"}, &out, os.Stderr)
	fmt.Println("exit:", code)
	fmt.Println(strings.SplitN(out.String(), "\n", 2)[0])
	// Output:
	// exit: 0
	// bitbucket
}

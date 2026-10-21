// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package schemas_test

import (
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/schemas"
)

// Validate checks a decoded record payload against its operation's schema.
func ExampleValidate() {
	err := schemas.Validate("cursor", "fetch_usage_events", []byte(`{"timestamp":1787184000000,"conversationId":"c"}`))
	fmt.Println(err != nil)
	err = schemas.Validate("cursor", "fetch_usage_events", []byte(`{"timestamp":"1787184000000","conversationId":"c"}`))
	fmt.Println(err)
	// Output:
	// true
	// <nil>
}

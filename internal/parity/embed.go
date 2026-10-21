// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package parity

import _ "embed"

// embeddedManifest is the checked-in parity.yaml, compiled into the binary so
// the arch-tests and any runtime consumer read the same bytes with no CWD or
// path dependency.
//
//go:embed parity.yaml
var embeddedManifest []byte

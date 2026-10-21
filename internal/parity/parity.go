// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package parity loads the connector Parity Manifest — the checked-in record of
// every deliberate cross-vendor asymmetry. A check that compares vendors
// consults it as its allowed-exception set: an asymmetry recorded here is a
// reviewed, intentional vendor limit; one that is not is a finding.
package parity

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Valid vendors an exception may name. An entry naming any other vendor is a
// hard load error — a typo must not silently widen the exception set.
var validVendors = map[string]struct{}{
	"github":    {},
	"gitlab":    {},
	"bitbucket": {},
}

// Exception kinds. struct-field and dispatch are queried by struct-shape and
// dispatch checks; behavioral is a doc-only record (a present-but-unpopulated
// field or a logic limit) that no static struct or dispatch diff matches.
const (
	KindStructField = "struct-field"
	KindDispatch    = "dispatch"
	KindBehavioral  = "behavioral"
)

var validKinds = map[string]struct{}{
	KindStructField: {},
	KindDispatch:    {},
	KindBehavioral:  {},
}

// Exception is one deliberate cross-vendor asymmetry.
type Exception struct {
	// Kind selects which guard the entry answers to (struct-field / dispatch /
	// behavioral), so a machine-queried arch-test filters to only the kinds it
	// enforces and never trips over a doc-only behavioral record.
	Kind string `yaml:"kind"`
	// Symbol is the code symbol the asymmetry attaches to (a struct.field,
	// dispatch-site, or function name the arch-test matches on).
	Symbol string `yaml:"symbol"`
	// AbsentFor is the vendor(s) the symbol is legitimately absent/limited for;
	// each must be one of github/gitlab/bitbucket.
	AbsentFor []string `yaml:"absent_for"`
	// Reason states why this is a genuine limit, not an oversight.
	Reason string `yaml:"reason"`
}

// Manifest is the parsed parity.yaml.
type Manifest struct {
	Version    int         `yaml:"version"`
	Exceptions []Exception `yaml:"exceptions"`
}

// IsException reports whether symbol is a recorded exception for vendor of ANY
// kind. Kind-agnostic by design — for a consumer that legitimately wants "is
// this a recorded vendor limit at all" regardless of kind (e.g. the vendor-limit
// display flag). A consumer that MUST NOT conflate kinds — a struct-field or
// dispatch check, where a behavioral doc-only record must never clear a static
// drift — must use IsExceptionOfKind instead.
func (m *Manifest) IsException(symbol, vendor string) bool {
	for _, e := range m.Exceptions {
		if e.Symbol != symbol {
			continue
		}
		for _, v := range e.AbsentFor {
			if v == vendor {
				return true
			}
		}
	}
	return false
}

// IsExceptionOfKind reports whether symbol is a recorded exception for vendor of
// exactly the given kind (KindStructField / KindDispatch / KindBehavioral). This
// is the kind-scoped form a struct-field or dispatch check uses, so a behavioral (doc-only)
// entry that happens to share a symbol can never exempt a struct-field or
// dispatch drift.
func (m *Manifest) IsExceptionOfKind(kind, symbol, vendor string) bool {
	for _, e := range m.Exceptions {
		if e.Kind != kind || e.Symbol != symbol {
			continue
		}
		for _, v := range e.AbsentFor {
			if v == vendor {
				return true
			}
		}
	}
	return false
}

// Load parses the embedded parity.yaml, failing closed on any problem. Callers
// (the arch-tests) treat a load error as a hard failure — never as an empty
// exception set, which would silently disable the parity guards.
func Load() (*Manifest, error) {
	m, err := parse(embeddedManifest)
	if err != nil {
		return nil, fmt.Errorf("parsing embedded parity manifest: %w", err)
	}
	return m, nil
}

// parse is the fail-closed core: a malformed document, an empty file, a manifest
// with no exceptions, or an entry missing any field / naming an unknown vendor
// is an error — never a silently-empty set. Separated from Load so tests can
// drive it with arbitrary bytes (the embedded file is fixed).
func parse(raw []byte) (*Manifest, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("parity manifest is empty")
	}
	// Strict decode: an unknown/misspelled key is a hard error, so a typo in an
	// exception cannot load clean and silently mean something other than intended.
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("parity manifest is empty")
		}
		return nil, fmt.Errorf("parse parity manifest: %w", err)
	}
	// A second YAML document would otherwise be ignored without a word, typos and all.
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("parse parity manifest: %w", err)
		}
		return nil, fmt.Errorf("parity manifest has more than one YAML document")
	}
	if m.Version == 0 {
		return nil, fmt.Errorf("parity manifest has no version")
	}
	if len(m.Exceptions) == 0 {
		return nil, fmt.Errorf("parity manifest has no exceptions; refusing an empty exception set")
	}
	for i, e := range m.Exceptions {
		if _, ok := validKinds[e.Kind]; !ok {
			return nil, fmt.Errorf("parity manifest exception[%d] (%s): invalid kind %q", i, e.Symbol, e.Kind)
		}
		if e.Symbol == "" {
			return nil, fmt.Errorf("parity manifest exception[%d]: empty symbol", i)
		}
		if len(e.AbsentFor) == 0 {
			return nil, fmt.Errorf("parity manifest exception[%d] (%s): empty absent_for", i, e.Symbol)
		}
		if e.Reason == "" {
			return nil, fmt.Errorf("parity manifest exception[%d] (%s): empty reason", i, e.Symbol)
		}
		for _, v := range e.AbsentFor {
			if _, ok := validVendors[v]; !ok {
				return nil, fmt.Errorf("parity manifest exception[%d] (%s): unknown vendor %q", i, e.Symbol, v)
			}
		}
	}
	return &m, nil
}

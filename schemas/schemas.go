// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package schemas holds a JSON Schema (draft 2020-12) for the payload of every
// record a connector emits, and validates payloads against them.
//
// # Layout
//
// Each schema is the file <connector-id>/<operation-id>.schema.json. The
// connector id is the registry id (github, claude-code, codex). The
// operation id is the record's operationId field, which is the id of the
// operation that emitted it.
//
// When one operation emits several record kinds under its own operationId
// (lifecycle events, companion records), its one schema is a oneOf with one
// branch per kind. When an operation emits records under another operationId,
// that operationId gets its own file. The schema always follows the record's
// operationId, never the operation that ran.
//
// Shapes that several connectors share, such as the RepositoryRecord protojson,
// live in common/<name>.json and are reached with a relative $ref. They are
// not record schemas and List does not return them.
//
// A schema describes the payload bytes only: the RawRecord Payload, which is
// the envelope event's payload after base64 decoding. ExternalID, DataDate and
// metadata are documented in docs/records.
//
// # Strictness
//
// A schema lists the keys the connector always writes as required and types
// every key it knows. Objects the vendor can grow keep additionalProperties
// true. Validate asserts formats (date-time, uri) and content keywords
// (contentEncoding base64 with contentMediaType application/json), which a
// generic draft 2020-12 validator treats as annotations unless told otherwise.
// A contentSchema applies only when contentEncoding and contentMediaType sit
// beside it in the same object, so a schema that narrows a shared base64 field
// repeats those two keywords.
package schemas

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed */*.schema.json common/*.json
var files embed.FS

// suffix is the file name suffix every schema carries.
const suffix = ".schema.json"

// baseURL prefixes each schema's location, so a relative $ref between schema
// files resolves inside this package.
const baseURL = "https://github.com/tetriz-ai/tetriz-connectors/schemas/"

// ErrNoSchema is returned by Validate when no schema exists for the pair.
var ErrNoSchema = errors.New("no schema")

// Ref names one schema: a connector id and a record operationId.
type Ref struct {
	Connector string
	Operation string
}

// String returns "connector/operation".
func (r Ref) String() string { return r.Connector + "/" + r.Operation }

// Path returns the schema's file path inside this package.
func (r Ref) Path() string { return r.Connector + "/" + r.Operation + suffix }

type set struct {
	raw      map[Ref][]byte
	compiled map[Ref]*jsonschema.Schema
	refs     []Ref
}

var (
	loadOnce sync.Once
	loaded   *set
	loadErr  error
)

func load() (*set, error) {
	loadOnce.Do(func() { loaded, loadErr = compileAll() })
	return loaded, loadErr
}

func compileAll() (*set, error) {
	s := &set{raw: map[Ref][]byte{}, compiled: map[Ref]*jsonschema.Schema{}}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.AssertContent()
	shared, err := fs.Glob(files, "common/*.json")
	if err != nil {
		return nil, fmt.Errorf("list shared schemas: %w", err)
	}
	for _, p := range shared {
		if err := addResource(c, p); err != nil {
			return nil, err
		}
	}
	paths, err := fs.Glob(files, "*/*"+suffix)
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	for _, p := range paths {
		if path.Dir(p) == "common" {
			continue
		}
		if err := addResource(c, p); err != nil {
			return nil, err
		}
		b, _ := files.ReadFile(p)
		ref := Ref{Connector: path.Dir(p), Operation: strings.TrimSuffix(path.Base(p), suffix)}
		s.raw[ref] = b
		s.refs = append(s.refs, ref)
	}
	for _, ref := range s.refs {
		sch, err := c.Compile(baseURL + ref.Path())
		if err != nil {
			return nil, fmt.Errorf("compile %s: %w", ref.Path(), err)
		}
		s.compiled[ref] = sch
	}
	sort.Slice(s.refs, func(i, j int) bool {
		if s.refs[i].Connector != s.refs[j].Connector {
			return s.refs[i].Connector < s.refs[j].Connector
		}
		return s.refs[i].Operation < s.refs[j].Operation
	})
	return s, nil
}

func addResource(c *jsonschema.Compiler, p string) error {
	b, err := files.ReadFile(p)
	if err != nil {
		return fmt.Errorf("read %s: %w", p, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("parse %s: %w", p, err)
	}
	if err := c.AddResource(baseURL+p, doc); err != nil {
		return fmt.Errorf("add %s: %w", p, err)
	}
	return nil
}

// Validate checks a record payload against the schema for connectorID and
// operationID. The payload is the RawRecord Payload: JSON bytes, not the
// base64 text an envelope carries. It returns an error wrapping ErrNoSchema
// when there is no schema for the pair, and a *jsonschema.ValidationError
// (wrapped) when the payload does not match.
func Validate(connectorID, operationID string, payload []byte) error {
	s, err := load()
	if err != nil {
		return err
	}
	ref := Ref{Connector: connectorID, Operation: operationID}
	sch, ok := s.compiled[ref]
	if !ok {
		return fmt.Errorf("%s: %w", ref, ErrNoSchema)
	}
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%s: payload is not JSON: %w", ref, err)
	}
	if err := sch.Validate(v); err != nil {
		return fmt.Errorf("%s: %w", ref, err)
	}
	return nil
}

// Schema returns the schema file for connectorID and operationID, and false
// when there is none.
func Schema(connectorID, operationID string) ([]byte, bool) {
	s, err := load()
	if err != nil {
		return nil, false
	}
	b, ok := s.raw[Ref{Connector: connectorID, Operation: operationID}]
	if !ok {
		return nil, false
	}
	return bytes.Clone(b), true
}

// List returns every schema, sorted by connector and then operation.
func List() []Ref {
	s, err := load()
	if err != nil {
		return nil
	}
	return append([]Ref(nil), s.refs...)
}

// Load compiles every schema and returns the first error. Validate, Schema
// and List report a broken schema set as no schema; call Load to see why.
func Load() error {
	_, err := load()
	return err
}

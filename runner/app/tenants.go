// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// StaticTenantDirectory is the runner's TenantDirectory when the host has no
// directory of its own. Every organization has kind Default unless Kinds
// names it.
type StaticTenantDirectory struct {
	Default sdk.TenantKind
	Kinds   map[string]sdk.TenantKind
}

var _ sdk.TenantDirectory = StaticTenantDirectory{}

// TenantType implements sdk.TenantDirectory.
func (d StaticTenantDirectory) TenantType(_ context.Context, orgID string) (sdk.TenantKind, error) {
	if k, ok := d.Kinds[orgID]; ok {
		return k, nil
	}
	return d.Default, nil
}

// Individuals returns the organizations Kinds marks individual. With an
// individual Default, the unnamed organizations are individual too; the
// schedule source resolves every organization through TenantType, so it
// skips and routes them the same way.
func (d StaticTenantDirectory) Individuals() map[string]struct{} {
	out := map[string]struct{}{}
	for org, k := range d.Kinds {
		if k == sdk.TenantKindIndividual {
			out[org] = struct{}{}
		}
	}
	return out
}

// ParseTenantKind reads "organization" or "individual".
func ParseTenantKind(s string) (sdk.TenantKind, error) {
	switch s {
	case "organization":
		return sdk.TenantKindOrganization, nil
	case "individual":
		return sdk.TenantKindIndividual, nil
	}
	return sdk.TenantKindUnspecified, fmt.Errorf("tenant kind must be organization or individual, not %q", s)
}

// LoadTenantKinds reads a tenant-kinds file: one organization id and one kind
// per line, separated by white space. Blank lines and lines starting with #
// are skipped. Each id must be a UUID, and is stored in its canonical
// (lower-case) form.
func LoadTenantKinds(path string) (map[string]sdk.TenantKind, error) {
	f, err := os.Open(path) //nolint:gosec // the operator names this file
	if err != nil {
		return nil, fmt.Errorf("tenant kinds file: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only
	kinds := map[string]sdk.TenantKind{}
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("tenant kinds file %s:%d: want an organization id and a kind", path, n)
		}
		if _, err := uuid.Parse(fields[0]); err != nil {
			return nil, fmt.Errorf("tenant kinds file %s:%d: organization id %q is not a UUID", path, n, fields[0])
		}
		k, err := ParseTenantKind(fields[1])
		if err != nil {
			return nil, fmt.Errorf("tenant kinds file %s:%d: %w", path, n, err)
		}
		// Keyed by the canonical id, the spelling the orchestrator and the
		// store use.
		kinds[orgid.Canonical(fields[0])] = k
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("tenant kinds file: %w", err)
	}
	return kinds, nil
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package settingskey patches one key of a connector's stored settings,
// leaving every other key as stored.
package settingskey

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// Patch reads the stored settings in the store's patch transaction, hands
// build the current raw value of key (nil when absent), and writes back only
// that key when build reports a change. Every other key, including ones this
// connector does not model, is left exactly as stored, so a concurrent
// settings write by an operator or another operation survives.
func Patch(ctx context.Context, u sdk.ConfigUpdater, orgID string, connectorID sdk.ConnectorID, key string,
	build func(current json.RawMessage) (next any, changed bool, err error),
) error {
	return u.PatchSettings(ctx, orgID, connectorID, func(stored json.RawMessage) (json.RawMessage, error) {
		fields := map[string]json.RawMessage{}
		if len(stored) > 0 && string(stored) != "null" {
			if err := json.Unmarshal(stored, &fields); err != nil {
				return nil, fmt.Errorf("parse stored settings: %w", err)
			}
		}
		next, changed, err := build(fields[key])
		if err != nil || !changed {
			return nil, err // nil writes nothing
		}
		raw, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("marshal %s: %w", key, err)
		}
		fields[key] = raw
		out, err := json.Marshal(fields)
		if err != nil {
			return nil, fmt.Errorf("marshal settings: %w", err)
		}
		return out, nil
	})
}

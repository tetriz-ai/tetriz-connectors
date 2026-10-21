// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/tetriz-ai/tetriz-connectors/cli" // links every package the command links
	"github.com/tetriz-ai/tetriz-connectors/connectorsvc"
	"github.com/tetriz-ai/tetriz-connectors/runner/app"
)

// Importing the module registers no collector anywhere. The default registry
// holds only the client library's own Go and process collectors, and every
// collector the module defines, the secret broker's included, still registers
// there because nothing took it first.
func TestImportingTheModuleRegistersNoCollector(t *testing.T) {
	fams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		name := f.GetName()
		assert.True(t, strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_"),
			"the default registry holds %s after import", name)
	}
	all := append(app.Collectors(), connectorsvc.Collectors()...)
	require.NotEmpty(t, all)
	for _, c := range all {
		if err := prometheus.DefaultRegisterer.Register(c); err != nil {
			t.Errorf("a collector was already registered at import: %v", err)
			continue
		}
		prometheus.DefaultRegisterer.Unregister(c)
	}
}

// RegisterMetrics can register the module's collectors with more than one
// registry in a process, each under its own prefix, and both report the same
// metrics.
func TestRegisterMetricsOnTwoRegistries(t *testing.T) {
	a, b := prometheus.NewRegistry(), prometheus.NewRegistry()
	require.NoError(t, app.RegisterMetrics(a, "a_"))
	require.NoError(t, app.RegisterMetrics(b, "b_"))
	names := func(g prometheus.Gatherer, prefix string) []string {
		fams, err := g.Gather()
		require.NoError(t, err)
		var out []string
		for _, f := range fams {
			require.True(t, strings.HasPrefix(f.GetName(), prefix), f.GetName())
			out = append(out, strings.TrimPrefix(f.GetName(), prefix))
		}
		return out
	}
	gotA := names(a, "a_")
	require.NotEmpty(t, gotA)
	assert.Equal(t, gotA, names(b, "b_"))
}

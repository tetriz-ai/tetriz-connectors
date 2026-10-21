// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/tetriz-ai/tetriz-connectors/connectors/anthropic"
	"github.com/tetriz-ai/tetriz-connectors/connectors/codex"
	"github.com/tetriz-ai/tetriz-connectors/connectors/cursor"
	"github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	"github.com/tetriz-ai/tetriz-connectors/connectors/linear"
	"github.com/tetriz-ai/tetriz-connectors/gateway/vendorapi"
	anthropicgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/anthropic"
	bitbucketgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/bitbucket"
	codexgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/codex"
	gitlabgw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/gitlab"
	lineargw "github.com/tetriz-ai/tetriz-connectors/internal/gateway/linear"
	ratelimitermetrics "github.com/tetriz-ai/tetriz-connectors/internal/gateway/ratelimiter/metrics"
	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
	"github.com/tetriz-ai/tetriz-connectors/internal/vcs/treewalker"
	"github.com/tetriz-ai/tetriz-connectors/runner"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
	"github.com/tetriz-ai/tetriz-connectors/runner/schedule/connectorsource"
	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"
	"github.com/tetriz-ai/tetriz-connectors/sdk/pacing"
	"github.com/tetriz-ai/tetriz-connectors/sink"
)

// Collectors returns every Prometheus collector the runner, its connectors and
// their gateways define, unregistered. The secret broker's are separate:
// connectorsvc.Collectors.
func Collectors() []prometheus.Collector {
	var cs []prometheus.Collector
	for _, pkg := range [][]prometheus.Collector{
		anthropic.Collectors(),
		codex.Collectors(),
		cursor.Collectors(),
		kiro.Collectors(),
		linear.Collectors(),
		anthropicgw.Collectors(),
		bitbucketgw.Collectors(),
		gitlabgw.Collectors(),
		codexgw.Collectors(),
		lineargw.Collectors(),
		ratelimitermetrics.Collectors(),
		vendorapi.Collectors(),
		metricutil.Collectors(),
		runner.Collectors(),
		schedule.Collectors(),
		connectorsource.Collectors(),
		obs.Collectors(),
		pacing.Collectors(),
		treewalker.Collectors(),
		sink.Collectors(),
	} {
		cs = append(cs, pkg...)
	}
	return cs
}

// RegisterMetrics registers Collectors with reg, each metric name prefixed
// with prefix (for example "myapp_"). Importing the module registers nothing,
// so a host that never calls it exports no metrics from the module. Serve
// calls it on its own registry with TETRIZ_METRICS_PREFIX.
//
// The collectors are package-level, so registering them with a second
// registry is allowed and both registries report the same values.
func RegisterMetrics(reg prometheus.Registerer, prefix string) error {
	target := prometheus.WrapRegistererWithPrefix(prefix, reg)
	var errs []error
	for _, c := range Collectors() {
		if err := target.Register(c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// registerServeMetrics registers what serve's /metrics exposes: the Go and
// process collectors, unprefixed, and Collectors under prefix. It returns the
// registerer for the scheduler's queue collector, which also takes prefix.
func registerServeMetrics(reg prometheus.Registerer, prefix string) (prometheus.Registerer, error) {
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, err
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, err
	}
	if err := RegisterMetrics(reg, prefix); err != nil {
		return nil, err
	}
	return prometheus.WrapRegistererWithPrefix(prefix, reg), nil
}

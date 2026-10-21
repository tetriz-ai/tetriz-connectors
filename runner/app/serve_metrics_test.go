// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package app

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tetriz-ai/tetriz-connectors/runner/schedule"
)

// descCapture is a Registerer that records each collector's descriptors.
type descCapture struct {
	collectors int
	descs      []string
}

func (c *descCapture) Register(col prometheus.Collector) error {
	c.collectors++
	ch := make(chan *prometheus.Desc)
	go func() { col.Describe(ch); close(ch) }()
	for d := range ch {
		c.descs = append(c.descs, d.String())
	}
	return nil
}

func (c *descCapture) MustRegister(cs ...prometheus.Collector) {
	for _, col := range cs {
		_ = c.Register(col)
	}
}

func (c *descCapture) Unregister(prometheus.Collector) bool { return true }

// Serve's /metrics carries the same metric names, help and labels as
// testdata/serve-metrics.golden, which lists every module metric with the
// default prefix. The Go and process collectors are left out, because their
// metrics depend on the Go release. The scheduler's queue collector is
// registered too; it describes its metrics only once queues exist.
func TestServeMetricsMatchTheGolden(t *testing.T) {
	capture := &descCapture{}
	schedulerMetrics, err := registerServeMetrics(capture, "tetriz_")
	require.NoError(t, err)
	before := capture.collectors
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	_, err = schedule.NewScheduler(context.Background(), schedule.Options{Metrics: schedulerMetrics}, rdb, nil,
		asynq.NewServeMux(), nil, schedule.NewDistLockFactory(rdb))
	require.NoError(t, err)
	assert.Equal(t, before+1, capture.collectors, "the scheduler registers its queue collector")

	var got []string
	for _, d := range capture.descs {
		if strings.Contains(d, `fqName: "go_`) || strings.Contains(d, `fqName: "process_`) {
			continue
		}
		got = append(got, d)
	}
	sort.Strings(got)
	want, err := os.ReadFile("testdata/serve-metrics.golden")
	require.NoError(t, err)
	assert.Equal(t, string(want), strings.Join(got, "\n")+"\n")
}

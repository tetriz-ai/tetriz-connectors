// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package testkit

import "github.com/tetriz-ai/tetriz-connectors/sink"

// ChannelSink is the in-memory sink.ChannelSink, re-exported so tests that
// only import the test kit can collect records.
type ChannelSink = sink.ChannelSink

// NewChannelSink returns an empty in-memory sink.
func NewChannelSink() *ChannelSink { return sink.NewChannelSink() }

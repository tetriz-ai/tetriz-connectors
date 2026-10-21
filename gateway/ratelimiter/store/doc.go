// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package store holds the sliding-window-log stores behind the rate limiters.
//
// NewSlidingWindowLogWithRedis shares limits across every process using the
// same Redis. NewSlidingWindowLogInMemory keeps them in this process, for a
// single-process embedding with no Redis: limits then apply per process, so
// two processes calling one vendor account each get the full budget. Both
// implement ratelimiter.Store with the same behaviour, which one parity suite
// (parity_test.go) checks against both.
package store

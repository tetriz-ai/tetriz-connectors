// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package ratelimiter implements the rate limiters the gateways use:
// sliding-window request limits, priority shares and vendor budget floors.
//
// State lives in a Store (package store: Redis, or in process) and a
// BudgetStore (NewRedisBudgetStore or NewInMemoryBudgetStore). The in-process
// stores need no Redis but limit per process only. Package inprocess bundles
// every in-process store the built-in gateways need.
package ratelimiter

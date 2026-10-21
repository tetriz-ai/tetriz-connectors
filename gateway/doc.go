// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package gateway holds the configuration of the vendor gateways: each
// vendor's rate-limit groups, base URLs and simulator routing. It also holds
// the 429 circuit breaker the Bitbucket and GitLab gateways share
// (CircuitBreaker) and the CircuitBreakerStore it keeps its state in.
//
// A vendor gateway makes authenticated calls to one vendor's API and applies
// that vendor's rate limits, retries and circuit breaker. The built-in
// gateways are internal to this module; app.NewGatewayFactory builds them.
//
// Connector operations receive a sdk.VendorGateway through ExecutionParams and
// use it for every call to the vendor.
package gateway

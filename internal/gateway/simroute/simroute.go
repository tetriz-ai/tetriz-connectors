// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package simroute rewrites a vendor endpoint onto the simulator.
package simroute

import (
	"fmt"
	"net/url"
)

// URL returns the simulator URL for a vendor group whose real base is baseURL:
// simulatorBaseURL, then vendorPrefix (such as "/github"), then baseURL's path.
// Only the path is kept, so a configured base on any host, such as a GitHub
// Enterprise server, lands under the simulator root.
func URL(simulatorBaseURL, vendorPrefix, baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("simulator routing: base URL %q: %w", baseURL, err)
	}
	return simulatorBaseURL + vendorPrefix + u.EscapedPath(), nil
}

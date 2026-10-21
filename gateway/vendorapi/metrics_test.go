// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package vendorapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestVendorFromURL guards the classifier that labels every vendor metric. A
// missing host case is invisible in real use: the request still succeeds and
// its metrics still land, just under "unknown", so the vendor's own latency and
// failure series never exist and the fallback series absorbs them. That is why
// the unknown cases below assert the fallback explicitly rather than being left
// untested — "unknown" has to be a decision, not a default nobody checked.
func TestVendorFromURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "cursor admin api", url: "https://api.cursor.com/teams/members", want: "cursor"},
		{name: "anthropic analytics api", url: "https://api.anthropic.com/v1/organizations/analytics/users", want: "anthropic"},
		{name: "anthropic member roster", url: "https://api.anthropic.com/v1/organizations/users", want: "anthropic"},
		{name: "github rest", url: "https://api.github.com/repos/o/r", want: "github"},
		{name: "bitbucket", url: "https://api.bitbucket.org/2.0/repositories", want: "bitbucket"},
		{name: "gitlab", url: "https://gitlab.com/api/v4/projects", want: "gitlab"},
		{name: "jira cloud", url: "https://api.atlassian.com/ex/jira/x/rest/api/3/search", want: "jira"},
		{name: "slack web", url: "https://slack.com/api/conversations.list", want: "slack"},

		// Suffix matching is deliberate — a vendor's subdomains classify as that
		// vendor rather than as unknown.
		{name: "a vendor subdomain classifies as that vendor", url: "https://enterprise.api.github.com/x", want: "github"},

		{name: "an unrecognised host is unknown", url: "https://example.com/x", want: vendorUnknown},
		{name: "a URL with no host is unknown", url: "/teams/members", want: vendorUnknown},
		{name: "an unparseable URL is unknown", url: "://nope", want: vendorUnknown},
		{name: "an empty URL is unknown", url: "", want: vendorUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, vendorFromURL(tt.url))
		})
	}
}

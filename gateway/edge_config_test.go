// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package gateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func jiraRate(rate string) []byte {
	return []byte("Jira:\n  - Group: jira-rest\n    BaseURL: https://api.atlassian.com\n    RequestsPerSecond: " + rate + "\n    BurstSize: 1\n")
}

// A rate that is not a finite positive number is rejected at load. .nan
// already is; .inf passes the "above 0" check and would convert to an
// undefined int64 budget downstream (effectively unlimited or negative).
func TestEdge_ParseConfigRejectsNonFiniteRate(t *testing.T) {
	for _, rate := range []string{".nan", ".inf", "-.inf"} {
		_, err := ParseConfig(jiraRate(rate))
		assert.Error(t, err, "RequestsPerSecond %s", rate)
	}
}

// One vendor listing the same group twice is a typo whose second entry
// silently replaces the first in every *Endpoints() map. It is rejected.
func TestEdge_ParseConfigRejectsDuplicateGroup(t *testing.T) {
	y := "Jira:\n" +
		"  - Group: jira-rest\n    BaseURL: https://api.atlassian.com\n    RequestsPerSecond: 10\n    BurstSize: 1\n" +
		"  - Group: jira-rest\n    BaseURL: https://evil.example\n    RequestsPerSecond: 1000\n    BurstSize: 1\n"
	c, err := ParseConfig([]byte(y))
	if err == nil {
		ep := c.JiraEndpoints()["jira-rest"]
		t.Errorf("duplicate group accepted; effective entry is BaseURL=%s rate=%v", ep.BaseURL, ep.RequestsPerSecond)
	}
}

// Unknown keys are rejected at every nesting level, not only on an endpoint.
func TestEdge_ParseConfigRejectsUnknownTopLevelKey(t *testing.T) {
	_, err := ParseConfig([]byte("Jiraa:\n  - Group: x\n"))
	assert.Error(t, err)
	_, err = ParseConfig([]byte("SimulatorOrgIds: [a]\n"))
	assert.Error(t, err, "a mis-cased SimulatorOrgIDs must not silently disable simulator routing")
}

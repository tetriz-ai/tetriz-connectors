// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A path whose date segments name no real calendar day is refused, never
// normalised into the next month (time.Date would turn Feb 30 into Mar 2).
func TestEdge_ParseKeyRefusesImpossibleDates(t *testing.T) {
	for _, d := range [][3]string{{"2026", "02", "30"}, {"2025", "02", "29"}, {"2026", "13", "01"}, {"2026", "00", "10"}, {"2026", "04", "31"}} {
		_, err := parseKey(key("KIRO_IDE", d[0], d[1], d[2]))
		assert.Error(t, err, "%s-%s-%s", d[0], d[1], d[2])
	}
	p, err := parseKey(key("KIRO_IDE", "2024", "02", "29"))
	require.NoError(t, err, "a real leap day parses")
	assert.Equal(t, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), p.Day)
}

// A customer prefix that itself contains "AWSLogs" or "KiroLogs" segments, or a
// leading slash, must not shift the capture groups.
func TestEdge_ParseKeyWithAPrefixThatMimicsTheLayout(t *testing.T) {
	k := "/AWSLogs/x/KiroLogs/AWSLogs/" + testAccount + "/KiroLogs/user_report/eu-west-1/2026/12/31/00/KIRO_CLI_" + testAccount + "_user_report_1.csv"
	p, err := parseKey(k)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", p.Region)
	assert.Equal(t, "KIRO_CLI", p.ClientType)
	assert.Equal(t, time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC), p.Day)
}

// A client type whose own segments end in digits is still split at the account id.
func TestEdge_ParseKeyClientTypeWithDigitSegments(t *testing.T) {
	p, err := parseKey(testPrefix + "/AWSLogs/" + testAccount + "/KiroLogs/user_report/" + testRegion +
		"/2026/01/01/00/KIRO_IDE_2_" + testAccount + "_user_report_1717171717.csv")
	require.NoError(t, err)
	assert.Equal(t, "KIRO_IDE_2", p.ClientType)
}

// dayPrefix is in UTC whatever the zone of the day passed in, and slashes on the
// configured prefix are normalised away.
func TestEdge_DayPrefixZoneAndSlashes(t *testing.T) {
	tokyo := time.FixedZone("JST", 9*3600)
	day := time.Date(2027, 1, 1, 3, 0, 0, 0, tokyo) // 2026-12-31T18:00Z
	assert.Equal(t, "kiro/AWSLogs/a/KiroLogs/user_report/r/2026/12/31/00/", dayPrefix("/kiro/", "a", "r", day))
	assert.Equal(t, "a/b/AWSLogs/a/KiroLogs/user_report/r/2026/12/31/00/", dayPrefix("a/b//", "a", "r", day))
}

// validateSettings refuses an empty prefix because the vendor never writes to a
// bucket root. A prefix made only of slashes is the same bucket root: dayPrefix
// trims it to nothing and lists "/AWSLogs/...", a key no S3 object has, so
// every day reads as empty and the walk advances past data it never saw. It
// must be refused like the empty prefix.
func TestEdge_SlashOnlyPrefixIsRefusedLikeAnEmptyOne(t *testing.T) {
	s := Settings{Bucket: "b", Prefix: "/", Region: "us-east-1", AccountID: testAccount}
	listed := dayPrefix(s.Prefix, s.AccountID, s.Region, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.True(t, strings.HasPrefix(listed, "/AWSLogs/"), "precondition: the listing prefix is %q", listed)
	assert.Error(t, validateSettings(s), "a slash-only prefix addresses the bucket root")
}

// The high-water mark only rises, including across a year boundary and when a
// non-UTC day is passed in.
func TestEdge_CoveredThroughOnlyRisesAcrossTheYear(t *testing.T) {
	var s syncState
	s.raiseCoveredThrough(time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	s.raiseCoveredThrough(time.Date(2026, 12, 30, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, "2026-12-31", s.CoveredThrough)
	// 2027-01-01T05:00 in UTC+9 is still 2026-12-31 in UTC: no rise.
	s.raiseCoveredThrough(time.Date(2027, 1, 1, 5, 0, 0, 0, time.FixedZone("JST", 9*3600)))
	assert.Equal(t, "2026-12-31", s.CoveredThrough)
	s.raiseCoveredThrough(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, "2027-01-01", s.CoveredThrough)
}

// Failure bookkeeping is per object: one object reaching the threshold does not
// settle its sibling, and clearing a day leaves the next day's counts alone.
func TestEdge_FailureCountsArePerObjectAndPerDay(t *testing.T) {
	var s syncState
	d1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.AddDate(0, 0, 1)
	for i := 1; i < maxConsecutiveFailures; i++ {
		assert.False(t, s.recordFailure(d1, "a.csv", "bad"))
	}
	s.recordFailure(d1, "b.csv", "bad")
	s.recordFailure(d2, "a.csv", "bad")
	assert.True(t, s.recordFailure(d1, "a.csv", "bad"), "the fifth consecutive failure settles a.csv")
	assert.True(t, s.isObjectTerminal(d1, "a.csv"))
	assert.False(t, s.isObjectTerminal(d1, "b.csv"))

	s.clearFailures(d1)
	assert.Equal(t, 1, s.FailureCounts[failureKey(d2, "a.csv")], "the next day's count survives")
	assert.Equal(t, 1, s.clearTerminal(d1))
	assert.False(t, s.isObjectTerminal(d1, "a.csv"))
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPrefix  = "kiro"
	testAccount = "123456789012"
	testRegion  = "us-east-1"
)

func key(clientType, y, m, d string) string {
	return testPrefix + "/AWSLogs/" + testAccount + "/KiroLogs/user_report/" + testRegion +
		"/" + y + "/" + m + "/" + d + "/00/" + clientType + "_" + testAccount + "_user_report_1717171717.csv"
}

func TestDayPrefixIsScopedToOneDay(t *testing.T) {
	t.Parallel()

	got := dayPrefix(testPrefix, testAccount, testRegion, time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC))
	assert.Equal(t,
		"kiro/AWSLogs/"+testAccount+"/KiroLogs/user_report/"+testRegion+"/2026/08/30/00/", got,
		"the prefix must reach the day directory: a wider one truncates silently at a thousand keys")
	// The property that matters is day-scoping, so assert it directly: two days in one
	// month must not share a prefix. A month-scoped prefix would list both — and, past
	// roughly eleven months of history, would truncate without saying so.
	other := dayPrefix(testPrefix, testAccount, testRegion, time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	assert.NotEqual(t, got, other, "each day must have its own prefix")
}

// The business date comes from the key and is in co-ordinated universal time. This is
// asserted under a deliberately non-UTC process zone because that is the case which
// passes on a developer's machine and mis-files an entire history in real use.
func TestParseKeyDayIsUTCRegardlessOfProcessZone(t *testing.T) {
	t.Setenv("TZ", "Pacific/Chatham")
	time.Local = time.FixedZone("CHAST", 12*3600+2700)
	t.Cleanup(func() { time.Local = time.UTC })

	p, err := parseKey(key("KIRO_IDE", "2026", "08", "30"))
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC), p.Day)
	assert.Equal(t, time.UTC, p.Day.Location())
}

func TestParseKeyDerivesClientTypeAndRegion(t *testing.T) {
	t.Parallel()

	p, err := parseKey(key("KIRO_CLI", "2026", "07", "04"))
	require.NoError(t, err)
	assert.Equal(t, "KIRO_CLI", p.ClientType)
	assert.Equal(t, testRegion, p.Region)
}

// The identity must not move when the vendor republishes a day under a new publication
// timestamp. If it did, a corrected day would fork into a second record and double-count
// instead of updating the first.
func TestIdentityIsStableAcrossRepublication(t *testing.T) {
	t.Parallel()

	base := key("KIRO_IDE", "2026", "08", "30")
	republished := testPrefix + "/AWSLogs/" + testAccount + "/KiroLogs/user_report/" + testRegion +
		"/2026/08/30/00/KIRO_IDE_" + testAccount + "_user_report_9999999999.csv"

	a, err := parseKey(base)
	require.NoError(t, err)
	b, err := parseKey(republished)
	require.NoError(t, err)

	assert.Equal(t, a.ClientType, b.ClientType, "identity must not depend on the publication timestamp")
	assert.Equal(t, a.Day, b.Day)
}

func TestParseKeyRefusesAnUnknownLayout(t *testing.T) {
	t.Parallel()

	for name, k := range map[string]string{
		"no AWSLogs marker": "kiro/2026/08/30/00/KIRO_IDE_x_user_report_1.csv",
		"month granularity": "kiro/AWSLogs/a/KiroLogs/user_report/r/2026/08/00/KIRO_IDE_x_user_report_1.csv",
		"wrong report kind": "kiro/AWSLogs/a/KiroLogs/by_user_analytic/r/2026/08/30/00/KIRO_IDE_x_1.csv",
		"not a csv":         key("KIRO_IDE", "2026", "08", "30") + ".gz",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseKey(k)
			assert.Error(t, err, "an unrecognised layout must fail loudly, never parse best-effort")
		})
	}
}

func TestOneClientHasOneIdentityWhateverItsCasing(t *testing.T) {
	t.Parallel()

	// The client type is the record's identity AND the destination key's last segment, so
	// two spellings of one client in a day would become two records and two objects —
	// double-counting the day. The pattern accepts either casing; only this makes them one.
	day := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	base := "kiro/AWSLogs/111122223333/KiroLogs/user_report/us-east-1/2026/06/02/00/"

	upper, err := ParseKey(base + "KIRO_IDE_111122223333_user_report_20260602T000000Z.csv")
	require.NoError(t, err)
	lower, err := ParseKey(base + "kiro_ide_111122223333_user_report_20260602T000000Z.csv")
	require.NoError(t, err)

	assert.Equal(t, "KIRO_IDE", upper.ClientType)
	assert.Equal(t, upper.ClientType, lower.ClientType,
		"one client must not become two identities on a casing difference")
	assert.Equal(t, day, upper.Day)
}

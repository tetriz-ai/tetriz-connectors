// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package kiro ingests the daily user-activity reports Kiro writes into a
// customer-owned object-storage bucket.
package kiro

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// The vendor writes a report under a fixed hierarchy beneath the customer's own prefix:
//
//	<prefix>/AWSLogs/<accountID>/KiroLogs/user_report/<region>/<yyyy>/<mm>/<dd>/00/
//	    <clientType>_<accountID>_user_report_<timestamp>.csv
//
// Three derivations depend on that shape and they are the only three: which prefix a
// day's objects live under, which day an object belongs to, and the region the vendor
// embedded — the last used at connect time to check the vendor's own requirement that a
// bucket's region matches its profile's.
//
// All three live in this file, and that containment is the design's answer to a layout
// nobody has yet observed against a real enterprise bucket: a correction is this file
// plus its fixtures, and nothing else in the connector needs to know the path's shape.
// Everywhere else works in the terms these functions return — a day and a client type —
// never in string surgery on a key.
const (
	reportKind    = "user_report"
	awsLogsMarker = "AWSLogs"
	// The vendor writes every report under an hour-00 directory. It is part of the
	// documented path rather than a value we choose, so it is named here and nowhere else.
	hourSegment = "00"
)

// dayPrefix returns the prefix holding one day's objects for an organisation.
//
// Listing is always scoped to a single day, never wider, and that is a contract rather
// than a convention: the shared object-store helpers issue one list call with no
// continuation token, so a wider prefix truncates at a thousand keys and reports nothing
// about having done so. A day's prefix holds at most one object per client type.
func dayPrefix(cfgPrefix, accountID, region string, day time.Time) string {
	d := day.UTC()
	return strings.Join([]string{
		strings.Trim(cfgPrefix, "/"),
		awsLogsMarker, accountID, "KiroLogs", reportKind, region,
		fmt.Sprintf("%04d", d.Year()),
		fmt.Sprintf("%02d", int(d.Month())),
		fmt.Sprintf("%02d", d.Day()),
		hourSegment,
	}, "/") + "/"
}

// parsedKey is everything the rest of the connector needs from a vendor key. Nothing
// outside this file reads a key directly.
type parsedKey struct {
	// Day is the business date, in co-ordinated universal time. Every record's date is
	// set from this and never left to default: an unset date silently falls back to the
	// day of the fetch, which on a first run would file an entire history under today.
	Day time.Time
	// ClientType is the record's identity. The day supplies uniqueness alongside it, so
	// the client type alone distinguishes one of a day's objects from another.
	ClientType string
	// Region is what the vendor embedded in the path, for the connect-time cross-check.
	Region string
}

// keyPattern matches the documented layout. Two anchoring choices are deliberate.
//
// It starts at the AWSLogs marker rather than at the configured prefix, so a customer
// prefix containing slashes, or the marker's own name, cannot shift the capture groups.
//
// And the client type is captured non-greedily against the filename's own documented
// structure — `<clientType>_<accountID>_user_report_<timestamp>.csv` — because client
// types contain underscores themselves. A greedy character class swallows the account id
// and the report kind along with them, yielding a client type of
// "KIRO_CLI_123456789012_user_report": a value that looks plausible, forks the record
// identity, and double-counts the day.
var keyPattern = regexp.MustCompile(
	`/` + awsLogsMarker + `/[^/]+/KiroLogs/` + reportKind +
		`/([^/]+)/(\d{4})/(\d{2})/(\d{2})/` + hourSegment +
		`/([A-Za-z][A-Za-z0-9_]*?)_\d+_` + reportKind + `_[^/]*\.csv$`)

// parseKey derives the day, the client type and the region from a vendor key.
//
// A key that does not match the documented layout is an error rather than a best-effort
// parse. Guessing would file a report under the wrong day, which is one of the two ways
// this design can produce data that looks right and is wrong.
func parseKey(key string) (parsedKey, error) {
	m := keyPattern.FindStringSubmatch("/" + strings.TrimPrefix(key, "/"))
	if m == nil {
		return parsedKey{}, fmt.Errorf("key does not match the documented Kiro report layout: %q", key)
	}
	day, err := time.Parse("2006-01-02", m[2]+"-"+m[3]+"-"+m[4])
	if err != nil {
		return parsedKey{}, fmt.Errorf("parsing the day out of %q: %w", key, err)
	}
	// Upper-cased once, here, because the client type IS the record's identity and the
	// destination key's last segment. The vendor documents it upper-case, but the pattern
	// accepts either — and two spellings of one client in a day would otherwise become two
	// records under different identities and two objects, double-counting the day. It also
	// keeps the metric label from splitting, since only the documented spelling is bounded.
	return parsedKey{Day: day.UTC(), Region: m[1], ClientType: strings.ToUpper(m[5])}, nil
}

// DayPrefix and ParseKey expose the two layout derivations to the callers outside this
// package that legitimately need them: the read-only inspection script, and the connect
// handler that reads a bucket before any configuration is stored.
//
// They exist so those callers exercise the SAME parsing the operation depends on rather
// than a second copy of it. A separate implementation would be worse than none: it could
// agree with the documentation while the operation disagreed with the bucket, and both
// the script and the connect dialogue would report success against a layout the connector
// cannot actually read. That is the whole value of the containment these wrap.
func DayPrefix(cfgPrefix, accountID, region string, day time.Time) string {
	return dayPrefix(cfgPrefix, accountID, region, day)
}

// ParsedKey is what a caller reads back from a vendor key.
type ParsedKey struct {
	Day        time.Time
	ClientType string
	Region     string
}

// ParseKey derives the day, client type and region from a vendor key.
func ParseKey(key string) (ParsedKey, error) {
	p, err := parseKey(key)
	if err != nil {
		return ParsedKey{}, fmt.Errorf("parsing a vendor key: %w", err)
	}
	return ParsedKey(p), nil
}

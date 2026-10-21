// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorkiro "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// The vendor's layout, above the point where the connector's own key parsing takes over.
// These three segments are fixed by the vendor and are what the walk descends through to
// discover the account and the region it never asked for.
const (
	kiroAWSLogsSegment    = "AWSLogs"
	kiroLogsSegment       = "KiroLogs"
	kiroReportKindSegment = "user_report"
)

const (
	// kiroMaxAccountsExamined bounds the search for the account holding the reports.
	//
	// The customer owns this key space, and a bucket that aggregates an organisation's
	// logs legitimately holds one prefix per account — dozens of them, without anyone
	// being hostile. Each one costs a listing, sequentially, inside a request an
	// administrator is waiting on. Past a handful the right answer is to say the layout is
	// ambiguous rather than to keep walking.
	// kiroMaxAccountsExamined bounds the account walk. A prefix holding logs for more
	// accounts than this is ambiguous rather than large — the administrator pointed at
	// something wider than one Kiro profile — so the walk stops and says so instead of
	// listing an unbounded number of prefixes inside a request someone is waiting on.
	kiroMaxAccountsExamined = 8

	// kiroProbeTimeout bounds the whole walk, because every call in it goes to a bucket
	// someone else controls. Without it the ceiling on one request is whatever deadline
	// the caller happened to set, and a slow or throttled object store holds a server
	// goroutine for as long as it likes.
	kiroProbeTimeout = 20 * time.Second

	// kiroSampleBudget bounds the recent-window sampling inside the deadline above. It is
	// the only step whose result is optional, so it is the only one allowed to run out of
	// time — and giving it its own share is what stops it consuming a step whose result is
	// not optional.
	kiroSampleBudget = 8 * time.Second
)

// The date segments the vendor writes are fixed-width and zero-padded, which is the only
// reason ordering them as strings orders them as dates. Anything else under these levels is
// something we did not write and cannot place, so it is skipped rather than sorted: a
// non-numeric name sorts after every digit and would otherwise be chosen as the latest day.
var (
	kiroYearSegment       = regexp.MustCompile(`^\d{4}$`)
	kiroMonthOrDaySegment = regexp.MustCompile(`^\d{2}$`)
)

// kiroUnreadableMsg is what an administrator is told when part of the hierarchy could not
// be read: the prefix it stopped at, and the one action that fixes it.
const kiroUnreadableMsg = "this bucket could not be read past %s; the read policy must cover the whole prefix"

// logKiroUnreadable records what the object store actually said.
//
// The two audiences need different things, and only one of them is served by the returned
// status. Vendor error text on a setup screen means nothing to the person reading it; but
// without it in the log, nobody can tell a refused listing from a timeout when the report
// says "the policy must cover the whole prefix" and the policy plainly does.
func logKiroUnreadable(ctx context.Context, what, where string, cause error) {
	logger.WarnWithCtx(ctx, "kiro setup: "+what, zap.String("prefix", where), zap.Error(cause))
}

// probeKiroBucket establishes what a bucket holds and whether we can actually read it.
//
// The distinction this function is most careful about is EMPTY versus UNREADABLE. They
// arrive as the same shape from a listing and mean opposite things: empty is a bucket whose
// reporting was never switched on, which is saveable; unreadable is a bucket we failed to
// see, which is not. Rendering the second as the first would save a configuration whose
// grant was never proven and leave the failure to surface on the first scheduled run, with
// the administrator long gone — the exact outcome the single-object probe exists to prevent.
// So only a listing that SUCCEEDS and returns nothing may reach the provisional state.
func (s *Service) probeKiroBucket(
	ctx context.Context, orgID, region, bucket, prefix string,
) (*kiroBucketProbe, error) {
	ctx, cancel := context.WithTimeout(ctx, kiroProbeTimeout)
	defer cancel()

	reader, err := s.kiroReader(ctx, region, orgID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reaching region %s: %v", region, err)
	}

	probe := &kiroBucketProbe{}

	// The account is discovered, never asked for. It sits in the vendor's key path, so a
	// typed one could disagree with the bucket and every later listing would address a
	// path that does not exist — which reads as an empty bucket rather than as a mistake.
	//
	// This first listing is the one place a failure means "the grant is not there yet",
	// because it is the first thing the grant would allow. Below this point a failure is
	// unexpected and is reported as one.
	accounts, err := reader.ListPrefixes(ctx, bucket, joinKiroPrefix(prefix, kiroAWSLogsSegment)+"/")
	if err != nil {
		// Not an error to the caller — this is the expected state before the policy is
		// added — but the cause is logged, because "needs grant" is also what a wrong
		// bucket name and a network fault look like from here.
		logger.Info(ctx, "kiro setup: bucket not listable yet",
			zap.String("bucket", bucket), zap.String("prefix", prefix), zap.Error(err))
		probe.state = connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_NEEDS_GRANT
		return probe, nil
	}

	account, keyRegion, err := s.resolveKiroAccountAndRegion(ctx, reader, bucket, prefix, accounts)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	if account == "" {
		// Listed cleanly and holding nothing this connector recognises. That is the
		// reporting-never-enabled case, and it cannot prove a read grant — nothing exists
		// to read. Saved as provisional rather than confirmed, and said so on screen.
		probe.state = connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL
		return probe, nil
	}
	probe.accountID, probe.keyRegion = account, keyRegion

	// The vendor requires the bucket's region to match the region of the profile writing
	// into it. Both halves are observed — one from the object store, one from the vendor's
	// own key path — so the comparison means something. A region taken from the
	// administrator would make it circular: the typed value would build the very prefix
	// the keys are read from, and the two would always agree.
	if keyRegion != "" && keyRegion != region {
		return nil, status.Errorf(codes.FailedPrecondition,
			"this bucket is in %s but its reports were written by a Kiro profile in %s; "+
				"the vendor requires them to match, so the bucket is not the one this profile writes to",
			region, keyRegion)
	}

	if err := s.walkKiroBoundaries(ctx, reader, bucket, prefix, probe); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	if probe.firstDay == "" {
		probe.state = connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL
		return probe, nil
	}

	if probe.oldestKey == "" {
		// A day exists but holds no object we could name. Nothing to read means nothing to
		// prove, and claiming otherwise is the false confidence this whole step avoids.
		probe.state = connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL
		return probe, nil
	}

	// Exactly one object, purely to prove the grant end to end. A policy granting listing
	// and omitting reading, or reports under an encryption key our role cannot use, both
	// let every listing succeed and every fetch fail — so a listing-only check would save
	// a connector that cannot collect, and the failure would surface on the first
	// scheduled run with the administrator long gone. Nothing is read but the permission:
	// this parses no report and downloads none.
	if err := reader.ProbeReadable(ctx, bucket, probe.oldestKey); err != nil {
		logger.WarnWithCtx(ctx, "kiro setup: the bucket lists but its reports cannot be read",
			zap.String("bucket", bucket), zap.String("object", probe.oldestKey), zap.Error(err))
		return nil, status.Error(codes.FailedPrecondition,
			"this bucket can be listed but its reports cannot be read; the policy needs s3:GetObject "+
				"as well as s3:ListBucket, and any encryption key the reports use must permit our role")
	}
	// Sampled AFTER the read is proven, and under a budget of its own.
	//
	// The order is the point. Everything above establishes something the answer depends
	// on; this is a recognition aid — a file count and the client types, so an
	// administrator can tell their own bucket from a stranger's. It also costs up to
	// thirty sequential listings. Run first, on a slow day it could spend the whole
	// deadline and leave the one step that proves anything failing on a cancelled context
	// — reported to the administrator as "the policy needs s3:GetObject", which is
	// specific, actionable, and wrong. Its own sub-budget means it degrades to fewer days
	// sampled instead of starving what matters.
	sampleCtx, cancelSample := context.WithTimeout(ctx, kiroSampleBudget)
	defer cancelSample()
	s.countKiroRecentFiles(sampleCtx, reader, bucket, prefix, account, keyRegion, probe)

	probe.state = connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_CONFIRMED
	return probe, nil
}

// resolveKiroAccountAndRegion picks the account and region that actually hold reports.
//
// A bucket may carry more than one account's logs, and only some of them Kiro's, so this
// descends one level into each until it finds the report root the connector understands.
// A listing that FAILS ends the search rather than being skipped: the difference between
// "this account holds no reports" and "we could not see whether it does" is the difference
// between a bucket that is empty and one we have not read.
func (s *Service) resolveKiroAccountAndRegion(
	ctx context.Context, reader KiroBucketReader, bucket, prefix string, accounts []string,
) (account, keyRegion string, err error) {
	examined := 0
	for _, a := range accounts {
		if a == "" {
			continue
		}
		if examined >= kiroMaxAccountsExamined {
			return "", "", status.Errorf(codes.FailedPrecondition,
				"this prefix holds logs for more than %d accounts, so which one carries the Kiro reports is "+
					"ambiguous; point the path at the account's own prefix", kiroMaxAccountsExamined)
		}
		examined++

		root := joinKiroPrefix(prefix, kiroAWSLogsSegment, a, kiroLogsSegment, kiroReportKindSegment) + "/"
		regions, lerr := reader.ListPrefixes(ctx, bucket, root)
		if lerr != nil {
			logKiroUnreadable(ctx, "an account prefix could not be listed", root, lerr)
			return "", "", status.Errorf(codes.FailedPrecondition, kiroUnreadableMsg, root)
		}
		if len(regions) == 0 {
			continue
		}
		sort.Strings(regions)
		if account != "" {
			// Only one account is stored, and every day prefix the scheduled run builds
			// comes from it — so a second account's reports would never be collected and
			// nothing would say so. Refusing sends the administrator to a narrower path,
			// which is a question they can answer and this code cannot.
			return "", "", status.Errorf(codes.FailedPrecondition,
				"this prefix holds Kiro reports for more than one account (%s and %s); "+
					"point the path at one account's own prefix", account, a)
		}
		if len(regions) > 1 {
			// Refused for the reason a second account is, four lines above: every day
			// prefix the scheduled run builds carries one region, so a second region's
			// reports would never be collected and nothing would say so. Taking the first
			// silently was worse than losing them — the region chosen here is also what
			// the bucket's own location is checked against, so picking the wrong one
			// refuses the connection citing a profile mismatch that does not exist.
			return "", "", status.Errorf(codes.FailedPrecondition,
				"this prefix holds Kiro reports for more than one region (%s); "+
					"point the path at one region's own prefix",
				strings.Join(regions, " and "))
		}
		account, keyRegion = a, regions[0]
	}
	return account, keyRegion, nil
}

// walkKiroBoundaries descends the boundary path only: the earliest year, then its earliest
// month and day, and the same for the latest.
//
// It walks the hierarchy rather than stepping back day by day because a day-by-day walk has
// no honest stopping rule. Nothing tells us when the customer switched reporting on, so
// "stop after N consecutive empty days" is a heuristic that silently truncates an
// organisation whose history has a gap in it. Asking which years exist has an answer.
//
// Only the boundaries are descended because only boundary facts are wanted — the date range
// and the oldest object. The interior is never listed, which is what keeps this a handful
// of calls rather than one per day of history.
func (s *Service) walkKiroBoundaries(
	ctx context.Context, reader KiroBucketReader, bucket, prefix string, probe *kiroBucketProbe,
) error {
	base := kiroReportRoot(prefix, probe.accountID, probe.keyRegion)

	years, err := reader.ListPrefixes(ctx, bucket, base)
	if err != nil {
		logKiroUnreadable(ctx, "the report root could not be listed", base, err)
		return status.Errorf(codes.FailedPrecondition, kiroUnreadableMsg, base)
	}
	years = kiroDateSegments(years, kiroYearSegment)
	if len(years) == 0 {
		return nil
	}

	first, err := s.descendKiroBoundary(ctx, reader, bucket, base, years[0], true)
	if err != nil {
		return err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	last, err := s.descendKiroBoundary(ctx, reader, bucket, base, years[len(years)-1], false)
	if err != nil {
		return err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	if first == "" {
		return nil
	}
	probe.firstDay, probe.lastDay = first, last
	if probe.lastDay == "" {
		probe.lastDay = first
	}

	// The oldest object is the honest statement of collectable depth, so it is read from
	// the earliest day rather than inferred from the range. The prefix is passed down
	// rather than recovered from the built path: recovering it means finding a segment
	// name inside a string that now contains that name twice, and an administrator whose
	// own prefix contains it would have the wrong half returned.
	day, err := time.Parse("2006-01-02", probe.firstDay)
	if err != nil {
		// The segments were filtered to the vendor's fixed-width numeric shape before this,
		// so a value that will not parse means the layout is not what it was taken to be.
		logger.WarnWithCtx(ctx, "kiro setup: a boundary day did not parse",
			zap.String("day", probe.firstDay), zap.Error(err))
		return nil
	}
	dayPrefix := connectorkiro.DayPrefix(prefix, probe.accountID, probe.keyRegion, day)
	objects, lerr := reader.ListDay(ctx, bucket, dayPrefix)
	if lerr != nil {
		logKiroUnreadable(ctx, "the earliest day could not be listed", dayPrefix, lerr)
		return status.Errorf(codes.FailedPrecondition, kiroUnreadableMsg, dayPrefix)
	}
	if len(objects) == 0 {
		return nil
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })

	// The object that proves the grant must be one the connector could also collect. A key
	// under the right directories whose filename this connector cannot place is skipped by
	// every scheduled run — so confirming on it would hand back a bucket that reads
	// perfectly and produces nothing, which is the failure the layout is most likely to
	// take given it has never been observed against a real enterprise bucket.
	for _, o := range objects {
		_, perr := connectorkiro.ParseKey(o.Key)
		if perr == nil {
			probe.oldestKey = o.Key
			return nil
		}
		logger.WarnWithCtx(ctx, "kiro setup: an object does not match the report layout",
			zap.String("key", o.Key), zap.Error(perr))
	}
	return status.Errorf(codes.FailedPrecondition,
		"the reports in this bucket do not match the layout this connector reads (for example %s); "+
			"the path may point at a different kind of export", objects[0].Key)
}

// descendKiroBoundary returns the earliest or latest day under one year, as YYYY-MM-DD.
func (s *Service) descendKiroBoundary(
	ctx context.Context, reader KiroBucketReader, bucket, base, year string, earliest bool,
) (string, error) {
	months, err := reader.ListPrefixes(ctx, bucket, base+year+"/")
	if err != nil {
		logKiroUnreadable(ctx, "a year could not be listed", base+year+"/", err)
		return "", status.Errorf(codes.FailedPrecondition, kiroUnreadableMsg, base+year+"/")
	}
	months = kiroDateSegments(months, kiroMonthOrDaySegment)
	if len(months) == 0 {
		return "", nil
	}
	month := kiroPickBoundary(months, earliest)

	days, err := reader.ListPrefixes(ctx, bucket, base+year+"/"+month+"/")
	if err != nil {
		logKiroUnreadable(ctx, "a month could not be listed", base+year+"/"+month+"/", err)
		return "", status.Errorf(codes.FailedPrecondition, kiroUnreadableMsg, base+year+"/"+month+"/")
	}
	days = kiroDateSegments(days, kiroMonthOrDaySegment)
	if len(days) == 0 {
		return "", nil
	}
	return year + "-" + month + "-" + kiroPickBoundary(days, earliest), nil
}

// kiroDateSegments keeps only the fixed-width numeric names the vendor writes, sorted.
func kiroDateSegments(in []string, shape *regexp.Regexp) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if shape.MatchString(s) {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func kiroPickBoundary(sorted []string, earliest bool) string {
	if earliest {
		return sorted[0]
	}
	return sorted[len(sorted)-1]
}

// countKiroRecentFiles samples the recent window, and only the recent window.
//
// Best-effort by design: a day that cannot be listed is skipped rather than failing the
// call, because this half of the inventory is a recognition aid on a confirmation screen
// and not a fact anything downstream depends on. The scheduled operation reads the true
// depth from the bucket regardless of what was displayed here.
func (s *Service) countKiroRecentFiles(
	ctx context.Context, reader KiroBucketReader, bucket, prefix, account, keyRegion string,
	probe *kiroBucketProbe,
) {
	seen := map[string]struct{}{}
	end := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	for i := 0; i < kiroRecentWindowDays; i++ {
		day := end.AddDate(0, 0, -i)
		objects, err := reader.ListDay(ctx, bucket, connectorkiro.DayPrefix(prefix, account, keyRegion, day))
		if err != nil {
			// Skipped rather than fatal — this half of the inventory is a recognition aid,
			// not a fact anything depends on — but logged, because a run of these means
			// the sampled figures understate the bucket and the screen would not say so.
			logger.WarnWithCtx(ctx, "kiro setup: a sampled day could not be listed",
				zap.String("day", day.Format("2006-01-02")), zap.Error(err))
			continue
		}
		for _, o := range objects {
			probe.fileCount++
			parsed, perr := connectorkiro.ParseKey(o.Key)
			if perr != nil {
				logger.WarnWithCtx(ctx, "kiro setup: a sampled object does not match the report layout",
					zap.String("key", o.Key), zap.Error(perr))
				continue
			}
			seen[parsed.ClientType] = struct{}{}
		}
	}
	probe.clientTypes = make([]string, 0, len(seen))
	for ct := range seen {
		probe.clientTypes = append(probe.clientTypes, ct)
	}
	sort.Strings(probe.clientTypes)
}

// inventory renders the probe for the confirmation screen, keeping the exact figures and
// the sampled ones apart.
func (p *kiroBucketProbe) inventory() *connintv1.KiroBucketInventory {
	if p.firstDay == "" {
		return nil
	}
	return &connintv1.KiroBucketInventory{
		FirstDay:          p.firstDay,
		LastDay:           p.lastDay,
		OldestObjectKey:   p.oldestKey,
		CollectableDays:   int32(kiroDaysBetween(p.firstDay, p.lastDay)), //nolint:gosec // G115: a day span cannot overflow int32
		RecentFileCount:   int32(p.fileCount),                            //nolint:gosec // G115: a bounded window's file count cannot overflow int32
		RecentClientTypes: p.clientTypes,
		RecentWindowDays:  kiroRecentWindowDays,
	}
}

// kiroDaysBetween counts both ends, because a bucket holding one day of reports offers one
// day of history rather than none.
func kiroDaysBetween(first, last string) (days int) {
	f, ferr := time.Parse("2006-01-02", first)
	if ferr != nil {
		logger.ErrorNoCtx(fmt.Sprintf("kiro inventory: unparseable first day %q: %v", first, ferr))
		return 0
	}
	l, lerr := time.Parse("2006-01-02", last)
	if lerr != nil {
		logger.ErrorNoCtx(fmt.Sprintf("kiro inventory: unparseable last day %q: %v", last, lerr))
		return 0
	}
	if l.Before(f) {
		return 0
	}
	return int(l.Sub(f).Hours()/24) + 1
}

func joinKiroPrefix(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.Trim(p, "/"); t != "" {
			cleaned = append(cleaned, t)
		}
	}
	return strings.Join(cleaned, "/")
}

// kiroReportRoot is the prefix the year directories sit directly under.
func kiroReportRoot(prefix, account, region string) string {
	return joinKiroPrefix(prefix, kiroAWSLogsSegment, account, kiroLogsSegment, kiroReportKindSegment, region) + "/"
}

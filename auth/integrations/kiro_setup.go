// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorkiro "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	connintv1 "github.com/tetriz-ai/tetriz-connectors/gen/integrations/v1"
	kirogateway "github.com/tetriz-ai/tetriz-connectors/internal/gateway/kiro"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

const connectorIDKiro = "kiro"

// checkKiroOperationallyEnabled applies the environment-wide switch, which is a different
// gate from the per-organisation flag and is checked in a different place.
//
// A host applies any per-organization gate before it calls this service.
// This switch lives here and decides whether the environment serves this connector at all
// — an operator's off switch, refusing every organisation or none. Checking only the flag
// would mean an organisation flagged in could connect while the operator believed the
// switch still held it back, which is the one direction a off switch must never fail.
func (s *Service) checkKiroOperationallyEnabled() error {
	if !s.connectorEnabled(connectorIDKiro) {
		return status.Error(codes.FailedPrecondition, "the Kiro connector is not enabled in this environment")
	}
	return nil
}

// kiroRecentWindowDays bounds the sampled half of the inventory.
//
// The exact half — the date range and the oldest object — comes from a boundary walk that
// costs a handful of listings whatever the history. A file count does not: it is a
// property of the keys inside EVERY day, so a complete one would cost one listing per day
// of history, hundreds of calls inside a request an administrator is waiting on. What the
// confirmation screen is for is recognising that the bucket holds the right sort of thing
// and how far back it reaches, and both survive a bounded count.
const kiroRecentWindowDays = 30

// KiroBucketReader is the connect-time view of a customer's bucket: enough to walk the key
// hierarchy and prove the grant, and nothing else.
//
// Deps.KiroReaderFor supplies it, and a test passes a fake so it needs no AWS.
type KiroBucketReader interface {
	ListPrefixes(ctx context.Context, bucket, prefix string) ([]string, error)
	ListDay(ctx context.Context, bucket, prefix string) ([]connectorkiro.SourceObject, error)
	// ProbeReadable answers whether one object can be read, without reading it. The
	// question is about the grant, and downloading a report to answer it would spend an
	// administrator's request on bytes nobody looks at.
	ProbeReadable(ctx context.Context, bucket, key string) error
}

// The three seams as named types.
type (
	// KiroRegionResolver reports which region a bucket lives in.
	KiroRegionResolver func(ctx context.Context, bucket string) (string, error)
	// KiroReaderFactory builds a reader for a bucket in one region.
	KiroReaderFactory func(ctx context.Context, region string) (KiroBucketReader, error)
	// KiroPrincipalResolver reports the identity a customer's bucket policy must name.
	KiroPrincipalResolver func(ctx context.Context) (string, error)
)

// kiroRegion and kiroReader resolve the seams, falling back to the real object store so a
// constructor that supplies neither still reaches a real bucket.
func (s *Service) kiroRegion(ctx context.Context, bucket string) (string, error) {
	locate := s.kiroRegionOf
	if locate == nil {
		locate = kirogateway.DiscoverRegion
	}
	region, err := locate(ctx, bucket)
	if err != nil {
		return "", fmt.Errorf("locating %s: %w", bucket, err)
	}
	return region, nil
}

// The default reader sends orgID's tenant marker, so a probe passes only under
// the policy generated for that organization, as its scheduled runs will.
func (s *Service) kiroReader(ctx context.Context, region, orgID string) (KiroBucketReader, error) {
	if s.kiroReaderFor != nil {
		reader, err := s.kiroReaderFor(ctx, region)
		if err != nil {
			return nil, fmt.Errorf("building a bucket reader for %s: %w", region, err)
		}
		return reader, nil
	}
	gw, err := kirogateway.NewGatewayForOrganization(ctx, region, orgID)
	if err != nil {
		return nil, fmt.Errorf("building a bucket reader for %s: %w", region, err)
	}
	return gw, nil
}

// parseKiroBucketPath splits the single value the administrator types.
//
// One field rather than four because the other three are discoverable and a typed value is
// not checkable: the region is read from the object store, and the account appears in the
// vendor's own key path. A prefix is required because the vendor refuses to write reports
// to a bucket root, so a path without one describes a layout it never produces.
func parseKiroBucketPath(raw string) (bucket, prefix string, err error) {
	p := strings.TrimSpace(raw)
	p = strings.TrimPrefix(p, "s3://")
	p = strings.Trim(p, "/")
	bucket, prefix, found := strings.Cut(p, "/")
	switch {
	case bucket == "":
		return "", "", fmt.Errorf("a bucket is required, as s3://bucket/prefix")
	case !found || strings.TrimSpace(prefix) == "":
		return "", "", fmt.Errorf(
			"a prefix is required, as s3://%s/<prefix>: the vendor does not write reports to a bucket root", bucket)
	}
	if !s3BucketName.MatchString(bucket) || strings.Contains(bucket, "..") {
		// "not accepted here", not "not valid". The pattern is today's naming rule, and
		// buckets predating it can legally carry uppercase or an underscore — telling
		// that customer their bucket is invalid is both wrong and unactionable. What is
		// true is that this value is interpolated into a policy document, so the accepted
		// shape is deliberately narrow.
		return "", "", fmt.Errorf(
			"%q is not a bucket name this connector accepts: it allows lower-case letters, "+
				"digits, dots and hyphens, because the name is written into a policy document",
			bucket)
	}
	// Normalised to one canonical spelling, because the stored value is compared as a
	// string to decide whether the path CHANGED — and a path change clears the cursor. So
	// an administrator who retypes the same location with a stray slash (`bucket//kiro`
	// leaves a prefix of `/kiro`, since the split takes only the first separator) would
	// otherwise reconnect to the same bucket and restart a backfill of up to four hundred
	// days. Collapsing repeats and trimming the ends makes every spelling of one location
	// compare equal.
	prefix = kiroRepeatedSlash.ReplaceAllString(prefix, "/")
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", "", fmt.Errorf(
			"a prefix is required, as s3://%s/<prefix>: the vendor does not write reports to a bucket root", bucket)
	}
	if err := checkKiroPrefix(prefix); err != nil {
		return "", "", fmt.Errorf("checking the prefix: %w", err)
	}
	return bucket, prefix, nil
}

// kiroRepeatedSlash collapses `a//b` to `a/b` so one location has one spelling.
var kiroRepeatedSlash = regexp.MustCompile(`/{2,}`)

// s3BucketName is the vendor's own naming rule, applied here because this value is
// interpolated into a policy document rather than merely used as an argument.
var s3BucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// checkKiroPrefix refuses the characters that would widen the grant this path produces.
//
// The prefix reaches two very different places. To the object store it is a literal
// string, so a strange character there merely finds nothing. In the bucket policy it is a
// PATTERN: IAM reads `*` and `?` as wildcards, and `*` matches `/` as well — so a prefix
// of `kiro*` would generate a grant over every key in the bucket beginning with `kiro`,
// including sibling paths that have nothing to do with these reports. The administrator
// would be pasting a policy that hands us more of their own bucket than the screen said,
// which is precisely the artifact they are least able to check.
//
// Control characters are refused for the same reason in a milder form: JSON escaping keeps
// them from breaking the document, but a policy no human can read is one nobody can audit.
func checkKiroPrefix(prefix string) error {
	if i := strings.IndexAny(prefix, "*?"); i >= 0 {
		return fmt.Errorf(
			"a prefix may not contain %q: the read policy would treat it as a wildcard and grant more of the bucket than these reports",
			prefix[i])
	}
	if strings.Contains(prefix, "..") {
		return fmt.Errorf("a prefix may not contain \"..\"")
	}
	for _, r := range prefix {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("a prefix may not contain control characters")
		}
	}
	return nil
}

// kiroBucketProbe is what one pass over a customer's bucket established.
type kiroBucketProbe struct {
	state       connintv1.KiroBucketAccessState
	accountID   string
	keyRegion   string
	firstDay    string
	lastDay     string
	oldestKey   string
	fileCount   int
	clientTypes []string
}

// ValidateKiroBucket builds the grant to paste and reports what could be seen, persisting
// nothing.
func (s *Service) ValidateKiroBucket(
	ctx context.Context, req *connintv1.ValidateKiroBucketRequest,
) (*connintv1.ValidateKiroBucketResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if err := s.checkKiroOperationallyEnabled(); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	bucket, prefix, err := parseKiroBucketPath(req.GetBucketPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// The policy is returned whatever happens next, because the call that cannot reach the
	// bucket is the one whose answer the administrator most needs: it is the step where
	// they have not added the grant yet.
	principal, err := s.kiroReaderPrincipal(ctx)
	if err != nil {
		// Refused rather than guessed. A policy naming the wrong principal is a grant that
		// looks right to whoever pastes it and never works, and it is the artifact an
		// administrator is least able to check.
		logger.Error(ctx, "ValidateKiroBucket: resolving the runner's principal", zap.Error(err))
		return nil, status.Error(codes.Internal, "could not determine the identity the grant must name")
	}
	policy := kiroReadPolicy(bucket, prefix, principal, orgID)

	region, err := s.kiroRegion(ctx, bucket)
	if err != nil {
		// A bucket that does not exist and one we may not reach arrive here as the same
		// failure, and they are reported as the same thing: paste the policy, then check
		// again. Separating them would be worth doing — a typo and a missing grant need
		// different words — but it takes an observation of what the object store actually
		// returns for each, and asserting a difference we have not seen would put a branch
		// in front of administrators that never runs.
		logger.Info(ctx, "kiro setup: bucket not yet reachable",
			zap.String(logger.OrgID, orgID), zap.String("bucket", bucket), zap.Error(err))
		return &connintv1.ValidateKiroBucketResponse{
			ReadPolicy: policy,
			State:      connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_NEEDS_GRANT,
		}, nil
	}

	probe, err := s.probeKiroBucket(ctx, orgID, region, bucket, prefix)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}

	return &connintv1.ValidateKiroBucketResponse{
		ReadPolicy: policy,
		Region:     region,
		State:      probe.state,
		Inventory:  probe.inventory(),
	}, nil
}

// SubmitKiroBucket persists a confirmed bucket and enables the connector.
//
// It re-runs the bucket walk and stores what that walk finds, not what the caller submits.
// Validation is optional and leaves no server-side record.
func (s *Service) SubmitKiroBucket(
	ctx context.Context, req *connintv1.SubmitKiroBucketRequest,
) (*connintv1.SubmitKiroBucketResponse, error) {
	orgID := orgid.Canonical(req.GetOrgId())
	if err := s.checkKiroOperationallyEnabled(); err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	bucket, prefix, err := parseKiroBucketPath(req.GetBucketPath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	region, err := s.kiroRegion(ctx, bucket)
	if err != nil {
		logger.WarnWithCtx(ctx, "SubmitKiroBucket: bucket not reachable",
			zap.String(logger.OrgID, orgID), zap.String("bucket", bucket), zap.Error(err))
		return nil, status.Error(codes.FailedPrecondition,
			"this bucket cannot be reached with the read policy in place; add the policy, then connect")
	}

	probe, err := s.probeKiroBucket(ctx, orgID, region, bucket, prefix)
	if err != nil {
		return nil, err //nolint:wrapcheck // status pass-through; %w would corrupt status.Code()
	}
	switch probe.state {
	case connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_NEEDS_GRANT:
		return nil, status.Error(codes.FailedPrecondition,
			"this bucket cannot be listed with the read policy in place; add the policy, then connect")
	case connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_UNSPECIFIED:
		// Unreachable by construction: the probe sets one of the three states above on
		// every path. Named rather than left to a default so that a state added later
		// fails this switch at compile time instead of falling silently into "save it".
		return nil, status.Error(codes.Internal, "the bucket check returned no verdict")
	case connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_CONFIRMED:
		// The only saveable state, and the one the rest of this function handles.
	case connintv1.KiroBucketAccessState_KIRO_BUCKET_ACCESS_STATE_PROVISIONAL:
		// A bucket holding no reports yet cannot be saved, and the reason is stronger than
		// an unproven grant: the account the vendor writes under is discovered from a key,
		// so a bucket with no keys yields no account — and the scheduled run refuses a
		// configuration without one. Saving would record a connection that cannot run
		// until someone reconnects, which is worse than not saving and saying why.
		return nil, status.Error(codes.FailedPrecondition,
			"this bucket lists cleanly but holds no Kiro reports yet, so reading is unproven and there is "+
				"nothing to configure from; switch reporting on in Kiro, wait for the first daily export, "+
				"then connect")
	}

	// Every stored value except the path is one this call observed. The region came from
	// the object store, the account from the vendor's own key path — neither was accepted
	// from the request, and neither can be wrong in a way the administrator could not see.
	observed := connectorkiro.Settings{
		Bucket:    bucket,
		Prefix:    prefix,
		Region:    region,
		AccountID: probe.accountID,
	}

	cfg := &sdk.ConnectorConfig{
		ConnectorID:    sdk.ConnectorID(connectorIDKiro),
		OrganizationID: orgID,
	}
	// The connection fields are REPLACED rather than merged into, because every one of
	// them was observed by this call and a value left over from an earlier connect is a
	// value nothing has re-checked. It is also what lets an administrator correct a prefix
	// — a merge would accept the correction and quietly ignore it.
	//
	// The two tuning dials are the exception, and they are carried over. Nothing in this
	// call observes them: they are an operator's answers about read cost and backfill
	// depth, and reconnecting to fix a typo in a path is not a statement about either.
	// Replacing them would silently return an organisation to defaults, changing how much
	// it re-reads and how far a correction can reach back.
	//
	// Written in one row-locked transaction rather than composed from a save and an
	// enable, so two connects racing cannot end with a pair neither submitted while both
	// are told they succeeded.
	var pathChanged bool
	replaceSettings := func(current json.RawMessage) (json.RawMessage, error) {
		next := observed
		if len(current) > 0 {
			var previous connectorkiro.Settings
			if err := json.Unmarshal(current, &previous); err != nil {
				// The stored settings cannot be read, so neither the operator's tuning nor
				// the old path can be recovered. The cursor is cleared on that basis: a
				// full re-read costs repeated listings and lands the same objects under
				// the same keys, while skipping a reset we could not rule out would leave
				// a gap nobody sees. The tuning falls back to defaults, which is the loss
				// this cannot avoid — so it is logged rather than passed over.
				logger.Error(ctx, "SubmitKiroBucket: unreadable stored settings; restarting collection",
					zap.String(logger.OrgID, orgID), zap.Error(err))
				pathChanged = true
			} else {
				next.TrailingOverlapDays = previous.TrailingOverlapDays
				next.FirstRunWindowDays = previous.FirstRunWindowDays
				pathChanged = previous.Bucket != "" && (previous.Bucket != bucket || previous.Prefix != prefix)
			}
		}
		merged, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("marshalling kiro settings: %w", err)
		}
		return merged, nil
	}
	if err := s.connectorDAO.SaveCredentialsEnableWithSettings(ctx, cfg, replaceSettings); err != nil {
		if isKiroBucketAlreadyClaimed(err) {
			// The database refused it, which is the only place this can be refused
			// without a race: two organisations submitting one bucket can both pass an
			// application check before either writes.
			logger.WarnWithCtx(ctx, "SubmitKiroBucket: bucket already claimed by another organisation",
				zap.String(logger.OrgID, orgID), zap.String("bucket", bucket))
			return nil, status.Error(codes.AlreadyExists,
				"this bucket is already connected to another organisation")
		}
		logger.Error(ctx, "SubmitKiroBucket: save connector config",
			zap.String(logger.OrgID, orgID), zap.Error(err))
		return nil, status.Errorf(codes.Internal, "save connector config: %v", err)
	}
	s.resetConnectorHealth(ctx, orgID, sdk.ConnectorID(connectorIDKiro))

	// A new path is a new history, so the mark from the old one must not survive it. The
	// cursor is a high-water mark and the walk is anchored to it: left in place, the first
	// run against the new bucket would begin at the old mark's trailing window and every
	// report older than it would be skipped — a backfill that silently never happens.
	if pathChanged {
		if err := s.syncStateDAO.DeleteByConnector(ctx, orgID, sdk.ConnectorID(connectorIDKiro)); err != nil {
			logger.Error(ctx, "SubmitKiroBucket: clearing cursor state after a path change",
				zap.String(logger.OrgID, orgID), zap.Error(err))
			return nil, status.Errorf(codes.Internal,
				"the bucket was saved but its collection state could not be reset; reconnect to retry: %v", err)
		}
		logger.Info(ctx, "kiro setup: bucket path changed, collection restarts from the beginning",
			zap.String(logger.OrgID, orgID), zap.String("bucket", bucket))
	}

	logger.Info(ctx, "kiro setup: connection saved",
		zap.String(logger.OrgID, orgID), zap.String("bucket", bucket),
		zap.String("region", region), zap.String(logger.State, probe.state.String()))
	return &connintv1.SubmitKiroBucketResponse{State: probe.state}, nil
}

// isKiroBucketAlreadyClaimed recognises the database's refusal by the index that made it.
//
// Matched on the index name rather than on a driver error type because the name is what
// says WHICH rule was broken: this handler writes one row and several constraints could
// refuse it, and reporting the wrong one would send an administrator to fix the wrong
// thing.
func isKiroBucketAlreadyClaimed(err error) bool {
	return isClaimedByAnotherOrganization(err, "uniq_kiro_bucket_per_org")
}

// isClaimedByAnotherOrganization recognises a unique index's refusal of a
// second organization's claim on one vendor resource, by the index's name.
func isClaimedByAnotherOrganization(err error, index string) bool {
	return err != nil && strings.Contains(err.Error(), index)
}

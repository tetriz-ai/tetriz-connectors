// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package kiro is the vendor gateway for the Kiro report connector.
//
// It is the first gateway in this repository that reads object storage rather than an
// HTTP API, and its shape reflects that: it satisfies the framework's vendor-gateway
// interface so the orchestrator can build it, and carries the narrow object-reading
// capability the operation actually uses. See the type doc for why that split exists
// rather than a new collaborator on the operation's parameters.
package kiro

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
	"github.com/tetriz-ai/tetriz-connectors/internal/orgid"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/proto"

	connectorkiro "github.com/tetriz-ai/tetriz-connectors/connectors/kiro"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// errNotHTTP is what every HTTP-shaped method returns.
//
// The framework's interface is shaped for HTTP — request execution, a JSON convenience
// wrapper, rate-limit scoping, a protocol-buffer call — and this connector makes no HTTP
// call at all. It needs a gateway regardless, because the orchestrator builds one per
// execution and treats a construction failure as a fatal configuration fault.
//
// That requirement is the reason this shape is right rather than merely expedient: the
// factory is where per-organisation credentials are already resolved for every other
// connector, so making that mandatory seam do the work beats adding a second mechanism
// beside it. The cost is these five methods, which nothing in this connector calls.
var errNotHTTP = fmt.Errorf("kiro reads object storage and makes no HTTP calls; use its object source instead")

// Gateway reads a customer's reports bucket.
type Gateway struct {
	client *awss3.Client
}

// compile-time proof that this satisfies both contracts. The narrow one is what the
// operation asserts for; the wide one is what the orchestrator requires.
var (
	_ sdk.VendorGateway          = (*Gateway)(nil)
	_ connectorkiro.ObjectSource = (*Gateway)(nil)
)

// NewGateway builds a gateway for one organisation's bucket.
//
// The region comes from the organisation's stored settings rather than from a shared
// process configuration, because a customer's bucket lives in whichever region their
// Kiro profile was installed in — a single shared client would address the wrong one.
// Loading a configuration makes no network call; it resolves the deployment's own
// credentials, which is what the customer's bucket policy grants.
func NewGateway(ctx context.Context, region string) (*Gateway, error) {
	if region == "" {
		return nil, fmt.Errorf("a region is required to reach a customer bucket")
	}
	cfg, err := awscreds.Load(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("resolving credentials for region %s: %w", region, err)
	}
	return &Gateway{client: awss3.NewFromConfig(cfg)}, nil
}

// TenantReferer is the Referer header a gateway built for orgID
// (NewGatewayForOrganization) sends with every request it makes, and the
// value the generated bucket policy requires. Region discovery
// (DiscoverRegion) is not such a request and sends none
// (aws:Referer). The reader principal is shared by every tenant, so the
// principal alone would let one tenant's run read a bucket another tenant
// granted; the condition binds a grant to the tenant that connected it. The
// header is set by the runner from the run's organization, never by the
// tenant, and the policy also names the principal, so the condition narrows a
// grant and is never the only check. orgID is canonicalized (orgid.Canonical),
// so every spelling of one organization's id yields the same value.
func TenantReferer(orgID string) string {
	return "tetriz-connectors/org/" + orgid.Canonical(orgID)
}

// NewGatewayForOrganization is NewGateway for one organization: every request
// carries TenantReferer(orgID), which the bucket policy the connect flow
// generates requires.
func NewGatewayForOrganization(ctx context.Context, region, orgID string) (*Gateway, error) {
	if orgID == "" {
		return nil, fmt.Errorf("an organization is required to reach a customer bucket")
	}
	g, err := NewGateway(ctx, region)
	if err != nil {
		return nil, err
	}
	g.client = awss3.New(g.client.Options(), withReferer(TenantReferer(orgID)))
	return g, nil
}

// withReferer sets the Referer header on every request the client sends.
func withReferer(v string) func(*awss3.Options) {
	return func(o *awss3.Options) {
		o.APIOptions = append(o.APIOptions, smithyhttp.SetHeaderValue("Referer", v))
	}
}

// --- the narrow capability the operation uses ---

// ListDay returns the objects under a single day's prefix.
//
// One list call with no continuation is correct here and only here: a day's prefix holds
// at most one object per client type, so there is no second page to miss. The same call
// over a wider prefix is the silent-truncation trap the connector's per-day listing
// exists to avoid, which is why this takes a prefix rather than building one.
func (g *Gateway) ListDay(ctx context.Context, bucket, prefix string) ([]connectorkiro.SourceObject, error) {
	out, err := g.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, classifyGrant(err))
	}
	objects := make([]connectorkiro.SourceObject, 0, len(out.Contents))
	for _, o := range out.Contents {
		if o.Key == nil {
			continue
		}
		obj := connectorkiro.SourceObject{Key: *o.Key}
		if o.LastModified != nil {
			obj.LastModified = o.LastModified.UTC()
		}
		if o.Size != nil {
			obj.Size = *o.Size
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

// Get returns one object's bytes.
//
// It reads through the client this gateway already holds rather than constructing a
// bucket-scoped wrapper per object: the wrapper's value is its helpers, and this needs
// none of them.
func (g *Gateway) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := g.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("reading s3://%s/%s: %w", bucket, key, classifyObject(err))
	}
	// Bounded, because the bucket belongs to the customer and this reads whatever is in
	// it into memory whole — the caller then hashes and re-uploads the same buffer. One
	// object larger than the process has room for takes the connector down for every
	// organisation it was serving, and nothing about a mistyped lifecycle rule or a
	// misdirected export in someone else's bucket should be able to do that. Reading one
	// byte past the cap is what makes an over-sized object detectable rather than silently
	// truncated into a record that looks complete.
	body, err := io.ReadAll(io.LimitReader(out.Body, connectorkiro.MaxReportBytes+1))
	if cerr := out.Body.Close(); cerr != nil && err == nil {
		return nil, fmt.Errorf("closing the body of s3://%s/%s: %w", bucket, key, cerr)
	}
	if err != nil {
		return nil, fmt.Errorf("reading the body of s3://%s/%s: %w", bucket, key, err)
	}
	if int64(len(body)) > connectorkiro.MaxReportBytes {
		// Object-scoped: re-reading will not shrink it, so it settles rather than pinning
		// the day, and the operator is told which object and how big the ceiling is.
		return nil, fmt.Errorf("reading s3://%s/%s: report exceeds the %d-byte ceiling: %w",
			bucket, key, connectorkiro.MaxReportBytes, connectorkiro.ErrObjectUnreadable)
	}
	return body, nil
}

// classify marks a bucket-level refusal as a configuration fault rather than a transient
// failure, so the operation can report it in the configuration-fault class.
//
// Only these shapes qualify. A throttle or a network blip is genuinely transient and must
// stay so: classifying it as a configuration fault would pause a connector that is merely
// busy, and pausing on a healthy connector is worse than a late retry.
// classifyGrant and classifyObject are two classifiers, and the split is
// the point: the SAME error code means different things depending on what has already been
// proven when it arrives.
//
// A denial on a listing is about the bucket — the grant is missing or revoked, and no
// number of retries reaches it. A denial on reading ONE object out of a day that listed
// successfully is about that object: we demonstrably hold the bucket grant, so what we
// cannot reach is this object's own encryption key. Only the second may accumulate toward
// settling an object as permanently unreadable.
//
// Classifying by code alone put "AccessDenied" in the bucket case unconditionally, which
// left the settling apparatus unreachable for the very failure it was built for: S3
// answers a GetObject it cannot decrypt with a plain AccessDenied at least as often as
// with a KMS-prefixed code, so the object never accrued, never settled, and one bad file
// pinned the organisation at its day forever.
// classifyGrant is for every call whose failure is about the bucket or our grant on it:
// locating the bucket, listing it, and the CONNECT-time read probe. The probe belongs here
// rather than with the object classifier because at connect time a refused read is exactly
// the finding an administrator must act on — the policy grants listing but not reading —
// whereas in steady state that grant has already been proven.
func classifyGrant(err error) error {
	if isBucketFault(err) {
		return fmt.Errorf("%w: %w", connectorkiro.ErrBucketUnreachable, err)
	}
	return err
}

// classifyObject is for the steady-state read of one object out of a day that listed
// successfully under the same grant.
func classifyObject(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		// Properties of the object rather than of our access to the bucket, so no retry
		// and no reconfiguration reaches them.
		//
		// AccessDenied and Forbidden are here as well as in the bucket set, and that is
		// the whole correction: reaching this function means a listing under the same
		// grant had already succeeded.
		case "AccessDenied", "Forbidden",
			"KMS.AccessDeniedException", "KMS.DisabledException", "KMS.KMSInvalidStateException",
			"KMS.NotFoundException", "KMSAccessDenied", "InvalidKeyId.NotFound",
			// An archived object is restorable, so settling it is not literally permanent
			// — but the alternative is worse. Left unclassified it retries forever, and an
			// incomplete day holds the high-water mark, so one archived file stops the
			// organisation collecting anything newer. Settling keeps collection moving,
			// and ReportsAbandonedTotal counts the settled object.
			"InvalidObjectState":
			return fmt.Errorf("%w: %w", connectorkiro.ErrObjectUnreadable, err)
		}
	}
	// A bucket-shaped failure can still reach a read — the bucket deleted between the
	// listing and the fetch, say — so that set is still consulted, second.
	if isBucketFault(err) {
		return fmt.Errorf("%w: %w", connectorkiro.ErrBucketUnreachable, err)
	}
	return err
}

// isBucketFault reports whether a failure is about the bucket or our grant on it. It
// answers the question rather than returning an error, so each classifier states its own
// verdict and no caller passes a decided error through unwrapped.
func isBucketFault(err error) bool {
	var noBucket *types.NoSuchBucket
	if errors.As(err, &noBucket) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "Forbidden", "NoSuchBucket", "AllAccessDisabled", "InvalidAccessKeyId":
			return true
		}
	}
	return false
}

// --- the framework's HTTP-shaped interface, unimplemented by design ---

// Do is not applicable: this connector makes no HTTP calls.
func (g *Gateway) Do(context.Context, *http.Request) (*http.Response, error) { return nil, errNotHTTP }

// DoJSON is not applicable: this connector makes no HTTP calls.
func (g *Gateway) DoJSON(context.Context, string, string, any, any) error { return errNotHTTP }

// WithRateLimits is not applicable: there is no vendor request budget to scope.
func (g *Gateway) WithRateLimits([]sdk.RateLimitConfig) sdk.VendorGateway { return g }

// ForGroup is not applicable: there are no endpoint groups to scope to.
func (g *Gateway) ForGroup(string) sdk.VendorGateway { return g }

// Execute is not applicable: this connector exchanges no protocol-buffer messages.
func (g *Gateway) Execute(context.Context, proto.Message) (proto.Message, error) {
	return nil, errNotHTTP
}

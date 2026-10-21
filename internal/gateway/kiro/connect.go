// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"errors"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// bootstrapRegion is where the region lookup starts, not where the bucket is.
//
// Resolving a bucket's region needs a client, and a client needs a region, so something has
// to break the circle. Any region does: the lookup reads the location the service reports
// for the bucket, wherever it is asked from.
const bootstrapRegion = "us-east-1"

// DiscoverRegion reports the region a bucket actually lives in.
//
// It sends no Referer, by design: it is not a gateway for an organization,
// and it needs no grant. S3 names the bucket's region on its answer to the
// HEAD whether or not it allows the request, so under the bucket policy the
// connect flow generates, whose statements require TenantReferer, the HEAD is
// refused and the region is still read from it
// (TestDiscoverRegionWorksUnderTheConditionedPolicy). Only the listing and the
// reads that follow, through NewGatewayForOrganization, carry the Referer.
//
// Asked rather than typed, on purpose. The vendor requires the bucket's region to match the
// region of the Kiro profile writing into it, and the profile's region is embedded in the
// object keys — so the two can be compared, but only if this half is observed rather than
// supplied. A region taken from the administrator would make the comparison circular: a
// wrong value would build the very prefix the keys are then read from, and the
// two halves would always agree.
func DiscoverRegion(ctx context.Context, bucket string) (string, error) {
	cfg, err := awscreds.Load(ctx, awsconfig.WithRegion(bootstrapRegion))
	if err != nil {
		return "", fmt.Errorf("resolving credentials: %w", err)
	}
	region, err := manager.GetBucketRegion(ctx, awss3.NewFromConfig(cfg), bucket)
	if err != nil {
		return "", fmt.Errorf("locating bucket %s: %w", bucket, classifyGrant(err))
	}
	return region, nil
}

// ListPrefixes returns the names present at ONE level below a prefix, not the keys beneath
// it — a delimited listing.
//
// It returns one page, so it is sound only for a level whose entries cannot approach a
// thousand: years, months, and days within a month. The same call over a prefix of keys is
// the silent-truncation trap the per-day listing exists to avoid. Why the caller walks
// levels at all belongs to the caller — see walkKiroBoundaries.
func (g *Gateway) ListPrefixes(ctx context.Context, bucket, prefix string) ([]string, error) {
	delimiter := "/"
	out, err := g.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket:    &bucket,
		Prefix:    &prefix,
		Delimiter: &delimiter,
	})
	if err != nil {
		return nil, fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, classifyGrant(err))
	}
	names := make([]string, 0, len(out.CommonPrefixes))
	for _, p := range out.CommonPrefixes {
		if p.Prefix == nil {
			continue
		}
		// The listing returns whole prefixes; the caller wants the segment it did not
		// already know, so the parent and the trailing separator come off here rather
		// than at every call site.
		name := (*p.Prefix)[len(prefix):]
		names = append(names, trimTrailingSlash(name))
	}
	return names, nil
}

func trimTrailingSlash(s string) string {
	if len(s) > 0 && s[len(s)-1] == '/' {
		return s[:len(s)-1]
	}
	return s
}

// ProbeReadable proves the read grant without downloading the object.
//
// A plain read would pull the whole report into memory to answer a yes/no question, inside
// a request an administrator is waiting on — and an enterprise daily report is not small.
// One byte is enough: the permission is evaluated before any content is returned, so a
// missing s3:GetObject or an encryption key we may not use fails here exactly as it would
// on a full read. An object with no byte zero is handled below rather than refused.
func (g *Gateway) ProbeReadable(ctx context.Context, bucket, key string) error {
	byteRange := "bytes=0-0"
	out, err := g.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &bucket, Key: &key, Range: &byteRange,
	})
	if err != nil {
		// A zero-length object has no byte zero, so S3 answers the range with 416 rather
		// than the content. That is a PASS: authorization is evaluated before the range is
		// validated, so being told the range is unsatisfiable means the read was allowed.
		// Treating it as a failure refused the whole connection — and blamed the
		// administrator's policy — for a vendor that wrote an empty file on a quiet day.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "InvalidRange" {
			return nil
		}
		return fmt.Errorf("probing s3://%s/%s: %w", bucket, key, classifyGrant(err))
	}
	if cerr := out.Body.Close(); cerr != nil {
		return fmt.Errorf("closing the probe of s3://%s/%s: %w", bucket, key, cerr)
	}
	return nil
}

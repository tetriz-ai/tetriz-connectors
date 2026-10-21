// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tetriz-ai/tetriz-connectors/internal/awscreds"
	kirogateway "github.com/tetriz-ai/tetriz-connectors/internal/gateway/kiro"

	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
)

// kiroReadPolicy builds the bucket policy statement the customer adds to grant the runner's role read access.
//
// Why it is built server-side rather than in the browser is the read_policy field's own
// contract, stated once on ValidateKiroBucketResponse in the connector proto.
//
// What is worth saying here is the shape. Two resources, and both are needed: listing is a permission on the BUCKET while reading
// is a permission on the OBJECTS under it, so a policy naming only one of them produces
// exactly the half-working grant the single-object probe exists to catch. The object scope
// is narrowed to the prefix the administrator gave, so the grant reaches the reports and
// nothing else in the bucket.
//
// The listing permission is additionally conditioned on that prefix, because s3:ListBucket
// is otherwise bucket-wide however the resource is written — a condition is the only way to
// scope it, and without one the customer would be granting a broader read of their own
// bucket than connecting Kiro requires.
//
// Both statements also require the tenant marker the runner's gateway sends for
// orgID (aws:Referer, see kirogateway.TenantReferer). The connect flow's region
// lookup (kirogateway.DiscoverRegion) sends no marker and is refused under this
// policy; S3 still names the bucket's region on the refusal, which is all the
// lookup reads. The reader principal is shared
// by every tenant, so without the condition a bucket granted for one tenant
// could be connected and read by another; with it, the grant serves only the
// organization it was generated for.
func kiroReadPolicy(bucket, prefix, principal, orgID string) string {
	scope := strings.Trim(prefix, "/")
	tenant := map[string]any{"aws:Referer": []string{kirogateway.TenantReferer(orgID)}}
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{
				"Sid":       "TetrizConnectorsKiroReportsList",
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": principal},
				"Action":    []string{"s3:ListBucket"},
				"Resource":  fmt.Sprintf("arn:aws:s3:::%s", bucket),
				"Condition": map[string]any{
					"StringLike":   map[string]any{"s3:prefix": []string{scope + "/*"}},
					"StringEquals": tenant,
				},
			},
			map[string]any{
				"Sid":       "TetrizConnectorsKiroReportsRead",
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": principal},
				"Action":    []string{"s3:GetObject"},
				"Resource":  fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, scope),
				"Condition": map[string]any{"StringEquals": tenant},
			},
		},
	}
	// Indented because a human pastes this into a console, and an unformatted policy is
	// one an administrator cannot check before granting it.
	out, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		// Unreachable: the value above is a literal of marshalable types. Logged rather
		// than dropped because an empty policy on screen would otherwise look like a
		// rendering problem, and returned empty rather than panicking so a formatting
		// fault cannot take down a request an administrator is waiting on.
		logger.ErrorNoCtx(fmt.Sprintf("kiro read policy failed to marshal: %v", err))
		return ""
	}
	return string(out)
}

// kiroReaderPrincipal is the identity a customer's bucket policy must name, discovered
// from the credentials this process actually holds rather than configured.
//
// Discovered because a configured value is a value that can drift: the policy an
// administrator pastes has to name the principal the scheduled collection will really read
// as, and nothing but asking guarantees those are the same. A stale configured ARN
// produces a grant that validates by hand and fails on the first run.
func (s *Service) kiroReaderPrincipal(ctx context.Context) (string, error) {
	resolve := s.kiroPrincipalOf
	if resolve == nil {
		resolve = DiscoverKiroReaderPrincipal
	}
	principal, err := resolve(ctx)
	if err != nil {
		return "", fmt.Errorf("asking who we are: %w", err)
	}
	return principal, nil
}

// DiscoverKiroReaderPrincipal is the default KiroPrincipalResolver: it asks the
// credentials chain who this process is.
func DiscoverKiroReaderPrincipal(ctx context.Context) (string, error) {
	cfg, err := awscreds.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving our own credentials: %w", err)
	}
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("asking who we are: %w", err)
	}
	if out.Arn == nil {
		return "", fmt.Errorf("our own identity carries no arn")
	}
	return kiroRolePrincipal(*out.Arn), nil
}

// kiroRolePrincipal turns the identity a running process reports into the one a bucket
// policy can name.
//
// A process using a role reports an assumed-role ARN carrying a session name, and that
// session is transient — a policy naming it would stop matching the moment the session
// rotated, which is a grant that works when pasted and silently fails later. The role
// behind it is the durable identity, so it is the one named. Any other shape is already
// durable and passes through untouched.
func kiroRolePrincipal(arn string) string {
	const marker = ":assumed-role/"
	i := strings.Index(arn, marker)
	if i < 0 {
		return arn
	}
	account := arn[:i]
	account = strings.TrimPrefix(account, "arn:aws:sts:")
	account = strings.Trim(account, ":")
	role, _, _ := strings.Cut(arn[i+len(marker):], "/")
	if account == "" || role == "" {
		return arn
	}
	return fmt.Sprintf("arn:aws:iam::%s:role/%s", account, role)
}

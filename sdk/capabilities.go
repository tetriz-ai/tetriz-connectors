// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
)

// TenantKind is the host's intent for a tenant. Its values match the
// TenantType enum in the repository's common protos, so a host converts one
// to the other with a plain cast.
type TenantKind int32

const (
	// TenantKindUnspecified means the host could not resolve the intent.
	TenantKindUnspecified TenantKind = 0
	// TenantKindOrganization is a multi-user organization.
	TenantKindOrganization TenantKind = 1
	// TenantKindIndividual is a single-user account.
	TenantKindIndividual TenantKind = 2
)

var tenantKindNames = map[TenantKind]string{
	TenantKindUnspecified:  "TENANT_TYPE_UNSPECIFIED",
	TenantKindOrganization: "TENANT_TYPE_ORGANIZATION",
	TenantKindIndividual:   "TENANT_TYPE_INDIVIDUAL",
}

// String returns the value's name in the common protos, or the number for an
// unknown value.
func (k TenantKind) String() string {
	if n, ok := tenantKindNames[k]; ok {
		return n
	}
	return strconv.Itoa(int(k))
}

// MarshalJSON writes the value's name, the form job payloads carry.
func (k TenantKind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// UnmarshalJSON reads a value name. An unknown name reads as
// TenantKindUnspecified; a JSON value that is not a string is an error.
func (k *TenantKind) UnmarshalJSON(b []byte) error {
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return err //nolint:wrapcheck // the decoder's error is the whole message
	}
	*k = TenantKindUnspecified
	for v, n := range tenantKindNames {
		if n == name {
			*k = v
		}
	}
	return nil
}

// BlobWriteOpts are optional attributes of a blob write.
type BlobWriteOpts struct {
	ContentType string
	ACL         string
	// ChecksumSHA256 is the base64 SHA-256 of the content. A store that
	// supports it verifies the body against it on write. Empty means none.
	ChecksumSHA256 string
}

// BlobStore stores the large bodies some operations write beside their
// records: diffs, descriptions, repository trees, message archives. Keys are
// paths within the host's bucket or directory.
type BlobStore interface {
	Write(ctx context.Context, key string, content []byte, acl string) error
	WriteWithOpts(ctx context.Context, key string, content []byte, opts *BlobWriteOpts) error
	// Delete removes the blob under key. Deleting a missing key is not an
	// error.
	Delete(ctx context.Context, key string) error
}

// RateBucket names the vendor rate-limit bucket a caller charges against.
// Vendors without separate buckets use the empty bucket.
type RateBucket string

// BudgetObserver is a header-aware view of a vendor's own rate-limit budget,
// shared across workers. Allow reports whether the remaining budget in a
// bucket is above the caller's floor, and how long to wait when it is not.
type BudgetObserver interface {
	Allow(ctx context.Context, orgID string, bucket RateBucket, floor int) (bool, time.Duration, error)
	UpdateFromResponse(ctx context.Context, orgID string, h http.Header) error
	GetLimit(ctx context.Context, orgID string, bucket RateBucket) (limit int, observedAt time.Time, ok bool, err error)
}

// WorkQueue is the host-owned work list that the diff and file-content
// operations drain. The host decides what needs fetching; the operation
// fetches, writes the body to the BlobStore and reports back.
type WorkQueue interface {
	ListPRsPendingDiffFetch(ctx context.Context, req *workqueuev1.ListPRsPendingDiffFetchRequest) (*workqueuev1.ListPRsPendingDiffFetchResponse, error)
	UpdatePRDiffStatus(ctx context.Context, req *workqueuev1.UpdatePRDiffStatusRequest) (*workqueuev1.UpdatePRDiffStatusResponse, error)
	ListPendingFileFetches(ctx context.Context, req *workqueuev1.ListPendingFileFetchesRequest) (*workqueuev1.ListPendingFileFetchesResponse, error)
}

// TrackedRefsResolver resolves which branches of each repository count for
// an organization.
type TrackedRefsResolver interface {
	ResolveTrackedRefs(ctx context.Context, req *workqueuev1.ResolveTrackedRefsRequest) (*workqueuev1.ResolveTrackedRefsResponse, error)
}

// TenantDirectory resolves an organization's tenant intent. An implementation
// returns TenantKindUnspecified with a nil error for an organization whose
// intent was never recorded. The schedule source schedules such an
// organization as an organization, but the connect flows in auth/integrations
// and a GitHub App unsuspend do not enable a connector for it: only
// TenantKindOrganization lands enabled, so an unrecorded intent waits for the
// tenant to confirm its repositories. An error means the lookup failed and
// nothing was learned.
type TenantDirectory interface {
	TenantType(ctx context.Context, orgID string) (TenantKind, error)
}

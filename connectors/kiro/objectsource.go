// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package kiro

import (
	"context"
	"errors"
	"time"
)

// ErrBucketUnreachable marks the two failures that mean the stored configuration no
// longer describes reality: a bucket that does not exist, and a read grant that has been
// revoked. Neither is transient and no retry fixes either.
//
// It exists so the operation can classify them as a configuration fault. Without it every
// day-level failure reaches the orchestrator as a generic error and is recorded in the
// internal class, where a failure rate says little about a once-a-day operation even
// in total outage. A revoked grant is the failure most likely to happen quietly months after
// onboarding, so it must land in the configuration-fault class.
var ErrBucketUnreachable = errors.New("the customer's bucket cannot be reached with the stored configuration")

// ErrObjectUnreadable marks an object that cannot be read because of something about the
// object itself: an encryption key we may not use, or a storage class that needs
// restoring first. These are the only failures that may accumulate toward an object being
// settled as permanently unreadable.
//
// The distinction is load-bearing and the reason is asymmetric cost. Counting a transient
// failure toward terminal means a multi-run outage settles objects as permanently
// unreadable, and a settled object is never attempted again — so its reports never
// arrive, silently, long after the outage ended. That is worse than the retry-forever
// behaviour terminal state exists to prevent, because retrying forever is at least
// visible in the failure metrics.
var ErrObjectUnreadable = errors.New("the object itself cannot be read")

// SourceObject is one object in the customer's bucket, as listing reports it.
type SourceObject struct {
	Key string
	// LastModified is set by the object store, and it is the tie-break when a day's
	// prefix holds more than one object for a client type.
	//
	// Ordering the vendor's filenames would have been the obvious choice and is quietly
	// wrong: it holds only if the publication timestamp is fixed-width and zero-padded,
	// which nothing documents, and comparable delivery pipelines append non-monotonic
	// suffixes. It would also make the filename shape a fourth thing depending on a
	// layout nobody has observed. Last-modified needs no parsing and is monotonic
	// whatever the vendor names its files.
	LastModified time.Time
	Size         int64
}

// ObjectSource is the narrow capability this connector actually needs from its gateway.
//
// The framework's vendor gateway is shaped for HTTP — request execution, a JSON
// convenience wrapper, rate-limit scoping, a protocol-buffer call — and none of those
// describe fetching an object from a bucket. This connector makes no HTTP call at all.
//
// It nonetheless needs a gateway, because the orchestrator builds one per execution and
// treats a construction failure as a configuration fault. That requirement is not incidental:
// the factory is where per-organisation credentials are already resolved for every other
// connector, so it is the right seam. The gateway therefore satisfies the framework's
// interface and carries this one alongside it, and the operation asserts for this.
//
// Declared here, in the connector package, because the consumer should own the capability
// it depends on: the gateway is then free to grow methods without widening this contract,
// and one that stops providing the capability fails at this assertion rather than somewhere
// inside a run. It does not remove the dependency edge — the gateway imports this package
// for the assertion and for SourceObject, as a gateway returning these types must.
type ObjectSource interface {
	// ListDay returns the objects under a single day's prefix.
	//
	// Always one day, never wider: the shared object-store helpers issue a single list
	// call with no continuation token, so a broader prefix truncates at a thousand keys
	// and reports nothing about having done so. A day holds at most one object per
	// client type.
	ListDay(ctx context.Context, bucket, prefix string) ([]SourceObject, error)

	// Get returns one object's bytes.
	Get(ctx context.Context, bucket, key string) ([]byte, error)
}

// MaxReportBytes caps a single report, both on the listing's Size and on the body read.
//
// The bucket belongs to the customer and a report is read into memory whole — the caller
// then hashes and re-uploads the same buffer — so one object larger than the process has
// room for takes the connector down for every organisation it was serving. Nothing about a
// mistyped lifecycle rule or a misdirected export in someone else's bucket should be able
// to do that.
//
// A day's report for one client type is a few hundred kilobytes at enterprise scale, so
// this is three orders of magnitude of headroom: wide enough that a real report is never
// refused, narrow enough that one object cannot exhaust the process.
const MaxReportBytes int64 = 256 << 20 // 256 MiB

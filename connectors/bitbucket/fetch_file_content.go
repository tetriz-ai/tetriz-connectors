// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package bitbucket

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/tetriz-connectors/sdk/obs"

	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetriz-ai/tetriz-connectors/gateway/ratelimiter"
	workqueuev1 "github.com/tetriz-ai/tetriz-connectors/gen/workqueue/v1"
	"github.com/tetriz-ai/tetriz-connectors/internal/constants"
	"github.com/tetriz-ai/tetriz-connectors/internal/logger"
	"github.com/tetriz-ai/tetriz-connectors/internal/vendorids"
	"github.com/tetriz-ai/tetriz-connectors/sdk"
)

// opFetchFileContent is the generic per-file content pull that feeds a
// consumer's copy of AI coding-agent configuration files (skills, rules, hooks,
// CLAUDE.md, AGENTS.md and the like; "harness artifacts" in the protos). It
// mirrors fetch_pr_diffs (reverse work-list, no connector-side change detection)
// rather than the cursor-based tree ops.
const opFetchFileContent = vendorids.OpFetchFileContent

// maxFileContentBytes is the cap for a stored file body (1 MB). It must equal
// the cap the work queue's consumer applies: a consumer that expects more treats
// the stored prefix as truncated and re-requests it every run, so the two
// change only together. Duplicated per vendor package (the github package pins the same
// value) rather than shared, matching the per-vendor duplication of the fetch ops.
const maxFileContentBytes = 1 << 20

// fileContentWorkListLimit caps how many whole artifacts one run pulls. Kept an
// in-code constant because the scheduled execution path registers connectors with
// nil config, so an Execute-time knob cannot be threaded through the connector
// struct. Bounded ≤ 500 by the RPC contract — already at that ceiling, so it
// cannot be raised further without widening the proto limit; the daily cadence
// plus the whole-repo tree_sha skip keep steady-state volume well under it.
const fileContentWorkListLimit = 500

// perFileFetchTimeout bounds a single file fetch so one stuck request cannot
// starve the run, matching fetch_pr_diffs' per-item deadline.
const perFileFetchTimeout = 30 * time.Second

// errFileContentNotAvailable signals a 404 on a file fetch — the file is gone
// from the pinned ref. It defers the WHOLE artifact (never a partial record), so
// the work-list re-offers it next run once the tree and manifest reconcile.
var errFileContentNotAvailable = errors.New("file content not available")

// fetchFileContentRequiresHost lists the host services the operation cannot run without.
var fetchFileContentRequiresHost = []sdk.HostService{sdk.HostWorkQueue}

// FetchFileContentOp pulls the changed files of each pending AI coding-agent
// configuration item (an artifact) at
// its pinned tree commit, stages each body in S3 under a record-unique key, and
// emits one per-artifact FileContentRecord for a consumer to persist.
//
// The connector holds NO change-detection state — the cursor is pass-through.
// The work queue's ListPendingFileFetches owns "what changed"/"what to retry", so the
// op cannot drift from the server's view (the fetch_pr_diffs model).
type FetchFileContentOp struct{}

// OperationID returns the operation identifier used by the registry.
func (op *FetchFileContentOp) OperationID() sdk.OperationID { return opFetchFileContent }

// Execute pulls one work-list batch. It fails, terminal, without the host's
// work queue, a blob store or a budget limiter. A gRPC error from the work-list RPC is a HARD failure
// (returned, so asynq retries); every per-artifact error only defers that
// artifact and never fails the run. The cursor is returned unchanged on every path.
func (op *FetchFileContentOp) Execute(parentCtx context.Context, params *sdk.ExecutionParams) (*sdk.Cursor, error) {
	orgID := params.Config.OrganizationID

	if missing := params.Host.Missing(fetchFileContentRequiresHost); len(missing) > 0 {
		return nil, sdk.MissingHostServicesError(connectorID, op.OperationID(), orgID, missing)
	}
	if params.BlobStore == nil || params.BudgetLimiter == nil {
		return nil, &sdk.SyncError{
			ConnectorID: connectorID, OperationID: op.OperationID(), OrganizationID: orgID,
			Kind: sdk.ErrKindConfigInvalid, Terminal: true,
			Cause: fmt.Errorf("fetch_file_content needs a blob store and a budget limiter (blob store set: %t, budget limiter set: %t)",
				params.BlobStore != nil, params.BudgetLimiter != nil),
		}
	}

	resp, err := params.Host.WorkQueue.ListPendingFileFetches(parentCtx, &workqueuev1.ListPendingFileFetchesRequest{
		OrganizationId: orgID,
		ConnectorId:    string(connectorID),
		Limit:          fileContentWorkListLimit,
	})
	if err != nil {
		logger.Error(parentCtx, "listing pending file fetches",
			zap.String(logger.OrgID, orgID),
			zap.String(logger.ConnectorID, string(connectorID)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("listing pending file fetches (org %s): %w", orgID, err)
	}

	records := resp.GetRecords()
	if len(records) == 0 {
		return params.Cursor, nil
	}

	gw := params.VendorGateway.ForGroup(constants.GroupBitbucketREST)
	for _, record := range records {
		op.processArtifact(parentCtx, gw, params.BlobStore, params.BudgetLimiter, params.Sink, orgID, record)
	}

	return params.Cursor, nil
}

// processArtifact reconciles one artifact. A tombstone is re-emitted as-is (no
// fetch). Otherwise the whole artifact's budget is reserved up front (atomic
// unit), every changed file is fetched at the pinned ref and staged, and exactly
// one filled record is emitted. Any file failure — 404 or transient — defers the
// WHOLE artifact (emits nothing, not even fetched siblings), so a partial manifest
// never exposes an unloadable skill.
func (op *FetchFileContentOp) processArtifact(
	parentCtx context.Context,
	gw sdk.VendorGateway,
	s3Client stagingWriter,
	budget sdk.BudgetObserver,
	sink sdk.DataSink,
	orgID string,
	record *workqueuev1.FileContentRecord,
) {
	if record.GetIsTombstone() {
		if err := op.emit(parentCtx, sink, orgID, record); err != nil {
			logger.Error(parentCtx, "fetch_file_content: emitting tombstone record",
				zap.String(logger.OrgID, orgID),
				zap.String("source_path", record.GetSourcePath()),
				zap.Error(err),
			)
			obs.RecordFileContentDeferredEmitErr(string(connectorID))
			return
		}
		obs.RecordFileContentTombstone(string(connectorID))
		return
	}

	workspace, repoSlug, ok := splitRepoFullName(record.GetRepoFullName())
	if !ok {
		logger.Error(parentCtx, "fetch_file_content: invalid repo full name — deferring artifact",
			zap.String(logger.OrgID, orgID),
			zap.String("repo", record.GetRepoFullName()),
			zap.String("source_path", record.GetSourcePath()),
		)
		obs.RecordFileContentDeferredBadRepo(string(connectorID))
		return
	}

	// A removal-only record (helper paths deleted, no files to fetch) makes zero vendor
	// requests and zero staging writes, so it must bypass the rate-limit gate entirely.
	// Gating it behind a depleted bucket — as the inflated floor would, since GetFiles()
	// is empty — strands the helper deletion indefinitely even though applying it costs
	// no API budget. Only reserve when there is at least one file to fetch.
	if len(record.GetFiles()) > 0 {
		// Reserve the whole artifact's budget BEFORE any fetch. The inflated floor
		// (base floor + this artifact's file count) admits only when every file fits
		// above the base reserve, so a large skill is deferred whole rather than
		// fetching a prefix and re-fetching the same prefix every run. The
		// inflated floor also makes fetch_file_content the lowest-priority op on the
		// shared bucket, so it yields to the higher-cadence PR ops under contention.
		floor := ratelimiter.BitbucketCoreFloor + len(record.GetFiles())
		admitted, wait, err := budget.Allow(parentCtx, orgID, ratelimiter.BucketDefault, floor)
		if err != nil {
			logger.Error(parentCtx, "fetch_file_content: budget check failed — deferring artifact",
				zap.String(logger.OrgID, orgID),
				zap.String("source_path", record.GetSourcePath()),
				zap.Error(err),
			)
			obs.RecordFileContentDeferredBudgetErr(string(connectorID))
			return
		}
		if !admitted {
			logger.Info(parentCtx, "fetch_file_content: budget floor hit — deferring artifact to next run",
				zap.String(logger.OrgID, orgID),
				zap.String("source_path", record.GetSourcePath()),
				zap.Int("file_count", len(record.GetFiles())),
				zap.Duration("reset_in", wait),
			)
			obs.RecordFileContentDeferredBudget(string(connectorID))
			return
		}
	}

	sourceDir := path.Dir(record.GetSourcePath())
	stagedKeys := make([]string, 0, len(record.GetFiles()))
	for _, file := range record.GetFiles() {
		repoPath := path.Join(sourceDir, file.GetRelativePath())

		// Self-defense against a server-supplied path escaping the harness-staging/{org}/ (staging key)
		// prefix. Git-tree paths cannot contain a ".." segment today, so a resolved path
		// that does is malformed input, not a real file — defer the whole artifact.
		if hasDotDotSegment(repoPath) {
			logger.Error(parentCtx, "fetch_file_content: path escapes staging prefix — deferring whole artifact",
				zap.String(logger.OrgID, orgID),
				zap.String("source_path", record.GetSourcePath()),
				zap.String("repo_path", repoPath),
			)
			obs.RecordFileContentDeferredPathEscape(string(connectorID))
			op.cleanupStagedBodies(parentCtx, s3Client, orgID, stagedKeys)
			return
		}

		ctx, cancel := context.WithTimeout(parentCtx, perFileFetchTimeout)
		storedBytes, truncated, originalBytes, header, fetchErr := fetchBitbucketFileContent(
			ctx, gw, orgID, workspace, repoSlug, record.GetRef(), repoPath)
		if header != nil {
			if refreshErr := budget.UpdateFromResponse(ctx, orgID, header); refreshErr != nil {
				logger.Error(ctx, "fetch_file_content: budget refresh from response headers failed",
					zap.String(logger.OrgID, orgID),
					zap.Error(refreshErr),
				)
			}
		}
		cancel()

		if fetchErr != nil {
			// Any file failure defers the whole artifact — emit nothing. Split the outcome
			// so a gone-from-ref file (404, self-heals once the tree reconciles) is legible
			// apart from a transient/unexpected fetch error worth investigating.
			notFound := errors.Is(fetchErr, errFileContentNotAvailable)
			logger.Info(parentCtx, "fetch_file_content: file fetch failed — deferring whole artifact",
				zap.String(logger.OrgID, orgID),
				zap.String("repo", record.GetRepoFullName()),
				zap.String("source_path", record.GetSourcePath()),
				zap.String("repo_path", repoPath),
				zap.Bool("not_found", notFound),
				zap.Error(fetchErr),
			)
			if notFound {
				obs.RecordFileContentDeferredNotFound(string(connectorID))
			} else {
				obs.RecordFileContentDeferredFetchError(string(connectorID))
			}
			op.cleanupStagedBodies(parentCtx, s3Client, orgID, stagedKeys)
			return
		}

		digest := sha256.Sum256(storedBytes)
		contentHash := hex.EncodeToString(digest[:])
		stagingKey := op.stagingKey(orgID, record, repoPath)

		// base64 of the RAW digest is the AWS checksum format; a consumer's Stat
		// verifies the promoted body against it before trusting the bytes.
		if writeErr := s3Client.WriteWithOpts(parentCtx, stagingKey, storedBytes, &sdk.BlobWriteOpts{
			ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
		}); writeErr != nil {
			logger.Info(parentCtx, "fetch_file_content: staging write failed — deferring whole artifact",
				zap.String(logger.OrgID, orgID),
				zap.String("source_path", record.GetSourcePath()),
				zap.String("s3_key", stagingKey),
				zap.Error(writeErr),
			)
			obs.RecordFileContentDeferredStagingWrite(string(connectorID))
			op.cleanupStagedBodies(parentCtx, s3Client, orgID, stagedKeys)
			return
		}
		stagedKeys = append(stagedKeys, stagingKey)

		file.ContentHash = contentHash
		file.S3Key = stagingKey
		file.Truncated = truncated
		file.OriginalBytes = originalBytes
		obs.RecordFileContentFetched(string(connectorID))
	}

	if err := op.emit(parentCtx, sink, orgID, record); err != nil {
		logger.Error(parentCtx, "fetch_file_content: emitting record",
			zap.String(logger.OrgID, orgID),
			zap.String("source_path", record.GetSourcePath()),
			zap.Error(err),
		)
		obs.RecordFileContentDeferredEmitErr(string(connectorID))
		op.cleanupStagedBodies(parentCtx, s3Client, orgID, stagedKeys)
		return
	}
	obs.RecordFileContentEmitted(string(connectorID))
}

// cleanupStagedBodies best-effort deletes the staging objects already written for an
// artifact whose reconciliation aborts before it emits. Staging keys are record-unique
// (never the content-addressed canonical key a promoted body lives under), so deleting
// them can never touch another record's data or an already-promoted body. A delete
// failure is logged, not retried: the leftover object is a small, bounded residual.
func (op *FetchFileContentOp) cleanupStagedBodies(ctx context.Context, s3Client stagingWriter, orgID string, keys []string) {
	for _, key := range keys {
		if err := s3Client.Delete(ctx, key); err != nil {
			logger.WarnWithCtx(ctx, "fetch_file_content: staging cleanup delete failed on abort",
				zap.String(logger.OrgID, orgID),
				zap.String("s3_key", key),
				zap.Error(err),
			)
		}
	}
}

// hasDotDotSegment reports whether a slash-separated path has a ".." element — a
// traversal segment that could push a staging key outside its org prefix.
func hasDotDotSegment(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

// stagingKey is the record-unique S3 key a file body is staged under. It is under
// the DISTINCT harness-staging/ prefix (outside the consumer's canonical namespace),
// so a consumer deletes staging after promotion and the connector
// deletes any bodies it staged if the artifact aborts before emit; a hard crash
// mid-artifact leaves at most that one artifact's staging objects orphaned — a small,
// bounded residual. The repo path (source-dir joined with the manifest-relative
// path) is unique per file in a tree, and the generation pins the tree, so the key
// is unique per {repository_id, generation, file}.
func (op *FetchFileContentOp) stagingKey(orgID string, record *workqueuev1.FileContentRecord, repoPath string) string {
	return path.Join(
		"harness-staging",
		orgID,
		string(connectorID),
		record.GetRepositoryId(),
		strconv.FormatInt(record.GetGeneration(), 10),
		repoPath,
	)
}

// emit publishes one per-artifact FileContentRecord. The ExternalID spans the full
// artifact identity (repository_id, ide_type, source_path) plus the generation:
// ide_type is part of the identity, so two artifacts that share repo+source_path but
// differ by ide_type must not collapse under the downstream dedup key, and the generation
// keeps two same-day generations distinct before a consumer's generation guard runs.
// DataDate is left zero so the consumer stamps data_date from FetchedAt (today) —
// matching fetch_repo_tree, so a same-day consumer pass reads these records on the day
// they are fetched.
func (op *FetchFileContentOp) emit(ctx context.Context, sink sdk.DataSink, orgID string, record *workqueuev1.FileContentRecord) error {
	payload, err := protojson.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshaling FileContentRecord (source %s): %w", record.GetSourcePath(), err)
	}
	externalID := fmt.Sprintf("%s#%d#%s#%d", record.GetRepositoryId(), record.GetIdeType(), record.GetSourcePath(), record.GetGeneration())
	metadata := map[string]string{"repo_full_name": record.GetRepoFullName()}
	if err := emitRecord(ctx, sink, opFetchFileContent, orgID, externalID, payload, metadata, time.Time{}); err != nil {
		return fmt.Errorf("emitting file-content record (source %s): %w", record.GetSourcePath(), err)
	}
	return nil
}

// bitbucketFileContentURL builds the raw-content URL for a file at a commit via the
// Bitbucket src/ API. The commit hash and each path segment are PathEscape'd
// individually so the "/" separators stay literal (mirroring buildSrcURL); a file
// path returns the raw bytes directly (no `format` param, no trailing slash — that
// selector is directory-only).
func bitbucketFileContentURL(orgID, workspace, repoSlug, commitHash, filePath string) string {
	apiURL := fmt.Sprintf("%s/repositories/%s/%s/src/%s",
		bitbucketRESTBase(orgID), workspace, repoSlug, url.PathEscape(commitHash))
	for _, segment := range strings.Split(filePath, "/") {
		if segment != "" {
			apiURL += "/" + url.PathEscape(segment)
		}
	}
	return apiURL
}

// fetchBitbucketFileContent fetches one file at the pinned commit in a SINGLE bounded
// call and returns the bytes to STORE (capped at maxFileContentBytes), whether the
// body was truncated, its ORIGINAL untruncated size, and the response header (for
// budget draw-down). A 404 returns errFileContentNotAvailable so the caller defers
// the whole artifact; any other non-2xx status is a plain error. original_bytes comes
// from Content-Length; if absent it is floored to the bytes read.
func fetchBitbucketFileContent(ctx context.Context, gw sdk.VendorGateway, orgID, workspace, repoSlug, commitHash, filePath string) (stored []byte, truncated bool, originalBytes int64, header http.Header, err error) {
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, bitbucketFileContentURL(orgID, workspace, repoSlug, commitHash, filePath), nil)
	if reqErr != nil {
		return nil, false, 0, nil, fmt.Errorf("building raw-content request for %q: %w", filePath, reqErr)
	}

	resp, doErr := gw.Do(ctx, req)
	if doErr != nil {
		return nil, false, 0, nil, fmt.Errorf("executing raw-content request for %q: %w", filePath, doErr)
	}
	respHeader := resp.Header
	defer resp.Body.Close() //nolint:errcheck // best-effort close on read-only content fetch

	switch resp.StatusCode {
	case http.StatusOK:
		// Read one byte past the cap: cap+1 bytes read means the file exceeds the cap
		// and is stored truncated; anything ≤ cap is the whole file. LimitReader bounds
		// memory regardless of how large the file actually is.
		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxFileContentBytes+1))
		if readErr != nil {
			return nil, false, 0, respHeader, fmt.Errorf("reading raw-content body for %q: %w", filePath, readErr)
		}
		st, tr, orig := boundFileContent(ctx, raw, resp.ContentLength, filePath)
		return st, tr, orig, respHeader, nil
	case http.StatusNotFound:
		return nil, false, 0, respHeader, errFileContentNotAvailable
	default:
		return nil, false, 0, respHeader, fmt.Errorf("unexpected status %d fetching content for %q", resp.StatusCode, filePath)
	}
}

// boundFileContent turns a cap+1-bounded read of the raw body into the stored bytes,
// the truncated flag, and the original (untruncated) size. raw holds at most
// maxFileContentBytes+1 bytes; reaching cap+1 means the file was larger than the cap.
// content_hash (computed by the caller) covers the STORED bytes. When truncated,
// original_bytes prefers the response Content-Length (the full size); if that is
// absent (-1) or not larger than the cap it is floored to the bytes read (which
// exceed the cap), so the proto's truncated⟹original_bytes>0 invariant always holds.
func boundFileContent(ctx context.Context, raw []byte, contentLength int64, filePath string) (stored []byte, truncated bool, originalBytes int64) {
	if int64(len(raw)) <= maxFileContentBytes {
		return raw, false, int64(len(raw))
	}
	stored = raw[:maxFileContentBytes]
	if contentLength > maxFileContentBytes {
		return stored, true, contentLength
	}
	logger.WarnWithCtx(ctx, "fetch_file_content: truncated body without a credible Content-Length; flooring original_bytes to bytes read",
		zap.String("repo_path", filePath),
		zap.Int64("content_length", contentLength),
	)
	return stored, true, int64(len(raw))
}

// stagingWriter is the narrow checksum-capable S3 surface this op needs. It matches
// s3.S3Client.WriteWithOpts so tests can stub the staging write — and assert the
// SHA-256 checksum — without the full AWS SDK surface.
type stagingWriter interface {
	WriteWithOpts(ctx context.Context, dst string, content []byte, opts *sdk.BlobWriteOpts) error
	Delete(ctx context.Context, dst string) error
}

// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tetriz-ai/tetriz-connectors/internal/metricutil"
)

// Row is a single sim_records row.
type Row struct {
	OrgName    string
	EntityType string
	Payload    []byte
	CursorVal  time.Time
}

// IssueRow is a Row plus the sim_records primary-key id, surfaced as the
// keyset tiebreak for the Jira ascending search. id is a UUID rendered as text:
// it is opaque (only compared, never parsed) so a caller can build a stable
// (cursor_val, id) resume token.
type IssueRow struct {
	Row
	ID string
}

// SimDAO reads from sim_records using a pgxpool connection pool.
type SimDAO struct {
	pool *pgxpool.Pool
}

// New creates a SimDAO backed by the given pool.
func New(pool *pgxpool.Pool) *SimDAO {
	return &SimDAO{pool: pool}
}

// ListPaged returns rows for (vendor, org, entityType), optionally filtered by
// repo name and cursor. Rows are ordered newest-first. Caller passes limit+1 to
// detect hasNextPage; limit=0 returns all rows.
func (d *SimDAO) ListPaged(ctx context.Context, vendor, org, entityType, repo string, before *time.Time, limit int) ([]Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListPaged", time.Now())
	args := pgx.NamedArgs{}
	where := "vendor = @vendor AND org_name = @org AND entity_type = @entityType"
	args["vendor"] = vendor
	args["org"] = org
	args["entityType"] = entityType

	if repo != "" {
		where += " AND payload->>'repo' = @repo"
		args["repo"] = repo
	}
	if before != nil {
		where += " AND cursor_val < @before"
		args["before"] = *before
	}

	q := "SELECT org_name, entity_type, payload, cursor_val FROM sim_records WHERE " + where + " ORDER BY cursor_val DESC"
	if limit > 0 {
		q += " LIMIT @limit"
		args["limit"] = limit
	}

	rows, err := d.pool.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("sim_records list paged: %w", err)
	}
	defer rows.Close()
	result, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records list paged: %w", err)
	}
	return result, nil
}

// ListAll returns all rows for (vendor, org, entityType) without cursor filtering,
// in a total order. The id tiebreak is load-bearing rather than cosmetic: the static
// entities (projects, sprints, repos, members) are all written with one shared
// cursor_val, so ordering on cursor_val alone leaves every row of a kind tied and
// PostgreSQL free to return them in any order. Callers window these by startAt, and
// an unstable order makes a multi-page walk repeat one row and skip another.
func (d *SimDAO) ListAll(ctx context.Context, vendor, org, entityType string) ([]Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListAll", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = $3
	      ORDER BY cursor_val DESC, id`
	rows, err := d.pool.Query(ctx, q, vendor, org, entityType)
	if err != nil {
		return nil, fmt.Errorf("sim_records list all: %w", err)
	}
	defer rows.Close()
	result, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records list all: %w", err)
	}
	return result, nil
}

// ListOrgs returns the names of the vendor's organizations that hold at least
// one repository, sorted.
func (d *SimDAO) ListOrgs(ctx context.Context, vendor string) ([]string, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListOrgs", time.Now())
	q := `SELECT DISTINCT org_name
	      FROM sim_records
	      WHERE vendor = $1 AND entity_type = 'repo'
	      ORDER BY org_name`
	rows, err := d.pool.Query(ctx, q, vendor)
	if err != nil {
		return nil, fmt.Errorf("sim_records list orgs: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("sim_records list orgs: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sim_records list orgs: %w", err)
	}
	return out, nil
}

// OrgExists reports whether the store holds any record of the vendor's
// organization. A vendor answers an organization (or Jira site) it does not
// know with 404; the handlers use this to do the same.
func (d *SimDAO) OrgExists(ctx context.Context, vendor, org string) (bool, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "OrgExists", time.Now())
	var exists bool
	if err := d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sim_records WHERE vendor = $1 AND org_name = $2)`, vendor, org).Scan(&exists); err != nil {
		return false, fmt.Errorf("sim_records org exists: %w", err)
	}
	return exists, nil
}

// ListDistinctRepos returns distinct repo names found in PR row payloads.
func (d *SimDAO) ListDistinctRepos(ctx context.Context, vendor, org string) ([]string, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListDistinctRepos", time.Now())
	q := `SELECT DISTINCT payload->>'repo'
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'pr'
	        AND payload->>'repo' IS NOT NULL`
	rows, err := d.pool.Query(ctx, q, vendor, org)
	if err != nil {
		return nil, fmt.Errorf("sim_records list distinct repos: %w", err)
	}
	defer rows.Close()

	var repos []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, fmt.Errorf("scan repo name: %w", err)
		}
		repos = append(repos, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sim_records list distinct repos: %w", err)
	}
	return repos, nil
}

// GetByPRNumber returns the PR row matching (vendor, org, repo, prNumber), or nil if not found.
func (d *SimDAO) GetByPRNumber(ctx context.Context, vendor, org, repo string, number int) (*Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "GetByPRNumber", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'pr'
	        AND payload->>'repo' = $3
	        AND (payload->>'number')::int = $4
	      LIMIT 1`
	rows, err := d.pool.Query(ctx, q, vendor, org, repo, number)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by pr number: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by pr number: %w", err)
	}
	if len(results) == 0 {
		return nil, nil //nolint:nilnil // documented optional-lookup contract: no error occurred, the PR just doesn't exist; every caller branches on the returned *Row being nil, never conflating it with err
	}
	return &results[0], nil
}

// ListPageWindow returns one page of rows for (vendor, org, entityType), optionally
// filtered by repo, ordered newest-first with an id tiebreak so page windows stay
// stable across a paging sequence. Serves Bitbucket's page-number pagination (limit =
// pagelen, offset = (page-1)*pagelen); GitHub uses keyset ListPaged instead.
func (d *SimDAO) ListPageWindow(ctx context.Context, vendor, org, entityType, repo string, limit, offset int) ([]Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListPageWindow", time.Now())
	args := pgx.NamedArgs{"vendor": vendor, "org": org, "entityType": entityType}
	where := "vendor = @vendor AND org_name = @org AND entity_type = @entityType"
	if repo != "" {
		where += " AND payload->>'repo' = @repo"
		args["repo"] = repo
	}
	args["limit"] = limit
	args["offset"] = offset
	q := "SELECT org_name, entity_type, payload, cursor_val FROM sim_records WHERE " + where +
		" ORDER BY cursor_val DESC, id LIMIT @limit OFFSET @offset"

	rows, err := d.pool.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("sim_records list page window: %w", err)
	}
	defer rows.Close()
	result, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records list page window: %w", err)
	}
	return result, nil
}

// ListPageWindowByProjectID returns rows for (vendor, org, entityType) filtered
// by (payload->>'project_id')::bigint, ordered newest-first with an id tiebreak.
// GitLab project-scoped lists (MRs, commits) cannot reuse ListPageWindow: that
// method filters payload->>'repo', which GitLab payloads do not carry.
// limit <= 0 omits LIMIT so a caller can filter/sort the full project set in
// Go (updated_after + sort=asc) before applying the page window.
func (d *SimDAO) ListPageWindowByProjectID(ctx context.Context, vendor, org, entityType string, projectID int64, limit, offset int) ([]Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListPageWindowByProjectID", time.Now())
	args := pgx.NamedArgs{
		"vendor":     vendor,
		"org":        org,
		"entityType": entityType,
		"projectID":  projectID,
	}
	q := `SELECT org_name, entity_type, payload, cursor_val FROM sim_records
	      WHERE vendor = @vendor AND org_name = @org AND entity_type = @entityType
	        AND (payload->>'project_id')::bigint = @projectID
	      ORDER BY cursor_val DESC, id`
	if limit > 0 {
		args["limit"] = limit
		args["offset"] = offset
		q += ` LIMIT @limit OFFSET @offset`
	}

	rows, err := d.pool.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("sim_records list page window by project id: %w", err)
	}
	defer rows.Close()
	result, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records list page window by project id: %w", err)
	}
	return result, nil
}

// GetByPRID returns the PR row matching (vendor, org, repo, id), keyed on the
// Bitbucket REST payload->>'id' (not GitHub's payload->>'number'), or nil if absent.
func (d *SimDAO) GetByPRID(ctx context.Context, vendor, org, repo string, id int) (*Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "GetByPRID", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'pr'
	        AND payload->>'repo' = $3
	        AND (payload->>'id')::int = $4
	      LIMIT 1`
	rows, err := d.pool.Query(ctx, q, vendor, org, repo, id)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by pr id: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by pr id: %w", err)
	}
	if len(results) == 0 {
		return nil, nil //nolint:nilnil // optional-lookup contract, same as GetByPRNumber
	}
	return &results[0], nil
}

// GetByProjectIDAndIID returns the PR row matching (vendor, org) on
// entity_type='pr' keyed on (payload->>'project_id')::bigint and
// (payload->>'iid')::int, or nil if absent. GitLab addresses an MR by that
// pair; Bitbucket's GetByPRID keys payload->>'id' and GitHub's GetByPRNumber
// keys payload->>'number', neither of which is this lookup.
func (d *SimDAO) GetByProjectIDAndIID(ctx context.Context, vendor, org string, projectID int64, iid int) (*Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "GetByProjectIDAndIID", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'pr'
	        AND (payload->>'project_id')::bigint = $3
	        AND (payload->>'iid')::int = $4
	      LIMIT 1`
	rows, err := d.pool.Query(ctx, q, vendor, org, projectID, iid)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by project id and iid: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by project id and iid: %w", err)
	}
	if len(results) == 0 {
		return nil, nil //nolint:nilnil // optional-lookup contract, same as GetByPRNumber
	}
	return &results[0], nil
}

// GetByCommitHash returns the commit row matching (vendor, org, repo, hash), or nil
// if absent. Serves the Bitbucket per-commit diffstat route.
func (d *SimDAO) GetByCommitHash(ctx context.Context, vendor, org, repo, hash string) (*Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "GetByCommitHash", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'commit'
	        AND payload->>'repo' = $3
	        AND payload->>'hash' = $4
	      LIMIT 1`
	rows, err := d.pool.Query(ctx, q, vendor, org, repo, hash)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by commit hash: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by commit hash: %w", err)
	}
	if len(results) == 0 {
		return nil, nil //nolint:nilnil // optional-lookup contract, same as GetByPRNumber
	}
	return &results[0], nil
}

// ListIssuesAscending returns issue rows for (vendor, org), oldest-first, for the
// Jira JQL search — the inverse of the newest-first convention the GitHub/Bitbucket
// queries use, because Jira's `updated >= … ORDER BY updated ASC` cursor is
// oldest-first. project filters to one Jira project key (empty = all projects);
// since is the inclusive `updated >=` lower bound (nil = none). afterCursor/afterID
// are the keyset resume point (both must be set to apply); ordering is
// (cursor_val ASC, id) so the (cursor_val, id) token gives a no-overlap next page.
func (d *SimDAO) ListIssuesAscending(ctx context.Context, vendor, org, project string, since, afterCursor *time.Time, afterID string, limit int) ([]IssueRow, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "ListIssuesAscending", time.Now())
	args := pgx.NamedArgs{"vendor": vendor, "org": org}
	where := "vendor = @vendor AND org_name = @org AND entity_type = 'issue'"
	if project != "" {
		where += " AND payload->'fields'->'project'->>'key' = @project"
		args["project"] = project
	}
	if since != nil {
		where += " AND cursor_val >= @since"
		args["since"] = *since
	}
	if afterCursor != nil && afterID != "" {
		where += " AND (cursor_val, id) > (@afterCursor, @afterID::uuid)"
		args["afterCursor"] = *afterCursor
		args["afterID"] = afterID
	}
	// id::text is aliased so `ORDER BY id` binds to the uuid COLUMN, not to this
	// output column. Unaliased, PostgreSQL resolves the sort to the text rendering,
	// which orders by a different type than the (cursor_val, id) row-comparison
	// above resumes on, and costs an incremental sort the index would otherwise
	// have satisfied outright.
	q := "SELECT org_name, entity_type, payload, cursor_val, id::text AS id_text FROM sim_records WHERE " + where +
		" ORDER BY cursor_val ASC, id"
	if limit > 0 {
		q += " LIMIT @limit"
		args["limit"] = limit
	}
	rows, err := d.pool.Query(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf("sim_records list issues ascending: %w", err)
	}
	defer rows.Close()
	result, err := scanIssueRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records list issues ascending: %w", err)
	}
	return result, nil
}

// GetByIssueKey returns the issue row whose payload `key` matches (e.g. "PLAT-42")
// for (vendor, org), or nil if absent. Serves the Jira worklog route, which is
// addressed by issue key.
func (d *SimDAO) GetByIssueKey(ctx context.Context, vendor, org, key string) (*Row, error) {
	defer metricutil.TrackDuration("simulator/dao", "SimDAO", "GetByIssueKey", time.Now())
	q := `SELECT org_name, entity_type, payload, cursor_val
	      FROM sim_records
	      WHERE vendor = $1 AND org_name = $2 AND entity_type = 'issue'
	        AND payload->>'key' = $3
	      LIMIT 1`
	rows, err := d.pool.Query(ctx, q, vendor, org, key)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by issue key: %w", err)
	}
	defer rows.Close()
	results, err := scanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("sim_records get by issue key: %w", err)
	}
	if len(results) == 0 {
		return nil, nil //nolint:nilnil // optional-lookup contract, same as GetByPRNumber
	}
	return &results[0], nil
}

// IDAO is the interface for simulator data access, enabling handler tests to
// inject a fake without a live database. Each vendor simulator package declares its
// own consumer-side interface with only the methods its handlers use; *SimDAO
// satisfies all of them (e.g. the bitbucket package's page-window/PR-by-id store).
type IDAO interface {
	ListPaged(ctx context.Context, vendor, org, entityType, repo string, before *time.Time, limit int) ([]Row, error)
	ListAll(ctx context.Context, vendor, org, entityType string) ([]Row, error)
	ListDistinctRepos(ctx context.Context, vendor, org string) ([]string, error)
	GetByPRNumber(ctx context.Context, vendor, org, repo string, number int) (*Row, error)
}

func scanIssueRows(rows pgx.Rows) ([]IssueRow, error) {
	var result []IssueRow
	for rows.Next() {
		var r IssueRow
		if err := rows.Scan(&r.OrgName, &r.EntityType, &r.Payload, &r.CursorVal, &r.ID); err != nil {
			return nil, fmt.Errorf("scan sim_records issue row: %w", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sim_records issue rows: %w", err)
	}
	return result, nil
}

func scanRows(rows pgx.Rows) ([]Row, error) {
	var result []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.OrgName, &r.EntityType, &r.Payload, &r.CursorVal); err != nil {
			return nil, fmt.Errorf("scan sim_records row: %w", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sim_records rows: %w", err)
	}
	return result, nil
}

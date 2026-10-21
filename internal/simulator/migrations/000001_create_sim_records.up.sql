-- Copyright 2026 Workloom Solutions Private Limited
-- SPDX-License-Identifier: MIT

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS sim_records (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    vendor      TEXT        NOT NULL,
    org_name    TEXT        NOT NULL,
    entity_type TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    cursor_val  TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    entity_id   TEXT
);

CREATE INDEX IF NOT EXISTS idx_sim_records_cursor
    ON sim_records (vendor, org_name, entity_type, cursor_val DESC);

-- One row per entity. Partial, so a row without an entity_id never conflicts.
CREATE UNIQUE INDEX IF NOT EXISTS uq_sim_records_entity
    ON sim_records (vendor, org_name, entity_type, entity_id)
    WHERE entity_id IS NOT NULL;

-- The JQL search pages issues oldest-first by (cursor_val, id), the only
-- ascending keyset in this table; every other query pages newest-first.
-- idx_sim_records_cursor is DESC and omits id, so it can serve neither the
-- ordering nor the row comparison. A keyset scan needs a (sort_col, id)
-- composite index.
CREATE INDEX IF NOT EXISTS idx_sim_records_cursor_asc
    ON sim_records (vendor, org_name, entity_type, cursor_val, id);

-- The same search filters issues to one Jira project by a JSONB path. Partial
-- to the rows that carry the field, so it stays small and the other vendors'
-- rows are untouched. entity_type is absent from the key because the partial
-- predicate already pins it.
CREATE INDEX IF NOT EXISTS idx_sim_records_jira_project
    ON sim_records (vendor, org_name, (payload -> 'fields' -> 'project' ->> 'key'), cursor_val, id)
    WHERE entity_type = 'issue';
